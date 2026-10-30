//! Concrete browser runtime for SDK embedders.
//!
//! The core crate defines the model-visible browser protocol; this SDK module is
//! the host/runtime piece that actually starts a Node + Playwright sidecar.

use std::collections::BTreeMap;
use std::fs::{File, Metadata};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use klaus_agent_core::browser::{
    context_bound, BrowserEventSink, BrowserExecutor, BrowserRuntime, BrowserRuntimeError,
    BrowserRuntimeErrorCode, BrowserRuntimeEvent, BrowserRuntimeRequest, BrowserRuntimeResponse,
};
use klaus_agent_core::capabilities::RuntimeCapabilitySource;
use klaus_agent_core::runtime_process::{
    RuntimeBrowserSidecarRequest, RuntimeChildStderr, RuntimeChildStdin, RuntimeChildStdout,
    RuntimePipeProcess, RuntimeProcessContext,
};
use klaus_agent_core::runtime_values::{
    runtime_codec, runtime_json, RuntimeCodecError, RuntimeDecode, RuntimeJson, RuntimeObject,
    RuntimeString,
};
use klaus_agent_core::types::abort::AbortSignal;
use serde::Deserialize;
use sha2::{Digest, Sha256};
// `AsyncReadExt` is gated with its only user, the unix broker-readiness handshake below;
// the Windows lane reads through its own named-pipe path.
#[cfg(not(windows))]
use tokio::io::AsyncReadExt;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::sync::Mutex;

#[cfg(windows)]
use crate::browser_host::start_windows_browser_broker;
#[cfg(not(windows))]
use crate::browser_host::{
    unix_package_load_permit, validate_unix_package_ready, UNIX_BROWSER_BOOTSTRAP_RECORD_LEN,
};
use crate::browser_host::{BrowserBrokerRuntime, BrowserGeneration, BrowserGenerationTarget};

const BUNDLED_BROWSER_ASSET_LOCK: &str = include_str!("../assets/bundled-browser-assets.lock.json");
const CUSTOM_SIDECAR_PATH_ERROR: &str =
    "Custom browser sidecar_path is not supported by the exact runtime; use the bundled brokered sidecar.";

#[derive(Clone, Copy)]
struct BundledBrowserAsset {
    path: &'static str,
    mode: &'static str,
    bytes: &'static [u8],
}

macro_rules! bundled_browser_asset {
    ($path:literal, $mode:literal) => {
        BundledBrowserAsset {
            path: $path,
            mode: $mode,
            bytes: include_bytes!(concat!("../assets/klaus-bundled/browser/", $path)),
        }
    };
}

const BUNDLED_BROWSER_ASSETS: [BundledBrowserAsset; 24] = [
    bundled_browser_asset!(".codex-plugin/plugin.json", "100644"),
    bundled_browser_asset!("docs/api.json", "100644"),
    bundled_browser_asset!("docs/browser-safety.md", "100644"),
    bundled_browser_asset!("docs/confirmations.md", "100644"),
    bundled_browser_asset!("docs/documents.json", "100644"),
    bundled_browser_asset!("docs/downloads.md", "100644"),
    bundled_browser_asset!("docs/file-uploads.md", "100644"),
    bundled_browser_asset!("docs/playwright.md", "100644"),
    bundled_browser_asset!("docs/tab-cleanup.md", "100644"),
    bundled_browser_asset!("docs/visibility.md", "100644"),
    bundled_browser_asset!("extension/manifest.json", "100644"),
    bundled_browser_asset!("extension/service-worker.js", "100644"),
    bundled_browser_asset!("runtime/native-host.mjs", "100755"),
    bundled_browser_asset!("runtime/playwright-sidecar.mjs", "100644"),
    bundled_browser_asset!("scripts/browser-client.mjs", "100644"),
    bundled_browser_asset!("scripts/check-extension-installed.mjs", "100755"),
    bundled_browser_asset!("scripts/check-native-host-manifest.mjs", "100755"),
    bundled_browser_asset!("scripts/chrome-common.mjs", "100644"),
    bundled_browser_asset!("scripts/chrome-is-running.mjs", "100644"),
    bundled_browser_asset!("scripts/extension-id.json", "100644"),
    bundled_browser_asset!("scripts/install-native-host-manifest.mjs", "100755"),
    bundled_browser_asset!("scripts/installed-browsers.mjs", "100644"),
    bundled_browser_asset!("scripts/open-chrome-window.mjs", "100644"),
    bundled_browser_asset!("skills/control-browser/SKILL.md", "100644"),
];

/// The first executable named by `names` that exists on `search_paths`.
///
/// A candidate that cannot be stat'd or is not executable is skipped rather than
/// ending the search: one unreadable PATH entry must not hide a usable runtime
/// further down the list.
fn executable_on_path(names: &[&str], search_paths: &[PathBuf]) -> Option<PathBuf> {
    for name in names {
        for dir in search_paths {
            let candidate = dir.join(name);
            if !candidate.is_file() {
                continue;
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let Ok(metadata) = candidate.metadata() else {
                    continue;
                };
                if metadata.permissions().mode() & 0o111 == 0 {
                    continue;
                }
            }
            return Some(candidate);
        }
    }
    None
}

/// The JavaScript runtime that runs the bundled browser sidecar.
///
/// Resolved rather than hardcoded to the string `node`: on a host without Node
/// the OS spawn failed with a bare ENOENT that named neither the missing
/// executable nor a remedy. Node stays the preference because the sidecar is
/// written against it; Bun is taken only when Node is absent.
fn default_browser_runtime_path() -> PathBuf {
    if let Some(path) = std::env::var_os("KLAUS_BROWSER_RUNTIME").filter(|path| !path.is_empty()) {
        return PathBuf::from(path);
    }
    let search_paths = std::env::var_os("PATH")
        .map(|path| std::env::split_paths(&path).collect::<Vec<_>>())
        .unwrap_or_default();
    #[cfg(windows)]
    let node_names = ["node.exe", "node"];
    #[cfg(not(windows))]
    let node_names = ["node"];
    #[cfg(windows)]
    let bun_names = ["bun.exe", "bun"];
    #[cfg(not(windows))]
    let bun_names = ["bun"];
    executable_on_path(&node_names, &search_paths)
        .or_else(|| executable_on_path(&bun_names, &search_paths))
        .unwrap_or_else(|| PathBuf::from("node"))
}

/// Configuration for the SDK's bundled Playwright browser runtime.
#[derive(Debug, Clone)]
pub struct PlaywrightBrowserRuntimeOptions {
    /// JavaScript runtime used to run the sidecar. Node is preferred; Bun is
    /// selected only when Node is absent, so the bundled sidecar still executes
    /// rather than failing with an opaque OS spawn error on Bun-only systems.
    pub node_path: PathBuf,
    /// Retained migration field. Any non-null value is rejected on the first
    /// browser operation; the exact runtime always uses its bundled sidecar.
    pub sidecar_path: Option<PathBuf>,
    /// Working directory for the sidecar process.
    pub cwd: Option<PathBuf>,
    /// Directory where screenshots and future browser artifacts are written.
    pub artifact_dir: Option<PathBuf>,
    /// Launch Chromium in headless mode.
    pub headless: bool,
    /// Additional environment variables for the sidecar.
    pub env: PlaywrightEnvOverlay,
    /// Fallback timeout for requests whose tool wrapper did not set one.
    pub default_timeout_ms: u64,
}

impl Default for PlaywrightBrowserRuntimeOptions {
    fn default() -> Self {
        Self {
            node_path: default_browser_runtime_path(),
            sidecar_path: None,
            cwd: None,
            artifact_dir: None,
            headless: true,
            env: PlaywrightEnvOverlay::default(),
            default_timeout_ms: 30_000,
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
struct RustStringOrd193(RuntimeString);

impl Ord for RustStringOrd193 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.code_points().cmp(other.0.code_points())
    }
}

impl PartialOrd for RustStringOrd193 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Exact caller environment mutations for the bundled Playwright runtime.
///
/// Keys are sorted with Rust 1.93 `String` ordering for ordinary Unicode while
/// retaining a deterministic numeric order for unpaired UTF-16 surrogates. The
/// backing map and its comparator key are intentionally private, so runtime
/// environment values cannot enter through Serde or a generic map decoder.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct PlaywrightEnvOverlay {
    entries: BTreeMap<RustStringOrd193, RuntimeString>,
}

impl PlaywrightEnvOverlay {
    /// Insert or replace one exact mutation. Replacing an exact key preserves
    /// its sorted position and returns the prior value.
    pub fn insert(
        &mut self,
        key: impl Into<RuntimeString>,
        value: impl Into<RuntimeString>,
    ) -> Option<RuntimeString> {
        self.entries
            .insert(RustStringOrd193(key.into()), value.into())
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (&RuntimeString, &RuntimeString)> {
        self.entries.iter().map(|(key, value)| (&key.0, value))
    }
}

impl std::fmt::Debug for PlaywrightEnvOverlay {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PlaywrightEnvOverlay")
            .field("entry_count", &self.entries.len())
            .finish()
    }
}

impl<K, V> Extend<(K, V)> for PlaywrightEnvOverlay
where
    K: Into<RuntimeString>,
    V: Into<RuntimeString>,
{
    fn extend<T: IntoIterator<Item = (K, V)>>(&mut self, iter: T) {
        for (key, value) in iter {
            self.insert(key, value);
        }
    }
}

impl<K, V> FromIterator<(K, V)> for PlaywrightEnvOverlay
where
    K: Into<RuntimeString>,
    V: Into<RuntimeString>,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let mut overlay = Self::default();
        overlay.extend(iter);
        overlay
    }
}

/// Opaque capability for a browser runtime that may create local processes.
///
/// Its process namespace and executable runtime are private. Safe external code
/// can obtain this capability only from a context-bound SDK runtime such as
/// [`PlaywrightBrowserRuntime`]; it cannot self-report or construct a matching
/// namespace identity.
///
/// ```compile_fail
/// use std::sync::Arc;
/// use klaus_agent_sdk::{browser::BrowserRuntime, LocalBrowserRuntime};
///
/// fn forge(runtime: Arc<dyn BrowserRuntime>) -> LocalBrowserRuntime {
///     LocalBrowserRuntime { runtime }
/// }
/// ```
#[derive(Clone)]
pub struct LocalBrowserRuntime {
    runtime: Arc<dyn BrowserRuntime>,
}

impl LocalBrowserRuntime {
    /// Attach this sealed local runtime to a capability-root source without
    /// exposing its process namespace or executable trait object.
    pub fn into_capability_root(
        self,
        source: RuntimeCapabilitySource,
    ) -> LocalBrowserCapabilityRoot {
        LocalBrowserCapabilityRoot {
            source,
            runtime: self,
        }
    }

    pub(crate) fn into_runtime(self) -> Arc<dyn BrowserRuntime> {
        self.runtime
    }
}

/// Opaque capability-root entry for a browser runtime that may create local
/// processes.
///
/// The source metadata is caller-selected, but the local runtime and its bound
/// namespace remain sealed. Safe external code can construct this value only by
/// consuming a [`LocalBrowserRuntime`].
///
/// ```compile_fail
/// use klaus_agent_sdk::{
///     capabilities::RuntimeCapabilitySource, LocalBrowserCapabilityRoot,
///     LocalBrowserRuntime,
/// };
///
/// fn forge(
///     source: RuntimeCapabilitySource,
///     runtime: LocalBrowserRuntime,
/// ) -> LocalBrowserCapabilityRoot {
///     LocalBrowserCapabilityRoot { source, runtime }
/// }
/// ```
#[derive(Clone)]
pub struct LocalBrowserCapabilityRoot {
    source: RuntimeCapabilitySource,
    runtime: LocalBrowserRuntime,
}

impl LocalBrowserCapabilityRoot {
    pub(crate) fn into_parts(self) -> (RuntimeCapabilitySource, LocalBrowserRuntime) {
        (self.source, self.runtime)
    }
}

/// A host-injected runtime backed by a persistent Node + Playwright sidecar.
///
/// The process namespace is mandatory; the pre-cutover context-free constructors
/// are intentionally unavailable:
///
/// ```compile_fail
/// use klaus_agent_sdk::PlaywrightBrowserRuntime;
///
/// let _ = PlaywrightBrowserRuntime::default();
/// ```
///
/// ```compile_fail
/// use klaus_agent_sdk::{PlaywrightBrowserRuntime, PlaywrightBrowserRuntimeOptions};
///
/// let _ = PlaywrightBrowserRuntime::new(PlaywrightBrowserRuntimeOptions::default());
/// ```
///
/// ```compile_fail
/// use klaus_agent_sdk::PlaywrightBrowserRuntime;
///
/// let _ = PlaywrightBrowserRuntime::with_defaults();
/// ```
///
/// A local-spawn runtime cannot be erased into the public spawn-free runtime
/// trait. It must first be sealed into [`LocalBrowserRuntime`], whose namespace
/// is checked by [`crate::AgentEngine`].
///
/// ```compile_fail
/// use std::sync::Arc;
/// use klaus_agent_sdk::{browser::BrowserRuntime, PlaywrightBrowserRuntime};
///
/// fn forge_spawn_free(runtime: PlaywrightBrowserRuntime) -> Arc<dyn BrowserRuntime> {
///     Arc::new(runtime)
/// }
/// ```
pub struct PlaywrightBrowserRuntime {
    process_context: RuntimeProcessContext,
    options: PlaywrightBrowserRuntimeOptions,
    next_id: AtomicU64,
    state: Mutex<SidecarState>,
    #[cfg(test)]
    test_sidecar_transport: Option<TestSidecarTransport>,
}

impl PlaywrightBrowserRuntime {
    pub fn new(
        process_context: RuntimeProcessContext,
        options: PlaywrightBrowserRuntimeOptions,
    ) -> Self {
        Self {
            process_context,
            options,
            next_id: AtomicU64::new(1),
            state: Mutex::new(SidecarState::default()),
            #[cfg(test)]
            test_sidecar_transport: None,
        }
    }

    pub fn with_defaults(process_context: RuntimeProcessContext) -> Self {
        Self::new(process_context, PlaywrightBrowserRuntimeOptions::default())
    }

    /// Seal this local-spawn runtime into an opaque process-context capability
    /// that can be supplied to [`crate::AgentEngineOptions`].
    pub fn into_local_browser_runtime(self) -> LocalBrowserRuntime {
        let namespace_id = self.process_context.namespace_id();
        // SAFETY: this is the SDK's audited local Playwright executor. Its only
        // process entrance is `RuntimeProcessContext::spawn_browser_sidecar`,
        // and `namespace_id` comes from that exact stored constructor context.
        let runtime =
            unsafe { context_bound(namespace_id, Arc::new(SealedPlaywrightBrowserRuntime(self))) };
        LocalBrowserRuntime { runtime }
    }

    #[cfg(test)]
    fn with_test_sidecar_transport(
        process_context: RuntimeProcessContext,
        options: PlaywrightBrowserRuntimeOptions,
        sidecar_path: PathBuf,
    ) -> Self {
        assert!(
            options.sidecar_path.is_none(),
            "the private test transport must not use the public migration field"
        );
        let mut runtime = Self::new(process_context, options);
        runtime.test_sidecar_transport = Some(TestSidecarTransport { sidecar_path });
        runtime
    }

    async fn ensure_sidecar(&self, state: &mut SidecarState) -> Result<(), BrowserRuntimeError> {
        let should_spawn = match state.sidecar.as_mut() {
            Some(sidecar) => !matches!(sidecar.child.try_wait(), Ok(None)),
            None => true,
        };
        if !should_spawn {
            return Ok(());
        }

        state
            .terminate_immediate(RuntimeString::from("Browser sidecar replacement."))
            .await?;
        #[cfg(not(test))]
        let sidecar_path = write_bundled_sidecar()?;
        #[cfg(test)]
        let sidecar_path = match &self.test_sidecar_transport {
            Some(transport) => transport.sidecar_path.clone(),
            None => {
                let sidecar_path = write_bundled_sidecar()?;
                materialize_test_pinned_runtime(&sidecar_path)?;
                sidecar_path
            }
        };
        state.sidecar =
            Some(spawn_sidecar(&self.process_context, &self.options, &sidecar_path).await?);
        Ok(())
    }

    fn next_request_id(&self) -> String {
        format!("browser-{}", self.next_id.fetch_add(1, Ordering::SeqCst))
    }

    async fn execute(
        &self,
        request: BrowserRuntimeRequest,
        signal: AbortSignal,
        emit: BrowserEventSink,
    ) -> Result<BrowserRuntimeResponse, BrowserRuntimeError> {
        if self.options.sidecar_path.is_some() {
            return Err(BrowserRuntimeError::new(
                BrowserRuntimeErrorCode::InvalidRequest,
                CUSTOM_SIDECAR_PATH_ERROR,
            ));
        }
        if signal.is_cancelled() {
            return Err(BrowserRuntimeError::new(
                BrowserRuntimeErrorCode::Cancelled,
                "Browser operation cancelled before sidecar dispatch.",
            ));
        }

        let request_id = self.next_request_id();
        let timeout_ms = request
            .timeout_ms
            .unwrap_or(self.options.default_timeout_ms)
            .max(1);
        let mut state = self.state.lock().await;
        let result = tokio::time::timeout(Duration::from_millis(timeout_ms), async {
            if let Some(message) = state
                .sidecar
                .as_ref()
                .and_then(|sidecar| sidecar.broker.fatal_error())
            {
                // Same reason as the timeout branch below: the broker lane reports how it
                // lost the sidecar, never why the sidecar went away.
                let said = state.take_sidecar_diagnostics().await;
                state.terminate_immediate(message.clone()).await?;
                let message = RuntimeString::from("Browser broker failed: ").concat(&message);
                let message = if said.is_empty() {
                    message
                } else {
                    message.concat(&RuntimeString::from(format!("; the sidecar said: {said}")))
                };
                return Err(BrowserRuntimeError::new(
                    BrowserRuntimeErrorCode::HostError,
                    message,
                ));
            }
            self.ensure_sidecar(&mut state).await?;
            let sidecar = state
                .sidecar
                .as_mut()
                .expect("sidecar exists after ensure_sidecar");
            let (generation, request_token) = {
                let mut generation = sidecar.generation.lock().await;
                let generation_id = generation.generation();
                let request_token = generation
                    .admit_request(generation_id)
                    .map_err(|error| browser_generation_error("admit request", error))?;
                (generation_id, request_token)
            };
            send_sidecar_request(&mut sidecar.stdin, &request_id, &request, &self.options).await?;
            let response = read_sidecar_response(
                &mut sidecar.stdout,
                &request_id,
                signal.clone(),
                emit.clone(),
            )
            .await;
            sidecar
                .generation
                .lock()
                .await
                .complete_request(generation, request_token)
                .map_err(|error| browser_generation_error("complete request", error))?;
            response
        })
        .await;

        match result {
            Ok(Ok(response)) => {
                for event in &response.events {
                    // Sidecars may choose to batch events in the final response
                    // instead of streaming every progress item independently.
                    (emit)(event.clone());
                }
                Ok(response)
            }
            Ok(Err(error)) => {
                if matches!(
                    error.code,
                    BrowserRuntimeErrorCode::Cancelled | BrowserRuntimeErrorCode::HostError
                ) {
                    state.terminate_immediate(error.message.clone()).await?;
                }
                Err(error)
            }
            Err(_elapsed) => {
                // Whatever the sidecar said before it stopped answering. A bare timeout names
                // the host's patience and nothing about why no response came.
                let said = state.take_sidecar_diagnostics().await;
                state
                    .terminate_immediate(RuntimeString::from("Browser sidecar request timed out."))
                    .await?;
                let message = if said.is_empty() {
                    format!("Browser sidecar request timed out after {timeout_ms}ms.")
                } else {
                    format!(
                        "Browser sidecar request timed out after {timeout_ms}ms; the sidecar said: {said}"
                    )
                };
                Err(BrowserRuntimeError::new(
                    BrowserRuntimeErrorCode::Timeout,
                    message,
                ))
            }
        }
    }
}

/// The only trait object that can expose the SDK's local Playwright runtime.
/// The type and its constructor remain private; callers receive only the
/// context-bound [`LocalBrowserRuntime`] capability above.
struct SealedPlaywrightBrowserRuntime(PlaywrightBrowserRuntime);

#[async_trait]
unsafe impl BrowserExecutor for SealedPlaywrightBrowserRuntime {
    async fn execute(
        &self,
        request: BrowserRuntimeRequest,
        signal: AbortSignal,
        emit: BrowserEventSink,
    ) -> Result<BrowserRuntimeResponse, BrowserRuntimeError> {
        self.0.execute(request, signal, emit).await
    }
}

#[derive(Default)]
struct SidecarState {
    sidecar: Option<SidecarProcess>,
}

impl SidecarState {
    /// What the live sidecar has written to stderr so far, drained.
    async fn take_sidecar_diagnostics(&self) -> String {
        self.sidecar
            .as_ref()
            .map(|sidecar| sidecar.diagnostics.take())
            .unwrap_or_default()
    }

    async fn terminate_immediate(
        &mut self,
        message: RuntimeString,
    ) -> Result<(), BrowserRuntimeError> {
        let Some(sidecar) = self.sidecar.take() else {
            return Ok(());
        };
        let result = sidecar
            .broker
            .begin_immediate_shutdown(message)
            .await
            .map_err(|error| BrowserRuntimeError::new(BrowserRuntimeErrorCode::HostError, error));
        drop(sidecar);
        result
    }
}

impl Drop for SidecarState {
    fn drop(&mut self) {
        let Some(sidecar) = self.sidecar.take() else {
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            drop(sidecar);
            return;
        };
        runtime.spawn(async move {
            let _ = sidecar.broker.begin_orderly_shutdown().await;
            drop(sidecar);
        });
    }
}

struct SidecarProcess {
    child: RuntimePipeProcess,
    generation: Arc<Mutex<BrowserGeneration>>,
    broker: BrowserBrokerRuntime,
    stdin: RuntimeChildStdin,
    stdout: Lines<BufReader<RuntimeChildStdout>>,
    diagnostics: SidecarDiagnostics,
}

fn browser_generation_error(action: &str, error: impl std::fmt::Debug) -> BrowserRuntimeError {
    BrowserRuntimeError::new(
        BrowserRuntimeErrorCode::HostError,
        format!("browser generation failed to {action}: {error:?}"),
    )
}

#[cfg(test)]
struct TestSidecarTransport {
    sidecar_path: PathBuf,
}

#[derive(Debug)]
struct SidecarResponseEnvelope {
    id: Option<RuntimeString>,
    event: Option<BrowserRuntimeEvent>,
    ok: Option<bool>,
    response: Option<BrowserRuntimeResponse>,
    error: Option<BrowserRuntimeError>,
}

impl RuntimeDecode for SidecarResponseEnvelope {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                id: object.optional("id", &[])?,
                event: object.optional("event", &[])?,
                ok: object.optional("ok", &[])?,
                response: object.optional("response", &[])?,
                error: object.optional("error", &[])?,
            })
        })
    }
}

fn invalid_sidecar_response(line: &str, error: impl std::fmt::Display) -> BrowserRuntimeError {
    let mut response_error = BrowserRuntimeError::new(
        BrowserRuntimeErrorCode::HostError,
        format!("browser sidecar returned invalid JSON: {error}"),
    );
    let mut details = RuntimeObject::new();
    details.insert("line", RuntimeJson::String(line.into()));
    response_error.details = Some(RuntimeJson::Object(details));
    response_error
}

async fn send_sidecar_request(
    stdin: &mut RuntimeChildStdin,
    id: &str,
    request: &BrowserRuntimeRequest,
    options: &PlaywrightBrowserRuntimeOptions,
) -> Result<(), BrowserRuntimeError> {
    let artifact_dir = options
        .artifact_dir
        .as_ref()
        .map(|path| path.to_string_lossy())
        .map(|path| path.into_owned());
    let mut sidecar_options = RuntimeObject::new();
    if let Some(artifact_dir) = artifact_dir {
        sidecar_options.insert("artifactDir", RuntimeJson::String(artifact_dir.into()));
    }
    sidecar_options.insert("headless", RuntimeJson::Bool(options.headless));
    let mut envelope = RuntimeObject::new();
    envelope.insert("id", RuntimeJson::String(id.into()));
    envelope.insert(
        "request",
        runtime_codec::encode_value(request).map_err(|err| {
            BrowserRuntimeError::new(
                BrowserRuntimeErrorCode::InvalidRequest,
                format!("failed to encode browser sidecar request: {err}"),
            )
        })?,
    );
    envelope.insert("options", RuntimeJson::Object(sidecar_options));
    let encoded = runtime_json::stringify(&RuntimeJson::Object(envelope));
    let encoded = encoded.try_to_utf8().map_err(|err| {
        BrowserRuntimeError::new(
            BrowserRuntimeErrorCode::InvalidRequest,
            format!("failed to encode browser sidecar request: {err}"),
        )
    })?;
    stdin.write_all(encoded.as_bytes()).await.map_err(|err| {
        BrowserRuntimeError::new(
            BrowserRuntimeErrorCode::Unavailable,
            format!("failed to write browser sidecar request: {err}"),
        )
    })?;
    stdin.write_all(b"\n").await.map_err(|err| {
        BrowserRuntimeError::new(
            BrowserRuntimeErrorCode::Unavailable,
            format!("failed to flush browser sidecar request: {err}"),
        )
    })?;
    stdin.flush().await.map_err(|err| {
        BrowserRuntimeError::new(
            BrowserRuntimeErrorCode::Unavailable,
            format!("failed to flush browser sidecar stdin: {err}"),
        )
    })
}

async fn read_sidecar_response(
    stdout: &mut Lines<BufReader<RuntimeChildStdout>>,
    id: &str,
    signal: AbortSignal,
    emit: BrowserEventSink,
) -> Result<BrowserRuntimeResponse, BrowserRuntimeError> {
    loop {
        let line = tokio::select! {
            _ = signal.cancelled() => {
                return Err(BrowserRuntimeError::new(
                    BrowserRuntimeErrorCode::Cancelled,
                    "Browser sidecar request cancelled.",
                ));
            }
            line = stdout.next_line() => line,
        }
        .map_err(|err| {
            BrowserRuntimeError::new(
                BrowserRuntimeErrorCode::HostError,
                format!("failed to read browser sidecar response: {err}"),
            )
        })?
        .ok_or_else(|| {
            BrowserRuntimeError::new(
                BrowserRuntimeErrorCode::Unavailable,
                "Browser sidecar exited before returning a response.",
            )
        })?;

        let value = runtime_json::parse(&RuntimeString::from(line.as_str()))
            .map_err(|error| invalid_sidecar_response(&line, error))?;
        let envelope = runtime_codec::decode_value::<SidecarResponseEnvelope>(&value)
            .map_err(|error| invalid_sidecar_response(&line, error))?;

        if envelope.id.as_ref().is_none_or(|value| value != id) {
            continue;
        }
        if let Some(event) = envelope.event {
            (emit)(event);
            continue;
        }
        if envelope.ok == Some(true) {
            return envelope.response.ok_or_else(|| {
                BrowserRuntimeError::new(
                    BrowserRuntimeErrorCode::HostError,
                    "Browser sidecar success response missing response payload.",
                )
            });
        }
        if envelope.ok == Some(false) {
            return Err(envelope.error.unwrap_or_else(|| {
                BrowserRuntimeError::new(
                    BrowserRuntimeErrorCode::HostError,
                    "Browser sidecar returned an error without details.",
                )
            }));
        }
    }
}

/// Refuse a Windows sidecar launch that has no broker addon to load.
///
/// The Windows handshake runs over named pipes the sidecar can only reach through the sealed
/// Node addon it loads as `klaus_browser_pipe.node` beside its own script. The bundled path
/// stages it there and the test fixtures stage it beside theirs; a caller that supplies its
/// own sidecar has to do the same. Without it the sidecar dies before saying anything and the
/// host reported only "bootstrap timed out", which names nothing actionable.
fn windows_broker_addon_present(sidecar_path: &Path) -> Result<(), BrowserRuntimeError> {
    if !cfg!(windows) {
        return Ok(());
    }
    let addon = sidecar_path.with_file_name("klaus_browser_pipe.node");
    if addon.is_file() {
        return Ok(());
    }
    Err(BrowserRuntimeError::new(
        BrowserRuntimeErrorCode::Unavailable,
        format!(
            "the Windows browser broker needs its Node addon beside the sidecar at {}",
            addon.display()
        ),
    ))
}

async fn spawn_sidecar(
    process_context: &RuntimeProcessContext,
    options: &PlaywrightBrowserRuntimeOptions,
    sidecar_path: &Path,
) -> Result<SidecarProcess, BrowserRuntimeError> {
    windows_broker_addon_present(sidecar_path)?;
    let mut request =
        RuntimeBrowserSidecarRequest::new(options.node_path.clone(), sidecar_path.to_path_buf())
            .with_headless(options.headless)
            .with_env(
                options
                    .env
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            );
    if let Some(cwd) = &options.cwd {
        request = request.with_cwd(cwd.clone());
    }
    if let Some(artifact_dir) = &options.artifact_dir {
        request = request.with_artifact_dir(artifact_dir.clone());
    }

    let mut child = process_context
        .spawn_browser_sidecar(request)
        .await
        .map_err(|err| {
            BrowserRuntimeError::new(
                BrowserRuntimeErrorCode::Unavailable,
                format!(
                    "failed to start browser sidecar {} with {}: {err}",
                    sidecar_path.display(),
                    options.node_path.display()
                ),
            )
        })?;
    let generation = child.browser_generation();
    #[cfg(not(windows))]
    let package_ready_challenge = *child.browser_package_ready_challenge();
    let target = if cfg!(windows) {
        BrowserGenerationTarget::Windows
    } else {
        BrowserGenerationTarget::Unix
    };
    let mut generation_state = BrowserGeneration::new(generation, target, generation)
        .map_err(|error| browser_generation_error("initialize", error))?;
    generation_state
        .root_committed(generation)
        .map_err(|error| browser_generation_error("commit root", error))?;
    let stdin = child.take_stdin().ok_or_else(|| {
        BrowserRuntimeError::new(
            BrowserRuntimeErrorCode::HostError,
            "browser sidecar stdin was not captured.",
        )
    })?;
    let stdout = child.take_stdout().ok_or_else(|| {
        BrowserRuntimeError::new(
            BrowserRuntimeErrorCode::HostError,
            "browser sidecar stdout was not captured.",
        )
    })?;
    let sync_inbound_endpoint = child.take_browser_sync_inbound().ok_or_else(|| {
        BrowserRuntimeError::new(
            BrowserRuntimeErrorCode::HostError,
            "browser sidecar synchronous inbound broker lane was not captured.",
        )
    })?;
    let sync_outbound_endpoint = child.take_browser_sync_outbound().ok_or_else(|| {
        BrowserRuntimeError::new(
            BrowserRuntimeErrorCode::HostError,
            "browser sidecar synchronous outbound broker lane was not captured.",
        )
    })?;
    let mux_inbound_endpoint = child.take_browser_mux_inbound().ok_or_else(|| {
        BrowserRuntimeError::new(
            BrowserRuntimeErrorCode::HostError,
            "browser sidecar mux inbound broker lane was not captured.",
        )
    })?;
    let mux_outbound_endpoint = child.take_browser_mux_outbound().ok_or_else(|| {
        BrowserRuntimeError::new(
            BrowserRuntimeErrorCode::HostError,
            "browser sidecar mux outbound broker lane was not captured.",
        )
    })?;
    #[cfg(not(windows))]
    let (sync_inbound, sync_outbound, mut mux_inbound, mut mux_outbound) = (
        sync_inbound_endpoint,
        sync_outbound_endpoint,
        mux_inbound_endpoint,
        mux_outbound_endpoint,
    );
    #[cfg(not(windows))]
    {
        let permit = unix_package_load_permit(generation, package_ready_challenge);
        mux_outbound.write_all(&permit).await.map_err(|_| {
            BrowserRuntimeError::new(
                BrowserRuntimeErrorCode::HostError,
                "Browser broker package readiness failed.",
            )
        })?;
        mux_outbound.flush().await.map_err(|_| {
            BrowserRuntimeError::new(
                BrowserRuntimeErrorCode::HostError,
                "Browser broker package readiness failed.",
            )
        })?;
        let mut ready = [0_u8; UNIX_BROWSER_BOOTSTRAP_RECORD_LEN];
        mux_inbound.read_exact(&mut ready).await.map_err(|_| {
            BrowserRuntimeError::new(
                BrowserRuntimeErrorCode::HostError,
                "Browser broker package readiness failed.",
            )
        })?;
        if !validate_unix_package_ready(&ready, generation, &package_ready_challenge) {
            return Err(BrowserRuntimeError::new(
                BrowserRuntimeErrorCode::HostError,
                "Browser broker package readiness failed.",
            ));
        }
    }
    #[cfg(windows)]
    let (sync_inbound, sync_outbound, mux_inbound, mux_outbound) = {
        let ready = start_windows_browser_broker(
            generation,
            *child.browser_windows_bootstrap_nonce(),
            child.browser_windows_client_pid(),
            child.browser_windows_bootstrap_deadline(),
            (sync_inbound_endpoint, sync_outbound_endpoint),
            (mux_inbound_endpoint, mux_outbound_endpoint),
        )
        .await
        .map_err(|error| {
            BrowserRuntimeError::new(BrowserRuntimeErrorCode::HostError, error.message())
        })?;
        // The Windows generation has a step the unix one does not: the broker has to be
        // adopted before the package can be declared ready. Nothing drove it, so every
        // Windows launch failed the readiness transition instead of opening.
        generation_state
            .broker_adopted(generation)
            .map_err(|error| browser_generation_error("adopt broker", error))?;
        (
            ready.sync_inbound,
            ready.sync_outbound,
            ready.mux_inbound,
            ready.mux_outbound,
        )
    };
    generation_state
        .package_ready(generation)
        .map_err(|error| browser_generation_error("publish package readiness", error))?;
    let generation_state = Arc::new(Mutex::new(generation_state));
    let broker = BrowserBrokerRuntime::start(
        process_context.clone(),
        child.browser_process_binding(),
        generation_state.clone(),
        sync_inbound,
        sync_outbound,
        mux_inbound,
        mux_outbound,
    );
    let diagnostics = if let Some(stderr) = child.take_stderr() {
        drain_stderr(stderr)
    } else {
        SidecarDiagnostics::default()
    };
    Ok(SidecarProcess {
        child,
        generation: generation_state,
        broker,
        stdin,
        stdout: BufReader::new(stdout).lines(),
        diagnostics,
    })
}

/// The last thing a sidecar wrote to stderr, kept for failure messages.
///
/// Its stderr used to be read and dropped, so a sidecar that died or complained mid-request
/// left the host reporting a bare timeout and nothing about the cause.
#[derive(Clone, Default)]
struct SidecarDiagnostics(Arc<std::sync::Mutex<String>>);

impl SidecarDiagnostics {
    const LIMIT: usize = 4096;

    fn record(&self, line: &str) {
        let mut held = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !held.is_empty() {
            held.push('\n');
        }
        held.push_str(line);
        if held.len() > Self::LIMIT {
            let cut = held.len() - Self::LIMIT;
            held.drain(..cut);
        }
    }

    fn take(&self) -> String {
        std::mem::take(
            &mut *self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

fn drain_stderr(stderr: RuntimeChildStderr) -> SidecarDiagnostics {
    let diagnostics = SidecarDiagnostics::default();
    let sink = diagnostics.clone();
    tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            sink.record(&line);
        }
    });
    diagnostics
}

/// Test-only pinned-runtime materialization: the bundled sidecar resolves its
/// process prelude and the pinned Playwright package from
/// `../../playwright-runtime-v1/` next to the published asset tree. Production
/// hosts receive that tree from the distribution verifier; tests stand in a
/// fake Playwright that re-exports the CJS module from the sidecar's cwd.
#[cfg(test)]
fn materialize_test_pinned_runtime(sidecar_path: &Path) -> Result<(), BrowserRuntimeError> {
    let cache_parent = sidecar_path
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .ok_or_else(|| {
            bundled_asset_error("bundled sidecar path is too shallow for a pinned runtime root")
        })?;
    let runtime_root = cache_parent.join("playwright-runtime-v1");
    let package_dir = runtime_root.join("node_modules/playwright");
    std::fs::create_dir_all(&package_dir).map_err(|error| {
        bundled_asset_error(format!("failed to create test pinned runtime: {error}"))
    })?;
    let prelude =
        include_str!("../assets/exact-browser-runtime-assets-v1/playwright-process-prelude.mjs");
    write_test_runtime_file(
        &runtime_root.join("playwright-process-prelude.mjs"),
        prelude,
    )?;
    write_test_runtime_file(
        &package_dir.join("index.mjs"),
        r#"import { createRequire } from 'node:module';
import path from 'node:path';
const require = createRequire(import.meta.url);
const mod = require(path.join(process.cwd(), 'node_modules', 'playwright', 'index.js'));
export const chromium = mod.chromium;
export default mod;
"#,
    )
}

#[cfg(test)]
fn write_test_runtime_file(path: &Path, contents: &str) -> Result<(), BrowserRuntimeError> {
    std::fs::write(path, contents).map_err(|error| {
        bundled_asset_error(format!(
            "failed to write test pinned runtime file {}: {error}",
            path.display()
        ))
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BundledBrowserAssetLock {
    version: u32,
    source_root: String,
    mirror_root: String,
    files: Vec<BundledBrowserAssetRecord>,
    path_set_sha256: String,
    tree_sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BundledBrowserAssetRecord {
    path: String,
    size: u64,
    mode: String,
    sha256: String,
}

fn bundled_asset_error(message: impl Into<String>) -> BrowserRuntimeError {
    BrowserRuntimeError::new(BrowserRuntimeErrorCode::Unavailable, message.into())
}

fn validate_bundled_browser_asset_lock() -> Result<BundledBrowserAssetLock, BrowserRuntimeError> {
    let lock: BundledBrowserAssetLock =
        serde_json::from_str(BUNDLED_BROWSER_ASSET_LOCK).map_err(|error| {
            bundled_asset_error(format!("invalid bundled browser asset lock: {error}"))
        })?;
    if lock.version != 1
        || lock.source_root != "plugins/klaus-bundled/browser"
        || lock.mirror_root != "crates/sdk/assets/klaus-bundled/browser"
        || lock.files.len() != BUNDLED_BROWSER_ASSETS.len()
    {
        return Err(bundled_asset_error(
            "bundled browser asset lock identity or path count drifted",
        ));
    }

    let mut path_set = Sha256::new();
    path_set.update(b"KLAUS_BROWSER_PATH_SET_V1\0");
    let mut tree = Sha256::new();
    tree.update(b"KLAUS_BROWSER_ASSET_TREE_V1\0");
    let mut previous_path: Option<&[u8]> = None;
    for (asset, record) in BUNDLED_BROWSER_ASSETS.iter().zip(&lock.files) {
        let path_bytes = record.path.as_bytes();
        let safe_path = record.path.is_ascii()
            && !record.path.contains('\\')
            && record
                .path
                .split('/')
                .all(|component| !component.is_empty() && component != "." && component != "..");
        if !safe_path
            || previous_path.is_some_and(|previous| previous >= path_bytes)
            || record.path != asset.path
            || record.mode != asset.mode
            || record.size != asset.bytes.len() as u64
            || !matches!(record.mode.as_str(), "100644" | "100755")
        {
            return Err(bundled_asset_error(format!(
                "bundled browser asset metadata drifted: {}",
                record.path
            )));
        }
        previous_path = Some(path_bytes);
        let digest_bytes = Sha256::digest(asset.bytes);
        let digest = format!("{digest_bytes:x}");
        if digest != record.sha256 {
            return Err(bundled_asset_error(format!(
                "bundled browser asset bytes drifted: {}",
                record.path
            )));
        }
        let path_length = u32::try_from(path_bytes.len())
            .map_err(|_| bundled_asset_error("bundled browser asset path is too long"))?;
        path_set.update(path_length.to_be_bytes());
        path_set.update(path_bytes);
        tree.update(path_length.to_be_bytes());
        tree.update(path_bytes);
        tree.update(record.mode.as_bytes());
        tree.update(record.size.to_be_bytes());
        tree.update(digest_bytes);
    }
    if format!("{:x}", path_set.finalize()) != lock.path_set_sha256
        || format!("{:x}", tree.finalize()) != lock.tree_sha256
    {
        return Err(bundled_asset_error(
            "bundled browser aggregate asset digest drifted",
        ));
    }
    Ok(lock)
}

#[cfg(unix)]
mod secure_asset_fs {
    use std::ffi::{CStr, CString, OsStr, OsString};
    use std::fs::{self, File, Metadata};
    use std::io;
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::path::{Component, Path, PathBuf};

    pub struct Dir {
        file: File,
        path: PathBuf,
    }

    pub struct Entry {
        file: File,
        metadata: Metadata,
        path: PathBuf,
    }

    impl Entry {
        pub fn is_dir(&self) -> bool {
            self.metadata.is_dir()
        }

        pub fn is_file(&self) -> bool {
            self.metadata.is_file()
        }

        pub fn metadata(&self) -> &Metadata {
            &self.metadata
        }

        pub fn into_file(self) -> File {
            self.file
        }

        pub fn into_dir(self) -> io::Result<Dir> {
            if !self.metadata.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::NotADirectory,
                    "entry is not a directory",
                ));
            }
            Ok(Dir {
                file: self.file,
                path: self.path,
            })
        }
    }

    impl Dir {
        pub fn open_or_create_private_path(path: &Path) -> io::Result<Self> {
            let parent_path = path.parent().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "private directory has no parent",
                )
            })?;
            let name = path.file_name().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "private directory has no name")
            })?;
            let parent = Self::open_ambient(parent_path)?;
            parent.open_or_create_private_child(name)
        }

        pub fn create_private_path(path: &Path) -> io::Result<Self> {
            let parent_path = path.parent().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "private directory has no parent",
                )
            })?;
            let name = path.file_name().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "private directory has no name")
            })?;
            Self::open_ambient(parent_path)?.create_private_child(name)
        }

        fn open_ambient(path: &Path) -> io::Result<Self> {
            let canonical = fs::canonicalize(path)?;
            if !canonical.is_absolute() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "canonical asset-cache parent is not absolute",
                ));
            }
            let root_fd = unsafe {
                libc::open(
                    c"/".as_ptr(),
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
                )
            };
            if root_fd < 0 {
                return Err(io::Error::last_os_error());
            }
            let mut current = Self {
                file: unsafe { File::from_raw_fd(root_fd) },
                path: PathBuf::from("/"),
            };
            for component in canonical.components() {
                match component {
                    Component::RootDir => {}
                    Component::Normal(name) => current = current.open_dir_child(name)?,
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "canonical asset-cache parent contains a non-normal component",
                        ));
                    }
                }
            }
            Ok(current)
        }

        pub fn path(&self) -> &Path {
            &self.path
        }

        pub fn try_clone(&self) -> io::Result<Self> {
            Ok(Self {
                file: self.file.try_clone()?,
                path: self.path.clone(),
            })
        }

        pub fn sync(&self) -> io::Result<()> {
            self.file.sync_all()
        }

        pub fn verify_private(&self) -> io::Result<()> {
            let metadata = self.file.metadata()?;
            if !metadata.is_dir()
                || metadata.uid() != unsafe { libc::geteuid() }
                || metadata.permissions().mode() & 0o777 != 0o700
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "directory is not current-user-owned mode 0700",
                ));
            }
            Ok(())
        }

        pub fn open_or_create_private_child(&self, name: &OsStr) -> io::Result<Self> {
            match self.create_private_child(name) {
                Ok(dir) => Ok(dir),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    let dir = self.open_dir_child(name)?;
                    let metadata = dir.file.metadata()?;
                    if metadata.uid() != unsafe { libc::geteuid() } {
                        return Err(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "existing private directory is not current-user-owned",
                        ));
                    }
                    dir.file
                        .set_permissions(fs::Permissions::from_mode(0o700))?;
                    dir.verify_private()?;
                    Ok(dir)
                }
                Err(error) => Err(error),
            }
        }

        pub fn create_private_child(&self, name: &OsStr) -> io::Result<Self> {
            let name = component_cstring(name)?;
            let result = unsafe { libc::mkdirat(self.file.as_raw_fd(), name.as_ptr(), 0o700) };
            if result != 0 {
                return Err(io::Error::last_os_error());
            }
            let dir = match self.open_dir_child_cstr(&name) {
                Ok(dir) => dir,
                Err(error) => {
                    let _ = self.unlink_name(OsStr::from_bytes(name.to_bytes()), true);
                    return Err(error);
                }
            };
            if let Err(error) = dir.verify_private().and_then(|()| self.sync()) {
                let _ = self.unlink_opened(OsStr::from_bytes(name.to_bytes()), &dir.file, true);
                return Err(error);
            }
            Ok(dir)
        }

        pub fn open_dir_child(&self, name: &OsStr) -> io::Result<Self> {
            self.open_dir_child_cstr(&component_cstring(name)?)
        }

        fn open_dir_child_cstr(&self, name: &CStr) -> io::Result<Self> {
            let fd = unsafe {
                libc::openat(
                    self.file.as_raw_fd(),
                    name.as_ptr(),
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                )
            };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            let file = unsafe { File::from_raw_fd(fd) };
            if !file.metadata()?.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::NotADirectory,
                    "entry is not a directory",
                ));
            }
            Ok(Self {
                file,
                path: self.path.join(OsStr::from_bytes(name.to_bytes())),
            })
        }

        pub fn open_entry(&self, name: &OsStr) -> io::Result<Entry> {
            let name = component_cstring(name)?;
            let fd = unsafe {
                libc::openat(
                    self.file.as_raw_fd(),
                    name.as_ptr(),
                    libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
                )
            };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            let file = unsafe { File::from_raw_fd(fd) };
            let metadata = file.metadata()?;
            Ok(Entry {
                file,
                metadata,
                path: self.path.join(OsStr::from_bytes(name.to_bytes())),
            })
        }

        pub fn create_new_file(&self, name: &OsStr, mode: u32) -> io::Result<File> {
            let name = component_cstring(name)?;
            let fd = unsafe {
                libc::openat(
                    self.file.as_raw_fd(),
                    name.as_ptr(),
                    libc::O_WRONLY
                        | libc::O_CREAT
                        | libc::O_EXCL
                        | libc::O_CLOEXEC
                        | libc::O_NOFOLLOW,
                    mode as libc::c_uint,
                )
            };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            let file = unsafe { File::from_raw_fd(fd) };
            file.set_permissions(fs::Permissions::from_mode(mode))?;
            Ok(file)
        }

        pub fn list_names(&self) -> io::Result<Vec<OsString>> {
            let fd = unsafe {
                libc::openat(
                    self.file.as_raw_fd(),
                    c".".as_ptr(),
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                )
            };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            let directory = unsafe { libc::fdopendir(fd) };
            if directory.is_null() {
                let error = io::Error::last_os_error();
                unsafe { libc::close(fd) };
                return Err(error);
            }
            struct Directory(*mut libc::DIR);
            impl Drop for Directory {
                fn drop(&mut self) {
                    unsafe { libc::closedir(self.0) };
                }
            }
            let directory = Directory(directory);
            let mut names = Vec::new();
            loop {
                clear_errno();
                let entry = unsafe { libc::readdir(directory.0) };
                if entry.is_null() {
                    let error = io::Error::last_os_error();
                    if error.raw_os_error() == Some(0) {
                        break;
                    }
                    return Err(error);
                }
                let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
                if name != b"." && name != b".." {
                    names.push(OsString::from_vec(name.to_vec()));
                }
            }
            Ok(names)
        }

        pub fn remove_tree_entry(&self, name: &OsStr, opened: &Dir) -> io::Result<()> {
            opened.remove_contents()?;
            self.unlink_opened(name, &opened.file, true)
        }

        fn remove_contents(&self) -> io::Result<()> {
            for name in self.list_names()? {
                match self.open_entry(&name) {
                    Ok(entry) if entry.is_dir() => {
                        let child = entry.into_dir()?;
                        child.remove_contents()?;
                        self.unlink_opened(&name, &child.file, true)?;
                    }
                    Ok(entry) => self.unlink_opened(&name, &entry.file, false)?,
                    Err(error)
                        if matches!(error.raw_os_error(), Some(libc::ELOOP | libc::ENOTDIR)) =>
                    {
                        self.unlink_name(&name, false)?;
                    }
                    Err(error) => return Err(error),
                }
            }
            Ok(())
        }

        fn unlink_opened(&self, name: &OsStr, opened: &File, directory: bool) -> io::Result<()> {
            let expected = opened.metadata()?;
            let name_c = component_cstring(name)?;
            let mut actual = std::mem::MaybeUninit::<libc::stat>::uninit();
            let result = unsafe {
                libc::fstatat(
                    self.file.as_raw_fd(),
                    name_c.as_ptr(),
                    actual.as_mut_ptr(),
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            };
            if result != 0 {
                return Err(io::Error::last_os_error());
            }
            let actual = unsafe { actual.assume_init() };
            if actual.st_dev as u64 != expected.dev() || actual.st_ino != expected.ino() {
                return Err(io::Error::other(
                    "asset-cache entry identity changed before cleanup",
                ));
            }
            self.unlink_name(name, directory)
        }

        fn unlink_name(&self, name: &OsStr, directory: bool) -> io::Result<()> {
            let name = component_cstring(name)?;
            let flags = if directory { libc::AT_REMOVEDIR } else { 0 };
            let result = unsafe { libc::unlinkat(self.file.as_raw_fd(), name.as_ptr(), flags) };
            if result == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        }

        pub fn publish_no_replace(
            &self,
            pending_dir: &Dir,
            pending_name: &OsStr,
            final_name: &OsStr,
        ) -> io::Result<()> {
            self.require_opened_identity(pending_name, &pending_dir.file)?;
            let pending = component_cstring(pending_name)?;
            let final_name = component_cstring(final_name)?;
            #[cfg(any(target_os = "linux", target_os = "android"))]
            let result = unsafe {
                libc::syscall(
                    libc::SYS_renameat2,
                    self.file.as_raw_fd(),
                    pending.as_ptr(),
                    self.file.as_raw_fd(),
                    final_name.as_ptr(),
                    libc::RENAME_NOREPLACE,
                ) as libc::c_int
            };
            #[cfg(any(target_os = "macos", target_os = "ios"))]
            let result = unsafe {
                libc::renameatx_np(
                    self.file.as_raw_fd(),
                    pending.as_ptr(),
                    self.file.as_raw_fd(),
                    final_name.as_ptr(),
                    libc::RENAME_EXCL,
                )
            };
            #[cfg(not(any(
                target_os = "linux",
                target_os = "android",
                target_os = "macos",
                target_os = "ios"
            )))]
            let result = {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "atomic no-replace directory publication is unsupported on this Unix target",
                ));
            };
            if result == 0 {
                self.sync()?;
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        }

        fn require_opened_identity(&self, name: &OsStr, opened: &File) -> io::Result<()> {
            let expected = opened.metadata()?;
            let name = component_cstring(name)?;
            let mut actual = std::mem::MaybeUninit::<libc::stat>::uninit();
            let result = unsafe {
                libc::fstatat(
                    self.file.as_raw_fd(),
                    name.as_ptr(),
                    actual.as_mut_ptr(),
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            };
            if result != 0 {
                return Err(io::Error::last_os_error());
            }
            let actual = unsafe { actual.assume_init() };
            if actual.st_dev as u64 == expected.dev()
                && actual.st_ino == expected.ino()
                && (actual.st_mode & libc::S_IFMT) == libc::S_IFDIR
            {
                Ok(())
            } else {
                Err(io::Error::other(
                    "pending asset tree identity changed before publication",
                ))
            }
        }
    }

    fn component_cstring(name: &OsStr) -> io::Result<CString> {
        let bytes = name.as_bytes();
        if bytes.is_empty() || bytes == b"." || bytes == b".." || bytes.contains(&b'/') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "asset-cache child name is not one normal path component",
            ));
        }
        CString::new(bytes).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "asset-cache child name contains NUL",
            )
        })
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn clear_errno() {
        unsafe { *libc::__errno_location() = 0 };
    }

    #[cfg(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    ))]
    fn clear_errno() {
        unsafe { *libc::__error() = 0 };
    }
}

#[cfg(windows)]
mod secure_asset_fs {
    use std::ffi::{OsStr, OsString};
    use std::fs::{self, File, Metadata, OpenOptions};
    use std::io;
    use std::os::windows::ffi::{OsStrExt, OsStringExt};
    use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle};
    use std::path::{Path, PathBuf};
    use std::ptr;

    use windows_sys::Wdk::Foundation::OBJECT_ATTRIBUTES;
    use windows_sys::Wdk::Storage::FileSystem::{
        FileRenameInformation, NtCreateFile, NtSetInformationFile, FILE_CREATE,
        FILE_DIRECTORY_FILE, FILE_NON_DIRECTORY_FILE, FILE_OPEN, FILE_OPEN_REPARSE_POINT,
        FILE_RENAME_INFORMATION, FILE_SYNCHRONOUS_IO_NONALERT,
    };
    use windows_sys::Win32::Foundation::{
        GetLastError, LocalFree, RtlNtStatusToDosError, ERROR_ALREADY_EXISTS, ERROR_FILE_EXISTS,
        ERROR_INSUFFICIENT_BUFFER, ERROR_NO_MORE_FILES, ERROR_SUCCESS, HANDLE,
        OBJ_CASE_INSENSITIVE, OBJ_DONT_REPARSE, UNICODE_STRING,
    };
    use windows_sys::Win32::Security::Authorization::{
        GetSecurityInfo, SetSecurityInfo, SE_FILE_OBJECT,
    };
    use windows_sys::Win32::Security::{
        AclSizeInformation, AddAccessAllowedAceEx, CopySid, EqualSid, GetAce, GetAclInformation,
        GetLengthSid, GetSecurityDescriptorControl, GetTokenInformation, InitializeAcl,
        InitializeSecurityDescriptor, SetSecurityDescriptorControl, SetSecurityDescriptorDacl,
        SetSecurityDescriptorOwner, TokenUser, ACCESS_ALLOWED_ACE, ACL, ACL_REVISION,
        ACL_SIZE_INFORMATION, CONTAINER_INHERIT_ACE, DACL_SECURITY_INFORMATION, OBJECT_INHERIT_ACE,
        OWNER_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR,
        PSID, SECURITY_DESCRIPTOR, SE_DACL_PROTECTED, TOKEN_QUERY, TOKEN_USER,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        FileDispositionInfo, FileDispositionInfoEx, FileIdBothDirectoryInfo,
        FileIdBothDirectoryRestartInfo, GetFileInformationByHandleEx, SetFileInformationByHandle,
        FILE_ALL_ACCESS, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_NORMAL,
        FILE_ATTRIBUTE_REPARSE_POINT, FILE_DISPOSITION_FLAG_DELETE,
        FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE, FILE_DISPOSITION_FLAG_POSIX_SEMANTICS,
        FILE_DISPOSITION_INFO, FILE_DISPOSITION_INFO_EX, FILE_FLAG_BACKUP_SEMANTICS,
        FILE_FLAG_OPEN_REPARSE_POINT, FILE_ID_BOTH_DIR_INFO, FILE_SHARE_DELETE, FILE_SHARE_READ,
        FILE_SHARE_WRITE,
    };
    use windows_sys::Win32::System::SystemServices::{
        ACCESS_ALLOWED_ACE_TYPE, SECURITY_DESCRIPTOR_REVISION,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};
    use windows_sys::Win32::System::IO::IO_STATUS_BLOCK;

    const SHARE_ALL: u32 = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;

    pub struct Dir {
        file: File,
        path: PathBuf,
    }

    pub struct Entry {
        file: File,
        metadata: Metadata,
        path: PathBuf,
    }

    impl Entry {
        pub fn is_dir(&self) -> bool {
            self.metadata.is_dir()
                && self.metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT == 0
        }

        pub fn is_file(&self) -> bool {
            self.metadata.is_file()
                && self.metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT == 0
        }

        pub fn metadata(&self) -> &Metadata {
            &self.metadata
        }

        pub fn into_file(self) -> File {
            self.file
        }

        pub fn into_dir(self) -> io::Result<Dir> {
            if !self.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::NotADirectory,
                    "entry is not a non-reparse directory",
                ));
            }
            Ok(Dir {
                file: self.file,
                path: self.path,
            })
        }
    }

    impl Dir {
        pub fn open_or_create_private_path(path: &Path) -> io::Result<Self> {
            let parent_path = path.parent().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "private directory has no parent",
                )
            })?;
            let name = path.file_name().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "private directory has no name")
            })?;
            let parent = Self::open_ambient(parent_path)?;
            parent.open_or_create_private_child(name)
        }

        pub fn create_private_path(path: &Path) -> io::Result<Self> {
            let parent_path = path.parent().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "private directory has no parent",
                )
            })?;
            let name = path.file_name().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "private directory has no name")
            })?;
            Self::open_ambient(parent_path)?.create_private_child(name)
        }

        #[cfg(test)]
        pub fn open_existing_private_path_for_test(path: &Path) -> io::Result<Self> {
            let parent_path = path.parent().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "private directory has no parent",
                )
            })?;
            let name = path.file_name().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "private directory has no name")
            })?;
            Self::open_ambient(parent_path)?.open_dir_child(name)
        }

        fn open_ambient(path: &Path) -> io::Result<Self> {
            let canonical = fs::canonicalize(path)?;
            let mut options = OpenOptions::new();
            options
                .access_mode(FILE_ALL_ACCESS)
                .share_mode(SHARE_ALL)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT);
            let file = options.open(&canonical)?;
            let metadata = file.metadata()?;
            if !metadata.is_dir() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "ambient asset-cache parent is not a real directory",
                ));
            }
            Ok(Self {
                file,
                // The verbatim prefix `canonicalize` returns is fine to open with and wrong to
                // hand out: `path()` ends up as a child process's argument, and node cannot
                // resolve `\\?\C:\…` as its main script — it splits the root wrong and lstats
                // `C:`, which fails with EISDIR and kills the sidecar before it says anything.
                path: dunce::simplified(&canonical).to_path_buf(),
            })
        }

        pub fn path(&self) -> &Path {
            &self.path
        }

        pub fn try_clone(&self) -> io::Result<Self> {
            Ok(Self {
                file: self.file.try_clone()?,
                path: self.path.clone(),
            })
        }

        pub fn sync(&self) -> io::Result<()> {
            Ok(())
        }

        pub fn verify_private(&self) -> io::Result<()> {
            let metadata = self.file.metadata()?;
            if !metadata.is_dir() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "private cache handle is not a non-reparse directory",
                ));
            }
            let security = CurrentUserSecurity::new()?;
            verify_current_user_only_security(raw_handle(&self.file), &security)
        }

        pub fn open_or_create_private_child(&self, name: &OsStr) -> io::Result<Self> {
            match self.create_private_child(name) {
                Ok(dir) => Ok(dir),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    let dir = self.open_dir_child(name)?;
                    secure_existing_private_directory(&dir.file)?;
                    dir.verify_private()?;
                    Ok(dir)
                }
                Err(error) => Err(error),
            }
        }

        pub fn create_private_child(&self, name: &OsStr) -> io::Result<Self> {
            let security = CurrentUserSecurity::new()?;
            let file = nt_create_relative(
                raw_handle(&self.file),
                name,
                FILE_CREATE,
                FILE_DIRECTORY_FILE | FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
                FILE_ATTRIBUTE_DIRECTORY,
                Some(security.descriptor()),
                true,
            )?;
            let dir = Self {
                file,
                path: self.path.join(name),
            };
            if let Err(error) = dir.verify_private() {
                let _ = mark_delete(&dir.file);
                return Err(error);
            }
            Ok(dir)
        }

        pub fn open_dir_child(&self, name: &OsStr) -> io::Result<Self> {
            self.open_entry(name)?.into_dir()
        }

        pub fn open_entry(&self, name: &OsStr) -> io::Result<Entry> {
            open_relative_entry(raw_handle(&self.file), &self.path, name, true)
        }

        fn open_entry_for_cleanup(&self, name: &OsStr) -> io::Result<Entry> {
            open_relative_entry(raw_handle(&self.file), &self.path, name, false)
        }

        pub fn create_new_file(&self, name: &OsStr, _mode: u32) -> io::Result<File> {
            nt_create_relative(
                raw_handle(&self.file),
                name,
                FILE_CREATE,
                FILE_NON_DIRECTORY_FILE | FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
                FILE_ATTRIBUTE_NORMAL,
                None,
                true,
            )
        }

        pub fn list_names(&self) -> io::Result<Vec<OsString>> {
            let mut names = Vec::new();
            let mut restart = true;
            loop {
                let mut storage = AlignedBuffer::new(64 * 1024);
                let class = if restart {
                    FileIdBothDirectoryRestartInfo
                } else {
                    FileIdBothDirectoryInfo
                };
                let ok = unsafe {
                    GetFileInformationByHandleEx(
                        raw_handle(&self.file),
                        class,
                        storage.as_mut_void(),
                        storage.len_u32()?,
                    )
                };
                if ok == 0 {
                    let code = unsafe { GetLastError() };
                    if code == ERROR_NO_MORE_FILES {
                        break;
                    }
                    return Err(io::Error::from_raw_os_error(code as i32));
                }
                parse_directory_buffer(storage.as_bytes(), &mut names)?;
                restart = false;
            }
            Ok(names)
        }

        pub fn remove_tree_entry(&self, _name: &OsStr, opened: &Dir) -> io::Result<()> {
            opened.remove_contents()?;
            mark_delete(&opened.file)
        }

        fn remove_contents(&self) -> io::Result<()> {
            for name in self.list_names()? {
                let entry = self.open_entry_for_cleanup(&name)?;
                let is_reparse =
                    entry.metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0;
                if entry.metadata.is_dir() && !is_reparse {
                    let child = Dir {
                        file: entry.file,
                        path: entry.path,
                    };
                    child.remove_contents()?;
                    mark_delete(&child.file)?;
                } else {
                    mark_delete(&entry.file)?;
                }
            }
            Ok(())
        }

        /// Rename `pending_dir` to `final_name` inside this directory, failing if the name
        /// is taken.
        ///
        /// `NtSetInformationFile` rather than `SetFileInformationByHandle`: only the NT entry
        /// point honours `RootDirectory`, so only it can express "this name, relative to this
        /// directory handle". The Win32 wrapper requires `RootDirectory` to be NULL with a
        /// full path in `FileName`, and rejected the handle-relative form outright with
        /// `ERROR_INVALID_PARAMETER` — every bundled-asset publication failed there. Naming a
        /// full path instead would also give up the point of the handle: the target directory
        /// could be swapped between the check and the rename.
        pub fn publish_no_replace(
            &self,
            pending_dir: &Dir,
            _pending_name: &OsStr,
            final_name: &OsStr,
        ) -> io::Result<()> {
            let wide = component_wide(final_name)?;
            let name_bytes = wide.len().checked_mul(2).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "publish name is too long")
            })?;
            let prefix = std::mem::offset_of!(FILE_RENAME_INFORMATION, FileName);
            let total = prefix.checked_add(name_bytes).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "rename buffer is too large")
            })?;
            let mut storage = AlignedBuffer::new(total);
            let info = storage.as_mut_ptr::<FILE_RENAME_INFORMATION>();
            unsafe {
                (*info).Anonymous.ReplaceIfExists = false;
                (*info).RootDirectory = raw_handle(&self.file);
                (*info).FileNameLength = u32::try_from(name_bytes).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "publish name is too long")
                })?;
                ptr::copy_nonoverlapping(
                    wide.as_ptr(),
                    ptr::addr_of_mut!((*info).FileName).cast::<u16>(),
                    wide.len(),
                );
            }
            let mut io_status = IO_STATUS_BLOCK::default();
            let status = unsafe {
                NtSetInformationFile(
                    raw_handle(&pending_dir.file),
                    &mut io_status,
                    storage.as_void(),
                    u32::try_from(total).map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidInput, "rename buffer is too large")
                    })?,
                    FileRenameInformation,
                )
            };
            if status < 0 {
                return Err(nt_status_error(status));
            }
            Ok(())
        }
    }

    fn open_relative_entry(
        parent: HANDLE,
        parent_path: &Path,
        name: &OsStr,
        reject_reparse: bool,
    ) -> io::Result<Entry> {
        let file = nt_create_relative(
            parent,
            name,
            FILE_OPEN,
            FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
            0,
            None,
            reject_reparse,
        )?;
        let metadata = file.metadata()?;
        if reject_reparse && metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "asset-cache entry is a reparse point",
            ));
        }
        Ok(Entry {
            file,
            metadata,
            path: parent_path.join(name),
        })
    }

    fn nt_create_relative(
        parent: HANDLE,
        name: &OsStr,
        disposition: u32,
        options: u32,
        attributes: u32,
        security_descriptor: Option<*const SECURITY_DESCRIPTOR>,
        reject_reparse: bool,
    ) -> io::Result<File> {
        let mut wide = component_wide(name)?;
        let byte_len = wide.len().checked_mul(2).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "asset-cache name is too long")
        })?;
        let length = u16::try_from(byte_len).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "asset-cache name is too long")
        })?;
        let unicode = UNICODE_STRING {
            Length: length,
            MaximumLength: length,
            Buffer: wide.as_mut_ptr(),
        };
        let object_attributes = OBJECT_ATTRIBUTES {
            Length: u32::try_from(std::mem::size_of::<OBJECT_ATTRIBUTES>())
                .expect("OBJECT_ATTRIBUTES size fits u32"),
            RootDirectory: parent,
            ObjectName: &unicode,
            Attributes: OBJ_CASE_INSENSITIVE | if reject_reparse { OBJ_DONT_REPARSE } else { 0 },
            SecurityDescriptor: security_descriptor.unwrap_or(ptr::null()),
            SecurityQualityOfService: ptr::null(),
        };
        let mut handle: HANDLE = ptr::null_mut();
        let mut io_status = IO_STATUS_BLOCK::default();
        let status = unsafe {
            NtCreateFile(
                &mut handle,
                FILE_ALL_ACCESS,
                &object_attributes,
                &mut io_status,
                ptr::null(),
                attributes,
                SHARE_ALL,
                disposition,
                options,
                ptr::null(),
                0,
            )
        };
        if status < 0 {
            return Err(nt_status_error(status));
        }
        if handle.is_null() {
            return Err(io::Error::other(
                "NtCreateFile succeeded without returning an asset-cache handle",
            ));
        }
        Ok(unsafe { File::from_raw_handle(handle as RawHandle) })
    }

    /// Convert an `NTSTATUS` failure into the `io::Error` the callers expect.
    ///
    /// The AlreadyExists mapping is the load-bearing part: `publish_no_replace` distinguishes
    /// "another process won the publication race" from a real failure by that kind, and NT
    /// reports the collision as `STATUS_OBJECT_NAME_COLLISION`, which Rust maps to no kind.
    fn nt_status_error(status: i32) -> io::Error {
        let code = unsafe { RtlNtStatusToDosError(status) };
        let kind = if code == ERROR_ALREADY_EXISTS || code == ERROR_FILE_EXISTS {
            io::ErrorKind::AlreadyExists
        } else {
            io::Error::from_raw_os_error(code as i32).kind()
        };
        io::Error::new(kind, io::Error::from_raw_os_error(code as i32))
    }

    fn component_wide(name: &OsStr) -> io::Result<Vec<u16>> {
        let wide = name.encode_wide().collect::<Vec<_>>();
        if wide.is_empty()
            || wide == [b'.' as u16]
            || wide == [b'.' as u16, b'.' as u16]
            || wide.iter().any(|unit| {
                *unit == 0 || *unit == b'/' as u16 || *unit == b'\\' as u16 || *unit == b':' as u16
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "asset-cache child name is not one safe normal component",
            ));
        }
        Ok(wide)
    }

    fn parse_directory_buffer(buffer: &[u8], output: &mut Vec<OsString>) -> io::Result<()> {
        let header = std::mem::offset_of!(FILE_ID_BOTH_DIR_INFO, FileName);
        let mut offset = 0usize;
        loop {
            if offset
                .checked_add(header)
                .is_none_or(|end| end > buffer.len())
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Windows directory enumeration returned a truncated record",
                ));
            }
            let info = unsafe { &*buffer.as_ptr().add(offset).cast::<FILE_ID_BOTH_DIR_INFO>() };
            let name_bytes = usize::try_from(info.FileNameLength).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "directory name is too long")
            })?;
            if name_bytes % 2 != 0
                || offset
                    .checked_add(header)
                    .and_then(|value| value.checked_add(name_bytes))
                    .is_none_or(|end| end > buffer.len())
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Windows directory enumeration returned an invalid name length",
                ));
            }
            let name = unsafe {
                std::slice::from_raw_parts(
                    ptr::addr_of!(info.FileName).cast::<u16>(),
                    name_bytes / 2,
                )
            };
            if name != [b'.' as u16] && name != [b'.' as u16, b'.' as u16] {
                output.push(OsString::from_wide(name));
            }
            if info.NextEntryOffset == 0 {
                break;
            }
            let next = usize::try_from(info.NextEntryOffset).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "directory offset is too large")
            })?;
            if next < header
                || offset
                    .checked_add(next)
                    .is_none_or(|end| end >= buffer.len())
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Windows directory enumeration returned an invalid next offset",
                ));
            }
            offset += next;
        }
        Ok(())
    }

    fn mark_delete(file: &File) -> io::Result<()> {
        let extended = FILE_DISPOSITION_INFO_EX {
            Flags: FILE_DISPOSITION_FLAG_DELETE
                | FILE_DISPOSITION_FLAG_POSIX_SEMANTICS
                | FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE,
        };
        let ok = unsafe {
            SetFileInformationByHandle(
                raw_handle(file),
                FileDispositionInfoEx,
                ptr::addr_of!(extended).cast(),
                u32::try_from(std::mem::size_of_val(&extended)).expect("disposition size fits u32"),
            )
        };
        if ok != 0 {
            return Ok(());
        }
        let basic = FILE_DISPOSITION_INFO { DeleteFile: true };
        let ok = unsafe {
            SetFileInformationByHandle(
                raw_handle(file),
                FileDispositionInfo,
                ptr::addr_of!(basic).cast(),
                u32::try_from(std::mem::size_of_val(&basic)).expect("disposition size fits u32"),
            )
        };
        if ok == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    fn secure_existing_private_directory(file: &File) -> io::Result<()> {
        let security = CurrentUserSecurity::new()?;
        verify_current_owner(raw_handle(file), &security)?;
        let code = unsafe {
            SetSecurityInfo(
                raw_handle(file),
                SE_FILE_OBJECT,
                OWNER_SECURITY_INFORMATION
                    | DACL_SECURITY_INFORMATION
                    | PROTECTED_DACL_SECURITY_INFORMATION,
                security.sid(),
                ptr::null_mut(),
                security.acl(),
                ptr::null(),
            )
        };
        if code != ERROR_SUCCESS {
            return Err(io::Error::from_raw_os_error(code as i32));
        }
        verify_current_user_only_security(raw_handle(file), &security)
    }

    fn verify_current_owner(handle: HANDLE, security: &CurrentUserSecurity) -> io::Result<()> {
        with_security_info(handle, |owner, _dacl, _descriptor| {
            if owner.is_null() || unsafe { EqualSid(owner, security.sid()) } == 0 {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "Windows cache directory is not owned by the current user",
                ))
            } else {
                Ok(())
            }
        })
    }

    fn verify_current_user_only_security(
        handle: HANDLE,
        security: &CurrentUserSecurity,
    ) -> io::Result<()> {
        with_security_info(handle, |owner, dacl, descriptor| {
            if owner.is_null() || unsafe { EqualSid(owner, security.sid()) } == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "Windows cache directory owner is not the current user",
                ));
            }
            if dacl.is_null() {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "Windows cache directory has a null DACL",
                ));
            }
            let mut control = 0u16;
            let mut revision = 0u32;
            if unsafe { GetSecurityDescriptorControl(descriptor, &mut control, &mut revision) } == 0
                || control & SE_DACL_PROTECTED == 0
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "Windows cache directory DACL is not protected",
                ));
            }
            let mut size = ACL_SIZE_INFORMATION::default();
            if unsafe {
                GetAclInformation(
                    dacl,
                    ptr::addr_of_mut!(size).cast(),
                    u32::try_from(std::mem::size_of::<ACL_SIZE_INFORMATION>())
                        .expect("ACL size information fits u32"),
                    AclSizeInformation,
                )
            } == 0
                || size.AceCount != 1
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "Windows cache directory DACL is not current-user-only",
                ));
            }
            let mut ace = ptr::null_mut();
            if unsafe { GetAce(dacl, 0, &mut ace) } == 0 || ace.is_null() {
                return Err(io::Error::last_os_error());
            }
            let ace = unsafe { &*ace.cast::<ACCESS_ALLOWED_ACE>() };
            let expected_flags = (OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE) as u8;
            let ace_sid = ptr::addr_of!(ace.SidStart) as PSID;
            if ace.Header.AceType != ACCESS_ALLOWED_ACE_TYPE as u8
                || ace.Header.AceFlags != expected_flags
                || ace.Mask != FILE_ALL_ACCESS
                || unsafe { EqualSid(ace_sid, security.sid()) } == 0
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "Windows cache directory DACL grants access beyond the current user",
                ));
            }
            Ok(())
        })
    }

    fn with_security_info<T>(
        handle: HANDLE,
        inspect: impl FnOnce(PSID, *mut ACL, PSECURITY_DESCRIPTOR) -> io::Result<T>,
    ) -> io::Result<T> {
        let mut owner = ptr::null_mut();
        let mut dacl = ptr::null_mut();
        let mut descriptor = ptr::null_mut();
        let code = unsafe {
            GetSecurityInfo(
                handle,
                SE_FILE_OBJECT,
                OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                &mut owner,
                ptr::null_mut(),
                &mut dacl,
                ptr::null_mut(),
                &mut descriptor,
            )
        };
        if code != ERROR_SUCCESS {
            return Err(io::Error::from_raw_os_error(code as i32));
        }
        struct LocalDescriptor(PSECURITY_DESCRIPTOR);
        impl Drop for LocalDescriptor {
            fn drop(&mut self) {
                unsafe { LocalFree(self.0.cast()) };
            }
        }
        let descriptor_guard = LocalDescriptor(descriptor);
        inspect(owner, dacl, descriptor_guard.0)
    }

    struct CurrentUserSecurity {
        sid_storage: AlignedBuffer,
        acl_storage: AlignedBuffer,
        descriptor: Box<SECURITY_DESCRIPTOR>,
    }

    impl CurrentUserSecurity {
        fn new() -> io::Result<Self> {
            let mut token = ptr::null_mut();
            if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
                return Err(io::Error::last_os_error());
            }
            let token = unsafe { OwnedHandle::from_raw_handle(token as RawHandle) };
            let mut required = 0u32;
            let first = unsafe {
                GetTokenInformation(
                    token.as_raw_handle() as HANDLE,
                    TokenUser,
                    ptr::null_mut(),
                    0,
                    &mut required,
                )
            };
            if first != 0 || unsafe { GetLastError() } != ERROR_INSUFFICIENT_BUFFER {
                return Err(io::Error::last_os_error());
            }
            let mut token_info = AlignedBuffer::new(usize::try_from(required).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "token information is too large")
            })?);
            if unsafe {
                GetTokenInformation(
                    token.as_raw_handle() as HANDLE,
                    TokenUser,
                    token_info.as_mut_void(),
                    required,
                    &mut required,
                )
            } == 0
            {
                return Err(io::Error::last_os_error());
            }
            let token_user = unsafe { &*token_info.as_ptr::<TOKEN_USER>() };
            let sid_len = unsafe { GetLengthSid(token_user.User.Sid) };
            if sid_len == 0 {
                return Err(io::Error::last_os_error());
            }
            let mut sid_storage = AlignedBuffer::new(usize::try_from(sid_len).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "current-user SID is too large")
            })?);
            if unsafe {
                CopySid(
                    sid_len,
                    sid_storage.as_mut_ptr::<core::ffi::c_void>(),
                    token_user.User.Sid,
                )
            } == 0
            {
                return Err(io::Error::last_os_error());
            }

            let acl_len = std::mem::size_of::<ACL>()
                .checked_add(std::mem::size_of::<ACCESS_ALLOWED_ACE>())
                .and_then(|value| value.checked_sub(std::mem::size_of::<u32>()))
                .and_then(|value| value.checked_add(sid_len as usize))
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "ACL is too large"))?;
            let mut acl_storage = AlignedBuffer::new(acl_len);
            let acl = acl_storage.as_mut_ptr::<ACL>();
            if unsafe {
                InitializeAcl(
                    acl,
                    u32::try_from(acl_len).map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "ACL is too large")
                    })?,
                    ACL_REVISION,
                )
            } == 0
                || unsafe {
                    AddAccessAllowedAceEx(
                        acl,
                        ACL_REVISION,
                        OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE,
                        FILE_ALL_ACCESS,
                        sid_storage.as_mut_ptr::<core::ffi::c_void>(),
                    )
                } == 0
            {
                return Err(io::Error::last_os_error());
            }

            let mut descriptor = Box::<SECURITY_DESCRIPTOR>::default();
            let descriptor_ptr =
                descriptor.as_mut() as *mut SECURITY_DESCRIPTOR as PSECURITY_DESCRIPTOR;
            if unsafe { InitializeSecurityDescriptor(descriptor_ptr, SECURITY_DESCRIPTOR_REVISION) }
                == 0
                || unsafe {
                    SetSecurityDescriptorOwner(
                        descriptor_ptr,
                        sid_storage.as_mut_ptr::<core::ffi::c_void>(),
                        0,
                    )
                } == 0
                || unsafe { SetSecurityDescriptorDacl(descriptor_ptr, 1, acl, 0) } == 0
                || unsafe {
                    SetSecurityDescriptorControl(
                        descriptor_ptr,
                        SE_DACL_PROTECTED,
                        SE_DACL_PROTECTED,
                    )
                } == 0
            {
                return Err(io::Error::last_os_error());
            }
            Ok(Self {
                sid_storage,
                acl_storage,
                descriptor,
            })
        }

        fn sid(&self) -> PSID {
            self.sid_storage.as_ptr::<core::ffi::c_void>() as PSID
        }

        fn acl(&self) -> *const ACL {
            self.acl_storage.as_ptr::<ACL>()
        }

        fn descriptor(&self) -> *const SECURITY_DESCRIPTOR {
            self.descriptor.as_ref()
        }
    }

    #[repr(C, align(8))]
    #[derive(Clone, Copy, Default)]
    struct AlignedBufferUnit(u64);

    const _: () = {
        assert!(
            std::mem::align_of::<AlignedBufferUnit>()
                >= std::mem::align_of::<FILE_ID_BOTH_DIR_INFO>()
        );
        assert!(
            std::mem::align_of::<AlignedBufferUnit>()
                >= std::mem::align_of::<FILE_RENAME_INFORMATION>()
        );
        assert!(std::mem::align_of::<AlignedBufferUnit>() >= std::mem::align_of::<TOKEN_USER>());
        assert!(std::mem::align_of::<AlignedBufferUnit>() >= std::mem::align_of::<ACL>());
    };

    #[cfg(target_arch = "x86")]
    const _: () = assert!(std::mem::align_of::<AlignedBufferUnit>() >= 8);

    struct AlignedBuffer {
        units: Vec<AlignedBufferUnit>,
        len: usize,
    }

    impl AlignedBuffer {
        fn new(len: usize) -> Self {
            assert!(len > 0, "Windows API buffers must not be empty");
            let unit = std::mem::size_of::<AlignedBufferUnit>();
            Self {
                units: vec![AlignedBufferUnit::default(); len.div_ceil(unit)],
                len,
            }
        }

        fn as_ptr<T>(&self) -> *const T {
            std::ptr::addr_of!(self.units[0].0).cast()
        }

        fn as_mut_ptr<T>(&mut self) -> *mut T {
            std::ptr::addr_of_mut!(self.units[0].0).cast()
        }

        fn as_void(&self) -> *const core::ffi::c_void {
            self.as_ptr()
        }

        fn as_mut_void(&mut self) -> *mut core::ffi::c_void {
            self.as_mut_ptr()
        }

        fn as_bytes(&self) -> &[u8] {
            unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len) }
        }

        fn len_u32(&self) -> io::Result<u32> {
            u32::try_from(self.len)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "buffer is too large"))
        }
    }

    #[cfg(test)]
    pub fn aligned_buffer_facts_for_test() -> (usize, usize, [usize; 4]) {
        let buffer = AlignedBuffer::new(std::mem::size_of::<FILE_ID_BOTH_DIR_INFO>());
        (
            std::mem::align_of::<AlignedBufferUnit>(),
            buffer.as_ptr::<u8>() as usize,
            [
                std::mem::align_of::<FILE_ID_BOTH_DIR_INFO>(),
                std::mem::align_of::<FILE_RENAME_INFORMATION>(),
                std::mem::align_of::<TOKEN_USER>(),
                std::mem::align_of::<ACL>(),
            ],
        )
    }

    fn raw_handle(file: &File) -> HANDLE {
        file.as_raw_handle() as HANDLE
    }
}

static BUNDLED_ASSET_TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);
static BUNDLED_SIDECAR_CACHE_ROOT: std::sync::Mutex<Option<PathBuf>> = std::sync::Mutex::new(None);

#[cfg(test)]
type BundledAssetPathHook = Box<dyn Fn(&Path) + Send + Sync>;

#[cfg(test)]
static BUNDLED_ASSET_BEFORE_PUBLISH_HOOK: std::sync::Mutex<Option<BundledAssetPathHook>> =
    std::sync::Mutex::new(None);
#[cfg(test)]
static BUNDLED_ASSET_BEFORE_FILE_CREATE_HOOK: std::sync::Mutex<Option<BundledAssetPathHook>> =
    std::sync::Mutex::new(None);
#[cfg(test)]
static BUNDLED_ASSET_FAIL_AFTER_PENDING_CREATE: std::sync::Mutex<Option<PathBuf>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
fn run_bundled_asset_path_hook(hook: &std::sync::Mutex<Option<BundledAssetPathHook>>, path: &Path) {
    if let Some(hook) = hook.lock().expect("bundled asset test hook lock").as_ref() {
        hook(path);
    }
}

fn random_bundled_sidecar_cache_suffix() -> io::Result<String> {
    use rand::RngCore;

    let mut bytes = [0u8; 16];
    rand::rngs::OsRng
        .try_fill_bytes(&mut bytes)
        .map_err(|error| {
            io::Error::other(format!("failed to obtain OS cache randomness: {error}"))
        })?;
    Ok(format!("{:032x}", u128::from_be_bytes(bytes)))
}

fn allocate_bundled_sidecar_cache_root_in(
    temp_root: &Path,
    resolution_cwd: &Path,
    mut next_suffix: impl FnMut() -> io::Result<String>,
) -> io::Result<PathBuf> {
    let lexical_temp_root = if temp_root.is_absolute() {
        temp_root.to_path_buf()
    } else {
        if !resolution_cwd.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cache resolution cwd must be absolute",
            ));
        }
        resolution_cwd.join(temp_root)
    };
    // Simplified, not raw `canonicalize`: on Windows that returns a `\\?\` verbatim path, and
    // every path derived from this root inherits the prefix — including the sidecar script a
    // child node process is asked to run, which it cannot resolve.
    let absolute_temp_root = dunce::canonicalize(&lexical_temp_root)?;
    if !absolute_temp_root.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "temporary cache root is not absolute",
        ));
    }

    for _ in 0..32 {
        let suffix = next_suffix()?;
        if suffix.len() != 32
            || !suffix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cache suffix must be exactly 32 lowercase hexadecimal characters",
            ));
        }
        let candidate = absolute_temp_root.join(format!("klaus-agent-sdk-{suffix}"));
        match secure_asset_fs::Dir::create_private_path(&candidate) {
            Ok(dir) => {
                dir.verify_private()?;
                let created = dir.path().to_path_buf();
                if !created.is_absolute() {
                    return Err(io::Error::other(
                        "created bundled sidecar cache root is not absolute",
                    ));
                }
                return Ok(created);
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "failed to allocate an unoccupied private bundled sidecar cache root",
    ))
}

fn bundled_sidecar_cache_root() -> Result<PathBuf, BrowserRuntimeError> {
    let mut cached = BUNDLED_SIDECAR_CACHE_ROOT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(path) = cached.as_ref() {
        let reusable = secure_asset_fs::Dir::open_or_create_private_path(path)
            .and_then(|dir| dir.verify_private());
        if reusable.is_ok() {
            return Ok(path.clone());
        }
    }

    let temp_root = std::env::temp_dir();
    let resolution_cwd = std::env::current_dir().map_err(|error| {
        bundled_asset_error(format!(
            "failed to resolve current directory for bundled sidecar cache: {error}"
        ))
    })?;
    let root = allocate_bundled_sidecar_cache_root_in(
        &temp_root,
        &resolution_cwd,
        random_bundled_sidecar_cache_suffix,
    )
    .map_err(|error| {
        bundled_asset_error(format!(
            "failed to allocate private bundled sidecar cache under {}: {error}",
            temp_root.display()
        ))
    })?;
    *cached = Some(root.clone());
    Ok(root)
}

fn write_bundled_sidecar() -> Result<PathBuf, BrowserRuntimeError> {
    let sdk_cache = bundled_sidecar_cache_root()?;
    ensure_private_cache_directory(&sdk_cache)?;
    let browser_cache = sdk_cache.join("browser-runtime");
    ensure_private_cache_directory(&browser_cache)?;
    // No version segment here. `write_bundled_sidecar_in` publishes under the
    // asset tree's own sha256 and verifies the tree before reuse, so the cache is
    // already content addressed: changed assets land in a new directory, and
    // unchanged assets are reused instead of rewritten. A crate-version segment
    // was strictly worse on both counts — it could not notice assets changing
    // within one version, and it forced a full rewrite of identical bytes on every
    // bump — and the engine no longer carries a version to segment by.
    write_bundled_sidecar_in(&browser_cache)
}

/// The file name the sidecar loads the Windows broker's Node addon under.
///
/// The sidecar resolves it beside its own script (`requireFromHere('./…')`), so the published
/// asset tree is where it has to land.
#[cfg(windows)]
const WINDOWS_BROKER_ADDON_NAME: &str = "klaus_browser_pipe.node";

/// The published asset path the addon occupies, next to the sidecar it serves.
const WINDOWS_BROKER_ADDON_ASSET_PATH: &str = "runtime/klaus_browser_pipe.node";

/// Where this build put the Windows broker's Node addon, and its bytes.
///
/// The addon is `klaus-browser-pipe-napi`'s cdylib: a build artifact of this workspace, not
/// one of the sidecar's static assets. It cannot be embedded in the committed asset tree,
/// because that tree is content addressed by a committed hash that has to mean the same
/// thing on every platform. So it is looked up as a file: beside the running executable,
/// which is the shipped layout, then one directory up, because cargo puts test binaries in
/// `deps/` and cdylibs in the profile root. `KLAUS_BROWSER_PIPE_ADDON` names it outright for
/// a host that stages it somewhere else.
///
/// Both the `.node` and the raw `.dll` are accepted: packaging renames the cdylib, a cargo
/// build does not.
#[cfg(windows)]
fn windows_broker_addon_source() -> Result<PathBuf, BrowserRuntimeError> {
    let mut searched = Vec::new();
    if let Some(explicit) = std::env::var_os("KLAUS_BROWSER_PIPE_ADDON") {
        let path = PathBuf::from(explicit);
        if path.is_file() {
            return Ok(path);
        }
        searched.push(path);
    }
    let executable = std::env::current_exe().map_err(|error| {
        bundled_asset_error(format!(
            "failed to resolve this executable while locating the Windows browser broker addon: {error}"
        ))
    })?;
    let mut directories = Vec::new();
    if let Some(parent) = executable.parent() {
        directories.push(parent.to_path_buf());
        if let Some(grandparent) = parent.parent() {
            directories.push(grandparent.to_path_buf());
        }
    }
    for directory in directories {
        for name in [WINDOWS_BROKER_ADDON_NAME, "klaus_browser_pipe.dll"] {
            let candidate = directory.join(name);
            if candidate.is_file() {
                return Ok(candidate);
            }
            searched.push(candidate);
        }
    }
    Err(bundled_asset_error(format!(
        "the Windows browser broker needs the klaus-browser-pipe-napi addon; none of these exist: {}",
        searched
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    )))
}

/// The addon's bytes plus the hex digest that identifies them.
#[cfg(windows)]
fn windows_broker_addon_payload() -> Result<(Vec<u8>, String), BrowserRuntimeError> {
    let source = windows_broker_addon_source()?;
    let bytes = std::fs::read(&source).map_err(|error| {
        bundled_asset_error(format!(
            "failed to read the Windows browser broker addon {}: {error}",
            source.display()
        ))
    })?;
    if bytes.is_empty() {
        return Err(bundled_asset_error(format!(
            "the Windows browser broker addon {} is empty",
            source.display()
        )));
    }
    let digest = Sha256::digest(&bytes);
    Ok((bytes, format!("{digest:x}")))
}

/// The published tree's directory name.
///
/// On Windows it binds the addon's digest as well as the committed tree's: the addon is not
/// covered by the committed hash, so without this a rebuilt addon would be expected to reuse
/// a directory that still holds the previous one.
fn published_bundled_asset_tree_name(
    tree_sha256: &str,
    #[cfg_attr(not(windows), allow(unused_variables))] addon_sha256: Option<&str>,
) -> String {
    #[cfg(windows)]
    if let Some(addon_sha256) = addon_sha256 {
        return format!("{tree_sha256}-{}", &addon_sha256[..16]);
    }
    tree_sha256.to_string()
}

fn write_bundled_sidecar_in(cache_parent: &Path) -> Result<PathBuf, BrowserRuntimeError> {
    let lock = validate_bundled_browser_asset_lock()?;
    let cache =
        secure_asset_fs::Dir::open_or_create_private_path(cache_parent).map_err(|error| {
            bundled_asset_error(format!(
                "failed to open private bundled browser cache {}: {error}",
                cache_parent.display()
            ))
        })?;
    cache.verify_private().map_err(|error| {
        bundled_asset_error(format!(
            "bundled browser cache is not private {}: {error}",
            cache_parent.display()
        ))
    })?;
    #[cfg(windows)]
    let (addon_bytes, addon_sha256) = {
        let (bytes, digest) = windows_broker_addon_payload()?;
        (bytes, Some(digest))
    };
    #[cfg(not(windows))]
    let addon_sha256: Option<String> = None;
    let tree_name = published_bundled_asset_tree_name(&lock.tree_sha256, addon_sha256.as_deref());
    let published_name = std::ffi::OsStr::new(&tree_name);
    let published = cache_parent.join(published_name);
    if let Some(existing) = open_published_bundled_asset_tree(&cache, published_name)? {
        verify_bundled_asset_tree(&existing)?;
        return Ok(published.join("runtime/playwright-sidecar.mjs"));
    }

    let mut pending = PendingBundledAssetTree::create(&cache, cache_parent, &tree_name)?;
    for asset in BUNDLED_BROWSER_ASSETS {
        write_new_bundled_asset(&pending.dir, asset)?;
    }
    #[cfg(windows)]
    write_new_bundled_asset_bytes(
        &pending.dir,
        WINDOWS_BROKER_ADDON_ASSET_PATH,
        "100644",
        &addon_bytes,
    )?;
    verify_bundled_asset_tree(&pending.dir)?;

    if let Some(existing) = open_published_bundled_asset_tree(&cache, published_name)? {
        verify_bundled_asset_tree(&existing)?;
        return Ok(published.join("runtime/playwright-sidecar.mjs"));
    }

    #[cfg(test)]
    run_bundled_asset_path_hook(&BUNDLED_ASSET_BEFORE_PUBLISH_HOOK, &published);

    match cache.publish_no_replace(&pending.dir, &pending.name, published_name) {
        Ok(()) => pending.published = true,
        Err(rename_error) if rename_error.kind() == std::io::ErrorKind::AlreadyExists => {
            if let Some(existing) = open_published_bundled_asset_tree(&cache, published_name)? {
                verify_bundled_asset_tree(&existing)?;
                return Ok(published.join("runtime/playwright-sidecar.mjs"));
            }
            return Err(bundled_asset_error(format!(
                "bundled browser publish winner disappeared before verification: {rename_error}"
            )));
        }
        Err(rename_error) => {
            return Err(bundled_asset_error(format!(
                "failed to atomically publish bundled browser assets without replacement: {rename_error}"
            )));
        }
    }

    let existing = open_published_bundled_asset_tree(&cache, published_name)?.ok_or_else(|| {
        bundled_asset_error("published bundled browser asset tree disappeared after publication")
    })?;
    verify_bundled_asset_tree(&existing)?;
    Ok(published.join("runtime/playwright-sidecar.mjs"))
}

fn open_published_bundled_asset_tree(
    cache: &secure_asset_fs::Dir,
    name: &std::ffi::OsStr,
) -> Result<Option<secure_asset_fs::Dir>, BrowserRuntimeError> {
    match cache.open_entry(name) {
        Ok(entry) if entry.is_dir() => {
            let dir = entry.into_dir().map_err(|error| {
                bundled_asset_error(format!(
                    "failed to retain published bundled browser directory handle: {error}"
                ))
            })?;
            Ok(Some(dir))
        }
        Ok(_) => Err(bundled_asset_error(format!(
            "published bundled browser cache entry is not a real directory: {}",
            cache.path().join(name).display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(bundled_asset_error(format!(
            "published bundled browser cache entry is a symlink, reparse point, or unreadable entry {}: {error}",
            cache.path().join(name).display()
        ))),
    }
}

struct PendingBundledAssetTree {
    parent: secure_asset_fs::Dir,
    dir: secure_asset_fs::Dir,
    name: std::ffi::OsString,
    path: PathBuf,
    published: bool,
}

impl PendingBundledAssetTree {
    fn create(
        parent: &secure_asset_fs::Dir,
        parent_path: &Path,
        tree_sha256: &str,
    ) -> Result<Self, BrowserRuntimeError> {
        let retained_parent = parent.try_clone().map_err(|error| {
            bundled_asset_error(format!(
                "failed to retain bundled browser cache handle: {error}"
            ))
        })?;
        for _ in 0..32 {
            let counter = BUNDLED_ASSET_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let name = std::ffi::OsString::from(format!(
                ".{tree_sha256}.tmp-{}-{counter}-{nanos}",
                std::process::id()
            ));
            let path = parent_path.join(&name);
            match parent.create_private_child(&name) {
                Ok(dir) => {
                    let pending = Self {
                        parent: retained_parent,
                        dir,
                        name,
                        path,
                        published: false,
                    };
                    #[cfg(test)]
                    {
                        let mut injected_parent = BUNDLED_ASSET_FAIL_AFTER_PENDING_CREATE
                            .lock()
                            .expect("pending-create test hook lock");
                        if injected_parent.as_deref() == Some(parent_path) {
                            *injected_parent = None;
                            return Err(bundled_asset_error(
                                "injected failure after bundled browser pending tree creation",
                            ));
                        }
                    }
                    pending.dir.verify_private().map_err(|error| {
                        bundled_asset_error(format!(
                            "bundled browser temporary tree is not private {}: {error}",
                            pending.path.display()
                        ))
                    })?;
                    return Ok(pending);
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(bundled_asset_error(format!(
                        "failed to create private bundled browser temporary tree: {error}"
                    )));
                }
            }
        }
        Err(bundled_asset_error(
            "failed to allocate a unique bundled browser temporary tree",
        ))
    }
}

impl Drop for PendingBundledAssetTree {
    fn drop(&mut self) {
        if !self.published {
            let _ = self.parent.remove_tree_entry(&self.name, &self.dir);
        }
    }
}

fn ensure_private_cache_directory(path: &Path) -> Result<(), BrowserRuntimeError> {
    let dir = secure_asset_fs::Dir::open_or_create_private_path(path).map_err(|error| {
        bundled_asset_error(format!(
            "failed to create or open private bundled browser cache directory {}: {error}",
            path.display()
        ))
    })?;
    dir.verify_private().map_err(|error| {
        bundled_asset_error(format!(
            "bundled browser cache directory is not private {}: {error}",
            path.display()
        ))
    })
}

fn write_new_bundled_asset(
    root: &secure_asset_fs::Dir,
    asset: BundledBrowserAsset,
) -> Result<(), BrowserRuntimeError> {
    write_new_bundled_asset_bytes(root, asset.path, asset.mode, asset.bytes)
}

/// Write one asset from bytes this build produced rather than embedded.
///
/// Split out for the Windows broker addon, which is a cdylib the workspace builds and so
/// cannot be `include_bytes!`-ed like the rest.
fn write_new_bundled_asset_bytes(
    root: &secure_asset_fs::Dir,
    asset_path: &str,
    mode: &str,
    bytes: &[u8],
) -> Result<(), BrowserRuntimeError> {
    let relative = Path::new(asset_path);
    let parent = relative
        .parent()
        .ok_or_else(|| bundled_asset_error("bundled browser asset has no parent"))?;
    let parent_dir = ensure_private_asset_subdirectories(root, parent)?;
    let path = root.path().join(relative);
    #[cfg(test)]
    run_bundled_asset_path_hook(&BUNDLED_ASSET_BEFORE_FILE_CREATE_HOOK, &path);
    let file_name = relative
        .file_name()
        .ok_or_else(|| bundled_asset_error("bundled browser asset has no file name"))?;
    let mut file = parent_dir
        .create_new_file(file_name, if mode == "100755" { 0o755 } else { 0o644 })
        .map_err(|error| {
            bundled_asset_error(format!(
                "failed to create bundled browser temporary asset {}: {error}",
                path.display()
            ))
        })?;
    file.write_all(bytes).map_err(|error| {
        bundled_asset_error(format!(
            "failed to write bundled browser temporary asset {}: {error}",
            path.display()
        ))
    })?;
    apply_locked_asset_mode(&file, &path, mode)?;
    file.sync_all().map_err(|error| {
        bundled_asset_error(format!(
            "failed to sync bundled browser temporary asset {}: {error}",
            path.display()
        ))
    })?;
    parent_dir.sync().map_err(|error| {
        bundled_asset_error(format!(
            "failed to sync bundled browser asset parent {}: {error}",
            parent_dir.path().display()
        ))
    })
}

fn ensure_private_asset_subdirectories(
    root: &secure_asset_fs::Dir,
    relative: &Path,
) -> Result<secure_asset_fs::Dir, BrowserRuntimeError> {
    let mut current = root.try_clone().map_err(|error| {
        bundled_asset_error(format!(
            "failed to retain bundled browser asset root handle: {error}"
        ))
    })?;
    for component in relative.components() {
        let std::path::Component::Normal(component) = component else {
            return Err(bundled_asset_error(
                "bundled browser asset parent is not root-relative",
            ));
        };
        current = current
            .open_or_create_private_child(component)
            .map_err(|error| {
                bundled_asset_error(format!(
                    "failed to create no-follow bundled browser asset parent {}: {error}",
                    current.path().join(component).display()
                ))
            })?;
    }
    Ok(current)
}

fn verify_bundled_asset_tree(root: &secure_asset_fs::Dir) -> Result<(), BrowserRuntimeError> {
    root.verify_private().map_err(|error| {
        bundled_asset_error(format!(
            "published bundled browser asset tree is not private {}: {error}",
            root.path().display()
        ))
    })?;
    let mut actual_paths = Vec::new();
    collect_bundled_asset_paths(root, Path::new(""), &mut actual_paths)?;
    actual_paths.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    // The committed assets, plus — on Windows only — the broker addon the host stages beside
    // the sidecar. The addon is a build artifact, so it is not in the committed set and not
    // covered by the committed tree hash; the published directory name binds its digest
    // instead, and its bytes are re-checked below against the source this build resolved.
    let mut expected_paths = BUNDLED_BROWSER_ASSETS
        .iter()
        .map(|asset| asset.path.to_string())
        .collect::<Vec<_>>();
    if cfg!(windows) {
        expected_paths.push(WINDOWS_BROKER_ADDON_ASSET_PATH.to_string());
    }
    expected_paths.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    if actual_paths != expected_paths {
        return Err(bundled_asset_error(
            "published bundled browser asset path set drifted",
        ));
    }
    for asset in BUNDLED_BROWSER_ASSETS {
        verify_one_bundled_asset(root, asset)?;
    }
    #[cfg(windows)]
    verify_windows_broker_addon(root)?;
    Ok(())
}

/// Check the staged addon still is the one this build resolves.
///
/// The published directory name binds the addon's digest, so a mismatch means the staged copy
/// was replaced after publication rather than that a new addon needs a new directory.
#[cfg(windows)]
fn verify_windows_broker_addon(root: &secure_asset_fs::Dir) -> Result<(), BrowserRuntimeError> {
    let (_, expected) = windows_broker_addon_payload()?;
    let staged =
        std::fs::read(root.path().join(WINDOWS_BROKER_ADDON_ASSET_PATH)).map_err(|error| {
            bundled_asset_error(format!(
                "failed to read the staged Windows browser broker addon: {error}"
            ))
        })?;
    if format!("{:x}", Sha256::digest(&staged)) != expected {
        return Err(bundled_asset_error(
            "staged Windows browser broker addon does not match this build's addon",
        ));
    }
    Ok(())
}

fn collect_bundled_asset_paths(
    directory: &secure_asset_fs::Dir,
    relative_directory: &Path,
    output: &mut Vec<String>,
) -> Result<(), BrowserRuntimeError> {
    directory.verify_private().map_err(|error| {
        bundled_asset_error(format!(
            "bundled browser asset directory is not private {}: {error}",
            directory.path().display()
        ))
    })?;
    let names = directory.list_names().map_err(|error| {
        bundled_asset_error(format!(
            "failed to enumerate bundled browser asset directory {}: {error}",
            directory.path().display()
        ))
    })?;
    for name in names {
        let relative = relative_directory.join(&name);
        let entry = directory.open_entry(&name).map_err(|error| {
            bundled_asset_error(format!(
                "bundled browser asset is a symlink, reparse point, or unreadable entry {}: {error}",
                directory.path().join(&name).display()
            ))
        })?;
        if entry.is_dir() {
            let child = entry.into_dir().map_err(|error| {
                bundled_asset_error(format!(
                    "failed to retain bundled browser asset directory handle: {error}"
                ))
            })?;
            collect_bundled_asset_paths(&child, &relative, output)?;
        } else if entry.is_file() {
            let relative = relative.to_str().ok_or_else(|| {
                bundled_asset_error("bundled browser asset path is not valid Unicode")
            })?;
            output.push(relative.replace(std::path::MAIN_SEPARATOR, "/"));
        } else {
            return Err(bundled_asset_error(format!(
                "bundled browser asset tree contains a nonregular entry: {}",
                directory.path().join(&name).display()
            )));
        }
    }
    Ok(())
}

fn verify_one_bundled_asset(
    root: &secure_asset_fs::Dir,
    asset: BundledBrowserAsset,
) -> Result<(), BrowserRuntimeError> {
    let relative = Path::new(asset.path);
    let parent = relative
        .parent()
        .ok_or_else(|| bundled_asset_error("bundled browser asset has no parent"))?;
    let mut current = root.try_clone().map_err(|error| {
        bundled_asset_error(format!(
            "failed to retain bundled browser verification root handle: {error}"
        ))
    })?;
    for component in parent.components() {
        let std::path::Component::Normal(component) = component else {
            return Err(bundled_asset_error(
                "bundled browser verification path is not root-relative",
            ));
        };
        let entry = current.open_entry(component).map_err(|error| {
            bundled_asset_error(format!(
                "bundled browser asset parent is a symlink, reparse point, or unreadable entry {}: {error}",
                current.path().join(component).display()
            ))
        })?;
        current = entry.into_dir().map_err(|_| {
            bundled_asset_error(format!(
                "bundled browser asset parent is not a directory: {}",
                current.path().join(component).display()
            ))
        })?;
    }
    let file_name = relative
        .file_name()
        .ok_or_else(|| bundled_asset_error("bundled browser asset has no file name"))?;
    let path = root.path().join(relative);
    let entry = current.open_entry(file_name).map_err(|error| {
        bundled_asset_error(format!(
            "failed to open bundled browser asset relative to its checked parent {}: {error}",
            path.display()
        ))
    })?;
    if !entry.is_file() || entry.metadata().len() != asset.bytes.len() as u64 {
        return Err(bundled_asset_error(format!(
            "bundled browser asset type or size drifted: {}",
            path.display()
        )));
    }
    verify_locked_asset_mode(entry.metadata(), &path, asset.mode)?;
    let mut file = entry.into_file();
    let mut bytes = Vec::with_capacity(asset.bytes.len());
    file.read_to_end(&mut bytes).map_err(|error| {
        bundled_asset_error(format!(
            "failed to read bundled browser asset {}: {error}",
            path.display()
        ))
    })?;
    if bytes != asset.bytes {
        return Err(bundled_asset_error(format!(
            "bundled browser asset bytes drifted after publication: {}",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(unix)]
fn apply_locked_asset_mode(
    file: &File,
    path: &Path,
    mode: &str,
) -> Result<(), BrowserRuntimeError> {
    use std::os::unix::fs::PermissionsExt;
    let permissions = std::fs::Permissions::from_mode(if mode == "100755" { 0o755 } else { 0o644 });
    file.set_permissions(permissions).map_err(|error| {
        bundled_asset_error(format!(
            "failed to apply locked mode to bundled browser asset {}: {error}",
            path.display()
        ))
    })
}

#[cfg(not(unix))]
fn apply_locked_asset_mode(
    _file: &File,
    _path: &Path,
    mode: &str,
) -> Result<(), BrowserRuntimeError> {
    if matches!(mode, "100644" | "100755") {
        Ok(())
    } else {
        Err(bundled_asset_error(
            "unsupported locked browser asset mode on Windows",
        ))
    }
}

#[cfg(unix)]
fn verify_locked_asset_mode(
    metadata: &Metadata,
    path: &Path,
    mode: &str,
) -> Result<(), BrowserRuntimeError> {
    use std::os::unix::fs::PermissionsExt;
    let expected = if mode == "100755" { 0o755 } else { 0o644 };
    let actual = metadata.permissions().mode() & 0o777;
    if actual != expected {
        return Err(bundled_asset_error(format!(
            "bundled browser asset mode drifted at {}: expected {expected:o}, got {actual:o}",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn verify_locked_asset_mode(
    _metadata: &Metadata,
    _path: &Path,
    mode: &str,
) -> Result<(), BrowserRuntimeError> {
    if matches!(mode, "100644" | "100755") {
        Ok(())
    } else {
        Err(bundled_asset_error(
            "unsupported locked browser asset mode on Windows",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Real Node, which every fixture program below is written against: custom
    /// `klaus:` ESM loader hooks, the Chrome native-messaging socket host and the
    /// `ws` upgrade handshake are all Node contracts. The product sidecar runs on
    /// whatever `default_browser_runtime_path` resolves, Bun included, but handing
    /// these fixtures to Bun makes them fail on runtime differences that say
    /// nothing about the product under test.
    ///
    /// `None` on a host without Node. The caller returns instead of asserting: a
    /// missing fixture runtime is a gap in the host, not a defect in the browser
    /// runtime, and reporting it as one buries real failures.
    fn node_fixture_runtime() -> Option<PathBuf> {
        let search_paths = std::env::var_os("PATH")
            .map(|path| std::env::split_paths(&path).collect::<Vec<_>>())
            .unwrap_or_default();
        let found = executable_on_path(&["node", "node.exe"], &search_paths);
        if found.is_none() {
            eprintln!(
                "skipping browser fixture: no `node` on PATH, and these fixtures are Node programs"
            );
        }
        found
    }

    /// Per-request budget for sidecar operations driven by these tests. A
    /// full-suite run saturates the host and each of these cases boots a real
    /// bundled sidecar process, so a 5s budget turned behavioral assertions into
    /// timeouts. These waits assert that the operation COMPLETES, never how fast,
    /// so the budget is deliberately far above the product default (30s).
    const SIDECAR_LIVENESS_TIMEOUT_MS: u64 = 120_000;

    use crate::{test_new_process_context, test_process_context};
    use klaus_agent_core::browser::{
        BrowserArtifactKind, BrowserClipboardAction, BrowserComputerAction, BrowserDialogAction,
        BrowserOperation, BrowserReadPageFilter, BrowserRuntimeErrorCode, BrowserRuntimeEvent,
        BrowserScreenshotFormat, BrowserScrollDirection, BrowserTabMarkStatus, BrowserTabsAction,
        BrowserTarget, BrowserWaitUntil,
    };
    use klaus_agent_core::types::json::Json;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Barrier, Mutex};

    #[test]
    fn sealed_local_runtime_binds_the_constructor_namespace() {
        let context = test_new_process_context();
        let expected = context.namespace_id();
        let capability =
            PlaywrightBrowserRuntime::with_defaults(context).into_local_browser_runtime();
        let runtime = capability.into_runtime();

        assert_eq!(
            runtime.process_binding(),
            klaus_agent_core::browser::BrowserProcessBinding::ContextBound(expected)
        );
    }

    #[test]
    fn sealed_local_capability_root_preserves_source_and_constructor_namespace() {
        let context = test_new_process_context();
        let expected_namespace = context.namespace_id();
        let expected_source = RuntimeCapabilitySource::plugin(
            "browser-plugin",
            Some("1.0.0"),
            "/plugins/browser-plugin",
        );
        let root = PlaywrightBrowserRuntime::with_defaults(context)
            .into_local_browser_runtime()
            .into_capability_root(expected_source.clone());
        let (actual_source, capability) = root.into_parts();
        let runtime = capability.into_runtime();

        assert_eq!(actual_source, expected_source);
        assert_eq!(
            runtime.process_binding(),
            klaus_agent_core::browser::BrowserProcessBinding::ContextBound(expected_namespace)
        );
    }

    fn unique_temp_dir(tag: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("ae-sdk-{tag}-{pid}-{n}-{nanos}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[cfg(unix)]
    #[test]
    fn runtime_path_prefers_node_and_falls_back_to_bun() {
        use std::os::unix::fs::PermissionsExt;

        let root = unique_temp_dir("browser-runtime-path");
        let node = root.join("node");
        let bun = root.join("bun");
        fs::write(&node, "#!/bin/sh\nexit 0\n").unwrap();
        fs::write(&bun, "#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&node, fs::Permissions::from_mode(0o755)).unwrap();
        fs::set_permissions(&bun, fs::Permissions::from_mode(0o755)).unwrap();

        assert_eq!(
            executable_on_path(&["node"], std::slice::from_ref(&root)),
            Some(node.clone())
        );
        assert_eq!(
            executable_on_path(&["node", "bun"], std::slice::from_ref(&root)),
            Some(node.clone())
        );
        fs::remove_file(&node).unwrap();
        assert_eq!(
            executable_on_path(&["node", "bun"], std::slice::from_ref(&root)),
            Some(bun)
        );
        fs::remove_dir_all(root).unwrap();
    }

    /// A unique endpoint for the fake native-host servers these tests run.
    ///
    /// On unix this is a filesystem socket path, and it has to stay in the temp root:
    /// `sun_path` holds 104 bytes on macOS, and a socket placed inside
    /// [`unique_temp_dir`] blew past that on its own — the private temp root plus the
    /// tagged directory name plus the file name came to ~130 bytes — after which
    /// `listen` failed with a bare `EINVAL` that says nothing about length.
    ///
    /// On Windows there is no filesystem socket to place anywhere: `net.Server.listen`
    /// binds a named pipe, so a `C:\...\*.sock` argument is read as a pipe name and
    /// rejected with `EACCES`. The product side already resolves its default endpoint
    /// per platform (`default_native_socket`, `defaultNativeBridgeSocketPath`), so the
    /// override these tests inject has to speak the same Windows form.
    fn unique_socket_path(tag: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let stem = format!("k{tag}{:x}{n:x}", std::process::id());
        #[cfg(windows)]
        {
            PathBuf::from(format!(r"\\.\pipe\{stem}"))
        }
        #[cfg(not(windows))]
        {
            let path = std::env::temp_dir().join(format!("{stem}.sock"));
            let _ = std::fs::remove_file(&path);
            path
        }
    }

    fn scalar_runtime_json(value: serde_json::Value) -> RuntimeJson {
        let scalar = klaus_agent_core::runtime_values::ScalarJson::validate_static_schema(value)
            .expect("test JSON must stay in the scalar domain");
        runtime_codec::encode_value(&scalar).expect("scalar test JSON must encode")
    }

    fn runtime_path_exists(path: &RuntimeString) -> bool {
        Path::new(&path.try_to_utf8().expect("test path must be scalar")).exists()
    }

    #[test]
    fn playwright_env_overlay_matches_rust_193_string_order_for_scalar_keys() {
        let keys = [
            "z",
            "\u{e9}",
            "\u{e000}",
            "\u{10000}",
            "\u{10ffff}",
            "alpha",
        ];
        let legacy = keys
            .iter()
            .map(|key| ((*key).to_owned(), format!("value-{key}")))
            .collect::<BTreeMap<_, _>>();
        let overlay = keys
            .iter()
            .map(|key| ((*key).to_owned(), format!("value-{key}")))
            .collect::<PlaywrightEnvOverlay>();

        let actual = overlay
            .iter()
            .map(|(key, value)| {
                (
                    key.try_to_utf8().expect("scalar key"),
                    value.try_to_utf8().expect("scalar value"),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, legacy.into_iter().collect::<Vec<_>>());
    }

    #[test]
    fn playwright_env_overlay_orders_lone_units_and_overwrites_exact_keys() {
        let lone_high = RuntimeString::from_utf16(vec![0xd800]);
        let lone_low = RuntimeString::from_utf16(vec![0xdc00]);
        let bmp = RuntimeString::from("\u{e000}");
        let astral = RuntimeString::from("\u{10000}");
        let mut overlay = PlaywrightEnvOverlay::default();
        overlay.insert(astral.clone(), RuntimeString::from("astral"));
        overlay.insert(bmp.clone(), RuntimeString::from("old"));
        overlay.insert(lone_low.clone(), RuntimeString::from_utf16(vec![0xdc01]));
        overlay.insert(lone_high.clone(), RuntimeString::from_utf16(vec![0xd801]));

        assert_eq!(
            overlay.insert(bmp.clone(), RuntimeString::from("new")),
            Some(RuntimeString::from("old"))
        );
        assert_eq!(overlay.len(), 4);
        assert_eq!(
            overlay
                .iter()
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>(),
            vec![lone_high, lone_low, bmp, astral]
        );
        assert_eq!(
            overlay.iter().nth(2).map(|(_, value)| value.clone()),
            Some(RuntimeString::from("new"))
        );
    }

    #[test]
    fn playwright_env_overlay_preserves_case_aliases_for_the_target_env_layer() {
        let mut overlay = PlaywrightEnvOverlay::default();
        overlay.insert("Path".to_owned(), "mixed".to_owned());
        overlay.insert("PATH".to_owned(), "upper".to_owned());
        overlay.insert("KLAUS_BROWSER_HEADLESS".to_owned(), "caller-1".to_owned());
        overlay.insert("KLAUS_BROWSER_HEADLESS".to_owned(), "caller-2".to_owned());

        assert_eq!(overlay.len(), 3);
        assert_eq!(
            overlay
                .iter()
                .map(|(key, value)| (
                    key.try_to_utf8().expect("scalar test key"),
                    value.try_to_utf8().expect("scalar test value"),
                ))
                .collect::<Vec<_>>(),
            vec![
                ("KLAUS_BROWSER_HEADLESS".to_owned(), "caller-2".to_owned()),
                ("PATH".to_owned(), "upper".to_owned()),
                ("Path".to_owned(), "mixed".to_owned()),
            ]
        );
    }

    fn bundled_browser_asset_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("assets/klaus-bundled/browser")
    }

    #[test]
    fn bundled_browser_assets_match_their_source_and_hash_contract() {
        let lock = validate_bundled_browser_asset_lock().expect("valid embedded full-tree lock");
        let vendored_root = bundled_browser_asset_root();
        let contract = std::fs::read_to_string(vendored_root.join("ASSET-SOURCES.sha256"))
            .expect("vendored browser assets must carry their source/hash contract");
        let entries = contract
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(|line| {
                let (digest, relative_path) = line
                    .split_once("  ")
                    .expect("asset contract entries use sha256sum format");
                (relative_path, digest)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            entries
                .iter()
                .map(|(relative_path, _)| *relative_path)
                .collect::<Vec<_>>(),
            lock.files
                .iter()
                .map(|asset| asset.path.as_str())
                .collect::<Vec<_>>(),
            "asset contract must cover the complete locked tree"
        );
        assert_eq!(entries.len(), BUNDLED_BROWSER_ASSETS.len());

        let source_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("plugins/klaus-bundled/browser");
        for ((relative_path, expected_digest), embedded) in
            entries.into_iter().zip(BUNDLED_BROWSER_ASSETS)
        {
            let vendored =
                std::fs::read(vendored_root.join(relative_path)).unwrap_or_else(|error| {
                    panic!("missing vendored browser asset {relative_path}: {error}")
                });
            let actual_digest = format!("{:x}", Sha256::digest(&vendored));
            assert_eq!(
                actual_digest, expected_digest,
                "vendored browser asset hash drifted: {relative_path}"
            );
            assert_eq!(vendored, embedded.bytes, "embedded browser asset drifted");

            if source_root.is_dir() {
                let source =
                    std::fs::read(source_root.join(relative_path)).unwrap_or_else(|error| {
                        panic!("missing repository browser source {relative_path}: {error}")
                    });
                assert_eq!(
                    vendored, source,
                    "vendored browser asset differs from repository source: {relative_path}"
                );
            }
        }
    }

    #[test]
    fn production_bundled_asset_cache_root_skips_a_presquatted_candidate() {
        let temp_root = unique_temp_dir("browser-assets-presquat-root");
        let first_suffix = "00000000000000000000000000000001";
        let second_suffix = "00000000000000000000000000000002";
        let squatted = temp_root.join(format!("klaus-agent-sdk-{first_suffix}"));
        fs::create_dir(&squatted).unwrap();
        fs::write(
            squatted.join("sentinel"),
            b"do not mutate the occupied name\n",
        )
        .unwrap();
        let mut suffixes = [first_suffix, second_suffix].into_iter();

        let cache_root = allocate_bundled_sidecar_cache_root_in(
            &temp_root,
            &std::env::current_dir().unwrap(),
            || {
                suffixes.next().map(str::to_owned).ok_or_else(|| {
                    std::io::Error::other("deterministic cache suffixes were exhausted")
                })
            },
        )
        .unwrap();

        assert_eq!(
            cache_root,
            // Simplified, matching what the allocator returns: a verbatim `\\?\` root would
            // propagate into the sidecar path node is asked to run, which node cannot resolve.
            dunce::canonicalize(&temp_root)
                .unwrap()
                .join(format!("klaus-agent-sdk-{second_suffix}"))
        );
        assert_eq!(
            fs::read(squatted.join("sentinel")).unwrap(),
            b"do not mutate the occupied name\n"
        );
        fs::remove_dir_all(temp_root).unwrap();
    }

    #[test]
    fn relative_tmpdir_is_frozen_to_an_absolute_cache_root_before_child_cwd() {
        let root = unique_temp_dir("browser-assets-relative-tmp");
        let resolution_cwd = root.join("resolution-cwd");
        let child_cwd = root.join("different-child-cwd");
        let relative_temp = Path::new("relative-tmp");
        fs::create_dir_all(resolution_cwd.join(relative_temp)).unwrap();
        fs::create_dir(&child_cwd).unwrap();

        let cache_root =
            allocate_bundled_sidecar_cache_root_in(relative_temp, &resolution_cwd, || {
                Ok("00000000000000000000000000000003".to_owned())
            })
            .unwrap();
        let canonical_temp_root = dunce::canonicalize(resolution_cwd.join(relative_temp)).unwrap();
        let browser_cache = cache_root.join("browser-runtime");
        ensure_private_cache_directory(&browser_cache).unwrap();
        let sidecar = write_bundled_sidecar_in(&browser_cache.join("test-version")).unwrap();

        assert!(cache_root.is_absolute());
        assert!(sidecar.is_absolute());
        assert!(sidecar.is_file());
        assert!(sidecar.starts_with(&cache_root));
        assert_eq!(cache_root.parent(), Some(canonical_temp_root.as_path()));
        assert_eq!(child_cwd.join(&sidecar), sidecar);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn bundled_asset_cache_rejects_symlink_without_touching_victim() {
        use std::os::unix::fs::symlink;

        let root = unique_temp_dir("browser-assets-symlink-cache");
        let cache = root.join("cache");
        let sidecar = write_bundled_sidecar_in(&cache).expect("initial canonical publication");
        let published = sidecar
            .parent()
            .and_then(Path::parent)
            .expect("sidecar is nested under the published tree");
        let target = published.join("docs/tab-cleanup.md");
        let victim = root.join("victim.txt");
        fs::write(&victim, b"victim must remain unchanged\n").unwrap();
        fs::remove_file(&target).unwrap();
        symlink(&victim, &target).unwrap();

        let error = write_bundled_sidecar_in(&cache)
            .expect_err("a symlinked cached asset must fail closed");
        assert!(
            error.message.contains("symlink") || error.message.contains("no-follow"),
            "unexpected error: {:?}",
            error.message
        );
        assert_eq!(
            fs::read(&victim).unwrap(),
            b"victim must remain unchanged\n"
        );
        assert!(
            fs::symlink_metadata(&target)
                .unwrap()
                .file_type()
                .is_symlink(),
            "validation must not replace or follow the cached symlink"
        );
        fs::remove_dir_all(root).unwrap();

        let root = unique_temp_dir("browser-assets-symlink-parent");
        let cache = root.join("cache");
        let sidecar = write_bundled_sidecar_in(&cache).expect("initial canonical publication");
        let published = sidecar
            .parent()
            .and_then(Path::parent)
            .expect("sidecar is nested under the published tree");
        let docs = published.join("docs");
        let victim_directory = root.join("victim-directory");
        fs::create_dir(&victim_directory).unwrap();
        fs::write(victim_directory.join("sentinel"), b"parent victim\n").unwrap();
        fs::remove_dir_all(&docs).unwrap();
        symlink(&victim_directory, &docs).unwrap();

        let error = write_bundled_sidecar_in(&cache)
            .expect_err("a symlinked cached parent must fail closed");
        assert!(
            error.message.contains("symlink"),
            "unexpected error: {:?}",
            error.message
        );
        assert_eq!(
            fs::read(victim_directory.join("sentinel")).unwrap(),
            b"parent victim\n"
        );
        assert!(fs::symlink_metadata(&docs)
            .unwrap()
            .file_type()
            .is_symlink());
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn bundled_asset_cache_is_current_user_only_on_windows() {
        let root = unique_temp_dir("browser-assets-private-dacl");
        let cache = root.join("cache");
        ensure_private_cache_directory(&cache).unwrap();

        let cache_dir = secure_asset_fs::Dir::open_existing_private_path_for_test(&cache).unwrap();
        cache_dir.verify_private().unwrap();

        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn bundled_asset_windows_buffers_cover_every_carried_alignment() {
        let (allocation_alignment, address, carried_alignments) =
            secure_asset_fs::aligned_buffer_facts_for_test();
        assert!(allocation_alignment >= 8);
        assert_eq!(address % allocation_alignment, 0);
        assert!(carried_alignments
            .into_iter()
            .all(|required| allocation_alignment >= required));
    }

    #[test]
    fn bundled_asset_cache_publishes_once_concurrently_and_reuses_canonical_tree() {
        let root = unique_temp_dir("browser-assets-concurrent-cache");
        let cache = root.join("cache");
        let workers = 8;
        let barrier = Arc::new(Barrier::new(workers));
        let handles = (0..workers)
            .map(|_| {
                let cache = cache.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    write_bundled_sidecar_in(&cache).expect("concurrent publication")
                })
            })
            .collect::<Vec<_>>();
        let published_paths = handles
            .into_iter()
            .map(|handle| handle.join().expect("publication worker did not panic"))
            .collect::<Vec<_>>();
        assert!(
            published_paths
                .iter()
                .all(|path| path == &published_paths[0]),
            "every materializer must resolve to the same content-addressed tree"
        );

        let sidecar = &published_paths[0];
        let before = fs::read(sidecar).unwrap();
        let reused = write_bundled_sidecar_in(&cache).expect("reuse canonical cache");
        assert_eq!(&reused, sidecar);
        assert_eq!(fs::read(&reused).unwrap(), before);
        let lock = validate_bundled_browser_asset_lock().unwrap();
        let cache_entries = fs::read_dir(&cache)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert_eq!(cache_entries.len(), 1, "temporary siblings must be removed");
        assert_eq!(
            cache_entries[0],
            std::ffi::OsStr::new(&expected_published_tree_name(&lock.tree_sha256))
        );
        let published_dir = secure_asset_fs::Dir::open_or_create_private_path(
            sidecar
                .parent()
                .and_then(Path::parent)
                .expect("sidecar is nested under the published tree"),
        )
        .unwrap();
        verify_bundled_asset_tree(&published_dir).unwrap();

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn bundled_asset_cache_never_replaces_a_racing_final_entry() {
        let root = unique_temp_dir("browser-assets-racing-final");
        let cache = root.join("cache");
        let lock = validate_bundled_browser_asset_lock().unwrap();
        let final_path = cache.join(expected_published_tree_name(&lock.tree_sha256));
        let expected_final = final_path.clone();
        let planted = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let planted_for_hook = planted.clone();
        *BUNDLED_ASSET_BEFORE_PUBLISH_HOOK
            .lock()
            .expect("publish test hook lock") = Some(Box::new(move |path| {
            if path == expected_final && !planted_for_hook.swap(true, Ordering::SeqCst) {
                fs::create_dir(path).expect("plant empty final directory in publish window");
            }
        }));

        let result = write_bundled_sidecar_in(&cache);
        *BUNDLED_ASSET_BEFORE_PUBLISH_HOOK
            .lock()
            .expect("publish test hook lock") = None;
        let final_entries = fs::read_dir(&final_path)
            .map(|entries| entries.count())
            .unwrap_or_default();
        fs::remove_dir_all(&root).unwrap();

        assert!(planted.load(Ordering::SeqCst), "publish hook did not run");
        assert!(result.is_err(), "a racing final entry must win by name");
        assert_eq!(
            final_entries, 0,
            "no-replace publication must leave existing final pollution untouched"
        );
    }

    #[cfg(unix)]
    #[test]
    fn bundled_asset_parent_swap_cannot_escape_the_pending_tree() {
        use std::os::unix::fs::symlink;

        let root = unique_temp_dir("browser-assets-parent-swap");
        let cache = root.join("cache");
        let victim = root.join("victim");
        fs::create_dir(&victim).unwrap();
        let victim_for_hook = victim.clone();
        let cache_for_hook = fs::canonicalize(&root).unwrap().join("cache");
        let swapped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let swapped_for_hook = swapped.clone();
        *BUNDLED_ASSET_BEFORE_FILE_CREATE_HOOK
            .lock()
            .expect("file-create test hook lock") = Some(Box::new(move |path| {
            if path.starts_with(&cache_for_hook)
                && path.ends_with(".codex-plugin/plugin.json")
                && !swapped_for_hook.swap(true, Ordering::SeqCst)
            {
                let parent = path.parent().expect("asset parent");
                fs::remove_dir(parent).expect("remove checked parent");
                symlink(&victim_for_hook, parent).expect("swap checked parent for symlink");
            }
        }));

        let result = write_bundled_sidecar_in(&cache);
        *BUNDLED_ASSET_BEFORE_FILE_CREATE_HOOK
            .lock()
            .expect("file-create test hook lock") = None;
        let victim_entries = fs::read_dir(&victim).unwrap().count();
        fs::remove_dir_all(&root).unwrap();

        assert!(
            swapped.load(Ordering::SeqCst),
            "parent-swap hook did not run"
        );
        assert!(result.is_err(), "a swapped parent must fail closed");
        assert_eq!(
            victim_entries, 0,
            "asset creation must be relative to the checked directory handle"
        );
    }

    #[test]
    fn pending_tree_creation_failure_cleans_the_created_entry() {
        let root = unique_temp_dir("browser-assets-pending-create-failure");
        let cache = root.join("cache");
        ensure_private_cache_directory(&cache).unwrap();
        let cache_dir = secure_asset_fs::Dir::open_or_create_private_path(&cache).unwrap();
        *BUNDLED_ASSET_FAIL_AFTER_PENDING_CREATE
            .lock()
            .expect("pending-create test hook lock") = Some(cache.clone());

        let error = match PendingBundledAssetTree::create(&cache_dir, &cache, "injected-tree") {
            Ok(_) => panic!("injected post-create failure must propagate"),
            Err(error) => error,
        };
        let leftovers = fs::read_dir(&cache).unwrap().count();
        fs::remove_dir_all(&root).unwrap();

        assert!(error.message.contains("injected failure"));
        assert_eq!(leftovers, 0, "failed pending creation must not leak a tree");
    }

    /// The published directory name these cases expect, which on Windows also carries the
    /// broker addon's digest because the addon is not covered by the committed tree hash.
    fn expected_published_tree_name(tree_sha256: &str) -> String {
        #[cfg(windows)]
        {
            let (_, addon) =
                windows_broker_addon_payload().expect("the broker addon must be built");
            published_bundled_asset_tree_name(tree_sha256, Some(&addon))
        }
        #[cfg(not(windows))]
        published_bundled_asset_tree_name(tree_sha256, None)
    }

    /// KBU1 package-load handshake preamble shared by every scripted fake
    /// sidecar on unix: consume the permit on fd 6 and acknowledge package readiness
    /// on fd 5, exactly like the bundled brokered sidecar.
    #[cfg(not(windows))]
    const FAKE_SIDECAR_KBU1_PREAMBLE: &str = r#"
import fs from 'node:fs';

{
  const permit = Buffer.allocUnsafe(48);
  let offset = 0;
  while (offset < permit.length) {
    const count = fs.readSync(6, permit, offset, permit.length - offset, null);
    if (count === 0) process.exit(70);
    offset += count;
  }
  const ready = Buffer.from(permit);
  ready[4] = 2;
  ready[5] = 2;
  ready[6] = 3;
  ready[7] = 0;
  offset = 0;
  while (offset < ready.length) {
    offset += fs.writeSync(5, ready, offset, ready.length - offset);
  }
}
"#;

    /// The Windows counterpart of the KBU1 preamble.
    ///
    /// There are no inherited descriptors here: the client half of the broker handshake
    /// (hello, adoption, commit, package-ready) lives in the sealed Node addon, reached over
    /// named pipes named by the bootstrap the host put in the environment. So a fake sidecar
    /// loads the addon and marks readiness, which is exactly what the bundled sidecar does.
    #[cfg(windows)]
    const FAKE_SIDECAR_KBU1_PREAMBLE: &str = r#"
import { createRequire } from 'node:module';

{
  const ENV_ARG = '--klaus-browser-broker-env-key=';
  const switches = process.argv.slice(2).filter((argument) => argument.startsWith(ENV_ARG));
  if (switches.length !== 1) process.exit(70);
  const key = switches[0].slice(ENV_ARG.length);
  const bootstrap = process.env[key];
  if (typeof bootstrap !== 'string') process.exit(70);
  delete process.env[key];
  process.stderr.write('fixture: opening broker pipes\n');
  const addon = createRequire(import.meta.url)('./klaus_browser_pipe.node');
  const pipes = addon.openWindowsBrokerPipesV1(bootstrap);
  process.stderr.write('fixture: pipes open, starting mux\n');
  // `startMux` before `markPackageReady`, in that order: the addon refuses readiness until
  // the mux lane is running, exactly as the bundled sidecar sequences it. A fake sidecar
  // answers requests on stdout, so the mux bytes it receives are only drained, never used.
  pipes.startMux(Object.freeze({
    bytes: () => {},
    close: () => {},
  }));
  process.stderr.write('fixture: mux started, marking package ready\n');
  await new Promise((resolve, reject) => {
    pipes.markPackageReady((error) => (error ? reject(error) : resolve()));
  });
  process.stderr.write('fixture: package ready, serving requests\n');
}
"#;

    /// Copy the Windows broker addon next to a fixture sidecar script.
    ///
    /// The sidecar loads it from its own directory, and a fixture script lives in a temp
    /// directory rather than in the published asset tree the host stages, so the fixture has
    /// to provide it.
    #[cfg(windows)]
    fn stage_fake_sidecar_addon(dir: &std::path::Path) {
        let source = windows_broker_addon_source()
            .expect("the Windows broker addon must be built for the sidecar fixtures");
        std::fs::copy(&source, dir.join(WINDOWS_BROKER_ADDON_NAME))
            .expect("staging the Windows broker addon beside a fixture sidecar must succeed");
    }

    #[cfg(not(windows))]
    fn stage_fake_sidecar_addon(_dir: &std::path::Path) {}

    /// Write a fake sidecar script: the platform's broker preamble plus `body`, with whatever
    /// the preamble needs staged beside it.
    fn write_fake_sidecar(script: &std::path::Path, body: &str) {
        std::fs::write(script, format!("{FAKE_SIDECAR_KBU1_PREAMBLE}{body}")).unwrap();
        stage_fake_sidecar_addon(
            script
                .parent()
                .expect("a fixture sidecar path has a directory"),
        );
    }

    /// Write the test-host `klaus:browser-host-transport` fixture: a module
    /// loader hook plus a transport implementation that spawns the given
    /// sidecar with the four Unix broker lanes and completes the KBU1
    /// package-load handshake, mirroring the Rust host contract. Returns the
    /// loader path a smoke script must `module.register`.
    ///
    /// Unix-only along with its callers: it inherits the four extra descriptors the unix lane
    /// contract uses, and Windows has no equivalent to inherit.
    #[cfg(not(windows))]
    fn write_klaus_host_transport(
        dir: &std::path::Path,
        sidecar_path: &std::path::Path,
        sidecar_cwd: &std::path::Path,
    ) -> PathBuf {
        let docs = include_str!("../assets/klaus-bundled/browser/docs/playwright.md");
        let transport_path = dir.join("klaus-browser-host-transport.mjs");
        let sidecar_json =
            serde_json::to_string(&sidecar_path.to_string_lossy().into_owned()).unwrap();
        let cwd_json = serde_json::to_string(&sidecar_cwd.to_string_lossy().into_owned()).unwrap();
        let docs_json = serde_json::to_string(docs).unwrap();
        std::fs::write(
            &transport_path,
            format!(
                r#"import {{ spawn }} from 'node:child_process';
import {{ randomBytes }} from 'node:crypto';

const SIDECAR_PATH = {sidecar_json};
const SIDECAR_CWD = {cwd_json};

export function isBrowserRuntimeProxyV1(_value) {{
  return false;
}}

export const browserHostDefaults = Object.freeze({{ backend: 'launch' }});

export const browserDocumentationText = {docs_json};

export function assertBrowserHostTransportV1(transport) {{
  if (
    transport === null ||
    typeof transport !== 'object' ||
    typeof transport.open !== 'function' ||
    typeof transport.send !== 'function' ||
    typeof transport.close !== 'function'
  ) {{
    const error = new Error('Rust browser host transport is required');
    error.code = 'host_error';
    throw error;
  }}
}}

export function acquireBrowserHostTransportV1() {{
  let child = null;
  let buffered = Buffer.alloc(0);
  return {{
    open(configuration, handlers) {{
      const env = {{ ...process.env }};
      env.KLAUS_BROWSER_HEADLESS = configuration.headless === false ? '0' : '1';
      if (configuration.artifactDir !== undefined) {{
        env.KLAUS_BROWSER_ARTIFACT_DIR = configuration.artifactDir;
      }}
      child = spawn(process.execPath, [SIDECAR_PATH], {{
        cwd: SIDECAR_CWD,
        env,
        stdio: ['pipe', 'pipe', 'inherit', 'pipe', 'pipe', 'pipe', 'pipe'],
      }});
      // KBU1 package-load permit on the child's fd 6; the ready record and
      // later mux frames arrive on fd 5 (consumed, not re-framed here).
      const permit = Buffer.alloc(48);
      permit.write('KBU1', 0, 'ascii');
      permit[4] = 1;
      permit[5] = 3;
      permit[6] = 2;
      permit[7] = 0;
      permit.writeBigUInt64BE(1n, 8);
      randomBytes(32).copy(permit, 16);
      child.stdio[6].write(permit);
      child.stdio[5].on('data', () => {{}});
      child.stdio[3].on('data', () => {{}});
      child.stdout.on('data', (chunk) => {{
        buffered = Buffer.concat([buffered, chunk]);
        let newline;
        while ((newline = buffered.indexOf(10)) !== -1) {{
          const line = buffered.subarray(0, newline);
          buffered = buffered.subarray(newline + 1);
          handlers.message(new Uint8Array(line));
        }}
      }});
      child.on('exit', () => handlers.close(null));
    }},
    send(bytes, done) {{
      const framed = Buffer.concat([Buffer.from(bytes), Buffer.from([10])]);
      child.stdin.write(framed, done);
    }},
    close() {{
      if (child) {{
        // Close every lane first: the sidecar's blocking mux read on fd 6
        // only returns on EOF, and its exit path joins that thread.
        for (const stream of child.stdio) {{
          try {{
            stream?.destroy?.();
          }} catch {{}}
        }}
        child.kill('SIGTERM');
        child = null;
      }}
    }},
  }};
}}
"#
            ),
        )
        .unwrap();
        let loader_path = dir.join("klaus-transport-loader.mjs");
        let transport_json =
            serde_json::to_string(&transport_path.to_string_lossy().into_owned()).unwrap();
        std::fs::write(
            &loader_path,
            format!(
                r#"import {{ pathToFileURL }} from 'node:url';

const TRANSPORT_URL = pathToFileURL({transport_json}).href;

export function resolve(specifier, context, next) {{
  if (specifier === 'klaus:browser-host-transport') {{
    return {{ shortCircuit: true, url: TRANSPORT_URL }};
  }}
  return next(specifier, context);
}}
"#
            ),
        )
        .unwrap();
        loader_path
    }

    fn fake_browser_sidecar(dir: &std::path::Path) -> PathBuf {
        let script = dir.join("fake-browser-sidecar.mjs");
        let body = r#"
import readline from 'node:readline';

const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });

rl.on('line', (line) => {
  const msg = JSON.parse(line);
  const op = msg.request.operation || {};
  const value = valueForOperation(op);
  process.stdout.write(JSON.stringify({
    id: msg.id,
    event: {
      type: 'navigation_started',
      url: op.url,
      sessionId: msg.request.target.sessionId ?? 'session-1',
      tabId: msg.request.target.tabId ?? 'tab-1'
    }
  }) + '\n');
  process.stdout.write(JSON.stringify({
    id: msg.id,
    ok: true,
    response: {
      sessionId: 'session-1',
      tabId: 'tab-1',
      url: op.url || 'https://client.test/',
      title: 'Fake Browser',
      text: 'loaded by fake sidecar',
      value,
      events: [{ type: 'status', message: 'sidecar completed request' }]
    }
  }) + '\n');
});

function valueForOperation(op) {
  if (op.type === 'playwright_wait_for_event' && op.event === 'download') {
    return { event: 'download', path: '/tmp/fake-download.txt', suggestedFilename: 'fake-download.txt', url: 'https://client.test/download.txt' };
  }
  if (op.type === 'playwright_wait_for_event' && op.event === 'filechooser') {
    return { event: 'filechooser', chooserId: 'fake-filechooser', isMultiple: false };
  }
  if (op.type === 'playwright_file_chooser_set_files') {
    return { chooserId: op.chooserId, count: op.files?.length || 0 };
  }
  if (op.type === 'locator') {
    switch (op.action) {
      case 'count':
        return 1;
      case 'inner_text':
      case 'text_content':
        return 'Go';
      case 'get_attribute':
        return op.value === 'id' ? 'go' : null;
      case 'is_enabled':
      case 'is_visible':
        return true;
      default:
        return { action: op.action || null };
    }
  }
  if (op.type === 'playwright_dom_snapshot') {
    return { text: '<html><body>Fake DOM</body></html>' };
  }
  if (op.type === 'playwright_element_info') {
    return [{ selector: '#go', tagName: 'button', text: 'Go', visible: true }];
  }
  if (op.type === 'playwright_element_screenshot') {
    return { id: 'fake-element-screenshot', kind: 'screenshot', path: '/tmp/fake-element.png' };
  }
  return undefined;
}
"#;
        write_fake_sidecar(&script, body);
        script
    }

    fn error_browser_sidecar(dir: &std::path::Path) -> PathBuf {
        let script = dir.join("error-browser-sidecar.mjs");
        let body = r#"
import readline from 'node:readline';

const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
rl.on('line', (line) => {
  const msg = JSON.parse(line);
  process.stdout.write(JSON.stringify({
    id: msg.id,
    ok: false,
    error: {
      code: 'navigation_failed',
      message: 'navigation failed in fake sidecar',
      details: { url: msg.request.operation.url }
    }
  }) + '\n');
});
"#;
        write_fake_sidecar(&script, body);
        script
    }

    fn timeout_then_recover_browser_sidecar(dir: &std::path::Path) -> PathBuf {
        let script = dir.join("timeout-recover-browser-sidecar.mjs");
        let body = r#"
import readline from 'node:readline';

const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
rl.on('line', (line) => {
  const msg = JSON.parse(line);
  if (msg.request.metadata?.hang === true) {
    return;
  }
  process.stdout.write(JSON.stringify({
    id: msg.id,
    ok: true,
    response: {
      sessionId: 'session-after-restart',
      tabId: 'tab-after-restart',
      url: msg.request.operation.url,
      title: 'Recovered'
    }
  }) + '\n');
});
"#;
        write_fake_sidecar(&script, body);
        script
    }

    fn free_tcp_port() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    }

    /// Unix-only, and not for want of the broker addon: this case stands the HOST side of the
    /// broker up from Node, spawning the sidecar with the four extra descriptors the unix lane
    /// contract inherits. Windows has no descriptor inheritance for those lanes — its host side
    /// is the Rust broker's named pipes — so a Node fixture cannot play it. The Windows lane is
    /// covered by the cases that go through the real Rust host.
    #[cfg(not(windows))]
    #[test]
    fn extension_websocket_bridge_requires_token_and_extension_origin() {
        let Some(node) = node_fixture_runtime() else {
            return;
        };
        let dir = unique_temp_dir("browser-extension-bridge-auth");
        let port = free_tcp_port();
        let sidecar_path = write_bundled_sidecar_in(&dir.join("cache")).unwrap();
        materialize_test_pinned_runtime(&sidecar_path).unwrap();
        let script_path = dir.join("extension-bridge-auth-smoke.mjs");
        std::fs::write(
            &script_path,
            format!(
                r#"
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import {{ spawn }} from 'node:child_process';
import net from 'node:net';

const sidecarPath = {sidecar_path};
const port = {port};
const token = 'test-extension-bridge-token';
const extensionOrigin = 'chrome-extension://gjamccelebgpogpbiceajehcajghphnb';
const child = spawn(process.execPath, [sidecarPath], {{
  env: {{
    ...process.env,
    KLAUS_BROWSER_BACKEND: 'extension',
    KLAUS_BROWSER_DISABLE_NATIVE_BRIDGE: '1',
    KLAUS_BROWSER_EXTENSION_BRIDGE_PORT: String(port),
    KLAUS_BROWSER_EXTENSION_BRIDGE_TOKEN: token,
    KLAUS_BROWSER_EXTENSION_CONNECT_TIMEOUT_MS: '100',
  }},
  stdio: ['pipe', 'pipe', 'pipe', 'pipe', 'pipe', 'pipe', 'pipe'],
}});
// KBU1 package-load permit on the sidecar's fd 6; consume the ready record
// and any mux frames from fd 5.
{{
  const permit = Buffer.alloc(48);
  permit.write('KBU1', 0, 'ascii');
  permit[4] = 1;
  permit[5] = 3;
  permit[6] = 2;
  permit[7] = 0;
  permit.writeBigUInt64BE(1n, 8);
  crypto.randomBytes(32).copy(permit, 16);
  child.stdio[6].write(permit);
  child.stdio[5].on('data', () => {{}});
  child.stdio[3].on('data', () => {{}});
}}
child.stdin.write(`${{JSON.stringify({{
  id: 'start-bridge',
  request: {{ operation: {{ type: 'tabs', action: 'list' }}, target: {{}}, timeoutMs: 100 }},
}})}}\n`);

try {{
  const noToken = await upgrade('/klaus-browser', extensionOrigin);
  assert.notEqual(noToken, 101, 'bridge accepted a connection without a token');

  const badOrigin = await upgrade(`/klaus-browser?token=${{encodeURIComponent(token)}}`, 'http://localhost:3000');
  assert.notEqual(badOrigin, 101, 'bridge accepted an untrusted page origin');

  const authorized = await upgrade(`/klaus-browser?token=${{encodeURIComponent(token)}}`, extensionOrigin);
  assert.equal(authorized, 101, 'bridge rejected the authenticated bundled extension');
}} finally {{
  child.kill('SIGKILL');
}}

async function upgrade(requestPath, origin) {{
  const deadline = Date.now() + 3000;
  while (true) {{
    try {{
      return await connectOnce(requestPath, origin);
    }} catch (error) {{
      if (Date.now() >= deadline) throw error;
      await new Promise((resolve) => setTimeout(resolve, 25));
    }}
  }}
}}

function connectOnce(requestPath, origin) {{
  return new Promise((resolve, reject) => {{
    const socket = net.connect({{ host: '127.0.0.1', port }});
    let settled = false;
    let data = '';
    socket.once('connect', () => {{
      const key = crypto.randomBytes(16).toString('base64');
      socket.write([
        `GET ${{requestPath}} HTTP/1.1`,
        `Host: 127.0.0.1:${{port}}`,
        `Origin: ${{origin}}`,
        'Upgrade: websocket',
        'Connection: Upgrade',
        `Sec-WebSocket-Key: ${{key}}`,
        'Sec-WebSocket-Version: 13',
        '',
        '',
      ].join('\r\n'));
    }});
    socket.on('data', (chunk) => {{
      data += chunk.toString('utf8');
      if (!data.includes('\r\n\r\n')) return;
      const match = /^HTTP\/1\.1 (\d+)/.exec(data);
      finish(Number(match?.[1] || 0));
    }});
    socket.once('close', () => finish(0));
    socket.once('error', (error) => {{
      if (!settled) reject(error);
    }});
    function finish(status) {{
      if (settled) return;
      settled = true;
      socket.destroy();
      resolve(status);
    }}
  }});
}}
"#,
                sidecar_path =
                    serde_json::to_string(&sidecar_path.to_string_lossy().into_owned()).unwrap(),
            ),
        )
        .unwrap();

        let output = std::process::Command::new(&node)
            .arg(&script_path)
            .output()
            .expect("node should run extension bridge auth smoke test");
        assert!(
            output.status.success(),
            "extension bridge auth smoke failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn fake_chrome_extension_client(dir: &std::path::Path) -> PathBuf {
        let script = dir.join("fake-chrome-extension-client.mjs");
        std::fs::write(
            &script,
            r#"
import crypto from 'node:crypto';
import fs from 'node:fs';
import net from 'node:net';

const port = Number(process.argv[2]);
const logPath = process.argv[3];
const token = process.argv[4];
const origin = process.argv[5];
let socket = null;
let buffer = Buffer.alloc(0);
let handshaken = false;
let retryTimer = null;

connect();

function connect() {
  socket = net.connect({ host: '127.0.0.1', port }, () => {
    const key = crypto.randomBytes(16).toString('base64');
    socket.write([
      `GET /klaus-browser?token=${encodeURIComponent(token)} HTTP/1.1`,
      'Host: 127.0.0.1',
      `Origin: ${origin}`,
      'Upgrade: websocket',
      'Connection: Upgrade',
      `Sec-WebSocket-Key: ${key}`,
      'Sec-WebSocket-Version: 13',
      '',
      ''
    ].join('\r\n'));
  });
  socket.on('data', onData);
  socket.on('error', scheduleRetry);
  socket.on('close', () => {
    if (!handshaken) scheduleRetry();
  });
}

function scheduleRetry() {
  if (handshaken || retryTimer) return;
  retryTimer = setTimeout(() => {
    retryTimer = null;
    connect();
  }, 25);
}

function onData(chunk) {
  buffer = Buffer.concat([buffer, chunk]);
  if (!handshaken) {
    const end = buffer.indexOf('\r\n\r\n');
    if (end === -1) return;
    const header = buffer.subarray(0, end).toString('utf8');
    if (!header.includes('101 Switching Protocols')) process.exit(2);
    buffer = buffer.subarray(end + 4);
    handshaken = true;
    send({
      type: 'hello',
      deviceId: 'fake-chrome-device',
      name: 'Fake Chrome',
      platform: 'test',
      version: '0.0.0-test',
      capabilities: ['tabs', 'read_page', 'computer']
    });
  }
  drainFrames();
}

function drainFrames() {
  while (true) {
    const parsed = readFrame(buffer);
    if (!parsed) return;
    buffer = buffer.subarray(parsed.consumed);
    if (parsed.opcode !== 0x1) continue;
    const message = JSON.parse(parsed.payload.toString('utf8'));
    if (message.type !== 'call') continue;
    fs.writeFileSync(logPath, JSON.stringify({
      operation: message.request.operation.type,
      action: message.request.operation.action,
      target: message.request.target
    }));
    send({
      type: 'response',
      id: message.id,
      ok: true,
      response: {
        sessionId: 'fake-chrome-device',
        tabId: 'chrome-11',
        value: [{
          id: 'chrome-11',
          type: 'page',
          title: 'Inbox',
          url: 'https://mail.test/',
          selected: true
        }],
        text: 'Fake Chrome tabs'
      }
    });
    setTimeout(() => process.exit(0), 10);
  }
}

function readFrame(data) {
  if (data.length < 2) return null;
  const opcode = data[0] & 0x0f;
  let length = data[1] & 0x7f;
  let offset = 2;
  if (length === 126) {
    if (data.length < offset + 2) return null;
    length = data.readUInt16BE(offset);
    offset += 2;
  } else if (length === 127) {
    if (data.length < offset + 8) return null;
    length = data.readUInt32BE(offset) * 2 ** 32 + data.readUInt32BE(offset + 4);
    offset += 8;
  }
  const masked = (data[1] & 0x80) !== 0;
  let mask = null;
  if (masked) {
    if (data.length < offset + 4) return null;
    mask = data.subarray(offset, offset + 4);
    offset += 4;
  }
  if (data.length < offset + length) return null;
  const payload = Buffer.from(data.subarray(offset, offset + length));
  if (mask) {
    for (let i = 0; i < payload.length; i += 1) {
      payload[i] ^= mask[i % 4];
    }
  }
  return { opcode, payload, consumed: offset + length };
}

function send(message) {
  const payload = Buffer.from(JSON.stringify(message), 'utf8');
  socket.write(maskedFrame(payload));
}

function maskedFrame(payload) {
  const mask = crypto.randomBytes(4);
  let header;
  if (payload.length < 126) {
    header = Buffer.from([0x81, 0x80 | payload.length]);
  } else if (payload.length < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x81;
    header[1] = 0x80 | 126;
    header.writeUInt16BE(payload.length, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x81;
    header[1] = 0x80 | 127;
    header.writeUInt32BE(Math.floor(payload.length / 2 ** 32), 2);
    header.writeUInt32BE(payload.length >>> 0, 6);
  }
  const encoded = Buffer.from(payload);
  for (let i = 0; i < encoded.length; i += 1) {
    encoded[i] ^= mask[i % 4];
  }
  return Buffer.concat([header, mask, encoded]);
}
"#,
        )
        .unwrap();
        script
    }

    fn fake_chrome_native_socket_client(dir: &std::path::Path) -> PathBuf {
        let script = dir.join("fake-chrome-native-socket-client.mjs");
        std::fs::write(
            &script,
            r#"
import fs from 'node:fs';
import net from 'node:net';

const socketPath = process.argv[2];
const logPath = process.argv[3];
let socket = null;
let buffer = '';
let retryTimer = null;
let connected = false;

connect();

function connect() {
  socket = net.connect({ path: socketPath }, () => {
    connected = true;
    send({
      type: 'hello',
      deviceId: 'native-socket-device',
      name: 'Native Socket Chrome',
      platform: 'test',
      version: '0.0.0-test',
      capabilities: ['tabs', 'read_page', 'computer', 'console', 'network']
    });
  });
  socket.on('data', onData);
  socket.on('error', scheduleRetry);
  socket.on('close', () => {
    if (!connected) scheduleRetry();
  });
}

function scheduleRetry() {
  if (connected || retryTimer) return;
  retryTimer = setTimeout(() => {
    retryTimer = null;
    connect();
  }, 25);
}

function onData(chunk) {
  buffer += chunk.toString('utf8');
  while (buffer.includes('\n')) {
    const index = buffer.indexOf('\n');
    const raw = buffer.slice(0, index);
    buffer = buffer.slice(index + 1);
    if (!raw.trim()) continue;
    const message = JSON.parse(raw);
    if (message.type !== 'call') continue;
    fs.writeFileSync(logPath, JSON.stringify({
      operation: message.request.operation.type,
      action: message.request.operation.action,
      target: message.request.target
    }));
    send({
      type: 'response',
      id: message.id,
      ok: true,
      response: {
        sessionId: 'native-socket-device',
        tabId: 'chrome-22',
        value: [{
          id: 'chrome-22',
          type: 'page',
          title: 'Native Inbox',
          url: 'https://native.test/',
          selected: true
        }],
        text: 'Native Socket Chrome tabs'
      }
    });
    setTimeout(() => process.exit(0), 10);
  }
}

function send(message) {
  socket.write(`${JSON.stringify(message)}\n`);
}
"#,
        )
        .unwrap();
        script
    }

    fn write_fake_playwright_project(dir: &std::path::Path) {
        let module_dir = dir.join("node_modules/playwright");
        std::fs::create_dir_all(&module_dir).unwrap();
        std::fs::write(
            dir.join("package.json"),
            r#"{"type":"commonjs","dependencies":{"playwright":"0.0.0-fake"}}"#,
        )
        .unwrap();
        std::fs::write(module_dir.join("package.json"), r#"{"main":"index.js"}"#).unwrap();
        std::fs::write(
            module_dir.join("index.js"),
            r#"
const fs = require('node:fs');

class FakeLocator {
  constructor(page, selector) {
    this.page = page;
    this.selector = selector;
  }
  first() {
    return this;
  }
  nth() {
    return this;
  }
  last() {
    return this;
  }
  filter() {
    return this;
  }
  and() {
    return this;
  }
  or() {
    return this;
  }
  async waitFor() {}
  async evaluate(fn) {
    const tagName = this.selector === '#name' ? 'INPUT' : 'BUTTON';
    return fn({
      tagName,
      outerHTML: this.selector === '#name' ? `<input id="name" value="${this.page.name}">` : `<button id="go">${this.page.text}</button>`,
      getAttribute: (name) => this.selector === '#name' && name === 'type' ? 'text' : null,
      isContentEditable: false
    });
  }
  async innerText() {
    if (this.selector === 'body') {
      return `Name ${this.page.name} ${this.page.text}`.trim();
    }
    return this.selector === '#name' ? this.page.name : this.page.text;
  }
  async textContent() {
    if (this.selector === 'body') {
      return `Name ${this.page.name} ${this.page.text}`.trim();
    }
    return this.selector === '#name' ? this.page.name : this.page.text;
  }
  async getAttribute(name) {
    if (name === 'id' && this.selector.includes('#go')) return 'go';
    if (name === 'id' && this.selector.includes('#name')) return 'name';
    if (name === 'type' && this.selector.includes('#file')) return 'file';
    return null;
  }
  async screenshot(options) {
    const bytes = Buffer.from('fake-locator-screenshot'); if (options.path) fs.writeFileSync(options.path, bytes); return bytes;
  }
  async click() {
    this.page.clicked = true;
  }
  async dblclick() {
    this.page.clicked = true;
  }
  async fill(text) {
    if (this.selector === '#name') {
      this.page.name = text;
    } else {
      this.page.text = text;
    }
  }
  async type(text) {
    if (this.selector === '#name') {
      this.page.name += text;
    } else {
      this.page.text += text;
    }
  }
  async press() {}
  async count() {
    return this.selector === '#missing' ? 0 : 1;
  }
  async allTextContents() {
    return [this.selector === '#name' ? this.page.name : this.page.text];
  }
  async isVisible() {
    return true;
  }
  async isEnabled() {
    return true;
  }
  async scrollIntoViewIfNeeded() {}
  async check() {}
  async uncheck() {}
  async setChecked(checked) {
    this.page.checked = checked;
  }
  async selectOption(value) {
    this.page.name = Array.isArray(value) ? value[0] : value;
    return [this.page.name];
  }
  async setInputFiles(paths) {
    const files = Array.isArray(paths) ? paths : [paths];
    this.page.uploadedFiles.push({ selector: this.selector, files });
  }
}

class FakePage {
  constructor() {
    this._url = 'about:blank';
    this.text = 'Go';
    this.name = '';
    this.clicked = false;
    this.uploadedFiles = [];
    this.checked = false;
    this.shortcutExecutions = [];
    this.shortcuts = [
      { id: 'shortcut-summarize', command: 'summarize', description: 'Summarize the current page.', isWorkflow: true },
      { id: 'shortcut-debug', command: 'debug', description: 'Debug page state.', isWorkflow: false }
    ];
    this.mouse = {
      click: async () => { this.clicked = true; },
      move: async () => {},
      down: async () => {},
      up: async () => {},
      wheel: async () => {}
    };
    this.keyboard = {
      type: async (text) => { this.text += text; },
      press: async () => {}
    };
  }
  on() {}
  isClosed() {
    return false;
  }
  async close() {}
  async setViewportSize() {}
  async waitForTimeout() {}
  async waitForLoadState() {}
  async waitForURL(url) {
    this._url = String(url);
  }
  async waitForEvent(event) {
    if (event === 'download') {
      return {
        suggestedFilename: () => 'fake-download.txt',
        url: () => 'https://fake.test/download.txt',
        path: async () => null,
        saveAs: async (filePath) => {
          fs.writeFileSync(filePath, Buffer.from('fake download'));
        }
      };
    }
    if (event === 'filechooser') {
      return {
        isMultiple: () => false,
        setFiles: async (paths) => {
          const files = Array.isArray(paths) ? paths : [paths];
          this.uploadedFiles.push({ selector: 'filechooser', files });
        }
      };
    }
    throw new Error(`unsupported fake event ${event}`);
  }
  async goto(url) {
    this._url = url;
    return { status: () => 200 };
  }
  url() {
    return this._url;
  }
  async title() {
    return 'Fake Playwright Title';
  }
  locator(selector) {
    return new FakeLocator(this, selector);
  }
  frameLocator(selector) {
    return {
      locator: (childSelector) => new FakeLocator(this, `${selector} ${childSelector}`)
    };
  }
  async content() {
    return `<html><head><title>Fake Playwright Title</title></head><body><label for="name">Name</label><input id="name" value="${this.name}"><input id="file" type="file"><button id="go">${this.text}</button></body></html>`;
  }
  async screenshot(options) {
    const bytes = Buffer.from('fake-page-screenshot'); if (options.path) fs.writeFileSync(options.path, bytes); return bytes;
  }
  async evaluate(fn, arg) {
    const source = String(fn);
    if (source.includes('__klausExecuteShortcut')) {
      const name = arg?.name || arg;
      const found = this.shortcuts.find((shortcut) => shortcut.command === name || shortcut.id === name);
      this.shortcutExecutions.push(name);
      return { executed: !!found, shortcut: found || null, executions: this.shortcutExecutions.slice() };
    }
    if (source.includes('__klausShortcuts')) {
      return this.shortcuts;
    }
    if (source.includes('document.documentElement') && source.includes('createTreeWalker')) {
      if (arg) return 'body Name Go';
      return '<html><head><title>Fake Playwright Title</title></head><body><button id="go">Go</button></body></html>';
    }
    if (String(fn).includes('querySelectorAll')) {
      const elements = [
        {
          selector: '#name',
          role: 'textbox',
          name: 'Name',
          text: this.name || 'Name',
          tagName: 'input',
          x: 1,
          y: 2,
          width: 180,
          height: 24,
          visible: true,
          disabled: false,
          interactive: true
        },
        {
          selector: '#file',
          role: 'input',
          name: 'Upload',
          text: '',
          tagName: 'input',
          x: 1,
          y: 60,
          width: 180,
          height: 24,
          visible: true,
          disabled: false,
          interactive: true
        },
        {
          selector: '#go',
          role: 'button',
          name: null,
          text: this.text,
          tagName: 'button',
          x: 1,
          y: 32,
          width: 80,
          height: 24,
          visible: true,
          disabled: false,
          interactive: true
        }
      ];
      return arg ? elements.filter((element) => element.selector === arg) : elements;
    }
    return fn(arg);
  }
}

module.exports = {
  chromium: {
    async launch() {
      return {
        async newContext() {
          return {
            async newPage() {
              return new FakePage();
            },
            async close() {}
          };
        },
        async close() {}
      };
    }
  }
};
"#,
        )
        .unwrap();
    }

    #[tokio::test]
    async fn rejects_custom_sidecar_before_precancellation_and_dispatch() {
        let signal = AbortSignal::new();
        signal.cancel();
        let runtime = PlaywrightBrowserRuntime::new(
            test_process_context(),
            PlaywrightBrowserRuntimeOptions {
                node_path: PathBuf::from("node-must-not-be-spawned"),
                sidecar_path: Some(PathBuf::from("custom-sidecar-must-not-be-spawned.mjs")),
                ..Default::default()
            },
        );

        let error = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Navigate {
                        url: "https://must-not-dispatch.test/".into(),
                        wait_until: None,
                        viewport: None,
                    },
                    BrowserTarget::default(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                signal,
                Arc::new(|_| {}),
            )
            .await
            .expect_err("custom sidecars must fail before cancellation or dispatch");

        assert_eq!(error.code, BrowserRuntimeErrorCode::InvalidRequest);
        assert_eq!(
            error.message,
            "Custom browser sidecar_path is not supported by the exact runtime; use the bundled brokered sidecar."
        );
    }

    #[tokio::test]
    async fn invokes_sidecar_and_streams_events() {
        let dir = unique_temp_dir("browser-sidecar");
        let runtime = PlaywrightBrowserRuntime::with_test_sidecar_transport(
            test_process_context(),
            PlaywrightBrowserRuntimeOptions {
                artifact_dir: Some(dir.join("artifacts")),
                ..Default::default()
            },
            fake_browser_sidecar(&dir),
        );
        let events: Arc<Mutex<Vec<BrowserRuntimeEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let seen = events.clone();

        let response = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Navigate {
                        url: "https://example.test/".into(),
                        wait_until: None,
                        viewport: None,
                    },
                    BrowserTarget::default(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(move |event| {
                    seen.lock().unwrap().push(event);
                }),
            )
            .await
            .unwrap();

        assert_eq!(
            response.session_id.as_ref(),
            Some(&RuntimeString::from("session-1"))
        );
        assert_eq!(
            response.tab_id.as_ref(),
            Some(&RuntimeString::from("tab-1"))
        );
        assert_eq!(
            response.url.as_ref(),
            Some(&RuntimeString::from("https://example.test/"))
        );
        assert!(events.lock().unwrap().iter().any(|event| {
            matches!(
                event,
                BrowserRuntimeEvent::NavigationStarted { url, .. }
                    if url == "https://example.test/"
            )
        }));
    }

    #[tokio::test]
    async fn preserves_structured_sidecar_errors() {
        let dir = unique_temp_dir("browser-sidecar-error");
        let runtime = PlaywrightBrowserRuntime::with_test_sidecar_transport(
            test_process_context(),
            PlaywrightBrowserRuntimeOptions {
                artifact_dir: Some(dir.join("artifacts")),
                ..Default::default()
            },
            error_browser_sidecar(&dir),
        );

        let err = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Navigate {
                        url: "https://error.test/".into(),
                        wait_until: None,
                        viewport: None,
                    },
                    BrowserTarget::default(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap_err();

        assert_eq!(err.code, BrowserRuntimeErrorCode::NavigationFailed);
        assert_eq!(err.message, "navigation failed in fake sidecar");
        assert_eq!(
            err.details
                .as_ref()
                .and_then(|details| details.get("url"))
                .and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("https://error.test/"))
        );
    }

    #[tokio::test]
    async fn kills_timed_out_sidecar_and_recovers() {
        let dir = unique_temp_dir("browser-sidecar-timeout");
        let runtime = PlaywrightBrowserRuntime::with_test_sidecar_transport(
            test_process_context(),
            PlaywrightBrowserRuntimeOptions {
                artifact_dir: Some(dir.join("artifacts")),
                ..Default::default()
            },
            timeout_then_recover_browser_sidecar(&dir),
        );

        let mut hanging = BrowserRuntimeRequest::new(
            BrowserOperation::Navigate {
                url: "https://hang.test/".into(),
                wait_until: None,
                viewport: None,
            },
            BrowserTarget::default(),
            Some(50),
        );
        hanging
            .metadata
            .insert("hang".into(), RuntimeJson::Bool(true));
        let err = runtime
            .execute(hanging, AbortSignal::new(), Arc::new(|_| {}))
            .await
            .unwrap_err();
        assert_eq!(err.code, BrowserRuntimeErrorCode::Timeout);

        let recovered = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Navigate {
                        url: "https://recover.test/".into(),
                        wait_until: None,
                        viewport: None,
                    },
                    BrowserTarget::default(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();

        assert_eq!(
            recovered.session_id.as_ref(),
            Some(&RuntimeString::from("session-after-restart"))
        );
        assert_eq!(
            recovered.url.as_ref(),
            Some(&RuntimeString::from("https://recover.test/"))
        );
    }

    #[test]
    fn bundled_sidecar_writes_chrome_extension_assets() {
        let sidecar_path = write_bundled_sidecar().unwrap();
        let bundled_root = sidecar_path.parent().unwrap().parent().unwrap();
        assert_eq!(
            sidecar_path,
            bundled_root.join("runtime/playwright-sidecar.mjs")
        );
        let extension_dir = bundled_root.join("extension");
        assert!(extension_dir.join("manifest.json").exists());
        assert!(extension_dir.join("service-worker.js").exists());
        assert!(bundled_root.join("runtime/native-host.mjs").exists());
        assert!(bundled_root.join("scripts/extension-id.json").exists());
        assert!(bundled_root
            .join("scripts/check-extension-installed.mjs")
            .exists());
        assert!(bundled_root
            .join("scripts/check-native-host-manifest.mjs")
            .exists());
        assert!(bundled_root
            .join("scripts/install-native-host-manifest.mjs")
            .exists());
        assert!(bundled_root.join("scripts/chrome-is-running.mjs").exists());
        assert!(bundled_root.join("scripts/installed-browsers.mjs").exists());
        assert!(bundled_root.join("scripts/open-chrome-window.mjs").exists());
        assert!(bundled_root.join("scripts/browser-client.mjs").exists());
        assert!(bundled_root
            .join("skills/control-browser/SKILL.md")
            .exists());
        assert!(bundled_root.join("docs/api.json").exists());
        assert!(bundled_root.join(".codex-plugin/plugin.json").exists());
        assert!(std::fs::read_to_string(extension_dir.join("manifest.json"))
            .unwrap()
            .contains("Klaus Browser Connector"));
    }

    #[test]
    fn chrome_extension_collects_console_and_network_events() {
        let Some(node) = node_fixture_runtime() else {
            return;
        };
        let dir = unique_temp_dir("browser-extension-diagnostics");
        let plugin_dir = bundled_browser_asset_root();
        let service_worker_path = plugin_dir.join("extension/service-worker.js");
        let script_path = dir.join("extension-diagnostics-smoke.mjs");
        std::fs::write(
            &script_path,
            format!(
                r#"
import fs from 'node:fs';
import assert from 'node:assert/strict';
import vm from 'node:vm';

const source = fs.readFileSync({service_worker_path}, 'utf8') + `
globalThis.__klausTest = {{
  executeRequest,
  debuggerListenerCount: () => chrome.debugger.onEvent._listeners.length
}};
`;

const debuggerListeners = [];
const tabs = new Map([[7, {{ id: 7, windowId: 2, active: true, title: 'Diagnostics', url: 'https://diagnostics.test/', status: 'complete' }}]]);
const sandbox = {{
  console,
  URL,
  setTimeout,
  clearTimeout,
  navigator: {{ platform: 'test' }},
  crypto: {{ randomUUID: () => 'device-test' }},
  WebSocket: class FakeWebSocket {{
    static OPEN = 1;
    static CONNECTING = 0;
    constructor() {{ this.readyState = FakeWebSocket.CONNECTING; }}
    addEventListener() {{}}
    send() {{}}
  }},
  chrome: {{
    runtime: {{
      connectNative: undefined,
      getManifest: () => ({{ version: 'test' }}),
      onStartup: {{ addListener() {{}} }},
      onInstalled: {{ addListener() {{}} }},
    }},
    alarms: {{
      create() {{}},
      onAlarm: {{ addListener() {{}} }},
    }},
    storage: {{
      local: {{
        async get() {{ return {{}}; }},
        async set() {{}},
      }},
    }},
    tabs: {{
      async query() {{ return [...tabs.values()]; }},
      async get(id) {{ return tabs.get(id); }},
      async update(id, update) {{ Object.assign(tabs.get(id), update); return tabs.get(id); }},
      async create(update) {{
        const tab = {{ id: 8, windowId: 2, active: true, title: 'New', url: update.url || 'about:blank', status: 'complete' }};
        tabs.set(8, tab);
        return tab;
      }},
      async remove() {{}},
      async captureVisibleTab() {{ return 'data:image/png;base64,AA=='; }},
      onUpdated: {{ addListener() {{}}, removeListener() {{}} }},
    }},
    windows: {{ async update() {{}} }},
    scripting: {{
      async executeScript() {{ return [{{ result: null }}]; }},
    }},
    debugger: {{
      async attach() {{}},
      async sendCommand() {{}},
      onEvent: {{
        _listeners: debuggerListeners,
        addListener(listener) {{ debuggerListeners.push(listener); }},
      }},
      onDetach: {{
        addListener() {{}},
      }},
    }},
  }},
}};

vm.runInNewContext(source, sandbox, {{ filename: 'service-worker.js' }});
const request = (operation) => ({{ target: {{ tabId: 'chrome-7' }}, timeoutMs: 1000, operation }});

await sandbox.__klausTest.executeRequest(request({{ type: 'console', clear: true }}));
assert.equal(sandbox.__klausTest.debuggerListenerCount(), 1);
for (const listener of debuggerListeners) {{
  listener({{ tabId: 7 }}, 'Runtime.consoleAPICalled', {{
    type: 'error',
    args: [{{ value: 'boom' }}],
    timestamp: Date.now(),
  }});
}}
const consoleResult = await sandbox.__klausTest.executeRequest(request({{ type: 'console', levels: ['error'], limit: 10 }}));
assert.equal(consoleResult.console.length, 1);
assert.equal(consoleResult.console[0].level, 'error');
assert.match(consoleResult.console[0].text, /boom/);

await sandbox.__klausTest.executeRequest(request({{ type: 'network', clear: true }}));
for (const listener of debuggerListeners) {{
  listener({{ tabId: 7 }}, 'Network.requestWillBeSent', {{
    requestId: 'req-1',
    timestamp: 1,
    request: {{ url: 'https://diagnostics.test/api', method: 'POST' }},
  }});
  listener({{ tabId: 7 }}, 'Network.responseReceived', {{
    requestId: 'req-1',
    timestamp: 2,
    response: {{ url: 'https://diagnostics.test/api', status: 201, statusText: 'Created', mimeType: 'application/json' }},
  }});
}}
const networkResult = await sandbox.__klausTest.executeRequest(request({{ type: 'network', urlFilter: 'api', limit: 10 }}));
assert.equal(networkResult.network.length, 1);
assert.equal(networkResult.network[0].method, 'POST');
assert.equal(networkResult.network[0].status, 201);
assert.equal(networkResult.network[0].url, 'https://diagnostics.test/api');
"#,
                service_worker_path =
                    serde_json::to_string(&service_worker_path.to_string_lossy().into_owned())
                        .unwrap(),
            ),
        )
        .unwrap();

        let output = std::process::Command::new(&node)
            .arg(&script_path)
            .output()
            .expect("node should run extension diagnostics smoke test");
        assert!(
            output.status.success(),
            "extension diagnostics smoke failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn chrome_extension_executes_chrome_mcp_parity_operations() {
        let Some(node) = node_fixture_runtime() else {
            return;
        };
        let dir = unique_temp_dir("browser-extension-chrome-mcp-parity");
        let upload_path = dir.join("upload.txt");
        std::fs::write(&upload_path, "upload fixture").unwrap();
        let plugin_dir = bundled_browser_asset_root();
        let service_worker_path = plugin_dir.join("extension/service-worker.js");
        let script_path = dir.join("extension-chrome-mcp-parity-smoke.mjs");
        std::fs::write(
            &script_path,
            format!(
                r#"
import fs from 'node:fs';
import assert from 'node:assert/strict';
import vm from 'node:vm';

const source = fs.readFileSync({service_worker_path}, 'utf8') + `
globalThis.__klausTest = {{
  executeRequest
}};
`;

const commands = [];
let clipboardText = '';
const downloadCreatedListeners = [];
const tabs = new Map([[7, {{ id: 7, windowId: 2, active: true, title: 'Parity', url: 'https://parity.test/', status: 'complete' }}]]);
const sandbox = {{
  console,
  URL,
  setTimeout,
  clearTimeout,
  navigator: {{ platform: 'test-platform' }},
  crypto: {{ randomUUID: () => 'device-test' }},
  WebSocket: class FakeWebSocket {{
    static OPEN = 1;
    static CONNECTING = 0;
    constructor() {{ this.readyState = FakeWebSocket.CONNECTING; }}
    addEventListener() {{}}
    send() {{}}
  }},
  chrome: {{
    runtime: {{
      connectNative: undefined,
      getManifest: () => ({{ version: 'test-version' }}),
      onStartup: {{ addListener() {{}} }},
      onInstalled: {{ addListener() {{}} }},
    }},
    alarms: {{
      create() {{}},
      onAlarm: {{ addListener() {{}} }},
    }},
    storage: {{
      local: {{
        async get() {{ return {{ klausBrowserDeviceId: 'device-test' }}; }},
        async set() {{}},
      }},
    }},
    commands: {{
      async getAll() {{
        return [
          {{ name: 'summarize', description: 'Summarize page', shortcut: 'Ctrl+Shift+S' }},
          {{ name: 'debug', description: 'Debug page', shortcut: '' }},
        ];
      }},
    }},
    history: {{
      async search() {{
        return [{{ url: 'https://history.test/', title: 'History Test', lastVisitTime: Date.now() }}];
      }},
    }},
    downloads: {{
      async download(options) {{
        commands.push({{ method: 'downloads.download', params: options }});
        return 99;
      }},
      onCreated: {{
        addListener(listener) {{ downloadCreatedListeners.push(listener); }},
        removeListener(listener) {{
          const index = downloadCreatedListeners.indexOf(listener);
          if (index >= 0) downloadCreatedListeners.splice(index, 1);
        }},
      }},
    }},
    tabs: {{
      async query() {{ return [...tabs.values()]; }},
      async get(id) {{ return tabs.get(id); }},
      async update(id, update) {{ Object.assign(tabs.get(id), update); return tabs.get(id); }},
      async create(update) {{
        const tab = {{ id: 8, windowId: 2, active: true, title: 'New', url: update.url || 'about:blank', status: 'complete' }};
        tabs.set(8, tab);
        return tab;
      }},
      async remove() {{}},
      async captureVisibleTab() {{ return 'data:image/png;base64,AA=='; }},
      onUpdated: {{ addListener() {{}}, removeListener() {{}} }},
    }},
    windows: {{ async update() {{}} }},
    scripting: {{
      async executeScript(details) {{
        const source = String(details.func);
        if (source.includes('clipboardWriteTextScript')) {{
          clipboardText = details.args[0];
          return [{{ result: true }}];
        }}
        if (source.includes('clipboardReadTextScript')) {{
          return [{{ result: clipboardText }}];
        }}
	        if (source.includes('htmlContentScript')) {{
	          return [{{ result: '<html><body>Parity HTML</body></html>' }}];
	        }}
	        if (source.includes('domSnapshotScript')) {{
	          return [{{ result: 'body Parity visible DOM' }}];
	        }}
	        if (source.includes('elementInfoScript')) {{
	          return [{{ result: [{{ selector: '#file', tagName: 'input', text: '', visible: true }}] }}];
	        }}
	        if (source.includes('elementTextScript') || source.includes('document.body')) {{
	          return [{{ result: 'Parity text content' }}];
	        }}
        return [{{ result: [
          {{ ref: 'ref_1', selector: '#file', role: 'input', name: 'Upload', tagName: 'input', interactive: true }},
        ] }}];
      }},
    }},
    debugger: {{
      async attach() {{}},
      async sendCommand(_target, method, params = {{}}) {{
        commands.push({{ method, params }});
        if (method === 'Runtime.evaluate') return {{ result: {{ objectId: 'object-1' }} }};
        if (method === 'DOM.requestNode') return {{ nodeId: 42 }};
        return {{}};
      }},
      onEvent: {{
        addListener() {{}},
      }},
      onDetach: {{
        addListener() {{}},
      }},
    }},
  }},
}};

vm.runInNewContext(source, sandbox, {{ filename: 'service-worker.js' }});
const request = (operation) => ({{ target: {{ tabId: 'chrome-7' }}, timeoutMs: 1000, operation }});

const browsers = await sandbox.__klausTest.executeRequest(request({{ type: 'list_connected_browsers', localOnly: true }}));
assert.equal(browsers.value[0].deviceId, 'device-test');
assert.equal(browsers.value[0].isLocal, true);

const selected = await sandbox.__klausTest.executeRequest(request({{ type: 'select_browser', deviceId: 'device-test' }}));
assert.match(selected.message, /Connected to browser/);

const switched = await sandbox.__klausTest.executeRequest(request({{ type: 'switch_browser', prompt: 'pick one', waitMs: 10 }}));
assert.equal(switched.value.deviceId, 'device-test');

const shortcuts = await sandbox.__klausTest.executeRequest(request({{ type: 'shortcuts_list' }}));
assert.equal(shortcuts.value[0].command, 'summarize');
const executed = await sandbox.__klausTest.executeRequest(request({{ type: 'shortcuts_execute', name: 'summarize' }}));
assert.equal(executed.value.command, 'summarize');

const uploaded = await sandbox.__klausTest.executeRequest(request({{ type: 'file_upload', paths: [{upload_path}], selector: '#file' }}));
assert.deepEqual(uploaded.value.files, [{upload_path}]);
assert(commands.some((entry) => entry.method === 'DOM.setFileInputFiles'));
assert.deepEqual(commands.find((entry) => entry.method === 'DOM.setFileInputFiles').params.files, [{upload_path}]);

const history = await sandbox.__klausTest.executeRequest(request({{ type: 'history', queries: ['history'], limit: 5 }}));
assert.equal(history.value[0].url, 'https://history.test/');

const tabsContent = await sandbox.__klausTest.executeRequest(request({{ type: 'tabs_content', urls: ['https://content.test/'], contentType: 'text' }}));
assert.equal(tabsContent.value[0].content, 'Parity text content');

const exported = await sandbox.__klausTest.executeRequest(request({{ type: 'content_export', format: 'html' }}));
assert.match(exported.value.content, /Parity HTML/);

const download = await sandbox.__klausTest.executeRequest(request({{ type: 'download', url: 'https://download.test/file.txt', filename: 'file.txt' }}));
assert.equal(download.value.downloadId, 99);
assert(commands.some((entry) => entry.method === 'downloads.download'));

const downloadWait = sandbox.__klausTest.executeRequest(request({{ type: 'playwright_wait_for_event', event: 'download' }}));
await waitFor(() => downloadCreatedListeners.length === 1);
assert.equal(downloadCreatedListeners.length, 1);
downloadCreatedListeners[0]({{ id: 100, url: 'https://download.test/waited.txt', filename: '/tmp/waited.txt', mime: 'text/plain' }});
const waitedDownload = await downloadWait;
assert.equal(waitedDownload.value.path, '/tmp/waited.txt');

const chooser = await sandbox.__klausTest.executeRequest(request({{ type: 'playwright_wait_for_event', event: 'filechooser', selector: '#file' }}));
assert.equal(chooser.value.isMultiple, false);
await sandbox.__klausTest.executeRequest(request({{ type: 'playwright_file_chooser_set_files', chooserId: chooser.value.chooserId, files: [{upload_path}] }}));
assert(commands.filter((entry) => entry.method === 'DOM.setFileInputFiles').length >= 2);

const loadState = await sandbox.__klausTest.executeRequest(request({{ type: 'playwright_wait_for_load_state', state: 'load' }}));
assert.equal(loadState.value.state, 'load');

const waitedUrl = await sandbox.__klausTest.executeRequest(request({{ type: 'playwright_wait_for_url', url: 'https://parity.test/' }}));
assert.equal(waitedUrl.value.url, 'https://parity.test/');

const domSnapshot = await sandbox.__klausTest.executeRequest(request({{ type: 'playwright_dom_snapshot', visibleOnly: true }}));
assert.match(domSnapshot.text, /Parity visible DOM/);
const elementInfo = await sandbox.__klausTest.executeRequest(request({{ type: 'playwright_element_info', selector: '#file' }}));
assert.equal(elementInfo.value[0].selector, '#file');
const elementScreenshot = await sandbox.__klausTest.executeRequest(request({{ type: 'playwright_element_screenshot', selector: '#file' }}));
assert.equal(elementScreenshot.artifacts[0].kind, 'screenshot');

await sandbox.__klausTest.executeRequest(request({{ type: 'clipboard', action: 'write_text', text: 'Copied' }}));
const clipboard = await sandbox.__klausTest.executeRequest(request({{ type: 'clipboard', action: 'read_text' }}));
assert.equal(clipboard.text, 'Copied');

const dialog = await sandbox.__klausTest.executeRequest(request({{ type: 'dialog', action: 'get' }}));
assert.equal(dialog.value, null);

const marked = await sandbox.__klausTest.executeRequest(request({{ type: 'mark_tab', status: 'deliverable' }}));
assert.equal(marked.value.status, 'deliverable');

async function waitFor(predicate, timeoutMs = 1000) {{
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {{
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }}
  throw new Error('timed out waiting for condition');
}}
"#,
                service_worker_path =
                    serde_json::to_string(&service_worker_path.to_string_lossy().into_owned())
                        .unwrap(),
                upload_path =
                    serde_json::to_string(&upload_path.to_string_lossy().into_owned()).unwrap(),
            ),
        )
        .unwrap();

        let output = std::process::Command::new(&node)
            .arg(&script_path)
            .output()
            .expect("node should run extension parity smoke test");
        assert!(
            output.status.success(),
            "extension parity smoke failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[tokio::test]
    async fn extension_backend_bridges_to_chrome_connector() {
        let Some(node) = node_fixture_runtime() else {
            return;
        };
        let dir = unique_temp_dir("browser-extension-bridge");
        let port = free_tcp_port();
        let log_path = dir.join("extension-call.json");
        let bridge_token = "test-extension-backend-token";
        let extension_origin = "chrome-extension://gjamccelebgpogpbiceajehcajghphnb";
        let mut client = std::process::Command::new(&node)
            .arg(fake_chrome_extension_client(&dir))
            .arg(port.to_string())
            .arg(&log_path)
            .arg(bridge_token)
            .arg(extension_origin)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("fake Chrome extension client should start");

        let mut env = PlaywrightEnvOverlay::default();
        env.insert("KLAUS_BROWSER_BACKEND".to_string(), "extension".to_string());
        env.insert(
            "KLAUS_BROWSER_EXTENSION_BRIDGE_PORT".to_string(),
            port.to_string(),
        );
        env.insert(
            "KLAUS_BROWSER_EXTENSION_BRIDGE_TOKEN".to_string(),
            bridge_token.to_string(),
        );
        env.insert(
            "KLAUS_BROWSER_EXTENSION_CONNECT_TIMEOUT_MS".to_string(),
            "3000".to_string(),
        );
        let socket_path = unique_socket_path("eb");
        env.insert(
            "KLAUS_BROWSER_NATIVE_BRIDGE_SOCKET".to_string(),
            socket_path.to_string_lossy().into_owned(),
        );
        let runtime = PlaywrightBrowserRuntime::new(
            test_process_context(),
            PlaywrightBrowserRuntimeOptions {
                artifact_dir: Some(dir.join("artifacts")),
                env,
                ..Default::default()
            },
        );

        let result = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Tabs {
                        action: BrowserTabsAction::List,
                        url: None,
                        name: None,
                    },
                    BrowserTarget::default(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await;
        // The runtime error comes first: this test kills the fake client when the request
        // fails, so asserting on the client's exit status alone reports a killed process
        // with empty output and hides the reason the request failed.
        let request_error = result.as_ref().err().map(ToString::to_string);
        if result.is_err() {
            let _ = client.kill();
        }
        let output = client
            .wait_with_output()
            .expect("fake Chrome extension client should exit");
        assert!(
            output.status.success(),
            "fake extension failed\nrequest error: {request_error:?}\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let response = result.unwrap();
        assert_eq!(
            response.session_id.as_ref(),
            Some(&RuntimeString::from("fake-chrome-device"))
        );
        assert_eq!(
            response.tab_id.as_ref(),
            Some(&RuntimeString::from("chrome-11"))
        );
        assert_eq!(
            response
                .value
                .as_ref()
                .and_then(RuntimeJson::as_array)
                .and_then(|value| value.first())
                .and_then(|tab| tab.get("url"))
                .and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("https://mail.test/"))
        );
        let logged: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&log_path).unwrap())
                .expect("fake extension should log bridged request");
        assert_eq!(
            logged.get("operation").and_then(serde_json::Value::as_str),
            Some("tabs")
        );
        assert_eq!(
            logged.get("action").and_then(serde_json::Value::as_str),
            Some("list")
        );
    }

    #[tokio::test]
    async fn extension_backend_accepts_secure_native_socket_connector() {
        let Some(node) = node_fixture_runtime() else {
            return;
        };
        let dir = unique_temp_dir("browser-extension-native-socket-bridge");
        let socket_path = unique_socket_path("ns");
        let log_path = dir.join("native-socket-call.json");
        let mut client = std::process::Command::new(&node)
            .arg(fake_chrome_native_socket_client(&dir))
            .arg(&socket_path)
            .arg(&log_path)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("fake native socket Chrome client should start");

        let mut env = PlaywrightEnvOverlay::default();
        env.insert("KLAUS_BROWSER_BACKEND".to_string(), "extension".to_string());
        env.insert(
            "KLAUS_BROWSER_EXTENSION_BRIDGE_PORT".to_string(),
            free_tcp_port().to_string(),
        );
        env.insert(
            "KLAUS_BROWSER_NATIVE_BRIDGE_SOCKET".to_string(),
            socket_path.to_string_lossy().into_owned(),
        );
        env.insert(
            "KLAUS_BROWSER_EXTENSION_CONNECT_TIMEOUT_MS".to_string(),
            "3000".to_string(),
        );
        let runtime = PlaywrightBrowserRuntime::new(
            test_process_context(),
            PlaywrightBrowserRuntimeOptions {
                artifact_dir: Some(dir.join("artifacts")),
                env,
                ..Default::default()
            },
        );

        let result = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Tabs {
                        action: BrowserTabsAction::List,
                        url: None,
                        name: None,
                    },
                    BrowserTarget::default(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await;
        // Same reason as the bridge case above: the request error is what explains a fake
        // client that this test killed.
        let request_error = result.as_ref().err().map(ToString::to_string);
        if result.is_err() {
            let _ = client.kill();
        }
        let output = client
            .wait_with_output()
            .expect("fake native socket Chrome client should exit");
        assert!(
            output.status.success(),
            "fake native socket client failed\nrequest error: {request_error:?}\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let response = result.unwrap();
        assert_eq!(
            response.session_id.as_ref(),
            Some(&RuntimeString::from("native-socket-device"))
        );
        assert_eq!(
            response.tab_id.as_ref(),
            Some(&RuntimeString::from("chrome-22"))
        );
        let logged: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&log_path).unwrap())
                .expect("fake native socket client should log bridged request");
        assert_eq!(
            logged.get("operation").and_then(serde_json::Value::as_str),
            Some("tabs")
        );
        assert_eq!(
            logged.get("action").and_then(serde_json::Value::as_str),
            Some("list")
        );
    }

    /// The legacy JavaScript native host is a migration shim: executed
    /// directly it must refuse with the documented exit code and point the
    /// installer at the Rust-owned `klaus-browser` command.
    #[test]
    fn chrome_native_host_shim_exits_with_migration_guidance() {
        let Some(node) = node_fixture_runtime() else {
            return;
        };
        let plugin_dir = bundled_browser_asset_root();
        let native_host_path = plugin_dir.join("runtime/native-host.mjs");
        let output = std::process::Command::new(&node)
            .arg(&native_host_path)
            .output()
            .expect("node should run the native host shim");
        assert_eq!(output.status.code(), Some(64));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("klaus-browser install-native-host-manifest"),
            "shim stderr must name the Rust migration command: {stderr}"
        );
    }

    /// Importing the shim as a module must expose the frozen migration
    /// metadata without side effects, so callers can discover the Rust
    /// replacement programmatically.
    #[test]
    fn chrome_native_host_shim_exports_migration_metadata() {
        let Some(node) = node_fixture_runtime() else {
            return;
        };
        let plugin_dir = bundled_browser_asset_root();
        let native_host_path = plugin_dir.join("runtime/native-host.mjs");
        let host_json =
            serde_json::to_string(&native_host_path.to_string_lossy().into_owned()).unwrap();
        let script = format!(
            r#"
import {{ pathToFileURL }} from 'node:url';
const mod = await import(pathToFileURL({host_json}).href);
console.log(JSON.stringify(mod.migration));
"#
        );
        let output = std::process::Command::new(&node)
            .args(["--input-type=module", "-e", &script])
            .output()
            .expect("node should import the native host shim");
        assert!(
            output.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let parsed: serde_json::Value =
            serde_json::from_str(String::from_utf8(output.stdout).unwrap().trim()).unwrap();
        assert_eq!(
            parsed.get("version").and_then(serde_json::Value::as_u64),
            Some(1)
        );
        assert_eq!(
            parsed.get("command").and_then(serde_json::Value::as_str),
            Some("klaus-browser")
        );
        assert_eq!(
            parsed.get("subcommand").and_then(serde_json::Value::as_str),
            Some("install-native-host-manifest")
        );
        assert_eq!(
            parsed
                .get("directExitCode")
                .and_then(serde_json::Value::as_u64),
            Some(64)
        );
    }

    #[tokio::test]
    async fn bundled_sidecar_executes_browser_operations() {
        let dir = unique_temp_dir("browser-bundled-sidecar");
        write_fake_playwright_project(&dir);
        let runtime = PlaywrightBrowserRuntime::new(
            test_process_context(),
            PlaywrightBrowserRuntimeOptions {
                cwd: Some(dir.clone()),
                artifact_dir: Some(dir.join("artifacts")),
                ..Default::default()
            },
        );

        let navigated = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Navigate {
                        url: "https://bundled-sidecar.test/".into(),
                        wait_until: Some(BrowserWaitUntil::DomContentLoaded),
                        viewport: None,
                    },
                    BrowserTarget::default(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        assert_eq!(
            navigated.title.as_ref(),
            Some(&RuntimeString::from("Fake Playwright Title"))
        );

        let snapshot = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Snapshot {
                        selector: None,
                        include_text: true,
                        include_links: true,
                    },
                    BrowserTarget::default(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        assert!(snapshot
            .snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.elements.iter().any(|el| {
                el.selector.as_ref() == Some(&RuntimeString::from("#go"))
                    && el.text.as_ref() == Some(&RuntimeString::from("Go"))
            })));

        let evaluated = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Eval {
                        script: "args.answer + 1".into(),
                        args: Some(scalar_runtime_json(serde_json::json!({ "answer": 41 }))),
                        await_promise: true,
                    },
                    BrowserTarget::default(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        assert_eq!(
            evaluated.value,
            Some(scalar_runtime_json(serde_json::json!(42)))
        );

        let screenshot = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Screenshot {
                        criteria: Default::default(),
                        ref_id: None,
                        selector: None,
                        full_page: true,
                        format: Some(BrowserScreenshotFormat::Png),
                        quality: None,
                        save: true,
                    },
                    BrowserTarget::default(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        let artifact = screenshot.artifacts.first().unwrap();
        assert_eq!(artifact.kind, BrowserArtifactKind::Screenshot);
        assert!(runtime_path_exists(artifact.path.as_ref().unwrap()));
        let files_before = std::fs::read_dir(dir.join("artifacts")).unwrap().count();
        let observation = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Screenshot {
                        criteria: Default::default(),
                        ref_id: None,
                        selector: None,
                        full_page: false,
                        format: Some(BrowserScreenshotFormat::Png),
                        quality: None,
                        save: false,
                    },
                    BrowserTarget::default(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        assert!(observation.artifacts.is_empty());
        let value = observation.value.as_ref().unwrap();
        assert_eq!(value.get("saved"), Some(&RuntimeJson::Bool(false)));
        assert!(value.get("path").is_none());
        assert!(value
            .get("dataUrl")
            .unwrap()
            .as_string()
            .unwrap()
            .starts_with("data:image/png;base64,"));
        assert_eq!(
            std::fs::read_dir(dir.join("artifacts")).unwrap().count(),
            files_before
        );
    }

    #[tokio::test]
    async fn bundled_sidecar_executes_rich_browser_operations() {
        let dir = unique_temp_dir("browser-rich-sidecar");
        write_fake_playwright_project(&dir);
        let runtime = PlaywrightBrowserRuntime::new(
            test_process_context(),
            PlaywrightBrowserRuntimeOptions {
                cwd: Some(dir.clone()),
                artifact_dir: Some(dir.join("artifacts")),
                ..Default::default()
            },
        );

        let new_tab = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Tabs {
                        action: BrowserTabsAction::New,
                        url: Some("https://rich-sidecar.test/".into()),
                        name: None,
                    },
                    BrowserTarget::default(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        assert!(new_tab
            .tab_id
            .as_ref()
            .is_some_and(|id| id.starts_with("tab-")));

        let read_page = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::ReadPage {
                        filter: Some(BrowserReadPageFilter::Interactive),
                        depth: Some(6),
                        ref_id: None,
                        max_chars: Some(20_000),
                    },
                    BrowserTarget {
                        session_id: None,
                        tab_id: new_tab.tab_id.clone(),
                        browser_id: None,
                    },
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        let read_value = read_page.value.as_ref().unwrap();
        assert!(read_value["text"].as_str().unwrap().contains("Name"));

        let found = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Find {
                        query: "name".into(),
                        limit: Some(3),
                    },
                    BrowserTarget {
                        session_id: None,
                        tab_id: new_tab.tab_id.clone(),
                        browser_id: None,
                    },
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        let first_ref = found.value.as_ref().unwrap()["matches"][0]["ref"]
            .as_str()
            .unwrap()
            .clone();

        runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::FormInput {
                        reference: first_ref,
                        value: scalar_runtime_json(serde_json::json!("Ada")),
                    },
                    BrowserTarget {
                        session_id: None,
                        tab_id: new_tab.tab_id.clone(),
                        browser_id: None,
                    },
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();

        let locator = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Locator {
                        selector: "#name".into(),
                        action: "text_content".into(),
                        value: None,
                        options: scalar_runtime_json(serde_json::json!({})),
                    },
                    BrowserTarget {
                        session_id: None,
                        tab_id: new_tab.tab_id.clone(),
                        browser_id: None,
                    },
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        assert_eq!(
            locator.value,
            Some(scalar_runtime_json(serde_json::json!("Ada")))
        );

        let computer = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Computer {
                        action: BrowserComputerAction::Scroll,
                        coordinate: Some(vec![10.0, 10.0]),
                        start_coordinate: None,
                        text: None,
                        duration: None,
                        scroll_direction: Some(BrowserScrollDirection::Down),
                        amount: Some(120.0),
                        ref_id: None,
                        save: false,
                        keys: None,
                        path: None,
                        scroll_x: None,
                        scroll_y: None,
                    },
                    BrowserTarget {
                        session_id: None,
                        tab_id: new_tab.tab_id,
                        browser_id: None,
                    },
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        assert_eq!(
            computer.message.as_ref(),
            Some(&RuntimeString::from("computer action completed"))
        );
    }

    #[tokio::test]
    async fn bundled_sidecar_executes_chrome_mcp_parity_operations() {
        let dir = unique_temp_dir("browser-chrome-mcp-parity-sidecar");
        write_fake_playwright_project(&dir);
        let upload_path = dir.join("upload.txt");
        std::fs::write(&upload_path, "upload fixture").unwrap();
        let image_path = dir.join("image.png");
        std::fs::write(&image_path, b"\x89PNG\r\n\x1a\nfake").unwrap();
        let runtime = PlaywrightBrowserRuntime::new(
            test_process_context(),
            PlaywrightBrowserRuntimeOptions {
                cwd: Some(dir.clone()),
                artifact_dir: Some(dir.join("artifacts")),
                ..Default::default()
            },
        );

        let tab = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Tabs {
                        action: BrowserTabsAction::New,
                        url: Some("https://chrome-mcp-parity.test/".into()),
                        name: None,
                    },
                    BrowserTarget::default(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        let target = BrowserTarget {
            session_id: None,
            tab_id: tab.tab_id.clone(),
            browser_id: None,
        };

        let gif = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::GifCreator {
                        filename: Some("parity-recording.gif".into()),
                        duration_seconds: Some(0.05),
                        fps: Some(1),
                        full_page: true,
                    },
                    target.clone(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        let gif_artifact = gif.artifacts.first().expect("gif artifact");
        assert_eq!(gif_artifact.kind, BrowserArtifactKind::Gif);
        assert!(runtime_path_exists(gif_artifact.path.as_ref().unwrap()));
        assert!(gif_artifact
            .metadata
            .get(&RuntimeString::from("frames"))
            .and_then(Json::as_u64)
            .is_some_and(|frames| frames >= 1));

        let file_upload = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::FileUpload {
                        criteria: Default::default(),
                        paths: vec![upload_path.to_string_lossy().into_owned().into()],
                        selector: Some("#file".into()),
                        ref_id: None,
                    },
                    target.clone(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        assert_eq!(
            file_upload
                .value
                .as_ref()
                .and_then(|value| value.get("files"))
                .and_then(Json::as_array)
                .and_then(|files| files.first())
                .and_then(Json::as_str),
            Some(&RuntimeString::from(
                upload_path.to_string_lossy().into_owned()
            ))
        );

        let image_upload = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::UploadImage {
                        criteria: Default::default(),
                        ref_id: None,
                        path: image_path.to_string_lossy().into_owned().into(),
                        selector: Some("#file".into()),
                        prompt: Some("fake upload".into()),
                    },
                    target.clone(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        assert_eq!(
            image_upload
                .value
                .as_ref()
                .and_then(|value| value.get("files"))
                .and_then(Json::as_array)
                .and_then(|files| files.first())
                .and_then(Json::as_str),
            Some(&RuntimeString::from(
                image_path.to_string_lossy().into_owned()
            ))
        );

        let shortcuts = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::ShortcutsList {
                        query: Some("sum".into()),
                    },
                    target.clone(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        assert!(shortcuts
            .value
            .as_ref()
            .and_then(Json::as_array)
            .is_some_and(|items| items.iter().any(|item| {
                item.get("command").and_then(RuntimeJson::as_str)
                    == Some(&RuntimeString::from("summarize"))
            })));

        let executed = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::ShortcutsExecute {
                        name: "summarize".into(),
                        wait_for_completion: true,
                    },
                    target.clone(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        assert_eq!(
            executed
                .value
                .as_ref()
                .and_then(|value| value.get("executed"))
                .and_then(Json::as_bool),
            Some(true)
        );

        let browsers = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::ListConnectedBrowsers { local_only: true },
                    target.clone(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        assert_eq!(
            browsers
                .value
                .as_ref()
                .and_then(Json::as_array)
                .and_then(|items| items.first())
                .and_then(|item| item.get("deviceId"))
                .and_then(Json::as_str),
            Some(&RuntimeString::from("session-1"))
        );

        runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::SelectBrowser {
                        device_id: "session-1".into(),
                    },
                    target.clone(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::SwitchBrowser {
                        prompt: Some("Pick local browser".into()),
                        wait_ms: Some(10),
                    },
                    target,
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn bundled_sidecar_executes_codex_api_parity_operations() {
        let dir = unique_temp_dir("browser-codex-api-parity-sidecar");
        write_fake_playwright_project(&dir);
        let runtime = PlaywrightBrowserRuntime::new(
            test_process_context(),
            PlaywrightBrowserRuntimeOptions {
                cwd: Some(dir.clone()),
                artifact_dir: Some(dir.join("artifacts")),
                ..Default::default()
            },
        );

        let navigated = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Navigate {
                        url: "https://codex-api-parity.test/".into(),
                        wait_until: Some(BrowserWaitUntil::Load),
                        viewport: None,
                    },
                    BrowserTarget::default(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        let target = BrowserTarget {
            session_id: None,
            tab_id: navigated.tab_id.clone(),
            browser_id: None,
        };

        let history = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::History {
                        queries: vec!["codex-api".into()],
                        limit: Some(10),
                        from: None,
                        to: None,
                    },
                    target.clone(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        assert_eq!(
            history
                .value
                .as_ref()
                .and_then(Json::as_array)
                .and_then(|items| items.first())
                .and_then(|item| item.get("url"))
                .and_then(Json::as_str),
            Some(&RuntimeString::from("https://codex-api-parity.test/"))
        );

        let tabs_content = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::TabsContent {
                        urls: vec!["https://content-api.test/".into()],
                        content_type: Some("text".into()),
                        max_chars: Some(1_000),
                        wait_until: Some(BrowserWaitUntil::Load),
                    },
                    target.clone(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        assert!(tabs_content
            .value
            .as_ref()
            .and_then(Json::as_array)
            .and_then(|items| items.first())
            .and_then(|item| item.get("content"))
            .and_then(Json::as_str)
            .is_some_and(|text| text.contains("Go")));

        let exported = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::ContentExport {
                        format: Some("html".into()),
                        selector: None,
                        max_chars: Some(20_000),
                    },
                    target.clone(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        let export_path = exported.artifacts[0].path.as_ref().unwrap();
        assert!(runtime_path_exists(export_path));

        let downloaded = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Download {
                        selector: None,
                        url: Some("data:text/plain,downloaded".into()),
                        x: None,
                        y: None,
                        filename: Some("download.txt".into()),
                        wait_only: false,
                    },
                    target.clone(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        assert_eq!(downloaded.artifacts[0].kind, BrowserArtifactKind::Download);
        assert!(runtime_path_exists(
            downloaded.artifacts[0].path.as_ref().unwrap()
        ));

        runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Clipboard {
                        action: BrowserClipboardAction::WriteText,
                        text: Some("clipboard text".into()),
                        items: None,
                    },
                    target.clone(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        let clipboard = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Clipboard {
                        action: BrowserClipboardAction::ReadText,
                        text: None,
                        items: None,
                    },
                    target.clone(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        assert_eq!(
            clipboard.text.as_ref(),
            Some(&RuntimeString::from("clipboard text"))
        );

        let dialog = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Dialog {
                        action: BrowserDialogAction::Get,
                        prompt_text: None,
                    },
                    target.clone(),
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        assert!(dialog.value.is_none());

        let marked = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::MarkTab {
                        status: BrowserTabMarkStatus::Deliverable,
                    },
                    target,
                    Some(SIDECAR_LIVENESS_TIMEOUT_MS),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        assert_eq!(
            marked
                .value
                .as_ref()
                .and_then(|value| value.get("status"))
                .and_then(Json::as_str),
            Some(&RuntimeString::from("deliverable"))
        );
    }

    #[tokio::test]
    async fn bundled_browser_plugin_loads_runtime_client_and_skill_assets() {
        let plugin_dir = bundled_browser_asset_root();

        let plugin = klaus_agent_core::extensions::plugins::loader::load_one_plugin(
            &plugin_dir.to_string_lossy(),
            klaus_agent_core::extensions::command_loader::CommandLoadOptions::default(),
        )
        .await
        .expect("bundled browser plugin should load");

        assert_eq!(plugin.name, "browser");
        assert_eq!(plugin.runtime_capabilities, vec!["browser".to_string()]);
        assert!(plugin
            .keywords
            .iter()
            .any(|keyword| keyword == "browser-client"));
        assert!(plugin
            .interface_capabilities
            .iter()
            .any(|capability| capability == "Interactive"));
        assert!(plugin
            .skills
            .iter()
            .any(|skill| skill.name == "control-browser"));
        assert!(plugin_dir.join("scripts/browser-client.mjs").exists());
        assert!(plugin_dir.join("runtime/playwright-sidecar.mjs").exists());
        assert!(plugin_dir.join("runtime/native-host.mjs").exists());
        assert!(plugin_dir.join("extension/manifest.json").exists());
        assert!(plugin_dir
            .join("scripts/check-extension-installed.mjs")
            .exists());
        assert!(plugin_dir
            .join("scripts/check-native-host-manifest.mjs")
            .exists());
        assert!(plugin_dir.join("scripts/chrome-is-running.mjs").exists());
        assert!(plugin_dir.join("scripts/installed-browsers.mjs").exists());
        assert!(plugin_dir.join("scripts/open-chrome-window.mjs").exists());
        assert!(plugin_dir.join("docs/documents.json").exists());
        assert!(plugin_dir.join("docs/browser-safety.md").exists());
        assert!(plugin_dir.join("docs/file-uploads.md").exists());
        assert!(plugin_dir.join("docs/downloads.md").exists());
    }

    /// Unix-only for the same reason as the other Node-hosted broker smokes: the fixture is
    /// the host, and it inherits the unix lanes into the sidecar.
    #[cfg(not(windows))]
    #[test]
    fn bundled_browser_client_script_controls_sidecar() {
        // The custom `klaus:` ESM scheme is a Node loader contract. Bun executes
        // the production sidecar but does not implement Node's loader hooks.
        let Some(node) = node_fixture_runtime() else {
            return;
        };
        let dir = unique_temp_dir("browser-client");
        let plugin_dir = bundled_browser_asset_root();
        let client_path = plugin_dir.join("scripts/browser-client.mjs");
        let sidecar_path = fake_browser_sidecar(&dir);
        let loader_path = write_klaus_host_transport(&dir, &sidecar_path, &dir);
        let script_path = dir.join("browser-client-smoke.mjs");
        let client_json =
            serde_json::to_string(&client_path.to_string_lossy().into_owned()).unwrap();
        let loader_json =
            serde_json::to_string(&loader_path.to_string_lossy().into_owned()).unwrap();
        std::fs::write(
            &script_path,
            format!(
                r#"
import {{ register }} from 'node:module';
import {{ pathToFileURL }} from 'node:url';

register(pathToFileURL({loader_json}));

const {{ setupBrowserRuntime, createBrowserRegistry }} = await import(pathToFileURL({client_json}).href);
await setupBrowserRuntime({{
  globals: globalThis,
  options: {{
    artifactDir: {artifact_dir},
    headless: true
  }}
}});

const browser = await agent.browsers.getDefault();
const extensionRegistry = createBrowserRegistry({{ backend: 'extension' }});
const extensionList = await extensionRegistry.list();
const docs = await browser.documentation();
const caps = await browser.capabilities.list();
await browser.nameSession('client smoke');
await browser.listConnectedBrowsers({{ localOnly: true }});
await browser.selectBrowser('session-1').catch(() => null);
await browser.switchBrowser({{ waitMs: 0 }});
const tab = await browser.tabs.getDefault();
const created = await browser.tabs.new();
const selected = await browser.tabs.selected();
const result = await tab.goto('https://client.test/');
await tab.find('button');
await tab.computer({{ action: 'wait', duration: 0 }});
await tab.gifCreator({{ filename: 'client.gif', durationSeconds: 0.05, fps: 1 }});
await tab.fileUpload(['/tmp/client-upload.txt'], {{ selector: '#file' }});
await tab.uploadImage('/tmp/client-image.png', {{ selector: '#file' }});
await tab.shortcuts.list();
await tab.shortcuts.execute('summarize').catch(() => null);
await tab.playwright.locator('#go').click();
await tab.playwright.waitForLoadState({{ state: 'load' }});
await tab.playwright.waitForTimeout(1);
await tab.playwright.waitForURL('https://client.test/');
const downloadPromise = tab.playwright.waitForEvent('download', {{ timeoutMs: 1000 }});
const download = await downloadPromise;
const downloadPath = await download.path();
const chooserPromise = tab.playwright.waitForEvent('filechooser', {{ timeoutMs: 1000 }});
const chooser = await chooserPromise;
await chooser.setFiles('/tmp/client-upload.txt');
const frameButton = tab.playwright.frameLocator('#frame').locator('#go');
await frameButton.count();
const locator = tab.playwright.locator('#go');
await locator.last().innerText();
await locator.filter({{ hasText: 'Go' }}).count();
const allLocators = await locator.all();
await allLocators[0].getAttribute('id');
await locator.isEnabled();
await tab.playwright.locator('#agree').setChecked(true);
await tab.cua.click({{ x: 1, y: 2 }});
await tab.cua.double_click({{ x: 1, y: 2 }});
await tab.cua.move({{ x: 1, y: 2 }});
await tab.cua.scroll({{ x: 1, y: 2, deltaY: 10 }});
await tab.cua.keypress({{ key: 'Enter' }});
await tab.cua.type({{ text: 'hello' }});
await tab.dom_cua.get_visible_dom();
await tab.dom_cua.click({{ selector: '#go' }});
await tab.dom_cua.double_click({{ selector: '#go' }});
await tab.dom_cua.scroll({{ selector: 'body', deltaY: 10 }});
await tab.dom_cua.keypress({{ key: 'Enter' }});
await tab.dom_cua.type({{ selector: '#name', text: 'Ada' }});
await tab.playwright.domSnapshot();
await tab.playwright.elementInfo({{ x: 1, y: 2 }});
await tab.playwright.elementScreenshot({{ x: 1, y: 2 }});
await extensionRegistry.dispose();
await agent.browsers.dispose();
console.log(JSON.stringify({{
  docs: docs.includes('Klaus Bundled Browser Runtime'),
  caps: caps.some((cap) => cap.id === 'playwright'),
  created: created.id,
  selected: selected.id,
  title: result.title,
  url: result.url,
  downloadPath,
  streamedEvents: result.streamedEvents.length,
  extensionType: extensionList[0].type
}}));
"#,
                artifact_dir =
                    serde_json::to_string(&dir.join("artifacts").to_string_lossy().into_owned())
                        .unwrap()
            ),
        )
        .unwrap();

        let output = std::process::Command::new(&node)
            .arg(&script_path)
            .output()
            .expect("node should run browser client smoke test");
        assert!(
            output.status.success(),
            "node failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
        assert_eq!(
            parsed.get("docs").and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            parsed.get("caps").and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert!(parsed
            .get("created")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|id| id.starts_with("tab-")));
        assert!(parsed
            .get("selected")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|id| id.starts_with("tab-")));
        assert_eq!(
            parsed.get("title").and_then(serde_json::Value::as_str),
            Some("Fake Browser")
        );
        assert_eq!(
            parsed.get("url").and_then(serde_json::Value::as_str),
            Some("https://client.test/")
        );
        assert_eq!(
            parsed
                .get("streamedEvents")
                .and_then(serde_json::Value::as_u64),
            Some(1)
        );
        assert_eq!(
            parsed
                .get("extensionType")
                .and_then(serde_json::Value::as_str),
            Some("extension")
        );
    }

    /// Unix-only for the same reason as the other Node-hosted broker smokes: the fixture is
    /// the host, and it inherits the unix lanes into the sidecar.
    #[cfg(not(windows))]
    #[test]
    fn bundled_browser_client_script_controls_real_sidecar_codex_sdk_depth() {
        // The custom `klaus:` ESM scheme is a Node loader contract. Bun executes
        // the production sidecar but does not implement Node's loader hooks.
        let Some(node) = node_fixture_runtime() else {
            return;
        };
        let dir = unique_temp_dir("browser-client-real-sidecar");
        write_fake_playwright_project(&dir);
        let upload_path = dir.join("upload.txt");
        std::fs::write(&upload_path, "upload-body").unwrap();
        let plugin_dir = bundled_browser_asset_root();
        let client_path = plugin_dir.join("scripts/browser-client.mjs");
        // Publish the real bundled sidecar into a private cache and stand up
        // the pinned runtime next to it, exactly like the Rust host path.
        let sidecar_path = write_bundled_sidecar_in(&dir.join("cache")).unwrap();
        materialize_test_pinned_runtime(&sidecar_path).unwrap();
        let loader_path = write_klaus_host_transport(&dir, &sidecar_path, &dir);
        let script_path = dir.join("browser-client-real-sidecar-smoke.mjs");
        let client_json =
            serde_json::to_string(&client_path.to_string_lossy().into_owned()).unwrap();
        let loader_json =
            serde_json::to_string(&loader_path.to_string_lossy().into_owned()).unwrap();
        let upload_json =
            serde_json::to_string(&upload_path.to_string_lossy().into_owned()).unwrap();
        let artifact_dir_json =
            serde_json::to_string(&dir.join("artifacts").to_string_lossy().into_owned()).unwrap();
        std::fs::write(
            &script_path,
            format!(
                r#"
import {{ register }} from 'node:module';
import {{ pathToFileURL }} from 'node:url';
import assert from 'node:assert/strict';

register(pathToFileURL({loader_json}));

const {{ setupBrowserRuntime }} = await import(pathToFileURL({client_json}).href);
await setupBrowserRuntime({{
  globals: globalThis,
  options: {{
    artifactDir: {artifact_dir_json},
    headless: true
  }}
}});

const browser = await agent.browsers.getDefault();
const tab = await browser.tabs.getDefault();
await tab.goto('https://codex-sdk-depth.test/');
await tab.playwright.waitForLoadState({{ state: 'load' }});
await tab.playwright.waitForTimeout(1);
await tab.playwright.waitForURL('https://codex-sdk-depth.test/');

const download = await tab.playwright.waitForEvent('download', {{ timeoutMs: 1000 }});
const downloadPath = await download.path();
assert.ok(downloadPath.endsWith('fake-download.txt'));

const chooser = await tab.playwright.waitForEvent('filechooser', {{ timeoutMs: 1000 }});
assert.equal(chooser.isMultiple(), false);
await chooser.setFiles({upload_json});

const frameCount = await tab.playwright.frameLocator('#frame').locator('#go').count();
const locator = tab.playwright.locator('#go');
const all = await locator.all();
const id = await all[0].getAttribute('id');
const text = await locator.last().innerText();
const filteredCount = await locator.filter({{ hasText: 'Go' }}).count();
const enabled = await locator.isEnabled();
await tab.playwright.locator('#agree').setChecked(true);
await tab.cua.click({{ x: 1, y: 2 }});
await tab.cua.double_click({{ x: 1, y: 2 }});
await tab.cua.move({{ x: 1, y: 2 }});
await tab.cua.scroll({{ x: 1, y: 2, deltaY: 10 }});
await tab.cua.keypress({{ key: 'Enter' }});
await tab.cua.type({{ text: 'hello' }});
const visibleDom = await tab.dom_cua.get_visible_dom();
await tab.dom_cua.click({{ selector: '#go' }});
await tab.dom_cua.double_click({{ selector: '#go' }});
await tab.dom_cua.scroll({{ selector: 'body', deltaY: 10 }});
await tab.dom_cua.keypress({{ key: 'Enter' }});
await tab.dom_cua.type({{ selector: '#name', text: 'Ada' }});
const domSnapshot = await tab.playwright.domSnapshot();
const elementInfo = await tab.playwright.elementInfo({{ selector: '#go' }});
const elementShot = await tab.playwright.elementScreenshot({{ selector: '#go' }});

await agent.browsers.dispose();
console.log(JSON.stringify({{
  downloadPath,
  frameCount: frameCount.value ?? frameCount,
  id: id.value ?? id,
  text: text.value ?? text,
  filteredCount: filteredCount.value ?? filteredCount,
  enabled: enabled.value ?? enabled,
  visibleDom: visibleDom.text ?? visibleDom.value?.text ?? '',
  domSnapshot,
  elementInfoCount: elementInfo.length,
  elementInfoHasButton: elementInfo.some((item) => item.selector === '#go' || item.text === 'Go'),
  elementShotKind: elementShot.kind
}}));
"#,
            ),
        )
        .unwrap();

        let output = std::process::Command::new(&node)
            .arg(&script_path)
            .output()
            .expect("node should run real sidecar browser client smoke test");
        assert!(
            output.status.success(),
            "real sidecar client smoke failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let parsed: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert!(parsed
            .get("downloadPath")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|path| path.ends_with("fake-download.txt")));
        assert_eq!(
            parsed.get("frameCount").and_then(serde_json::Value::as_u64),
            Some(1)
        );
        assert_eq!(
            parsed.get("id").and_then(serde_json::Value::as_str),
            Some("go")
        );
        assert_eq!(
            parsed.get("text").and_then(serde_json::Value::as_str),
            Some("Go")
        );
        assert_eq!(
            parsed
                .get("filteredCount")
                .and_then(serde_json::Value::as_u64),
            Some(1)
        );
        assert_eq!(
            parsed.get("enabled").and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert!(parsed
            .get("visibleDom")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|text| text.contains("Go")));
        assert!(parsed
            .get("domSnapshot")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|text| text.contains("Fake Playwright Title")));
        assert!(parsed
            .get("elementInfoCount")
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|count| count >= 1));
        assert_eq!(
            parsed
                .get("elementInfoHasButton")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            parsed
                .get("elementShotKind")
                .and_then(serde_json::Value::as_str),
            Some("screenshot")
        );
    }

    #[tokio::test]
    async fn live_browser_runtime_navigates_snapshots_and_screenshots() {
        if std::env::var("KLAUS_RUN_LIVE_BROWSER_TESTS")
            .ok()
            .as_deref()
            != Some("1")
        {
            return;
        }
        let dir = unique_temp_dir("browser-live");
        let cwd = std::env::var_os("KLAUS_BROWSER_TEST_NODE_PROJECT")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../..")
                    .to_path_buf()
            });
        let runtime = PlaywrightBrowserRuntime::new(
            test_process_context(),
            PlaywrightBrowserRuntimeOptions {
                cwd: Some(cwd),
                artifact_dir: Some(dir.join("artifacts")),
                ..Default::default()
            },
        );
        let response = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Navigate {
                        url: "data:text/html,<title>Klaus Browser Live</title><label for='name'>Name</label><input id='name'><button id='go'>Go</button>"
                            .into(),
                        wait_until: Some(BrowserWaitUntil::DomContentLoaded),
                        viewport: None,
                    },
                    BrowserTarget::default(),
                    Some(30_000),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        assert_eq!(
            response.title.as_ref(),
            Some(&RuntimeString::from("Klaus Browser Live"))
        );

        let snapshot = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Snapshot {
                        selector: None,
                        include_text: true,
                        include_links: true,
                    },
                    BrowserTarget::default(),
                    Some(30_000),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        assert!(snapshot
            .snapshot
            .as_ref()
            .and_then(|snap| snap.text.as_ref())
            .is_some_and(|text| text.contains("Go")));

        let found = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Find {
                        query: "name".into(),
                        limit: Some(5),
                    },
                    BrowserTarget::default(),
                    Some(30_000),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        let name_ref = found.value.as_ref().unwrap()["matches"][0]["ref"]
            .as_str()
            .unwrap()
            .clone();
        runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::FormInput {
                        reference: name_ref,
                        value: scalar_runtime_json(serde_json::json!("Live Ada")),
                    },
                    BrowserTarget::default(),
                    Some(30_000),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        let value = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Eval {
                        script: "document.querySelector('#name').value".into(),
                        args: None,
                        await_promise: true,
                    },
                    BrowserTarget::default(),
                    Some(30_000),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        assert_eq!(
            value.value,
            Some(scalar_runtime_json(serde_json::json!("Live Ada")))
        );

        let locator_count = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Locator {
                        selector: "button".into(),
                        action: "count".into(),
                        value: None,
                        options: scalar_runtime_json(serde_json::json!({})),
                    },
                    BrowserTarget::default(),
                    Some(30_000),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        assert_eq!(
            locator_count.value,
            Some(scalar_runtime_json(serde_json::json!(1)))
        );

        runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Computer {
                        action: BrowserComputerAction::Wait,
                        coordinate: None,
                        start_coordinate: None,
                        text: None,
                        duration: Some(0.0),
                        scroll_direction: None,
                        amount: None,
                        ref_id: None,
                        save: false,
                        keys: None,
                        path: None,
                        scroll_x: None,
                        scroll_y: None,
                    },
                    BrowserTarget::default(),
                    Some(30_000),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();

        let screenshot = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Screenshot {
                        criteria: Default::default(),
                        ref_id: None,
                        selector: None,
                        full_page: true,
                        format: Some(BrowserScreenshotFormat::Png),
                        quality: None,
                        save: true,
                    },
                    BrowserTarget::default(),
                    Some(30_000),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        let path = screenshot.artifacts[0].path.as_ref().unwrap();
        assert!(runtime_path_exists(path));
    }
}
