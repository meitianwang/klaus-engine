//! Remote code-mode carriers: sessions whose cells run in a host process
//! instead of this one.
//!
//! One host connection is shared by every session a provider hands out, so a
//! single host serves the whole engine. Session isolation is the session's job
//! (each gets its own [`SessionId`] on the wire), not the carrier's.
//!
//! Two failure stances shape this module:
//!
//! - a host that cannot be STARTED is a configuration problem, so the provider
//!   may fall back to the in-process carrier once and keep serving;
//! - a host that starts and then dies is a runtime fault: the connection is
//!   marked dead, every in-flight request fails with the recorded reason, and
//!   every live cell is reported closed to its delegate so no caller waits
//!   forever.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use tokio::sync::{watch, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::engine::code_mode::host::{
    SessionId, WireReplEvalRequest, WireReplOutcome, WireToolDefinition,
};
use crate::engine::code_mode::protocol::{
    CellId, CodeModeNestedToolCall, CodeModeToolKind, ExecuteRequest, WaitOutcome, WaitRequest,
};
use crate::engine::code_mode::runtime::repl_session::{project_invoke_result, BRIDGE_UNAVAILABLE};
use crate::engine::code_mode::runtime::{InProcessCodeModeSession, InProcessReplHandle};
use crate::engine::code_mode::session::{
    CodeModeSession, CodeModeSessionDelegate, CodeModeSessionProvider,
    CodeModeSessionProviderFuture, CodeModeSessionResultFuture, NotificationFuture, ReplExecFuture,
    ReplExecOutcome, ReplExecRequest, ReplSessionHandle, StartedCell, ToolInvocationFuture,
};
use crate::runtime_process::RuntimeProcessContext;
use crate::runtime_values::{RuntimeJson, RuntimeString};
use crate::sync_ext::MutexExt;
use crate::types::errors::CodeModeToolRejection;
use crate::types::tool::InvokeToolFn;

use self::connection::{Connection, ConnectionError, RemoteSession, SessionCleanup};

mod connection;
mod driver;
mod driver_support;

/// Overrides the host program a process carrier launches.
pub const CODE_MODE_HOST_PATH_ENV: &str = "KLAUS_CODE_MODE_HOST_PATH";

type ShutdownResultReceiver = watch::Receiver<Option<Result<(), RuntimeString>>>;

/// Creates sessions backed by one lazily spawned host process.
pub struct ProcessOwnedCodeModeSessionProvider {
    state: StdMutex<ProviderState>,
    allow_in_process_fallback: bool,
}

enum ProviderState {
    OwnedProcess(Arc<OwnedCodeModeHost>),
    /// The host program was absent, so this provider serves in-process sessions
    /// for the rest of its life rather than failing every turn.
    InProcess,
}

impl ProcessOwnedCodeModeSessionProvider {
    pub fn with_host_program(
        process_context: RuntimeProcessContext,
        host_program: PathBuf,
    ) -> Self {
        Self {
            state: StdMutex::new(ProviderState::OwnedProcess(Arc::new(
                OwnedCodeModeHost::new(process_context, host_program),
            ))),
            allow_in_process_fallback: true,
        }
    }

    /// Refuse to serve in-process sessions: a deployment that requires host
    /// isolation must fail loudly rather than quietly run cells locally.
    pub fn without_in_process_fallback(mut self) -> Self {
        self.allow_in_process_fallback = false;
        self
    }

    fn process_host(&self) -> Option<Arc<OwnedCodeModeHost>> {
        match &*self.state.lock_recover() {
            ProviderState::OwnedProcess(process_host) => Some(Arc::clone(process_host)),
            ProviderState::InProcess => None,
        }
    }

    fn fall_back_to_in_process(&self) {
        *self.state.lock_recover() = ProviderState::InProcess;
    }
}

impl CodeModeSessionProvider for ProcessOwnedCodeModeSessionProvider {
    fn create_session<'a>(
        &'a self,
        delegate: Arc<dyn CodeModeSessionDelegate>,
    ) -> CodeModeSessionProviderFuture<'a> {
        Box::pin(async move {
            let Some(process_host) = self.process_host() else {
                return Ok(in_process_session(delegate));
            };

            match process_host.connection().await {
                Ok(_) => {}
                Err(error) if error.host_program_not_found() && self.allow_in_process_fallback => {
                    self.fall_back_to_in_process();
                    return Ok(in_process_session(delegate));
                }
                Err(error) => return Err(RuntimeString::from(error.to_string())),
            }
            create_host_session(delegate, process_host).await
        })
    }

    fn create_repl_session<'a>(
        &'a self,
    ) -> CodeModeSessionResultFuture<'a, Arc<dyn ReplSessionHandle>> {
        Box::pin(async move {
            let Some(process_host) = self.process_host() else {
                return Ok(in_process_repl_session());
            };

            match process_host.connection().await {
                Ok(_) => {}
                Err(error) if error.host_program_not_found() && self.allow_in_process_fallback => {
                    self.fall_back_to_in_process();
                    return Ok(in_process_repl_session());
                }
                // Fail closed: a deployment that requires host isolation gets
                // a clear infrastructure error here rather than a silent
                // in-process runtime.
                Err(error) => return Err(RuntimeString::from(error.to_string())),
            }
            create_host_repl_session(process_host).await
        })
    }
}

fn in_process_session(delegate: Arc<dyn CodeModeSessionDelegate>) -> Arc<dyn CodeModeSession> {
    Arc::new(InProcessCodeModeSession::with_delegate(delegate))
}

async fn create_host_session(
    delegate: Arc<dyn CodeModeSessionDelegate>,
    host: Arc<OwnedCodeModeHost>,
) -> Result<Arc<dyn CodeModeSession>, RuntimeString> {
    let session = ProcessOwnedCodeModeSession::with_host(delegate, host);
    // Open the remote session before handing it out, so a carrier that cannot
    // serve fails at creation instead of on the model's first call.
    session.connection().await?;
    Ok(Arc::new(session))
}

fn in_process_repl_session() -> Arc<dyn ReplSessionHandle> {
    Arc::new(InProcessReplHandle::default())
}

async fn create_host_repl_session(
    host: Arc<OwnedCodeModeHost>,
) -> Result<Arc<dyn ReplSessionHandle>, RuntimeString> {
    let bridge = Arc::new(ReplBridgeDelegate::new());
    let session = ProcessOwnedReplSession::with_host(Arc::clone(&bridge), host);
    // Open the remote session before handing the handle out, so a carrier
    // that cannot serve fails at creation instead of on the first call.
    session.inner.connection().await?;
    Ok(Arc::new(session))
}

/// The engine-side delegate of one host-backed REPL handle: nested tool calls
/// arriving over the connection run through the bridge the running evaluation
/// offered, crossing the caller's full pipeline.
///
/// REPL evaluations are serialized per handle, so a single bridge slot is
/// enough; it is offered before the evaluation request goes out and cleared
/// when the evaluation settles.
struct ReplBridgeDelegate {
    bridge: StdMutex<Option<InvokeToolFn>>,
}

impl ReplBridgeDelegate {
    fn new() -> Self {
        Self {
            bridge: StdMutex::new(None),
        }
    }

    fn offer(&self, invoke_tool: Option<InvokeToolFn>) {
        *self.bridge.lock_recover() = invoke_tool;
    }

    fn clear(&self) {
        *self.bridge.lock_recover() = None;
    }
}

impl CodeModeSessionDelegate for ReplBridgeDelegate {
    fn invoke_tool<'a>(
        &'a self,
        invocation: CodeModeNestedToolCall,
        cancellation_token: CancellationToken,
    ) -> ToolInvocationFuture<'a> {
        let bridge = self.bridge.lock_recover().clone();
        Box::pin(async move {
            let Some(invoke_tool) = bridge else {
                return Err(CodeModeToolRejection::plain(BRIDGE_UNAVAILABLE));
            };
            let tool_name = invocation.tool_name.clone();
            let tool_input = invocation.input.unwrap_or(RuntimeJson::Null);
            let result = tokio::select! {
                result = invoke_tool(tool_name.clone(), tool_input) => result,
                _ = cancellation_token.cancelled() => {
                    return Err(CodeModeToolRejection::plain("cancelled"))
                }
            };
            project_invoke_result(&tool_name, result)
        })
    }

    fn notify<'a>(
        &'a self,
        _call_id: RuntimeString,
        _cell_id: CellId,
        _text: RuntimeString,
        _cancellation_token: CancellationToken,
    ) -> NotificationFuture<'a> {
        // REPL evaluations raise no live status text, so nothing to route.
        Box::pin(async { Ok(()) })
    }

    fn cell_closed(&self, _cell_id: &CellId) {
        self.clear();
    }
}

/// A REPL handle assigned to a host process. The session envelope (open,
/// generations, shutdown) is shared with the cell machinery; evaluations
/// travel as `repl/eval` requests and nested tool calls come back through
/// the connection's delegate channel.
pub struct ProcessOwnedReplSession {
    inner: Arc<SessionInner>,
    bridge: Arc<ReplBridgeDelegate>,
    next_exec_id: AtomicU64,
}

impl ProcessOwnedReplSession {
    fn with_host(bridge: Arc<ReplBridgeDelegate>, host: Arc<OwnedCodeModeHost>) -> Self {
        Self {
            inner: Arc::new(SessionInner {
                host,
                delegate: Arc::clone(&bridge) as Arc<dyn CodeModeSessionDelegate>,
                state: StdMutex::new(SessionState::New),
                next_generation: AtomicU64::new(1),
                shutdown_requested: AtomicBool::new(false),
                shutdown_result: StdMutex::new(None),
                retired_cleanups: StdMutex::new(Vec::new()),
            }),
            bridge,
            next_exec_id: AtomicU64::new(1),
        }
    }

    async fn eval(
        &self,
        request: ReplExecRequest,
        signal: &CancellationToken,
    ) -> Result<ReplExecOutcome, String> {
        let binding = self
            .inner
            .connection()
            .await
            .map_err(|error| String::from_utf8_lossy(&error.to_node_utf8()).into_owned())?;
        let exec_id = RuntimeString::from(format!(
            "repl-{}",
            self.next_exec_id.fetch_add(1, Ordering::Relaxed)
        ));
        let wire_request = WireReplEvalRequest {
            exec_id: exec_id.clone(),
            code: request.code,
            timeout_ms: request.timeout_ms,
            tools: request
                .tools
                .into_iter()
                .map(|tool| WireToolDefinition {
                    name: tool.key,
                    tool_name: tool.name,
                    description: RuntimeString::default(),
                    kind: CodeModeToolKind::Function.into(),
                    input_schema: None,
                    output_schema: None,
                })
                .collect(),
        };
        let eval = binding
            .connection
            .repl_eval(binding.remote.clone(), wire_request);
        tokio::pin!(eval);
        let outcome: Result<WireReplOutcome, RuntimeString> = tokio::select! {
            result = &mut eval => result,
            _ = signal.cancelled() => {
                // Ask the host to interrupt this evaluation, then keep
                // waiting for the settled outcome so captured console output
                // is not lost.
                let _ = binding
                    .connection
                    .repl_terminate(binding.remote.clone(), exec_id)
                    .await;
                eval.await
            }
        };
        let outcome = outcome.map_err(|error: RuntimeString| {
            String::from_utf8_lossy(&error.to_node_utf8()).into_owned()
        })?;
        Ok(ReplExecOutcome {
            result: outcome.result,
            result_defined: outcome.result_defined,
            result_text: outcome.result_text,
            stdout: outcome.stdout,
            stderr: outcome.stderr,
            error: outcome.error,
        })
    }
}

impl ReplSessionHandle for ProcessOwnedReplSession {
    fn execute<'a>(
        &'a self,
        request: ReplExecRequest,
        invoke_tool: Option<InvokeToolFn>,
        signal: &'a CancellationToken,
    ) -> ReplExecFuture<'a> {
        Box::pin(async move {
            self.bridge.offer(invoke_tool);
            let result = self.eval(request, signal).await;
            self.bridge.clear();
            result
        })
    }
}

impl Drop for ProcessOwnedReplSession {
    fn drop(&mut self) {
        // Release the remote session even when the owner forgot to shut it
        // down; without a runtime there is nothing to drive the request.
        if tokio::runtime::Handle::try_current().is_ok() {
            self.inner.request_shutdown();
        }
    }
}

/// One host endpoint plus the single connection every session shares.
struct OwnedCodeModeHost {
    process_context: RuntimeProcessContext,
    host_program: PathBuf,
    connection: StdMutex<Option<Arc<Connection>>>,
    /// Serializes connect attempts so a burst of sessions starts ONE host.
    connect_permit: Semaphore,
    next_session_id: AtomicU64,
}

impl OwnedCodeModeHost {
    fn new(process_context: RuntimeProcessContext, host_program: PathBuf) -> Self {
        Self {
            process_context,
            host_program,
            connection: StdMutex::new(None),
            connect_permit: Semaphore::new(/*permits*/ 1),
            next_session_id: AtomicU64::new(1),
        }
    }

    async fn connection(&self) -> Result<Arc<Connection>, ConnectionError> {
        if let Some(connection) = self.live_connection() {
            return Ok(connection);
        }

        let _connect_permit = self.connect_permit.acquire().await.map_err(|_| {
            ConnectionError::Other("code-mode host connection coordinator closed".into())
        })?;
        // Re-check under the permit: another caller may have connected while
        // this one waited.
        if let Some(connection) = self.live_connection() {
            return Ok(connection);
        }
        let new_connection =
            Arc::new(Connection::spawn(&self.process_context, &self.host_program).await?);
        *self.connection.lock_recover() = Some(Arc::clone(&new_connection));
        Ok(new_connection)
    }

    fn live_connection(&self) -> Option<Arc<Connection>> {
        self.connection
            .lock_recover()
            .as_ref()
            .filter(|connection| connection.is_alive())
            .cloned()
    }

    fn allocate_session_id(&self) -> SessionId {
        let value = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        SessionId::new(RuntimeString::from(format!("session-{value}")))
            .expect("a generated session id is never blank")
    }
}

enum SessionState {
    New,
    Opening {
        remote: RemoteSession,
        result_rx: watch::Receiver<Option<Result<SessionBinding, RuntimeString>>>,
    },
    Open(SessionBinding),
    Closing,
    Closed,
}

#[derive(Clone)]
struct SessionBinding {
    connection: Arc<Connection>,
    remote: RemoteSession,
    cleanup: SessionCleanup,
}

struct SessionInner {
    host: Arc<OwnedCodeModeHost>,
    delegate: Arc<dyn CodeModeSessionDelegate>,
    state: StdMutex<SessionState>,
    /// Bumped per open attempt so cell ids of a replaced host generation cannot
    /// be mistaken for live ones.
    next_generation: AtomicU64,
    shutdown_requested: AtomicBool,
    shutdown_result: StdMutex<Option<ShutdownResultReceiver>>,
    /// Cleanups of connections this session outlived, awaited at shutdown so
    /// every cell has been reported closed before the session reports closed.
    retired_cleanups: StdMutex<Vec<SessionCleanup>>,
}

/// A logical session assigned to a host process.
pub struct ProcessOwnedCodeModeSession {
    inner: Arc<SessionInner>,
}

impl ProcessOwnedCodeModeSession {
    fn with_host(delegate: Arc<dyn CodeModeSessionDelegate>, host: Arc<OwnedCodeModeHost>) -> Self {
        Self {
            inner: Arc::new(SessionInner {
                host,
                delegate,
                state: StdMutex::new(SessionState::New),
                next_generation: AtomicU64::new(1),
                shutdown_requested: AtomicBool::new(false),
                shutdown_result: StdMutex::new(None),
                retired_cleanups: StdMutex::new(Vec::new()),
            }),
        }
    }

    async fn connection(&self) -> Result<SessionBinding, RuntimeString> {
        self.inner.connection().await
    }

    pub async fn execute(&self, request: ExecuteRequest) -> Result<StartedCell, RuntimeString> {
        let binding = self.connection().await?;
        binding.connection.execute(binding.remote, request).await
    }

    pub async fn wait(&self, request: WaitRequest) -> Result<WaitOutcome, RuntimeString> {
        let binding = self.connection().await?;
        binding.connection.wait(binding.remote, request).await
    }

    pub async fn terminate(&self, cell_id: CellId) -> Result<WaitOutcome, RuntimeString> {
        let binding = self.connection().await?;
        binding.connection.terminate(binding.remote, cell_id).await
    }

    pub async fn shutdown(&self) -> Result<(), RuntimeString> {
        wait_for_watch(self.inner.request_shutdown()).await
    }
}

impl SessionInner {
    /// The session's live binding, opening one (once) if needed.
    ///
    /// A binding whose connection died is discarded and re-opened under a new
    /// generation; concurrent callers share the single in-flight open.
    async fn connection(self: &Arc<Self>) -> Result<SessionBinding, RuntimeString> {
        loop {
            if self.shutdown_requested.load(Ordering::Acquire) {
                return Err(RuntimeString::from("code mode session is shutting down"));
            }
            let (result_rx, start) = {
                let mut state = self.state.lock_recover();
                match &*state {
                    SessionState::New => {
                        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
                        let remote = RemoteSession {
                            id: self.host.allocate_session_id(),
                            generation,
                        };
                        let (result_tx, result_rx) = watch::channel(None);
                        *state = SessionState::Opening {
                            remote: remote.clone(),
                            result_rx: result_rx.clone(),
                        };
                        (result_rx, Some((remote, result_tx)))
                    }
                    SessionState::Opening { result_rx, .. } => (result_rx.clone(), None),
                    SessionState::Open(binding) if binding.connection.is_alive() => {
                        return Ok(binding.clone());
                    }
                    SessionState::Open(binding) => {
                        self.retain_cleanup(binding.cleanup.clone());
                        *state = SessionState::New;
                        continue;
                    }
                    SessionState::Closing | SessionState::Closed => {
                        return Err(RuntimeString::from("code mode session is shutting down"));
                    }
                }
            };
            if let Some((remote, result_tx)) = start {
                let inner = Arc::clone(self);
                tokio::spawn(async move {
                    inner.open(remote, result_tx).await;
                });
            }
            return wait_for_watch(result_rx).await;
        }
    }

    async fn open(
        self: Arc<Self>,
        remote: RemoteSession,
        result_tx: watch::Sender<Option<Result<SessionBinding, RuntimeString>>>,
    ) {
        let result = match self.host.connection().await {
            Ok(connection) => connection
                .open_session(remote.clone(), Arc::clone(&self.delegate))
                .await
                .map(|cleanup| SessionBinding {
                    connection,
                    remote: remote.clone(),
                    cleanup,
                }),
            Err(error) => Err(RuntimeString::from(error.to_string())),
        };
        {
            let mut state = self.state.lock_recover();
            // Only the attempt that is still current may publish its outcome; a
            // shutdown or a newer generation supersedes it.
            if matches!(
                &*state,
                SessionState::Opening {
                    remote: opening_remote,
                    ..
                } if opening_remote == &remote
            ) {
                *state = match &result {
                    Ok(binding) => SessionState::Open(binding.clone()),
                    Err(_) => SessionState::New,
                };
            }
        }
        result_tx.send_replace(Some(result));
    }

    /// Start (or join) the single shutdown driver.
    fn request_shutdown(self: &Arc<Self>) -> ShutdownResultReceiver {
        self.shutdown_requested.store(true, Ordering::Release);
        let mut shutdown_result = self.shutdown_result.lock_recover();
        if let Some(result_rx) = shutdown_result.as_ref() {
            return result_rx.clone();
        }
        let (result_tx, result_rx) = watch::channel(None);
        *shutdown_result = Some(result_rx.clone());
        let inner = Arc::clone(self);
        tokio::spawn(async move {
            let result = inner.drive_shutdown().await;
            result_tx.send_replace(Some(result));
        });
        result_rx
    }

    async fn drive_shutdown(self: &Arc<Self>) -> Result<(), RuntimeString> {
        loop {
            let action = {
                let mut state = self.state.lock_recover();
                match &*state {
                    SessionState::New => {
                        *state = SessionState::Closed;
                        ShutdownAction::Finish
                    }
                    SessionState::Opening { result_rx, .. } => {
                        ShutdownAction::WaitForOpen(result_rx.clone())
                    }
                    // A dead connection cannot answer a shutdown request; wait
                    // for its cleanup to report every cell closed instead.
                    SessionState::Open(binding) if !binding.connection.is_alive() => {
                        let cleanup = binding.cleanup.clone();
                        *state = SessionState::Closing;
                        ShutdownAction::WaitForSessionCleanup(cleanup)
                    }
                    SessionState::Open(binding) => {
                        let binding = binding.clone();
                        *state = SessionState::Closing;
                        ShutdownAction::Close(binding)
                    }
                    SessionState::Closing => {
                        return Err(RuntimeString::from(
                            "code-mode session shutdown driver entered twice",
                        ));
                    }
                    SessionState::Closed => return Ok(()),
                }
            };
            match action {
                ShutdownAction::WaitForOpen(result_rx) => {
                    let _ = wait_for_watch(result_rx).await;
                }
                ShutdownAction::Finish => {
                    self.wait_for_retired_cleanups().await;
                    return Ok(());
                }
                ShutdownAction::WaitForSessionCleanup(cleanup) => {
                    cleanup.wait().await;
                    self.wait_for_retired_cleanups().await;
                    *self.state.lock_recover() = SessionState::Closed;
                    return Ok(());
                }
                ShutdownAction::Close(binding) => {
                    let result = binding.connection.shutdown_session(binding.remote).await;
                    if result.is_err() && !binding.connection.is_alive() {
                        binding.cleanup.wait().await;
                    }
                    self.wait_for_retired_cleanups().await;
                    *self.state.lock_recover() = SessionState::Closed;
                    return result;
                }
            }
        }
    }

    fn retain_cleanup(&self, cleanup: SessionCleanup) {
        let mut retired = self.retired_cleanups.lock_recover();
        retired.retain(|cleanup| !cleanup.is_complete());
        if !cleanup.is_complete() {
            retired.push(cleanup);
        }
    }

    async fn wait_for_retired_cleanups(&self) {
        let retired = std::mem::take(&mut *self.retired_cleanups.lock_recover());
        for cleanup in retired {
            cleanup.wait().await;
        }
    }
}

enum ShutdownAction {
    WaitForOpen(watch::Receiver<Option<Result<SessionBinding, RuntimeString>>>),
    Finish,
    WaitForSessionCleanup(SessionCleanup),
    Close(SessionBinding),
}

async fn wait_for_watch<T>(
    mut result_rx: watch::Receiver<Option<Result<T, RuntimeString>>>,
) -> Result<T, RuntimeString>
where
    T: Clone,
{
    loop {
        if let Some(result) = result_rx.borrow().clone() {
            return result;
        }
        result_rx
            .changed()
            .await
            .map_err(|_| RuntimeString::from("code-mode session transition stopped"))?;
    }
}

impl Drop for ProcessOwnedCodeModeSession {
    fn drop(&mut self) {
        // Release the remote session even when the owner forgot to shut it
        // down; without a runtime there is nothing to drive the request.
        if tokio::runtime::Handle::try_current().is_ok() {
            self.inner.request_shutdown();
        }
    }
}

impl CodeModeSession for ProcessOwnedCodeModeSession {
    fn execute<'a>(
        &'a self,
        request: ExecuteRequest,
    ) -> CodeModeSessionResultFuture<'a, StartedCell> {
        Box::pin(ProcessOwnedCodeModeSession::execute(self, request))
    }

    fn wait<'a>(&'a self, request: WaitRequest) -> CodeModeSessionResultFuture<'a, WaitOutcome> {
        Box::pin(ProcessOwnedCodeModeSession::wait(self, request))
    }

    fn terminate<'a>(&'a self, cell_id: CellId) -> CodeModeSessionResultFuture<'a, WaitOutcome> {
        Box::pin(ProcessOwnedCodeModeSession::terminate(self, cell_id))
    }

    fn shutdown<'a>(&'a self) -> CodeModeSessionResultFuture<'a, ()> {
        Box::pin(ProcessOwnedCodeModeSession::shutdown(self))
    }
}

/// The host program a process carrier launches: the `KLAUS_CODE_MODE_HOST_PATH`
/// override, else a sibling of the running executable.
pub fn default_host_program() -> PathBuf {
    resolve_host_program(
        crate::runtime_process::parent_runtime_env_value(CODE_MODE_HOST_PATH_ENV),
        std::env::current_exe(),
    )
}

fn resolve_host_program(
    override_path: Option<RuntimeString>,
    current_exe: std::io::Result<PathBuf>,
) -> PathBuf {
    if let Some(path) = override_path {
        return PathBuf::from(String::from_utf8_lossy(&path.to_node_utf8()).into_owned());
    }
    let executable_name = if cfg!(windows) {
        "klaus-code-mode-host.exe"
    } else {
        "klaus-code-mode-host"
    };
    if let Ok(current_exe) = current_exe {
        if let Some(parent) = current_exe.parent() {
            return parent.join(executable_name);
        }
    }
    PathBuf::from(executable_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_host_program_override_wins_over_the_executable_sibling() {
        let resolved = resolve_host_program(
            Some(RuntimeString::from("/opt/hosts/custom-host")),
            Ok(PathBuf::from("/usr/bin/klaus")),
        );
        assert_eq!(resolved, PathBuf::from("/opt/hosts/custom-host"));
    }

    #[test]
    fn without_an_override_the_host_is_a_sibling_of_the_running_executable() {
        let resolved = resolve_host_program(None, Ok(PathBuf::from("/usr/local/bin/klaus")));
        let expected = if cfg!(windows) {
            "/usr/local/bin/klaus-code-mode-host.exe"
        } else {
            "/usr/local/bin/klaus-code-mode-host"
        };
        assert_eq!(resolved, PathBuf::from(expected));
    }

    #[test]
    fn an_unknown_executable_path_falls_back_to_a_bare_program_name() {
        let resolved = resolve_host_program(None, Err(std::io::Error::other("no current exe")));
        assert_eq!(resolved.parent(), Some(std::path::Path::new("")));
        assert!(
            resolved
                .to_string_lossy()
                .starts_with("klaus-code-mode-host"),
            "{resolved:?}"
        );
    }
}
