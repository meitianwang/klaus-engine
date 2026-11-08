use super::protocol;
use super::protocol::{
    runtime_path_uri_to_host_path, ExecParams, ProcessSignal, ReadParams, ReadResponse,
    TerminateResponse, WriteParams, WriteResponse,
};
use crate::command_runtime::process as local;
use crate::fs::node_path::{runtime_path_from_native, runtime_string_from_native};
use crate::runtime_process::RuntimeProcessContext;
use crate::runtime_values::RuntimeString;
use crate::sandbox::runtime_policy::RuntimePolicySandbox;
use crate::types::permission::SandboxMode;
use crate::types::sandbox::{ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec};
use async_trait::async_trait;
use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use tokio::sync::{broadcast, watch};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecServerErrorKind {
    InvalidRequest,
    InvalidParams,
    MethodNotFound,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecServerError {
    pub kind: ExecServerErrorKind,
    pub message: String,
}

impl ExecServerError {
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self {
            kind: ExecServerErrorKind::InvalidRequest,
            message: message.into(),
        }
    }

    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            kind: ExecServerErrorKind::InvalidParams,
            message: message.into(),
        }
    }

    pub fn method_not_found(message: impl Into<String>) -> Self {
        Self {
            kind: ExecServerErrorKind::MethodNotFound,
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            kind: ExecServerErrorKind::Internal,
            message: message.into(),
        }
    }

    pub fn json_rpc_code(&self) -> i64 {
        match self.kind {
            ExecServerErrorKind::InvalidRequest => -32600,
            ExecServerErrorKind::MethodNotFound => -32601,
            ExecServerErrorKind::InvalidParams => -32602,
            ExecServerErrorKind::Internal => -32603,
        }
    }
}

impl fmt::Display for ExecServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ExecServerError {}

impl From<String> for ExecServerError {
    fn from(value: String) -> Self {
        Self::internal(value)
    }
}

impl From<&str> for ExecServerError {
    fn from(value: &str) -> Self {
        Self::internal(value)
    }
}

pub type ExecServerResult<T> = Result<T, ExecServerError>;

pub struct StartedExecProcess {
    pub process: Arc<dyn ExecProcess>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecProcessEvent {
    Output(protocol::ProcessOutputChunk),
    Exited {
        seq: u64,
        exit_code: Option<i32>,
        sandbox_denied: Option<bool>,
    },
    Closed {
        seq: u64,
    },
    Failed(String),
}

pub struct ExecProcessEventReceiver {
    replay: VecDeque<ExecProcessEvent>,
    local_rx: Option<local::ProcessEventReceiver>,
    live_rx: Option<broadcast::Receiver<ExecProcessEvent>>,
    _keepalive: Option<broadcast::Sender<ExecProcessEvent>>,
}

impl ExecProcessEventReceiver {
    pub fn empty() -> Self {
        let (live_tx, live_rx) = broadcast::channel(1);
        Self {
            replay: VecDeque::new(),
            local_rx: None,
            live_rx: Some(live_rx),
            _keepalive: Some(live_tx),
        }
    }

    fn from_local(local_rx: local::ProcessEventReceiver) -> Self {
        Self {
            replay: VecDeque::new(),
            local_rx: Some(local_rx),
            live_rx: None,
            _keepalive: None,
        }
    }

    pub async fn recv(&mut self) -> Result<ExecProcessEvent, broadcast::error::RecvError> {
        if let Some(event) = self.replay.pop_front() {
            return Ok(event);
        }
        if let Some(local_rx) = &mut self.local_rx {
            return local_rx.recv().await.map(map_local_event);
        }
        let Some(live_rx) = &mut self.live_rx else {
            return Err(broadcast::error::RecvError::Closed);
        };
        live_rx.recv().await
    }
}

#[async_trait]
pub trait ExecProcess: Send + Sync {
    fn process_id(&self) -> &str;

    fn subscribe_wake(&self) -> watch::Receiver<u64>;

    fn subscribe_events(&self) -> ExecProcessEventReceiver;

    async fn read(&self, params: ReadParams) -> ExecServerResult<ReadResponse>;

    async fn write(&self, params: WriteParams) -> ExecServerResult<WriteResponse>;

    async fn signal(&self, signal: ProcessSignal) -> ExecServerResult<()>;

    async fn terminate(&self) -> ExecServerResult<TerminateResponse>;
}

#[async_trait]
pub trait ExecBackend: Send + Sync {
    async fn start(&self, params: ExecParams) -> ExecServerResult<StartedExecProcess>;

    fn get_process(&self, _process_id: &str) -> Option<Arc<dyn ExecProcess>> {
        None
    }
}

#[derive(Clone)]
pub struct LocalExecBackend {
    process_context: RuntimeProcessContext,
    registry: Arc<local::LocalExecProcessRegistry>,
}

impl LocalExecBackend {
    pub fn new(process_context: RuntimeProcessContext) -> Self {
        Self {
            process_context,
            registry: Arc::new(local::LocalExecProcessRegistry::new()),
        }
    }

    pub fn with_registry(
        process_context: RuntimeProcessContext,
        registry: Arc<local::LocalExecProcessRegistry>,
    ) -> Self {
        Self {
            process_context,
            registry,
        }
    }
}

/// Enforce the broker route while leaving filesystem writes unrestricted.
struct ManagedNetworkOnlySandbox {
    inner: Arc<dyn SandboxBackend>,
}

impl SandboxBackend for ManagedNetworkOnlySandbox {
    fn kind(&self) -> SandboxKind {
        self.inner.kind()
    }
    fn is_available(&self) -> bool {
        self.inner.is_available()
    }
    fn enforces_planning_read_only(&self) -> bool {
        self.inner.enforces_planning_read_only()
    }
    fn task_resource_scope(
        &self,
    ) -> Option<Arc<crate::permission::task_scope::CanonicalTaskResourceScope>> {
        self.inner.task_resource_scope()
    }
    fn task_runtime_program(
        &self,
        program: &crate::runtime_values::RuntimePath,
    ) -> Option<Arc<dyn SandboxBackend>> {
        self.inner
            .task_runtime_program(program)
            .map(|inner| Arc::new(Self { inner }) as Arc<dyn SandboxBackend>)
    }
    fn transform(&self, request: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
        let mut policy = policy.clone();
        policy.unconfined_writes = Some(true);
        self.inner.transform(request, &policy)
    }
}

struct LocalExecProcess {
    registry: Arc<local::LocalExecProcessRegistry>,
    process_id: String,
    runtime_process_id: RuntimeString,
}

#[async_trait]
impl ExecBackend for LocalExecBackend {
    async fn start(&self, params: ExecParams) -> ExecServerResult<StartedExecProcess> {
        if params.argv.is_empty() {
            return Err(ExecServerError::invalid_params("argv must not be empty"));
        }
        if params.enforce_managed_network && params.managed_network.is_none() {
            return Err(ExecServerError::invalid_params(
                "managed network enforcement requires managedNetwork context",
            ));
        }
        let process_id = params.process_id.clone();
        let runtime_process_id = RuntimeString::from(&params.process_id);
        let cwd_native = runtime_path_uri_to_host_path(&params.cwd)?;
        let cwd = runtime_path_from_native(&cwd_native);
        let (sandbox, writable_roots, network_allowed) = match &params.sandbox {
            Some(context) => {
                runtime_path_uri_to_host_path(&context.cwd)
                    .map_err(ExecServerError::invalid_params)?;
                let roots = context
                    .writable_roots
                    .iter()
                    .map(|path| {
                        runtime_path_uri_to_host_path(path)
                            .map(|path| runtime_string_from_native(path.as_os_str()))
                            .map_err(ExecServerError::invalid_params)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let mode = if roots.is_empty() {
                    SandboxMode::ReadOnly
                } else {
                    SandboxMode::WorkspaceWrite
                };
                let backend = crate::sandbox::create_sandbox_for_mode(mode)
                    .map_err(ExecServerError::internal)?;
                let sandbox = Arc::new(RuntimePolicySandbox::new(
                    backend,
                    mode,
                    roots.clone(),
                    context.network_allowed,
                    false,
                    false,
                )) as Arc<dyn SandboxBackend>;
                (sandbox, Some(roots), Some(context.network_allowed))
            }
            None if params.enforce_managed_network => {
                let backend = crate::sandbox::create_sandbox_for_mode(SandboxMode::WorkspaceWrite)
                    .map_err(ExecServerError::internal)?;
                (
                    Arc::new(ManagedNetworkOnlySandbox { inner: backend })
                        as Arc<dyn SandboxBackend>,
                    None,
                    Some(true),
                )
            }
            None => (
                crate::sandbox::create_sandbox_for_mode(SandboxMode::DangerFullAccess)
                    .map_err(ExecServerError::internal)?,
                None,
                Some(true),
            ),
        };
        self.registry
            .start(
                self.process_context.clone(),
                local::LocalProcessStartParams {
                    process_id: Some(runtime_process_id.clone()),
                    argv: params.argv,
                    cwd,
                    env: Some(params.env),
                    sandbox,
                    network_allowed,
                    managed_network: params.managed_network,
                    writable_roots,
                    tty: params.tty,
                    pipe_stdin: params.pipe_stdin,
                    ..Default::default()
                },
            )
            .await
            .map_err(runtime_internal_error)?;

        Ok(StartedExecProcess {
            process: Arc::new(LocalExecProcess {
                registry: Arc::clone(&self.registry),
                process_id,
                runtime_process_id,
            }),
        })
    }

    fn get_process(&self, process_id: &str) -> Option<Arc<dyn ExecProcess>> {
        let runtime_process_id = RuntimeString::from(process_id);
        self.registry.info(&runtime_process_id)?;
        Some(Arc::new(LocalExecProcess {
            registry: Arc::clone(&self.registry),
            process_id: process_id.to_string(),
            runtime_process_id,
        }))
    }
}

#[async_trait]
impl ExecProcess for LocalExecProcess {
    fn process_id(&self) -> &str {
        &self.process_id
    }

    fn subscribe_wake(&self) -> watch::Receiver<u64> {
        self.registry
            .subscribe_wake(&self.runtime_process_id)
            .unwrap_or_else(|_| watch::channel(0).1)
    }

    fn subscribe_events(&self) -> ExecProcessEventReceiver {
        self.registry
            .subscribe_events(&self.runtime_process_id)
            .map(ExecProcessEventReceiver::from_local)
            .unwrap_or_else(|_| ExecProcessEventReceiver::empty())
    }

    async fn read(&self, params: ReadParams) -> ExecServerResult<ReadResponse> {
        self.ensure_process_id(&params.process_id)?;
        self.registry
            .read(local::LocalProcessReadParams {
                process_id: RuntimeString::from(params.process_id),
                after_seq: params.after_seq,
                after_offset: None,
                max_bytes: params.max_bytes,
                wait_ms: params.wait_ms,
                signal: None,
            })
            .await
            .map(map_read_response)
            .map_err(runtime_internal_error)
    }

    async fn write(&self, params: WriteParams) -> ExecServerResult<WriteResponse> {
        self.ensure_process_id(&params.process_id)?;
        self.registry
            .write(local::LocalProcessWriteParams {
                process_id: RuntimeString::from(params.process_id),
                write_id: params.write_id,
                chunk: params.chunk.into_inner(),
                yield_ms: Some(250),
                signal: None,
            })
            .await
            .map(|response| WriteResponse {
                status: map_write_status(response.status),
            })
            .map_err(runtime_internal_error)
    }

    async fn signal(&self, signal: ProcessSignal) -> ExecServerResult<()> {
        self.registry
            .signal(&self.runtime_process_id, map_signal(signal))
            .await
            .map(|_| ())
            .map_err(runtime_internal_error)
    }

    async fn terminate(&self) -> ExecServerResult<TerminateResponse> {
        self.registry
            .terminate(&self.runtime_process_id)
            .await
            .map(|response| TerminateResponse {
                running: response.running,
            })
            .map_err(runtime_internal_error)
    }
}

impl LocalExecProcess {
    fn ensure_process_id(&self, process_id: &str) -> ExecServerResult<()> {
        if process_id == self.process_id {
            Ok(())
        } else {
            Err(ExecServerError::invalid_params(format!(
                "process id mismatch: expected {}, got {process_id}",
                self.process_id
            )))
        }
    }
}

fn runtime_internal_error(message: RuntimeString) -> ExecServerError {
    ExecServerError::internal(
        message
            .try_to_utf8()
            .unwrap_or_else(|_| format!("{message:?}")),
    )
}

fn map_read_response(response: local::LocalProcessReadResponse) -> ReadResponse {
    ReadResponse {
        chunks: response.chunks.into_iter().map(map_output_chunk).collect(),
        next_seq: response.next_seq,
        exited: response.exited,
        exit_code: response.exit_code,
        closed: response.closed,
        failure: response.failure,
        sandbox_denied: false,
    }
}

fn map_output_chunk(chunk: local::ProcessOutputChunk) -> protocol::ProcessOutputChunk {
    protocol::ProcessOutputChunk {
        seq: chunk.seq,
        stream: map_output_stream(chunk.stream),
        chunk: chunk.chunk.into(),
    }
}

fn map_output_stream(stream: local::ExecOutputStream) -> protocol::ExecOutputStream {
    match stream {
        local::ExecOutputStream::Stdout => protocol::ExecOutputStream::Stdout,
        local::ExecOutputStream::Stderr => protocol::ExecOutputStream::Stderr,
        local::ExecOutputStream::Pty => protocol::ExecOutputStream::Pty,
    }
}

fn map_write_status(status: local::WriteStatus) -> protocol::WriteStatus {
    match status {
        local::WriteStatus::Accepted => protocol::WriteStatus::Accepted,
        local::WriteStatus::UnknownProcess => protocol::WriteStatus::UnknownProcess,
        local::WriteStatus::Starting => protocol::WriteStatus::Starting,
        local::WriteStatus::StdinClosed => protocol::WriteStatus::StdinClosed,
    }
}

fn map_signal(signal: ProcessSignal) -> local::ProcessSignal {
    match signal {
        ProcessSignal::Interrupt => local::ProcessSignal::Interrupt,
    }
}

fn map_local_event(event: local::ProcessEvent) -> ExecProcessEvent {
    match event {
        local::ProcessEvent::Output(chunk) => ExecProcessEvent::Output(map_output_chunk(chunk)),
        local::ProcessEvent::Exited { seq, exit_code } => ExecProcessEvent::Exited {
            seq,
            exit_code,
            sandbox_denied: None,
        },
        local::ProcessEvent::Closed { seq } => ExecProcessEvent::Closed { seq },
        local::ProcessEvent::Failed(message) => ExecProcessEvent::Failed(
            String::from_utf8(message.to_node_utf8())
                .expect("Node UTF-8 projection always produces valid scalar bytes"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command_runtime::exec_server::protocol::{
        runtime_path_uri_from_host_native_path, ExecOutputStream, ExecParams, ProcessSignal,
        ReadParams, WriteParams, WriteStatus,
    };
    use crate::runtime_process::RuntimeEnv;
    use tokio::time::{timeout, Duration};

    fn exec_params(process_id: &str, command: &str, pipe_stdin: bool) -> ExecParams {
        ExecParams {
            process_id: process_id.to_string(),
            argv: vec!["sh".into(), "-lc".into(), command.into()],
            cwd: runtime_path_uri_from_host_native_path(std::env::current_dir().unwrap()).unwrap(),
            env_policy: None,
            env: RuntimeEnv::new(),
            tty: false,
            pipe_stdin,
            arg0: None,
            sandbox: None,
            enforce_managed_network: false,
            managed_network: None,
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn forwarded_read_only_sandbox_never_starts_an_unconfined_write() {
        let root =
            std::env::temp_dir().join(format!("klaus-forwarded-sandbox-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let backend = LocalExecBackend::new(crate::runtime_process::test_runtime_process_context());
        let mut params = exec_params("forwarded-read-only", "printf escaped > escaped.txt", false);
        params.cwd = runtime_path_uri_from_host_native_path(&root).unwrap();
        params.sandbox = Some(protocol::FileSystemSandboxContext {
            cwd: params.cwd.clone(),
            writable_roots: Vec::new(),
            network_allowed: true,
        });
        match backend.start(params).await {
            Ok(started) => {
                let mut events = started.process.subscribe_events();
                timeout(Duration::from_secs(5), async {
                    loop {
                        if matches!(
                            events.recv().await.unwrap(),
                            ExecProcessEvent::Exited { .. }
                        ) {
                            break;
                        }
                    }
                })
                .await
                .expect("the confined command terminates");
            }
            Err(error) => assert_eq!(error.kind, ExecServerErrorKind::Internal),
        }
        assert!(
            !root.join("escaped.txt").exists(),
            "a forwarded sandbox must enforce read-only writes or refuse startup"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn local_backend_drives_a_real_process_through_the_trait() {
        let backend = LocalExecBackend::new(crate::runtime_process::test_runtime_process_context());
        let started = backend
            .start(exec_params("trait-cat", "cat", true))
            .await
            .unwrap();
        let process = started.process;

        assert_eq!(process.process_id(), "trait-cat");
        assert_eq!(
            process
                .write(WriteParams {
                    process_id: "trait-cat".to_string(),
                    write_id: "w1".into(),
                    chunk: b"hello from trait\n".to_vec().into(),
                })
                .await
                .unwrap()
                .status,
            WriteStatus::Accepted
        );

        let read = process
            .read(ReadParams {
                process_id: "trait-cat".to_string(),
                after_seq: Some(0),
                max_bytes: None,
                wait_ms: Some(1000),
            })
            .await
            .unwrap();
        assert_eq!(read.chunks.len(), 1);
        assert_eq!(read.chunks[0].stream, ExecOutputStream::Stdout);
        assert_eq!(
            read.chunks[0].chunk.clone().into_inner(),
            b"hello from trait\n"
        );

        process.terminate().await.unwrap();
    }

    #[tokio::test]
    async fn local_backend_preserves_raw_argv_boundaries_without_shell_quoting() {
        let backend = LocalExecBackend::new(crate::runtime_process::test_runtime_process_context());
        let mut params = exec_params("raw-argv", "unused", false);
        params.argv = vec![
            "printf".into(),
            "%s".into(),
            "argument with spaces;$(never-expanded)".into(),
        ];
        let process = backend.start(params).await.unwrap().process;
        let read = process
            .read(ReadParams {
                process_id: "raw-argv".to_string(),
                after_seq: Some(0),
                max_bytes: None,
                wait_ms: Some(1_000),
            })
            .await
            .unwrap();

        assert_eq!(read.chunks.len(), 1);
        assert_eq!(
            read.chunks[0].chunk.clone().into_inner(),
            b"argument with spaces;$(never-expanded)"
        );
        assert!(read.exited);
    }

    #[tokio::test]
    async fn local_backend_fails_closed_when_managed_network_is_required_without_context() {
        let backend = LocalExecBackend::new(crate::runtime_process::test_runtime_process_context());
        let mut params = exec_params("managed-network-missing", "echo should-not-run", false);
        params.enforce_managed_network = true;
        params.managed_network = None;

        let err = match backend.start(params).await {
            Ok(_) => panic!("managed network without context must fail closed"),
            Err(err) => err,
        };

        assert_eq!(err.kind, ExecServerErrorKind::InvalidParams);
        assert!(
            err.message.contains("managed network"),
            "unexpected error: {}",
            err.message
        );
    }

    #[tokio::test]
    async fn local_backend_replays_process_events_to_new_subscribers() {
        let backend = LocalExecBackend::new(crate::runtime_process::test_runtime_process_context());
        let started = backend
            .start(exec_params("trait-events", "printf event-ready", false))
            .await
            .unwrap();
        let process = started.process;

        let read = process
            .read(ReadParams {
                process_id: "trait-events".to_string(),
                after_seq: Some(0),
                max_bytes: None,
                // Generous budget: a real child process on a loaded test host
                // can take well over a second to spawn and exit; the wait only
                // ever runs long when the backend truly hangs.
                wait_ms: Some(30_000),
            })
            .await
            .unwrap();
        assert!(read.exited);

        let mut events = process.subscribe_events();
        let event = timeout(Duration::from_secs(30), events.recv())
            .await
            .expect("event replay timed out")
            .expect("event replay should be available");
        match event {
            ExecProcessEvent::Output(chunk) => {
                assert_eq!(chunk.chunk.into_inner(), b"event-ready");
            }
            other => panic!("expected output replay, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn signal_and_terminate_are_idempotent_on_exited_processes() {
        let backend = LocalExecBackend::new(crate::runtime_process::test_runtime_process_context());
        let started = backend
            .start(exec_params("trait-done", "printf done", false))
            .await
            .unwrap();
        let process = started.process;

        let read = process
            .read(ReadParams {
                process_id: "trait-done".to_string(),
                after_seq: Some(0),
                max_bytes: None,
                wait_ms: Some(1000),
            })
            .await
            .unwrap();
        assert!(read.exited);

        process.signal(ProcessSignal::Interrupt).await.unwrap();
        process.terminate().await.unwrap();
    }
}

#[cfg(test)]
mod managed_network_policy_tests {
    use super::*;
    use crate::runtime_values::RuntimePath;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Capture(Mutex<Option<SandboxPolicy>>);

    impl SandboxBackend for Capture {
        fn kind(&self) -> SandboxKind {
            SandboxKind::Seatbelt
        }
        fn is_available(&self) -> bool {
            true
        }
        fn transform(&self, request: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
            *self.0.lock().unwrap() = Some(policy.clone());
            SpawnSpec {
                file: "true".into(),
                args: Vec::new(),
                cwd: request.cwd.clone(),
                env: Default::default(),
            }
        }
    }

    #[test]
    fn network_only_policy_retains_broker_routes_and_allows_full_filesystem_writes() {
        let capture = Arc::new(Capture::default());
        let wrapper = ManagedNetworkOnlySandbox {
            inner: capture.clone(),
        };
        let incoming = SandboxPolicy {
            network_allowed: false,
            allow_loopback_ports: Some(vec![32123]),
            allow_unix_sockets: Some(vec!["/broker/socket".into()]),
            windows_network_proxy_restricting_sid: Some("route-sid".into()),
            windows_network_allow_local_binding: Some(false),
            ..Default::default()
        };
        wrapper.transform(
            &ExecRequest {
                argv: vec!["true".into()],
                cwd: RuntimePath::from("/outside-workspace"),
                env: None,
                timeout_ms: None,
            },
            &incoming,
        );
        let mut expected = incoming;
        expected.unconfined_writes = Some(true);
        assert_eq!(*capture.0.lock().unwrap(), Some(expected));
    }
}
