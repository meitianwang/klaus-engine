//! One host connection: spawn, negotiate, then run the driver plus its reader,
//! writer, and supervisor tasks.
//!
//! The connection is a fail-closed unit. Whichever of its parts stops first —
//! the host process, the reader, the writer, the driver — marks the whole
//! connection dead with that reason, and every later caller fails with the SAME
//! recorded reason rather than a generic one.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::engine::code_mode::host::{
    CapabilitySet, ClientHello, ClientToHost, EncodedFrame, FramedReader, FramedWriter,
    HostToClient, ProtocolVersion, RequestId, SupportedProtocolVersions, WireReplEvalRequest,
    WireReplOutcome,
};
use crate::engine::code_mode::protocol::{CellId, ExecuteRequest, WaitOutcome, WaitRequest};
use crate::engine::code_mode::session::{CodeModeSessionDelegate, StartedCell};
use crate::runtime_process::{RuntimeProcessContext, SpawnedRuntimeProcess};
use crate::runtime_values::RuntimeString;
use crate::sync_ext::MutexExt;

use super::driver::{ConnectionDriver, DriverLifecycle};
use super::driver_support::{DeliveredExecute, DriverCommand, DriverEvent};
pub(super) use super::driver_support::{RemoteSession, SessionCleanup};

#[cfg(test)]
#[path = "connection_tests.rs"]
mod tests;

const IPC_CHANNEL_CAPACITY: usize = 128;
const HOST_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Host spawn errors reach the model as tool output, so a configured path is
/// bounded while keeping the executable-bearing suffix that explains the
/// failure.
const MAX_DISPLAYED_HOST_PROGRAM_BYTES: usize = 512;
const TRUNCATED_HOST_PROGRAM_PREFIX: &str = "...";

#[derive(Debug)]
pub(super) enum ConnectionError {
    Spawn {
        host_program: PathBuf,
        message: String,
        not_found: bool,
    },
    Other(String),
}

impl ConnectionError {
    /// Whether the host program itself is missing, which a provider may answer
    /// by falling back to the in-process carrier.
    pub(super) fn host_program_not_found(&self) -> bool {
        matches!(self, Self::Spawn { not_found, .. } if *not_found)
    }
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Spawn {
                host_program,
                message,
                ..
            } => {
                let host_program = host_program.to_string_lossy();
                if host_program.len() <= MAX_DISPLAYED_HOST_PROGRAM_BYTES {
                    return write!(
                        formatter,
                        "failed to spawn code-mode host {host_program}: {message}"
                    );
                }

                let mut suffix_start = host_program.len()
                    - (MAX_DISPLAYED_HOST_PROGRAM_BYTES - TRUNCATED_HOST_PROGRAM_PREFIX.len());
                while !host_program.is_char_boundary(suffix_start) {
                    suffix_start += 1;
                }

                write!(
                    formatter,
                    "failed to spawn code-mode host {TRUNCATED_HOST_PROGRAM_PREFIX}{}: {message}",
                    &host_program[suffix_start..]
                )
            }
            Self::Other(message) => formatter.write_str(message),
        }
    }
}

pub(super) struct Connection {
    command_tx: mpsc::Sender<DriverCommand>,
    execute_claim_tx: mpsc::UnboundedSender<RequestId>,
    alive: Arc<AtomicBool>,
    failure: Arc<StdMutex<Option<RuntimeString>>>,
    cancellation: CancellationToken,
}

/// Cancels its request if the caller's future is dropped before the answer
/// arrives; disarmed once the answer is in hand.
struct CallerCancellation {
    token: CancellationToken,
    armed: bool,
}

impl CallerCancellation {
    fn new() -> Self {
        Self {
            token: CancellationToken::new(),
            armed: true,
        }
    }

    fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for CallerCancellation {
    fn drop(&mut self) {
        if self.armed {
            self.token.cancel();
        }
    }
}

impl Connection {
    pub(super) async fn spawn(
        process_context: &RuntimeProcessContext,
        host_program: &Path,
    ) -> Result<Self, ConnectionError> {
        let mut child = process_context
            .spawn_code_mode_host(host_program)
            .await
            .map_err(|error| ConnectionError::Spawn {
                host_program: host_program.to_path_buf(),
                message: error.host_message(),
                not_found: error.is_missing_program(),
            })?;

        if let Some(stderr) = child.take_stderr() {
            // The host's stderr is diagnostic only; drain it so a chatty host
            // cannot block on a full pipe.
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(_line)) = lines.next_line().await {}
            });
        }

        let stdin = child
            .take_stdin()
            .ok_or_else(|| ConnectionError::Other("spawned code-mode host has no stdin".into()))?;
        let stdout = child
            .take_stdout()
            .ok_or_else(|| ConnectionError::Other("spawned code-mode host has no stdout".into()))?;

        Self::establish(
            FramedReader::new(stdout),
            FramedWriter::new(stdin),
            ConnectionOwner::Process(Box::new(child)),
        )
        .await
    }

    /// Establish a connection over an arbitrary duplex transport with no owned
    /// process behind it, so the driver can be exercised against a scripted
    /// peer.
    #[cfg(test)]
    pub(super) async fn establish_for_tests<R, W>(
        reader: R,
        writer: W,
    ) -> Result<Self, ConnectionError>
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
        W: tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        Self::establish(
            FramedReader::new(reader),
            FramedWriter::new(writer),
            ConnectionOwner::Detached,
        )
        .await
    }

    /// Negotiate the protocol, then start the tasks that own the connection.
    async fn establish<R, W>(
        mut reader: FramedReader<R>,
        mut writer: FramedWriter<W>,
        mut owner: ConnectionOwner,
    ) -> Result<Self, ConnectionError>
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
        W: tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let handshake = async {
            let hello = ClientHello::new(
                SupportedProtocolVersions::try_new([ProtocolVersion::V1])
                    .map_err(|error| error.to_string())?,
                CapabilitySet::empty(),
                CapabilitySet::empty(),
            )
            .map_err(|error| error.to_string())?;
            writer
                .write(&ClientToHost::ClientHello(hello))
                .await
                .map_err(|error| format!("failed to write code-mode host hello: {error}"))?;
            match reader
                .read()
                .await
                .map_err(|error| format!("failed to read code-mode host hello: {error}"))?
            {
                Some(HostToClient::HostHello(hello))
                    if hello.selected_version() == ProtocolVersion::V1 =>
                {
                    Ok(())
                }
                Some(HostToClient::HandshakeRejected { reason }) => {
                    Err(format!("code-mode host rejected the handshake: {reason:?}"))
                }
                Some(message) => Err(format!(
                    "code-mode host returned an invalid handshake response: {message:?}"
                )),
                None => Err("code-mode host exited during handshake".to_string()),
            }
        };
        let handshake_result = match tokio::time::timeout(HOST_HANDSHAKE_TIMEOUT, handshake).await {
            Ok(result) => result,
            Err(_) => {
                owner.close().await;
                return Err(ConnectionError::Other(
                    "timed out negotiating with the code-mode host".into(),
                ));
            }
        };
        if let Err(error) = handshake_result {
            owner.close().await;
            return Err(ConnectionError::Other(error));
        }

        let (command_tx, command_rx) = mpsc::channel(IPC_CHANNEL_CAPACITY);
        let (event_tx, event_rx) = mpsc::channel(IPC_CHANNEL_CAPACITY);
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<EncodedFrame>(IPC_CHANNEL_CAPACITY);
        let cancellation = CancellationToken::new();
        let alive = Arc::new(AtomicBool::new(true));
        let failure = Arc::new(StdMutex::new(None));

        let writer_cancellation = cancellation.clone();
        let writer_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = writer_cancellation.cancelled() => return Ok(()),
                    frame = outgoing_rx.recv() => {
                        let Some(frame) = frame else {
                            return Err(RuntimeString::from("code-mode host outgoing stream closed"));
                        };
                        let result = tokio::select! {
                            _ = writer_cancellation.cancelled() => return Ok(()),
                            result = writer.write_frame(&frame) => result,
                        };
                        if let Err(error) = result {
                            return Err(RuntimeString::from(format!(
                                "failed to write code-mode host message: {error}"
                            )));
                        }
                    }
                }
            }
        });

        let reader_events = event_tx.clone();
        let reader_cancellation = cancellation.clone();
        let reader_task = tokio::spawn(async move {
            loop {
                let message = tokio::select! {
                    _ = reader_cancellation.cancelled() => return Ok(()),
                    result = reader.read() => result,
                };
                let message = match message {
                    Ok(Some(message)) => message,
                    Ok(None) => {
                        return Err(RuntimeString::from("code-mode host closed its output"))
                    }
                    Err(error) => {
                        return Err(RuntimeString::from(format!(
                            "failed to read code-mode host message: {error}"
                        )))
                    }
                };
                reader_events
                    .send(DriverEvent::HostMessage(message))
                    .await
                    .map_err(|_| RuntimeString::from("code-mode connection driver closed"))?;
            }
        });

        let (driver, execute_claim_tx) = ConnectionDriver::new(
            command_rx,
            event_rx,
            event_tx.clone(),
            outgoing_tx,
            DriverLifecycle {
                alive: Arc::clone(&alive),
                failure: Arc::clone(&failure),
                cancellation: cancellation.clone(),
            },
        );
        let driver_task = tokio::spawn(driver.run());
        tokio::spawn(
            ConnectionSupervisor {
                owner,
                event_tx,
                cancellation: cancellation.clone(),
                alive: Arc::clone(&alive),
                failure: Arc::clone(&failure),
                driver_task,
                reader_task,
                writer_task,
            }
            .run(),
        );

        Ok(Self {
            command_tx,
            execute_claim_tx,
            alive,
            failure,
            cancellation,
        })
    }

    pub(super) fn is_alive(&self) -> bool {
        if self.command_tx.is_closed() {
            mark_connection_dead(
                &self.alive,
                &self.failure,
                RuntimeString::from("code-mode connection driver closed"),
            );
        }
        self.alive.load(Ordering::Acquire)
    }

    pub(super) async fn open_session(
        &self,
        session: RemoteSession,
        delegate: Arc<dyn CodeModeSessionDelegate>,
    ) -> Result<SessionCleanup, RuntimeString> {
        let cleanup = SessionCleanup::new();
        let cancellation = CallerCancellation::new();
        let (response_tx, response_rx) = oneshot::channel();
        self.send(DriverCommand::OpenSession {
            session,
            delegate,
            cleanup: cleanup.clone(),
            caller_cancellation: cancellation.token(),
            response_tx,
        })
        .await?;
        let result = self.receive(response_rx).await;
        cancellation.disarm();
        result?;
        Ok(cleanup)
    }

    /// Start a cell. The started cell is claimed only after the caller has it,
    /// so a caller that vanishes mid-handoff cannot leak a running cell.
    pub(super) async fn execute(
        &self,
        session: RemoteSession,
        request: ExecuteRequest,
    ) -> Result<StartedCell, RuntimeString> {
        let cancellation = CallerCancellation::new();
        let (response_tx, response_rx) = oneshot::channel();
        self.send(DriverCommand::Execute {
            session,
            request,
            caller_cancellation: cancellation.token(),
            response_tx,
        })
        .await?;
        let delivered: DeliveredExecute = match self.receive(response_rx).await {
            Ok(delivered) => delivered,
            Err(error) => {
                cancellation.disarm();
                return Err(error);
            }
        };
        self.execute_claim_tx
            .send(delivered.request_id)
            .map_err(|_| self.failure_message())?;
        cancellation.disarm();
        Ok(delivered.started)
    }

    pub(super) async fn wait(
        &self,
        session: RemoteSession,
        request: WaitRequest,
    ) -> Result<WaitOutcome, RuntimeString> {
        let cancellation = CallerCancellation::new();
        let (response_tx, response_rx) = oneshot::channel();
        self.send(DriverCommand::Wait {
            session,
            request,
            caller_cancellation: cancellation.token(),
            response_tx,
        })
        .await?;
        let result = self.receive(response_rx).await;
        cancellation.disarm();
        result
    }

    pub(super) async fn terminate(
        &self,
        session: RemoteSession,
        cell_id: CellId,
    ) -> Result<WaitOutcome, RuntimeString> {
        let (response_tx, response_rx) = oneshot::channel();
        self.send(DriverCommand::Terminate {
            session,
            cell_id,
            response_tx,
        })
        .await?;
        self.receive(response_rx).await
    }

    /// Run one REPL evaluation on the host and await its settled outcome.
    pub(super) async fn repl_eval(
        &self,
        session: RemoteSession,
        request: WireReplEvalRequest,
    ) -> Result<WireReplOutcome, RuntimeString> {
        let cancellation = CallerCancellation::new();
        let (response_tx, response_rx) = oneshot::channel();
        self.send(DriverCommand::ReplEval {
            session,
            request,
            caller_cancellation: cancellation.token(),
            response_tx,
        })
        .await?;
        let result = self.receive(response_rx).await;
        cancellation.disarm();
        result
    }

    /// Ask the host to interrupt the running evaluation with this exec id.
    pub(super) async fn repl_terminate(
        &self,
        session: RemoteSession,
        exec_id: RuntimeString,
    ) -> Result<(), RuntimeString> {
        let (response_tx, response_rx) = oneshot::channel();
        self.send(DriverCommand::ReplTerminate {
            session,
            exec_id,
            response_tx,
        })
        .await?;
        self.receive(response_rx).await
    }

    pub(super) async fn shutdown_session(
        &self,
        session: RemoteSession,
    ) -> Result<(), RuntimeString> {
        let (response_tx, response_rx) = oneshot::channel();
        self.send(DriverCommand::ShutdownSession {
            session,
            response_tx,
        })
        .await?;
        self.receive(response_rx).await
    }

    async fn send(&self, command: DriverCommand) -> Result<(), RuntimeString> {
        if !self.is_alive() {
            return Err(self.failure_message());
        }
        self.command_tx
            .send(command)
            .await
            .map_err(|_| self.failure_message())
    }

    async fn receive<T>(
        &self,
        response_rx: oneshot::Receiver<Result<T, RuntimeString>>,
    ) -> Result<T, RuntimeString> {
        response_rx.await.map_err(|_| self.failure_message())?
    }

    fn failure_message(&self) -> RuntimeString {
        failure_message(&self.failure)
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        mark_connection_dead(
            &self.alive,
            &self.failure,
            RuntimeString::from("code-mode host connection closed"),
        );
        self.cancellation.cancel();
    }
}

/// Watches every part of the connection and records whichever stopped first.
struct ConnectionSupervisor {
    owner: ConnectionOwner,
    event_tx: mpsc::Sender<DriverEvent>,
    cancellation: CancellationToken,
    alive: Arc<AtomicBool>,
    failure: Arc<StdMutex<Option<RuntimeString>>>,
    driver_task: JoinHandle<()>,
    reader_task: JoinHandle<Result<(), RuntimeString>>,
    writer_task: JoinHandle<Result<(), RuntimeString>>,
}

impl ConnectionSupervisor {
    async fn run(mut self) {
        let mut owner_exited = false;
        let reason = tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => failure_message(&self.failure),
            result = &mut self.driver_task => match result {
                Ok(()) => RuntimeString::from("code-mode connection driver exited unexpectedly"),
                Err(error) => RuntimeString::from(format!(
                    "code-mode connection driver task failed: {error}"
                )),
            },
            result = &mut self.reader_task => task_failure("reader", result),
            result = &mut self.writer_task => task_failure("writer", result),
            reason = self.owner.wait() => {
                owner_exited = true;
                reason
            }
        };
        mark_connection_dead(&self.alive, &self.failure, reason.clone());
        let _ = self.event_tx.try_send(DriverEvent::Failed(reason));
        self.cancellation.cancel();
        if !owner_exited {
            self.owner.close().await;
        }
    }
}

enum ConnectionOwner {
    Process(Box<SpawnedRuntimeProcess>),
    /// No owned process: the transport itself is the connection's lifetime.
    #[cfg(test)]
    Detached,
}

impl ConnectionOwner {
    async fn wait(&mut self) -> RuntimeString {
        match self {
            Self::Process(child) => match child.wait().await {
                Ok(status) => {
                    RuntimeString::from(format!("code-mode host exited with status {status:?}"))
                }
                Err(error) => {
                    RuntimeString::from(format!("failed waiting for code-mode host: {error:?}"))
                }
            },
            #[cfg(test)]
            Self::Detached => std::future::pending().await,
        }
    }

    async fn close(&mut self) {
        match self {
            Self::Process(child) => {
                child
                    .controller()
                    .signal(crate::runtime_process::RuntimeSignal::Kill);
                let _ = child.wait().await;
            }
            #[cfg(test)]
            Self::Detached => {}
        }
    }
}

fn task_failure(
    task_name: &str,
    result: Result<Result<(), RuntimeString>, tokio::task::JoinError>,
) -> RuntimeString {
    match result {
        Ok(Ok(())) => RuntimeString::from(format!(
            "code-mode connection {task_name} exited unexpectedly"
        )),
        Ok(Err(error)) => error,
        Err(error) => RuntimeString::from(format!(
            "code-mode connection {task_name} task failed: {error}"
        )),
    }
}

fn mark_connection_dead(
    alive: &AtomicBool,
    failure: &StdMutex<Option<RuntimeString>>,
    reason: RuntimeString,
) {
    alive.store(false, Ordering::Release);
    let mut failure = failure.lock_recover();
    if failure.is_none() {
        *failure = Some(reason);
    }
}

fn failure_message(failure: &StdMutex<Option<RuntimeString>>) -> RuntimeString {
    failure
        .lock_recover()
        .clone()
        .unwrap_or_else(|| RuntimeString::from("code-mode host connection closed"))
}
