use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

use klaus_agent_core::runtime_process::{
    RuntimeBrowserBrokerRead, RuntimeBrowserBrokerWrite, RuntimeBrowserChildControl,
    RuntimeBrowserChildDuplex, RuntimeBrowserChildProcess, RuntimeChildStderr, RuntimeChildStdout,
    RuntimeExecFileStatus,
};
use klaus_agent_core::runtime_values::{RuntimeJson, RuntimeObject, RuntimeString};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, watch, Mutex, Notify};
use tokio::task::{AbortHandle, JoinHandle};

use super::broker_wire::{
    BrowserMuxDirection, BrowserMuxFrame, BrowserMuxKind, BrowserMuxPayload,
    BrowserMuxSequenceClass, BrowserMuxState,
};

const MUX_HEADER_LEN: usize = 40;
const MAX_MUX_REMAINING: usize = 256 * 1024 * 1024;
const NODE_READ_EVENT_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BrowserMuxChildKind {
    Playwright,
    Ffmpeg,
}

pub(super) struct BrowserMuxRuntime {
    handle: BrowserMuxHandle,
    reader_task: JoinHandle<()>,
    writer_task: JoinHandle<()>,
}

impl BrowserMuxRuntime {
    pub(super) fn start(
        generation: u64,
        inbound: RuntimeBrowserBrokerRead,
        outbound: RuntimeBrowserBrokerWrite,
        fatal_tx: watch::Sender<Option<RuntimeString>>,
    ) -> Self {
        let (outgoing_tx, outgoing_rx) = mpsc::channel(64);
        let handle = BrowserMuxHandle {
            inner: Arc::new(BrowserMuxInner {
                generation,
                protocol: Mutex::new(BrowserMuxProtocol {
                    wire: BrowserMuxState::new(generation),
                    outgoing_sequences: HashMap::new(),
                }),
                objects: Mutex::new(HashMap::new()),
                shutdown: StdMutex::new(HashMap::new()),
                outgoing_tx,
                fatal_tx,
                state_changed: Notify::new(),
            }),
        };
        let reader_task = tokio::spawn(run_mux_reader(handle.clone(), inbound));
        let writer_task = tokio::spawn(run_mux_writer(handle.inner.clone(), outbound, outgoing_rx));
        Self {
            handle,
            reader_task,
            writer_task,
        }
    }

    pub(super) fn handle(&self) -> BrowserMuxHandle {
        self.handle.clone()
    }

    pub(super) async fn begin_orderly_shutdown(&self) -> Result<(), RuntimeString> {
        self.handle.begin_orderly_shutdown().await
    }

    pub(super) fn begin_immediate_shutdown(&self) {
        self.handle.begin_immediate_shutdown();
    }
}

impl Drop for BrowserMuxRuntime {
    fn drop(&mut self) {
        self.reader_task.abort();
        self.writer_task.abort();
        let mut shutdown = self
            .handle
            .inner
            .shutdown
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (_, object) in shutdown.drain() {
            if let Some(control) = object.control {
                control.hard_kill();
            }
            for task in object.tasks {
                task.abort();
            }
        }
    }
}

#[derive(Clone)]
pub(super) struct BrowserMuxHandle {
    inner: Arc<BrowserMuxInner>,
}

struct BrowserMuxInner {
    generation: u64,
    protocol: Mutex<BrowserMuxProtocol>,
    objects: Mutex<HashMap<u64, BrowserMuxObject>>,
    shutdown: StdMutex<HashMap<u64, BrowserMuxShutdown>>,
    outgoing_tx: mpsc::Sender<Vec<u8>>,
    fatal_tx: watch::Sender<Option<RuntimeString>>,
    state_changed: Notify,
}

struct BrowserMuxProtocol {
    wire: BrowserMuxState,
    outgoing_sequences: HashMap<(u64, u16, usize), u64>,
}

struct BrowserMuxObject {
    pid: Option<u32>,
    kind: BrowserMuxChildKind,
    close_hooks_ready: bool,
    graceful_sequence: Option<u64>,
    force_sequence: Option<u64>,
    pending_cleanup_close: Option<u64>,
    activation: watch::Sender<bool>,
    read_commands: HashMap<u16, mpsc::Sender<ReadCommand>>,
    write_commands: HashMap<u16, mpsc::Sender<WriteCommand>>,
}

struct BrowserMuxShutdown {
    control: Option<RuntimeBrowserChildControl>,
    tasks: Vec<AbortHandle>,
}

enum ReadCommand {
    Credit,
    Close(u64),
    Stop,
}

enum WriteCommand {
    Write { sequence: u64, bytes: Vec<u8> },
    HalfClose { sequence: u64 },
    Close,
}

impl BrowserMuxHandle {
    pub(super) async fn register_child(
        &self,
        object_id: u64,
        mut child: RuntimeBrowserChildProcess,
        kind: BrowserMuxChildKind,
    ) -> Result<oneshot::Receiver<()>, RuntimeString> {
        {
            let mut protocol = self.inner.protocol.lock().await;
            protocol
                .wire
                .register_committed_object(object_id)
                .map_err(|error| mux_error("register committed object", error))?;
        }

        let control = child.control();
        let pid = child.pid();
        let (activation, activation_rx) = watch::channel(false);
        let mut read_commands = HashMap::new();
        let mut write_commands = HashMap::new();
        let mut stream_tasks = Vec::new();

        if kind == BrowserMuxChildKind::Playwright {
            let stdout = child.take_stdout().ok_or_else(|| {
                RuntimeString::from("Browser Playwright child stdout was not captured.")
            })?;
            let (sender, task) = self.spawn_read_stream(
                object_id,
                1,
                BrowserMuxKind::StdoutData,
                stdout,
                activation_rx.clone(),
            );
            read_commands.insert(1, sender);
            stream_tasks.push(task);
        }

        let stderr = child
            .take_stderr()
            .ok_or_else(|| RuntimeString::from("Browser child stderr was not captured."))?;
        let (sender, task) = self.spawn_read_stream(
            object_id,
            2,
            BrowserMuxKind::StderrData,
            stderr,
            activation_rx.clone(),
        );
        read_commands.insert(2, sender);
        stream_tasks.push(task);

        if kind == BrowserMuxChildKind::Playwright {
            let extra3 = child.take_extra3().ok_or_else(|| {
                RuntimeString::from("Browser Playwright child fd 3 was not captured.")
            })?;
            let (read_sender, write_sender, tasks) =
                self.spawn_duplex_stream(object_id, 3, extra3, activation_rx.clone());
            read_commands.insert(3, read_sender);
            write_commands.insert(3, write_sender);
            stream_tasks.extend(tasks);

            let extra4 = child.take_extra4().ok_or_else(|| {
                RuntimeString::from("Browser Playwright child fd 4 was not captured.")
            })?;
            let (read_sender, write_sender, tasks) =
                self.spawn_duplex_stream(object_id, 4, extra4, activation_rx.clone());
            read_commands.insert(4, read_sender);
            write_commands.insert(4, write_sender);
            stream_tasks.extend(tasks);
        }

        let (completion_tx, completion_rx) = oneshot::channel();
        let handle = self.clone();
        let wait_stream_tasks = stream_tasks.clone();
        let wait_task = tokio::spawn(async move {
            let mut activation_rx = activation_rx;
            if activation_rx.wait_for(|active| *active).await.is_err() {
                return;
            }
            let status = child.wait().await;
            let _ = completion_tx.send(());
            match status {
                Ok(status) => {
                    if let Err(message) = handle
                        .finish_committed_object(object_id, status, wait_stream_tasks)
                        .await
                    {
                        handle.fail(message);
                    }
                }
                Err(error) => handle.fail(RuntimeString::from(format!(
                    "Browser child wait failed: {}",
                    error.message()
                ))),
            }
        });
        stream_tasks.push(wait_task.abort_handle());

        self.inner
            .shutdown
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                object_id,
                BrowserMuxShutdown {
                    control: Some(control),
                    tasks: stream_tasks,
                },
            );

        let mut objects = self.inner.objects.lock().await;
        if objects
            .insert(
                object_id,
                BrowserMuxObject {
                    pid: Some(pid),
                    kind,
                    close_hooks_ready: false,
                    graceful_sequence: None,
                    force_sequence: None,
                    pending_cleanup_close: None,
                    activation,
                    read_commands,
                    write_commands,
                },
            )
            .is_some()
        {
            return Err(RuntimeString::from("Duplicate browser mux object owner."));
        }
        Ok(completion_rx)
    }

    pub(super) async fn register_spawn_failure(&self, object_id: u64) -> Result<(), RuntimeString> {
        {
            let mut protocol = self.inner.protocol.lock().await;
            protocol
                .wire
                .register_pidless_spawn_failure(object_id)
                .map_err(|error| mux_error("register pidless object", error))?;
        }
        let (activation, mut activation_rx) = watch::channel(false);
        let handle = self.clone();
        let task = tokio::spawn(async move {
            if activation_rx.wait_for(|active| *active).await.is_err() {
                return;
            }
            if let Err(message) = handle.finish_spawn_failure(object_id).await {
                handle.fail(message);
            }
        });
        self.inner
            .shutdown
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                object_id,
                BrowserMuxShutdown {
                    control: None,
                    tasks: vec![task.abort_handle()],
                },
            );
        self.inner.objects.lock().await.insert(
            object_id,
            BrowserMuxObject {
                pid: None,
                kind: BrowserMuxChildKind::Playwright,
                close_hooks_ready: false,
                graceful_sequence: None,
                force_sequence: None,
                pending_cleanup_close: None,
                activation,
                read_commands: HashMap::new(),
                write_commands: HashMap::new(),
            },
        );
        Ok(())
    }

    pub(super) async fn activate(&self, object_id: u64) -> Result<(), RuntimeString> {
        let objects = self.inner.objects.lock().await;
        let object = objects
            .get(&object_id)
            .ok_or_else(|| RuntimeString::from("Unknown browser mux object activation."))?;
        object.activation.send_replace(true);
        Ok(())
    }

    pub(super) async fn control_for_pid(
        &self,
        pid: u32,
    ) -> Result<RuntimeBrowserChildControl, RuntimeString> {
        let object_id = self
            .inner
            .objects
            .lock()
            .await
            .iter()
            .find_map(|(object_id, object)| (object.pid == Some(pid)).then_some(*object_id))
            .ok_or_else(|| RuntimeString::from("Browser tree-kill PID is not generation-owned."))?;
        self.inner
            .shutdown
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&object_id)
            .and_then(|object| object.control.clone())
            .ok_or_else(|| RuntimeString::from("Browser tree-kill target is not live."))
    }

    pub(super) async fn begin_orderly_shutdown(&self) -> Result<(), RuntimeString> {
        let candidates = {
            let objects = self.inner.objects.lock().await;
            objects
                .iter()
                .filter_map(|(object_id, object)| {
                    (object.kind == BrowserMuxChildKind::Playwright
                        && object.close_hooks_ready
                        && object.graceful_sequence.is_none())
                    .then_some(*object_id)
                })
                .collect::<Vec<_>>()
        };
        for object_id in candidates {
            let sequence = self
                .send(
                    object_id,
                    0,
                    BrowserMuxKind::BeginGracefulClose,
                    0,
                    BrowserMuxPayload::Empty,
                )
                .await?;
            if let Some(object) = self.inner.objects.lock().await.get_mut(&object_id) {
                object.graceful_sequence = Some(sequence);
            }
        }

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let has_live_playwright = self
                .inner
                .objects
                .lock()
                .await
                .values()
                .any(|object| object.kind == BrowserMuxChildKind::Playwright);
            if !has_live_playwright {
                return Ok(());
            }
            if tokio::time::timeout_at(deadline, self.inner.state_changed.notified())
                .await
                .is_err()
            {
                break;
            }
        }

        let remaining = {
            let objects = self.inner.objects.lock().await;
            objects
                .iter()
                .filter_map(|(object_id, object)| {
                    (object.kind == BrowserMuxChildKind::Playwright)
                        .then_some((*object_id, object.graceful_sequence))
                })
                .collect::<Vec<_>>()
        };
        for (object_id, graceful_sequence) in remaining {
            if let Some(graceful_sequence) = graceful_sequence {
                if let Ok(sequence) = self
                    .send(
                        object_id,
                        0,
                        BrowserMuxKind::BeginForceClose,
                        graceful_sequence,
                        BrowserMuxPayload::Empty,
                    )
                    .await
                {
                    if let Some(object) = self.inner.objects.lock().await.get_mut(&object_id) {
                        object.force_sequence = Some(sequence);
                    }
                }
            }
            self.hard_kill_object(object_id);
        }
        self.begin_immediate_shutdown();
        Ok(())
    }

    pub(super) fn begin_immediate_shutdown(&self) {
        let shutdown = self
            .inner
            .shutdown
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for object in shutdown.values() {
            if let Some(control) = &object.control {
                control.hard_kill();
            }
        }
    }

    fn hard_kill_object(&self, object_id: u64) {
        if let Some(control) = self
            .inner
            .shutdown
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&object_id)
            .and_then(|object| object.control.as_ref())
        {
            control.hard_kill();
        }
    }

    fn spawn_read_stream<R>(
        &self,
        object_id: u64,
        stream_id: u16,
        data_kind: BrowserMuxKind,
        reader: R,
        activation: watch::Receiver<bool>,
    ) -> (mpsc::Sender<ReadCommand>, AbortHandle)
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        let (commands_tx, commands_rx) = mpsc::channel(2);
        let handle = self.clone();
        let task = tokio::spawn(async move {
            if let Err(message) = run_read_stream(
                handle.clone(),
                object_id,
                stream_id,
                data_kind,
                reader,
                commands_rx,
                activation,
            )
            .await
            {
                handle.fail(message);
            }
        });
        (commands_tx, task.abort_handle())
    }

    fn spawn_duplex_stream(
        &self,
        object_id: u64,
        stream_id: u16,
        duplex: RuntimeBrowserChildDuplex,
        activation: watch::Receiver<bool>,
    ) -> (
        mpsc::Sender<ReadCommand>,
        mpsc::Sender<WriteCommand>,
        Vec<AbortHandle>,
    ) {
        let (reader, writer) = tokio::io::split(duplex);
        let (read_sender, read_task) = self.spawn_read_stream(
            object_id,
            stream_id,
            BrowserMuxKind::ExtraData,
            reader,
            activation.clone(),
        );
        let (write_sender, write_commands) = mpsc::channel(16);
        let handle = self.clone();
        let write_task = tokio::spawn(async move {
            if let Err(message) = run_write_stream(
                handle.clone(),
                object_id,
                stream_id,
                writer,
                write_commands,
                activation,
            )
            .await
            {
                handle.fail(message);
            }
        });
        (
            read_sender,
            write_sender,
            vec![read_task, write_task.abort_handle()],
        )
    }

    async fn accept_incoming(&self, frame: BrowserMuxFrame) -> Result<(), RuntimeString> {
        {
            let mut protocol = self.inner.protocol.lock().await;
            protocol
                .wire
                .accept(&frame)
                .map_err(|error| mux_error("accept incoming frame", error))?;
        }
        let (read_sender, write_sender) = {
            let objects = self.inner.objects.lock().await;
            let object = objects
                .get(&frame.object_id)
                .ok_or_else(|| RuntimeString::from("Unknown browser mux object owner."))?;
            (
                object.read_commands.get(&frame.stream_id).cloned(),
                object.write_commands.get(&frame.stream_id).cloned(),
            )
        };
        match frame.kind {
            BrowserMuxKind::ReadCredit => read_sender
                .ok_or_else(|| RuntimeString::from("Browser mux stream is not readable."))?
                .send(ReadCommand::Credit)
                .await
                .map_err(|_| RuntimeString::from("Browser mux readable owner closed.")),
            BrowserMuxKind::Write => {
                let BrowserMuxPayload::Bytes(bytes) = frame.payload else {
                    return Err(RuntimeString::from("Invalid browser mux write payload."));
                };
                write_sender
                    .ok_or_else(|| RuntimeString::from("Browser mux stream is not writable."))?
                    .send(WriteCommand::Write {
                        sequence: frame.sequence,
                        bytes,
                    })
                    .await
                    .map_err(|_| RuntimeString::from("Browser mux writable owner closed."))
            }
            BrowserMuxKind::HalfClose => {
                if let Some(sender) = read_sender {
                    sender
                        .send(ReadCommand::Stop)
                        .await
                        .map_err(|_| RuntimeString::from("Browser mux readable owner closed."))?;
                }
                write_sender
                    .ok_or_else(|| RuntimeString::from("Browser mux stream is not writable."))?
                    .send(WriteCommand::HalfClose {
                        sequence: frame.sequence,
                    })
                    .await
                    .map_err(|_| RuntimeString::from("Browser mux writable owner closed."))
            }
            BrowserMuxKind::StreamClose => {
                if let Some(sender) = read_sender {
                    sender
                        .send(ReadCommand::Close(frame.sequence))
                        .await
                        .map_err(|_| RuntimeString::from("Browser mux readable owner closed."))?;
                }
                if let Some(sender) = write_sender {
                    sender
                        .send(WriteCommand::Close)
                        .await
                        .map_err(|_| RuntimeString::from("Browser mux writable owner closed."))?;
                }
                Ok(())
            }
            BrowserMuxKind::CloseHooksReady => {
                if let Some(object) = self.inner.objects.lock().await.get_mut(&frame.object_id) {
                    object.close_hooks_ready = true;
                }
                self.inner.state_changed.notify_waiters();
                Ok(())
            }
            BrowserMuxKind::GracefulCloseResult | BrowserMuxKind::ForceCloseResult => {
                let pending_cleanup = {
                    let objects = self.inner.objects.lock().await;
                    objects.get(&frame.object_id).and_then(|object| {
                        let terminal_result = match frame.kind {
                            BrowserMuxKind::GracefulCloseResult => object.force_sequence.is_none(),
                            BrowserMuxKind::ForceCloseResult => true,
                            _ => false,
                        };
                        terminal_result
                            .then_some(object.pending_cleanup_close)
                            .flatten()
                    })
                };
                if let Some(close) = pending_cleanup {
                    self.complete_cleanup(frame.object_id, close).await?;
                }
                self.inner.state_changed.notify_waiters();
                Ok(())
            }
            BrowserMuxKind::StreamError => Ok(()),
            _ => Err(RuntimeString::from(
                "Unsupported JavaScript-to-host browser mux frame.",
            )),
        }
    }

    async fn send(
        &self,
        object_id: u64,
        stream_id: u16,
        kind: BrowserMuxKind,
        correlation: u64,
        payload: BrowserMuxPayload,
    ) -> Result<u64, RuntimeString> {
        let encoded;
        let sequence;
        {
            let mut protocol = self.inner.protocol.lock().await;
            let key = (object_id, stream_id, kind.sequence_class().index());
            sequence = protocol
                .outgoing_sequences
                .get(&key)
                .copied()
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| RuntimeString::from("Browser mux sequence exhausted."))?;
            let frame = BrowserMuxFrame::new(
                BrowserMuxDirection::HostToJs,
                self.inner.generation,
                object_id,
                stream_id,
                kind,
                sequence,
                correlation,
                0,
                payload,
            )
            .map_err(|error| mux_error("construct outgoing frame", error))?;
            protocol
                .wire
                .accept(&frame)
                .map_err(|error| mux_error("accept outgoing frame", error))?;
            encoded = frame
                .encode()
                .map_err(|error| mux_error("encode outgoing frame", error))?;
            protocol.outgoing_sequences.insert(key, sequence);
        }
        self.inner
            .outgoing_tx
            .send(encoded)
            .await
            .map_err(|_| RuntimeString::from("Browser mux writer closed."))?;
        Ok(sequence)
    }

    async fn finish_committed_object(
        &self,
        object_id: u64,
        status: RuntimeExecFileStatus,
        stream_tasks: Vec<AbortHandle>,
    ) -> Result<(), RuntimeString> {
        let exit = self
            .send(
                object_id,
                0,
                BrowserMuxKind::Exit,
                0,
                BrowserMuxPayload::RuntimeJson(exit_payload(status)),
            )
            .await?;
        for task in stream_tasks {
            if !task.is_finished() {
                task.abort();
            }
        }
        self.close_all_streams(object_id).await?;
        let close = self
            .send(
                object_id,
                0,
                BrowserMuxKind::Close,
                exit,
                BrowserMuxPayload::Empty,
            )
            .await?;
        let defer_cleanup = {
            let mut objects = self.inner.objects.lock().await;
            let Some(object) = objects.get_mut(&object_id) else {
                return Ok(());
            };
            if object.graceful_sequence.is_some() {
                object.pending_cleanup_close = Some(close);
                true
            } else {
                false
            }
        };
        if !defer_cleanup {
            self.complete_cleanup(object_id, close).await?;
        }
        Ok(())
    }

    async fn finish_spawn_failure(&self, object_id: u64) -> Result<(), RuntimeString> {
        let close = self
            .send(
                object_id,
                0,
                BrowserMuxKind::Close,
                0,
                BrowserMuxPayload::Empty,
            )
            .await?;
        self.complete_cleanup(object_id, close).await?;
        Ok(())
    }

    async fn complete_cleanup(&self, object_id: u64, close: u64) -> Result<(), RuntimeString> {
        self.send(
            object_id,
            0,
            BrowserMuxKind::CleanupComplete,
            close,
            BrowserMuxPayload::RuntimeJson(cleanup_payload(true, None)),
        )
        .await?;
        self.inner.objects.lock().await.remove(&object_id);
        self.inner
            .shutdown
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&object_id);
        self.inner.state_changed.notify_waiters();
        Ok(())
    }

    async fn close_all_streams(&self, object_id: u64) -> Result<(), RuntimeString> {
        let streams = {
            let objects = self.inner.objects.lock().await;
            let Some(object) = objects.get(&object_id) else {
                return Ok(());
            };
            object.read_commands.keys().copied().collect::<Vec<_>>()
        };
        for stream_id in streams {
            let _ = self
                .send(
                    object_id,
                    stream_id,
                    BrowserMuxKind::StreamClose,
                    0,
                    BrowserMuxPayload::Empty,
                )
                .await;
        }
        Ok(())
    }

    pub(super) fn fail(&self, message: RuntimeString) {
        if self.inner.fatal_tx.borrow().is_none() {
            self.inner.fatal_tx.send_replace(Some(message));
        }
    }
}

async fn run_mux_reader(handle: BrowserMuxHandle, mut inbound: RuntimeBrowserBrokerRead) {
    loop {
        let result = async {
            let bytes = read_mux_frame(&mut inbound).await?;
            let frame = BrowserMuxFrame::decode_exact(&bytes, BrowserMuxDirection::JsToHost)
                .map_err(|error| mux_error("decode incoming frame", error))?;
            handle.accept_incoming(frame).await
        }
        .await;
        if let Err(message) = result {
            handle.fail(message);
            return;
        }
    }
}

async fn run_mux_writer(
    inner: Arc<BrowserMuxInner>,
    mut outbound: RuntimeBrowserBrokerWrite,
    mut outgoing: mpsc::Receiver<Vec<u8>>,
) {
    while let Some(bytes) = outgoing.recv().await {
        if let Err(error) = outbound.write_all(&bytes).await {
            BrowserMuxHandle { inner }.fail(RuntimeString::from(format!(
                "Browser mux write failed: {error}"
            )));
            return;
        }
        if let Err(error) = outbound.flush().await {
            BrowserMuxHandle { inner }.fail(RuntimeString::from(format!(
                "Browser mux flush failed: {error}"
            )));
            return;
        }
    }
}

async fn run_read_stream<R>(
    handle: BrowserMuxHandle,
    object_id: u64,
    stream_id: u16,
    data_kind: BrowserMuxKind,
    mut reader: R,
    mut commands: mpsc::Receiver<ReadCommand>,
    mut activation: watch::Receiver<bool>,
) -> Result<(), RuntimeString>
where
    R: AsyncRead + Unpin,
{
    activation
        .wait_for(|active| *active)
        .await
        .map_err(|_| RuntimeString::from("Browser mux activation owner closed."))?;
    while let Some(command) = commands.recv().await {
        match command {
            ReadCommand::Credit => {
                let mut bytes = vec![0_u8; NODE_READ_EVENT_BYTES];
                let count = reader.read(&mut bytes).await.map_err(|error| {
                    RuntimeString::from(format!("Browser mux stream read failed: {error}"))
                })?;
                if count == 0 {
                    handle
                        .send(
                            object_id,
                            stream_id,
                            BrowserMuxKind::StreamClose,
                            0,
                            BrowserMuxPayload::Empty,
                        )
                        .await?;
                    return Ok(());
                }
                bytes.truncate(count);
                handle
                    .send(
                        object_id,
                        stream_id,
                        data_kind,
                        0,
                        BrowserMuxPayload::Bytes(bytes),
                    )
                    .await?;
            }
            ReadCommand::Close(sequence) => {
                handle
                    .send(
                        object_id,
                        stream_id,
                        BrowserMuxKind::StreamClose,
                        sequence,
                        BrowserMuxPayload::Empty,
                    )
                    .await?;
                return Ok(());
            }
            ReadCommand::Stop => return Ok(()),
        }
    }
    Ok(())
}

async fn run_write_stream<W>(
    handle: BrowserMuxHandle,
    object_id: u64,
    stream_id: u16,
    mut writer: W,
    mut commands: mpsc::Receiver<WriteCommand>,
    mut activation: watch::Receiver<bool>,
) -> Result<(), RuntimeString>
where
    W: AsyncWrite + Unpin,
{
    activation
        .wait_for(|active| *active)
        .await
        .map_err(|_| RuntimeString::from("Browser mux activation owner closed."))?;
    while let Some(command) = commands.recv().await {
        match command {
            WriteCommand::Write { sequence, bytes } => {
                writer.write_all(&bytes).await.map_err(|error| {
                    RuntimeString::from(format!("Browser mux stream write failed: {error}"))
                })?;
                writer.flush().await.map_err(|error| {
                    RuntimeString::from(format!("Browser mux stream flush failed: {error}"))
                })?;
                handle
                    .send(
                        object_id,
                        stream_id,
                        BrowserMuxKind::WriteAck,
                        sequence,
                        BrowserMuxPayload::Empty,
                    )
                    .await?;
            }
            WriteCommand::HalfClose { sequence } => {
                writer.shutdown().await.map_err(|error| {
                    RuntimeString::from(format!("Browser mux stream shutdown failed: {error}"))
                })?;
                handle
                    .send(
                        object_id,
                        stream_id,
                        BrowserMuxKind::StreamClose,
                        sequence,
                        BrowserMuxPayload::Empty,
                    )
                    .await?;
                return Ok(());
            }
            WriteCommand::Close => {
                writer.shutdown().await.map_err(|error| {
                    RuntimeString::from(format!("Browser mux stream shutdown failed: {error}"))
                })?;
                return Ok(());
            }
        }
    }
    Ok(())
}

async fn read_mux_frame(inbound: &mut RuntimeBrowserBrokerRead) -> Result<Vec<u8>, RuntimeString> {
    let mut prefix = [0_u8; 4];
    inbound
        .read_exact(&mut prefix)
        .await
        .map_err(|error| RuntimeString::from(format!("Browser mux read failed: {error}")))?;
    let remaining = u32::from_be_bytes(prefix) as usize;
    if !(MUX_HEADER_LEN..=MAX_MUX_REMAINING).contains(&remaining) {
        return Err(RuntimeString::from("Invalid browser mux frame length."));
    }
    let mut bytes = vec![0_u8; 4 + remaining];
    bytes[..4].copy_from_slice(&prefix);
    inbound
        .read_exact(&mut bytes[4..])
        .await
        .map_err(|error| RuntimeString::from(format!("Browser mux read failed: {error}")))?;
    Ok(bytes)
}

fn exit_payload(status: RuntimeExecFileStatus) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    object.insert(
        "code",
        status
            .code()
            .map(RuntimeJson::from)
            .unwrap_or(RuntimeJson::Null),
    );
    object.insert(
        "signal",
        status
            .signal()
            .map(posix_signal_name)
            .map(RuntimeJson::String)
            .unwrap_or(RuntimeJson::Null),
    );
    RuntimeJson::Object(object)
}

fn cleanup_payload(ok: bool, error: Option<RuntimeString>) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    object.insert("ok", RuntimeJson::Bool(ok));
    object.insert(
        "error",
        error.map(RuntimeJson::String).unwrap_or(RuntimeJson::Null),
    );
    RuntimeJson::Object(object)
}

fn posix_signal_name(signal: i32) -> RuntimeString {
    let name = match signal {
        1 => "SIGHUP",
        2 => "SIGINT",
        3 => "SIGQUIT",
        4 => "SIGILL",
        6 => "SIGABRT",
        8 => "SIGFPE",
        9 => "SIGKILL",
        11 => "SIGSEGV",
        13 => "SIGPIPE",
        14 => "SIGALRM",
        15 => "SIGTERM",
        _ => return RuntimeString::from(format!("SIG{signal}")),
    };
    RuntimeString::from(name)
}

fn mux_error(action: &str, error: impl std::fmt::Debug) -> RuntimeString {
    RuntimeString::from(format!("Browser mux failed to {action}: {error:?}"))
}
