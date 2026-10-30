use super::{
    CommandOutputBuffer, CommandProcessId, CommandProcessIdAllocator, CommandSpawnRequest,
};
use crate::command_runtime::process::{ProcessEvent, ProcessEventReceiver};
use crate::network_proxy::ManagedNetworkSandboxContext;
use crate::runtime_process::{
    RuntimeEnv, RuntimeProcessContext, RuntimeProcessController, RuntimeProcessSurface,
    RuntimeSignal, RuntimeSpawnRequest,
};
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, NodeChunkUtf8, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode,
    RuntimeEncode, RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimePath, RuntimeString,
};
use crate::schemas::background_process::{
    BackgroundProcessEvent, BackgroundProcessEventSubtype, BackgroundProcessOutput,
    BackgroundProcessRecord, BackgroundProcessSnapshot, BackgroundProcessStatus,
    BackgroundProcessStopResult,
};
use crate::tools::shell::Signal;
use crate::tools::shell_session::ShellSessionManager;
use crate::types::abort::AbortSignal;
use crate::types::sandbox::SandboxBackend;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::future::Future;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;
use tokio::sync::{watch, Mutex as AsyncMutex, Notify};
use tokio_util::sync::CancellationToken;

fn decode_provider_unit_enum<T: Copy>(
    value: &RuntimeJson,
    variants: &[RuntimeEnumVariant<'_>],
    values: &[T],
) -> Result<T, RuntimeCodecError> {
    runtime_codec::decode_external(value, variants, |variant| match variant {
        RuntimeExternalVariant::Unit { variant } => values
            .get(variant)
            .copied()
            .ok_or_else(|| RuntimeCodecError::new(RuntimeCodecErrorKind::UnknownEnumVariant)),
        RuntimeExternalVariant::Payload { .. } => Err(RuntimeCodecError::new(
            RuntimeCodecErrorKind::InvalidEnumShape,
        )),
    })
}

const KILL_FALLBACK_DELAY_SECS: u64 = 5;
const DEFAULT_HOST_READ_BYTES: usize = 64 * 1024;
const MAX_HOST_READ_BYTES: usize = 1024 * 1024;
pub const MIN_BACKGROUND_POLL_MS: u64 = 5_000;
pub const DEFAULT_MAX_BACKGROUND_POLL_MS: u64 = 300_000;
const POST_EXIT_CLOSE_WAIT: Duration = Duration::from_millis(50);
const TRAILING_OUTPUT_GRACE: Duration = Duration::from_millis(100);

/// Lifecycle state of a background command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackgroundCommandStatus {
    Running,
    Exited,
    Killed,
}

impl RuntimeEncode for BackgroundCommandStatus {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Running => "running",
            Self::Exited => "exited",
            Self::Killed => "killed",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for BackgroundCommandStatus {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("running", &[]),
                RuntimeEnumVariant::new("exited", &[]),
                RuntimeEnumVariant::new("killed", &[]),
            ],
            &[Self::Running, Self::Exited, Self::Killed],
        )
    }
}

/// The terminal result handed to [`StartBackgroundCommandOptions::on_complete`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundCommandComplete {
    pub id: RuntimeString,
    pub status: BackgroundCommandStatus,
    pub exit_code: Option<i32>,
}

impl RuntimeEncode for BackgroundCommandComplete {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("id", &self.id)?;
        object.field("status", &self.status)?;
        object.field("exitCode", &self.exit_code)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for BackgroundCommandComplete {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                id: object.required("id", &[])?,
                status: object.required("status", &[])?,
                exit_code: object.optional("exitCode", &[])?,
            })
        })
    }
}

/// Fired exactly once when a background command reaches a terminal state.
pub type BackgroundCommandOnComplete = Box<dyn FnOnce(BackgroundCommandComplete) + Send + 'static>;
pub type BackgroundProcessEventSink = Arc<dyn Fn(BackgroundProcessEvent) + Send + Sync>;
/// Acquiring capacity may yield. Committing the reserved record never yields.
pub struct PreparedBackgroundProcessAppend<'a> {
    commit: Box<dyn FnOnce(BackgroundProcessRecord) + Send + 'a>,
}

impl<'a> PreparedBackgroundProcessAppend<'a> {
    pub fn new(commit: impl FnOnce(BackgroundProcessRecord) + Send + 'a) -> Self {
        Self {
            commit: Box::new(commit),
        }
    }

    fn commit(self, record: BackgroundProcessRecord) {
        (self.commit)(record);
    }
}

#[async_trait::async_trait]
pub trait BackgroundProcessPersistence: Send + Sync {
    async fn prepare(&self) -> Result<PreparedBackgroundProcessAppend<'_>, RuntimeString>;
}

pub type BackgroundProcessPersistenceSink = Arc<dyn BackgroundProcessPersistence>;

/// The rollout owns writer admission; the registry owns process ordering.
pub struct RolloutBackgroundProcessPersistence {
    rollout: Arc<dyn crate::store::session_store::LiveRollout>,
}

impl RolloutBackgroundProcessPersistence {
    pub fn new(rollout: Arc<dyn crate::store::session_store::LiveRollout>) -> Self {
        Self { rollout }
    }
}

#[async_trait::async_trait]
impl BackgroundProcessPersistence for RolloutBackgroundProcessPersistence {
    async fn prepare(&self) -> Result<PreparedBackgroundProcessAppend<'_>, RuntimeString> {
        let prepared = self
            .rollout
            .prepare_append()
            .await
            .map_err(|error| RuntimeString::from(error.to_string()))?;
        Ok(PreparedBackgroundProcessAppend::new(move |record| {
            prepared.commit(
                crate::schemas::rollout::RolloutItem::BackgroundProcessLifecycle(
                    crate::schemas::rollout::BackgroundProcessLifecycleItem { record },
                ),
            );
        }))
    }
}
pub type BackgroundProcessNow = Arc<dyn Fn() -> i64 + Send + Sync>;
pub type BackgroundProcessStop =
    Arc<dyn Fn(&RuntimeString, Signal) -> bool + Send + Sync + 'static>;

/// Options for [`BackgroundCommandRegistry::start`].
pub struct StartBackgroundCommandOptions {
    pub command: RuntimeString,
    pub cwd: RuntimePath,
    pub sandbox: Arc<dyn SandboxBackend>,
    pub env: Option<RuntimeEnv>,
    pub max_output_bytes: usize,
    pub writable_roots: Option<Vec<RuntimeString>>,
    /// Waive the sandbox's write confinement; see
    /// [`CommandSpawnRequest::unconfined_writes`].
    pub unconfined_writes: bool,
    pub network_allowed: Option<bool>,
    pub managed_network: Option<ManagedNetworkSandboxContext>,
    pub on_complete: Option<BackgroundCommandOnComplete>,
}

/// A process that another runtime owns but this Session projects through the
/// same background-task lifecycle, persistence, output and stop surfaces.
pub struct AdoptBackgroundCommandOptions {
    pub id: RuntimeString,
    pub command: RuntimeString,
    pub cwd: RuntimePath,
    pub max_output_bytes: usize,
    pub events: ProcessEventReceiver,
    pub stop: BackgroundProcessStop,
    pub on_complete: Option<BackgroundCommandOnComplete>,
}

/// The result of [`BackgroundCommandRegistry::read_new`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadBackgroundCommandResult {
    pub found: bool,
    pub output: RuntimeString,
    pub status: BackgroundCommandStatus,
    pub exit_code: Option<i32>,
    pub dropped_bytes: usize,
}

impl RuntimeEncode for ReadBackgroundCommandResult {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("found", &self.found)?;
        object.field("output", &self.output)?;
        object.field("status", &self.status)?;
        object.field("exitCode", &self.exit_code)?;
        object.field("droppedBytes", &self.dropped_bytes)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ReadBackgroundCommandResult {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                found: object.required("found", &[])?,
                output: object.required("output", &[])?,
                status: object.required("status", &[])?,
                exit_code: object.optional("exitCode", &[])?,
                dropped_bytes: object.required("droppedBytes", &[])?,
            })
        })
    }
}

/// One row of [`BackgroundCommandRegistry::list`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundCommandListEntry {
    pub id: RuntimeString,
    pub command: RuntimeString,
    pub status: BackgroundCommandStatus,
    pub exit_code: Option<i32>,
}

impl RuntimeEncode for BackgroundCommandListEntry {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("id", &self.id)?;
        object.field("command", &self.command)?;
        object.field("status", &self.status)?;
        object.field("exitCode", &self.exit_code)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for BackgroundCommandListEntry {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                id: object.required("id", &[])?,
                command: object.required("command", &[])?,
                status: object.required("status", &[])?,
                exit_code: object.optional("exitCode", &[])?,
            })
        })
    }
}

struct SharedState {
    buffer: CommandOutputBuffer,
    record: BackgroundProcessRecord,
    on_complete: Option<BackgroundCommandOnComplete>,
    complete_fired: bool,
    controller: Option<RuntimeProcessController>,
    external_stop: Option<BackgroundProcessStop>,
    output_file: Option<std::fs::File>,
    sequence: Arc<AsyncMutex<()>>,
    stop_requested: CancellationToken,
    stop_notify: Arc<Notify>,
    pending_stop: Option<BackgroundProcessRecord>,
    stop_receipt: Option<watch::Sender<Option<Result<(), RuntimeString>>>>,
    fallback_at: Option<tokio::time::Instant>,
    persistence_error: Option<RuntimeString>,
    output_changed: Arc<Notify>,
    interaction: Arc<AsyncMutex<()>>,
    exit_observed: Option<Option<i32>>,
    output_closed: bool,
    model_output_collected: bool,
}

struct BackgroundCommand {
    shared: Arc<Mutex<SharedState>>,
    cursor: usize,
}

#[derive(Default)]
struct BackgroundCommandLifecycle {
    admission_closed: bool,
    tasks: Vec<Arc<BackgroundMonitor>>,
}

struct BackgroundMonitor {
    shared: Weak<Mutex<SharedState>>,
    task: AsyncMutex<Option<tokio::task::JoinHandle<()>>>,
    completion: watch::Receiver<Option<Result<(), RuntimeString>>>,
}

#[derive(Default)]
struct BackgroundProcessSubscribers {
    next_id: u64,
    sinks: BTreeMap<u64, BackgroundProcessEventSink>,
}

/// Registry of long-running background commands controlled by the engine runtime.
pub struct BackgroundCommandRegistry {
    commands: Arc<Mutex<Vec<BackgroundCommand>>>,
    ids: Mutex<CommandProcessIdAllocator>,
    lifecycle: Mutex<BackgroundCommandLifecycle>,
    now: BackgroundProcessNow,
    persistence: Option<BackgroundProcessPersistenceSink>,
    subscribers: Arc<Mutex<BackgroundProcessSubscribers>>,
    output_dir: Option<RuntimePath>,
    shell_sessions: Arc<ShellSessionManager>,
    initial_records: Mutex<Vec<BackgroundProcessRecord>>,
    initialization: AsyncMutex<()>,
    max_poll_wait_ms: AtomicU64,
    observation_pause: Mutex<crate::mcp::elicitation::ElicitationPauseState>,
}

impl Default for BackgroundCommandRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl BackgroundCommandRegistry {
    pub fn new() -> Self {
        Self::with_options(None, None, None, None, Vec::new())
    }

    pub fn with_options(
        now: Option<BackgroundProcessNow>,
        persistence: Option<BackgroundProcessPersistenceSink>,
        event_sink: Option<BackgroundProcessEventSink>,
        output_dir: Option<RuntimePath>,
        restored: Vec<BackgroundProcessRecord>,
    ) -> Self {
        let now = now.unwrap_or_else(|| Arc::new(system_now_ms));
        let now_ms = now();
        let mut initial_records = Vec::new();
        let commands = restored
            .into_iter()
            .map(|record| {
                let output_available = output_path(output_dir.as_ref(), &record.shell_id)
                    .as_ref()
                    .is_some_and(|path| output_file_covers_record(path, &record));
                let mut record = record;
                if !record.status.is_terminal() {
                    record = record.interrupted(now_ms, output_available);
                    initial_records.push(record.clone());
                } else {
                    record.output_available = output_available;
                }
                BackgroundCommand {
                    shared: Arc::new(Mutex::new(SharedState {
                        buffer: CommandOutputBuffer::new(1),
                        record,
                        on_complete: None,
                        complete_fired: true,
                        controller: None,
                        external_stop: None,
                        output_file: None,
                        sequence: Arc::new(AsyncMutex::new(())),
                        stop_requested: CancellationToken::new(),
                        stop_notify: Arc::new(Notify::new()),
                        pending_stop: None,
                        stop_receipt: None,
                        fallback_at: None,
                        persistence_error: None,
                        output_changed: Arc::new(Notify::new()),
                        interaction: Arc::new(AsyncMutex::new(())),
                        exit_observed: None,
                        output_closed: true,
                        model_output_collected: false,
                    })),
                    cursor: 0,
                }
            })
            .collect();
        Self {
            commands: Arc::new(Mutex::new(commands)),
            ids: Mutex::new(CommandProcessIdAllocator::random()),
            lifecycle: Mutex::new(BackgroundCommandLifecycle::default()),
            now,
            persistence,
            subscribers: Arc::new(Mutex::new(BackgroundProcessSubscribers {
                next_id: u64::from(event_sink.is_some()),
                sinks: event_sink.into_iter().map(|sink| (0, sink)).collect(),
            })),
            output_dir,
            shell_sessions: Arc::new(ShellSessionManager::new()),
            initial_records: Mutex::new(initial_records),
            initialization: AsyncMutex::new(()),
            max_poll_wait_ms: AtomicU64::new(DEFAULT_MAX_BACKGROUND_POLL_MS),
            observation_pause: Mutex::new(crate::mcp::elicitation::ElicitationPauseState::new()),
        }
    }

    pub fn set_observation_pause_state(
        &self,
        state: crate::mcp::elicitation::ElicitationPauseState,
    ) {
        *self
            .observation_pause
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = state;
    }

    pub fn set_max_poll_wait_ms(&self, milliseconds: u64) {
        self.max_poll_wait_ms
            .store(milliseconds.max(MIN_BACKGROUND_POLL_MS), Ordering::Relaxed);
    }

    pub fn model_output_collected(&self, id: &RuntimeString) -> bool {
        find_shared(&self.commands, id).is_some_and(|shared| {
            shared
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .model_output_collected
        })
    }

    pub fn poll_wait_ms(&self, requested: Option<u64>) -> u64 {
        requested.unwrap_or(250).clamp(
            MIN_BACKGROUND_POLL_MS,
            self.max_poll_wait_ms.load(Ordering::Relaxed),
        )
    }

    /// Transfer recovery records to the Session's retained initialization owner.
    /// Construction itself performs no I/O and requires no runtime.
    pub fn take_initial_records(&self) -> Vec<BackgroundProcessRecord> {
        std::mem::take(
            &mut *self
                .initial_records
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
        )
    }

    async fn persist_initial_records(&self) -> Result<(), RuntimeString> {
        let _initialization = self.initialization.lock().await;
        loop {
            if self
                .initial_records
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_empty()
            {
                return Ok(());
            }
            let prepared = prepare(&self.persistence).await?;
            let record = self
                .initial_records
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(0);
            prepared.commit(record);
        }
    }

    /// The interactive process manager owned by the same Session. `exec_command`,
    /// `write_stdin` and adopted process stop callbacks all use this exact table.
    pub fn shell_sessions(&self) -> Arc<ShellSessionManager> {
        self.shell_sessions.clone()
    }

    pub fn subscribe(&self, sink: BackgroundProcessEventSink) -> Box<dyn Fn() + Send + Sync> {
        let id = {
            let mut subscribers = self.subscribers.lock().unwrap_or_else(|e| e.into_inner());
            let id = subscribers.next_id;
            subscribers.next_id = subscribers
                .next_id
                .checked_add(1)
                .expect("background-process subscriber id overflow");
            subscribers.sinks.insert(id, sink);
            id
        };
        let subscribers = Arc::downgrade(&self.subscribers);
        Box::new(move || {
            if let Some(subscribers) = subscribers.upgrade() {
                subscribers
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .sinks
                    .remove(&id);
            }
        })
    }

    pub async fn start(
        &self,
        process_context: RuntimeProcessContext,
        opts: StartBackgroundCommandOptions,
    ) -> Result<RuntimeString, RuntimeString> {
        self.persist_initial_records().await?;
        self.check_admission()?;
        let request = CommandSpawnRequest {
            command: opts.command.clone(),
            cwd: opts.cwd.clone(),
            env: opts.env.clone(),
            writable_roots: opts
                .writable_roots
                .clone()
                .unwrap_or_else(|| vec![opts.cwd.as_runtime_string().clone()]),
            network_allowed: opts.network_allowed.unwrap_or(false),
            managed_network: opts.managed_network.clone(),
            unconfined_writes: opts.unconfined_writes,
        };
        let spec = request.to_spawn_spec(opts.sandbox.as_ref());

        let prepared = prepare(&self.persistence).await?;
        let mut lifecycle = self.lifecycle.lock().unwrap_or_else(|e| e.into_inner());
        if lifecycle.admission_closed {
            return Err(RuntimeString::from(
                "session background-process admission is closed",
            ));
        }
        let id = self.ids.lock().unwrap_or_else(|e| e.into_inner()).reserve();
        let id = id.as_runtime_string().clone();
        let now_ms = (self.now)();
        let output_file = output_path(self.output_dir.as_ref(), &id)
            .as_ref()
            .and_then(|path| create_output_file(path).ok());
        let record = BackgroundProcessRecord {
            shell_id: id.clone(),
            command: opts.command.clone(),
            cwd: opts.cwd.as_runtime_string().clone(),
            status: BackgroundProcessStatus::Pending,
            started_at_ms: now_ms,
            updated_at_ms: now_ms,
            completed_at_ms: None,
            exit_code: None,
            termination_signal: None,
            failure: None,
            stop_requested: false,
            output_start_offset: 0,
            output_next_offset: 0,
            dropped_bytes: 0,
            output_available: output_file.is_some(),
        };
        let shared = Arc::new(Mutex::new(SharedState {
            buffer: CommandOutputBuffer::new(opts.max_output_bytes),
            record: record.clone(),
            on_complete: opts.on_complete,
            complete_fired: false,
            controller: None,
            external_stop: None,
            output_file,
            sequence: Arc::new(AsyncMutex::new(())),
            stop_requested: CancellationToken::new(),
            stop_notify: Arc::new(Notify::new()),
            pending_stop: None,
            stop_receipt: None,
            fallback_at: None,
            persistence_error: None,
            output_changed: Arc::new(Notify::new()),
            interaction: Arc::new(AsyncMutex::new(())),
            exit_observed: None,
            output_closed: false,
            model_output_collected: false,
        }));

        {
            let mut commands = self.commands.lock().unwrap_or_else(|e| e.into_inner());
            commands.push(BackgroundCommand {
                shared: Arc::clone(&shared),
                cursor: 0,
            });
        }
        prepared.commit(record.clone());
        emit_item(
            &self.subscribers,
            BackgroundProcessEventSubtype::TaskStarted,
            record,
        );
        emit_snapshot(&self.commands, &self.subscribers);
        let monitor = retain_monitor(
            &shared,
            run_background_process(
                process_context,
                spec,
                Arc::clone(&shared),
                self.commands.clone(),
                self.persistence.clone(),
                self.subscribers.clone(),
                self.now.clone(),
            ),
        );
        lifecycle.tasks.push(monitor);

        Ok(id)
    }

    /// Project a yielded `exec_command` process into the authoritative
    /// background-process table without stealing the model's `write_stdin`
    /// cursor. The process event receiver replays output already produced before
    /// adoption and then continues live.
    pub async fn adopt(&self, opts: AdoptBackgroundCommandOptions) -> Result<(), RuntimeString> {
        self.persist_initial_records().await?;
        self.check_admission()?;
        let prepared = prepare(&self.persistence).await?;
        let started = {
            let mut lifecycle = self.lifecycle.lock().unwrap_or_else(|e| e.into_inner());
            if lifecycle.admission_closed {
                return Err(RuntimeString::from(
                    "session background-process admission is closed",
                ));
            }
            if self.has(&opts.id) {
                return Ok(());
            }
            let now_ms = (self.now)();
            let output_file = output_path(self.output_dir.as_ref(), &opts.id)
                .as_ref()
                .and_then(|path| create_output_file(path).ok());
            let record = BackgroundProcessRecord {
                shell_id: opts.id.clone(),
                command: opts.command,
                cwd: opts.cwd.as_runtime_string().clone(),
                status: BackgroundProcessStatus::Pending,
                started_at_ms: now_ms,
                updated_at_ms: now_ms,
                completed_at_ms: None,
                exit_code: None,
                termination_signal: None,
                failure: None,
                stop_requested: false,
                output_start_offset: 0,
                output_next_offset: 0,
                dropped_bytes: 0,
                output_available: output_file.is_some(),
            };
            let shared = Arc::new(Mutex::new(SharedState {
                buffer: CommandOutputBuffer::new(opts.max_output_bytes),
                record: record.clone(),
                on_complete: opts.on_complete,
                complete_fired: false,
                controller: None,
                external_stop: Some(opts.stop),
                output_file,
                sequence: Arc::new(AsyncMutex::new(())),
                stop_requested: CancellationToken::new(),
                stop_notify: Arc::new(Notify::new()),
                pending_stop: None,
                stop_receipt: None,
                fallback_at: None,
                persistence_error: None,
                output_changed: Arc::new(Notify::new()),
                interaction: Arc::new(AsyncMutex::new(())),
                exit_observed: None,
                output_closed: false,
                model_output_collected: false,
            }));
            self.commands
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(BackgroundCommand {
                    shared: shared.clone(),
                    cursor: 0,
                });
            prepared.commit(record.clone());
            emit_item(
                &self.subscribers,
                BackgroundProcessEventSubtype::TaskStarted,
                record,
            );
            emit_snapshot(&self.commands, &self.subscribers);
            let (started_send, started_receive) = tokio::sync::oneshot::channel();
            let monitor = retain_monitor(
                &shared,
                mirror_adopted_process(
                    shared.clone(),
                    self.commands.clone(),
                    self.persistence.clone(),
                    self.subscribers.clone(),
                    self.now.clone(),
                    opts.events,
                    started_send,
                ),
            );
            lifecycle.tasks.push(monitor);
            started_receive
        };
        started.await.map_err(|_| {
            RuntimeString::from("background adoption owner ended before running admission")
        })?
    }

    fn check_admission(&self) -> Result<(), RuntimeString> {
        if self
            .lifecycle
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .admission_closed
        {
            Err(RuntimeString::from(
                "session background-process admission is closed",
            ))
        } else {
            Ok(())
        }
    }

    pub fn read_new(&self, id: &RuntimeString) -> ReadBackgroundCommandResult {
        let mut commands = self.commands.lock().unwrap_or_else(|e| e.into_inner());
        let Some(command) = commands.iter_mut().find(|command| {
            command
                .shared
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .record
                .shell_id
                == *id
        }) else {
            return ReadBackgroundCommandResult {
                found: false,
                output: RuntimeString::default(),
                status: BackgroundCommandStatus::Exited,
                exit_code: None,
                dropped_bytes: 0,
            };
        };
        let (text, next_cursor, dropped_bytes, status, exit_code) = {
            let st = command.shared.lock().unwrap_or_else(|e| e.into_inner());
            let read = st.buffer.read_from(command.cursor);
            (
                read.text,
                read.next_cursor,
                read.dropped_bytes,
                compatibility_status(&st.record),
                st.record.exit_code,
            )
        };
        command.cursor = next_cursor;
        ReadBackgroundCommandResult {
            found: true,
            output: text,
            status,
            exit_code,
            dropped_bytes,
        }
    }

    /// Observe one process until its output closes or this observation expires.
    /// The wait owns no process cancellation and consumes output only on return.
    pub async fn read_new_wait(
        &self,
        id: &RuntimeString,
        wait_ms: u64,
        signal: &AbortSignal,
        mut pause: Option<watch::Receiver<bool>>,
    ) -> Result<ReadBackgroundCommandResult, RuntimeString> {
        let mut pause = pause.take().or_else(|| {
            Some(
                self.observation_pause
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .subscribe(),
            )
        });
        let Some(shared) = find_shared(&self.commands, id) else {
            return Ok(self.read_new(id));
        };
        let (interaction, changed) = {
            let state = shared.lock().unwrap_or_else(|e| e.into_inner());
            (state.interaction.clone(), state.output_changed.clone())
        };
        let _interaction = tokio::select! {
            biased;
            _ = signal.cancelled() => return Err("Background output observation cancelled".into()),
            guard = interaction.lock() => guard,
        };
        if shared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .model_output_collected
        {
            return Ok(ReadBackgroundCommandResult {
                found: false,
                output: RuntimeString::default(),
                status: BackgroundCommandStatus::Exited,
                exit_code: None,
                dropped_bytes: 0,
            });
        }
        let mut cursor = self
            .commands
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .find(|command| Arc::ptr_eq(&command.shared, &shared))
            .map_or(0, |command| command.cursor);
        let mut collected = CommandOutputBuffer::new(MAX_HOST_READ_BYTES);
        let mut dropped_bytes = 0;
        let mut deadline = tokio::time::Instant::now()
            .checked_add(Duration::from_millis(wait_ms))
            .ok_or_else(|| RuntimeString::from("Output observation duration is too large"))?;
        let mut post_exit_deadline = None;
        loop {
            if pause.as_ref().is_some_and(|receiver| *receiver.borrow()) {
                let paused_at = tokio::time::Instant::now();
                while pause.as_ref().is_some_and(|receiver| *receiver.borrow()) {
                    let receiver = pause.as_mut().expect("paused receiver exists");
                    tokio::select! {
                        biased;
                        _ = signal.cancelled() => return Err("Background output observation cancelled".into()),
                        result = receiver.changed() => if result.is_err() { pause = None; },
                    }
                }
                let elapsed = paused_at.elapsed();
                deadline += elapsed;
                if let Some(end) = post_exit_deadline.as_mut() {
                    *end += elapsed;
                }
            }
            let notified = changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let (exited, closed) = {
                let state = shared.lock().unwrap_or_else(|e| e.into_inner());
                let output = state.buffer.read_from(cursor);
                cursor = output.next_cursor;
                dropped_bytes += output.dropped_bytes;
                collected.push(&output.text.to_node_utf8());
                (
                    state.exit_observed.is_some() || state.record.status.is_terminal(),
                    state.output_closed,
                )
            };
            if exited && closed {
                break;
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                break;
            }
            let end = if exited {
                *post_exit_deadline.get_or_insert_with(|| deadline.min(now + POST_EXIT_CLOSE_WAIT))
            } else {
                deadline
            };
            if now >= end {
                break;
            }
            tokio::select! {
                biased;
                _ = signal.cancelled() => return Err("Background output observation cancelled".into()),
                _ = &mut notified => {},
                _ = tokio::time::sleep_until(end) => {},
                _ = async {
                    match pause.as_mut() {
                        Some(receiver) => { if receiver.changed().await.is_err() { pause = None; } },
                        None => std::future::pending::<()>().await,
                    }
                } => {},
            }
        }
        if signal.is_cancelled() {
            return Err("Background output observation cancelled".into());
        }
        let mut commands = self.commands.lock().unwrap_or_else(|e| e.into_inner());
        let Some(command) = commands
            .iter_mut()
            .find(|command| Arc::ptr_eq(&command.shared, &shared))
        else {
            return Ok(ReadBackgroundCommandResult {
                found: false,
                output: RuntimeString::default(),
                status: BackgroundCommandStatus::Exited,
                exit_code: None,
                dropped_bytes: 0,
            });
        };
        let mut state = shared.lock().unwrap_or_else(|e| e.into_inner());
        let tail = state.buffer.read_from(cursor);
        collected.push(&tail.text.to_node_utf8());
        command.cursor = tail.next_cursor;
        dropped_bytes += tail.dropped_bytes;
        let mut status = compatibility_status(&state.record);
        let mut exit_code = state.record.exit_code;
        if let Some(code) = state.exit_observed {
            if status == BackgroundCommandStatus::Running {
                status = BackgroundCommandStatus::Exited;
                exit_code = code;
            }
        }
        if status != BackgroundCommandStatus::Running {
            state.model_output_collected = true;
        }
        let output = collected.read_from(0);
        Ok(ReadBackgroundCommandResult {
            found: true,
            output: output.text,
            status,
            exit_code,
            dropped_bytes: dropped_bytes + output.dropped_bytes,
        })
    }

    pub fn read_output(
        &self,
        id: &RuntimeString,
        after_offset: Option<u64>,
        max_bytes: Option<usize>,
    ) -> BackgroundProcessOutput {
        let requested = after_offset.unwrap_or(0);
        let max_bytes = max_bytes
            .unwrap_or(DEFAULT_HOST_READ_BYTES)
            .clamp(1, MAX_HOST_READ_BYTES);
        let Some(shared) = find_shared(&self.commands, id) else {
            return BackgroundProcessOutput {
                found: false,
                output: RuntimeString::default(),
                after_offset: requested,
                next_offset: requested,
                dropped_bytes: 0,
                status: None,
                exit_code: None,
                eof: true,
            };
        };
        let state = shared.lock().unwrap_or_else(|e| e.into_inner());
        let (bytes, next_offset, dropped_bytes) = output_path(self.output_dir.as_ref(), id)
            .filter(|_| state.record.output_available)
            .and_then(|path| read_output_file(&path, requested, max_bytes).ok())
            .unwrap_or_else(|| {
                let read = state.buffer.read_bytes_from(requested as usize, max_bytes);
                (
                    read.bytes,
                    read.next_cursor as u64,
                    read.dropped_bytes as u64,
                )
            });
        BackgroundProcessOutput {
            found: true,
            output: NodeChunkUtf8::decode_chunk(&bytes),
            after_offset: requested,
            next_offset,
            dropped_bytes,
            status: Some(state.record.status),
            exit_code: state.record.exit_code,
            eof: state.record.status.is_terminal()
                && next_offset >= state.record.output_next_offset,
        }
    }

    pub fn snapshot(&self) -> BackgroundProcessSnapshot {
        snapshot_locked(&self.commands)
    }

    pub fn kill(&self, id: &RuntimeString) -> bool {
        self.kill_with_signal(id, Signal::Term)
    }

    pub fn kill_with_signal(&self, id: &RuntimeString, signal: Signal) -> bool {
        self.stop_with_signal(id, signal).accepted
    }

    pub fn stop_with_signal(
        &self,
        id: &RuntimeString,
        signal: Signal,
    ) -> BackgroundProcessStopResult {
        let Some(shared) = find_shared(&self.commands, id) else {
            return BackgroundProcessStopResult {
                found: false,
                accepted: false,
                process: None,
            };
        };
        request_stop(&shared, signal, &self.now)
    }

    /// The synchronous request signals first. The monitor owns the record even
    /// if this caller is cancelled while waiting for writer capacity.
    pub async fn stop_with_signal_and_wait(
        &self,
        id: &RuntimeString,
        signal: Signal,
    ) -> Result<BackgroundProcessStopResult, RuntimeString> {
        let Some(shared) = find_shared(&self.commands, id) else {
            return Ok(BackgroundProcessStopResult {
                found: false,
                accepted: false,
                process: None,
            });
        };
        let result = request_stop(&shared, signal, &self.now);
        wait_stop_receipt(&shared).await?;
        Ok(result)
    }

    pub fn list(&self) -> Vec<BackgroundCommandListEntry> {
        self.snapshot()
            .processes
            .into_iter()
            .map(|record| {
                let status = compatibility_status(&record);
                BackgroundCommandListEntry {
                    id: record.shell_id,
                    command: record.command,
                    status,
                    exit_code: record.exit_code,
                }
            })
            .collect()
    }

    pub fn has(&self, id: &RuntimeString) -> bool {
        find_shared(&self.commands, id).is_some()
    }

    pub fn remove(&self, id: &RuntimeString) {
        let removed = {
            let mut commands = self.commands.lock().unwrap_or_else(|e| e.into_inner());
            let before = commands.len();
            commands.retain(|command| {
                command
                    .shared
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .record
                    .shell_id
                    != *id
            });
            before != commands.len()
        };
        if removed {
            self.ids
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .release(&CommandProcessId::new(id.clone()));
        }
    }

    pub fn kill_all(&self) {
        self.close_admission_and_kill_all();
    }

    pub fn close_admission_and_kill_all(&self) {
        let monitors = {
            let mut lifecycle = self.lifecycle.lock().unwrap_or_else(|e| e.into_inner());
            lifecycle.admission_closed = true;
            lifecycle.tasks.clone()
        };
        // Removed rows remain owned by their monitors until process and writer
        // responsibilities have both finished.
        for monitor in monitors {
            if let Some(shared) = monitor.shared.upgrade() {
                request_stop(&shared, Signal::Kill, &self.now);
            }
        }
    }

    /// Fence accepted stop records and the current per-process write sequence.
    /// A raw rollout flush must follow this fence.
    pub async fn flush(&self) -> Result<(), RuntimeString> {
        self.persist_initial_records().await?;
        let monitors = self
            .lifecycle
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .tasks
            .clone();
        for monitor in monitors {
            let Some(shared) = monitor.shared.upgrade() else {
                match monitor.completion.borrow().clone() {
                    Some(result) => result?,
                    None => {
                        return Err(RuntimeString::from(
                            "background monitor ended without a completion",
                        ))
                    }
                }
                continue;
            };
            let sequence = shared
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .sequence
                .clone();
            loop {
                wait_stop_receipt(&shared).await?;
                let _sequence = sequence.lock().await;
                let state = shared.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(error) = &state.persistence_error {
                    return Err(error.clone());
                }
                if state.pending_stop.is_none() {
                    break;
                }
            }
        }
        Ok(())
    }

    /// A terminal error is still a completed owner. A pending monitor prevents
    /// the Session from closing the writer underneath accepted lifecycle data.
    pub fn persistence_is_fenced(&self) -> bool {
        self.lifecycle
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .tasks
            .iter()
            .all(|monitor| monitor.completion.borrow().is_some())
            && self
                .initial_records
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .is_empty()
    }

    pub async fn close_and_wait(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), RuntimeString> {
        self.close_admission_and_kill_all();
        let tasks = self
            .lifecycle
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .tasks
            .clone();
        tokio::time::timeout_at(deadline, async {
            let mut failure = None;
            for monitor in tasks {
                let mut task = monitor.task.lock().await;
                if let Some(handle) = task.as_mut() {
                    // Await by reference so cancellation retains the join handle.
                    if let Err(error) = handle.await {
                        failure.get_or_insert_with(|| RuntimeString::from(error.to_string()));
                    }
                    *task = None;
                }
            }
            let flushed = self.flush().await;
            failure.map_or(flushed, Err)
        })
        .await
        .map_err(|_| {
            RuntimeString::from("background processes did not stop before shutdown deadline")
        })?
    }
}

async fn mirror_adopted_process(
    shared: Arc<Mutex<SharedState>>,
    commands: Arc<Mutex<Vec<BackgroundCommand>>>,
    persistence: Option<BackgroundProcessPersistenceSink>,
    subscribers: Arc<Mutex<BackgroundProcessSubscribers>>,
    now: BackgroundProcessNow,
    mut events: ProcessEventReceiver,
    started: tokio::sync::oneshot::Sender<Result<(), RuntimeString>>,
) {
    let finished = CancellationToken::new();
    let work = async {
        update_status(
            &shared,
            BackgroundProcessStatus::Running,
            &commands,
            &persistence,
            &subscribers,
            &now,
        )
        .await;
        let result = shared
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .persistence_error
            .clone()
            .map_or(Ok(()), Err);
        let _ = started.send(result);
        loop {
            match events.recv().await {
                Ok(ProcessEvent::Output(chunk)) => {
                    push_output(
                        &shared,
                        &chunk.chunk,
                        &commands,
                        &persistence,
                        &subscribers,
                        &now,
                    )
                    .await;
                }
                Ok(ProcessEvent::Failed(message)) => {
                    terminalize(
                        &shared,
                        BackgroundProcessStatus::Failed,
                        None,
                        None,
                        Some(message),
                        &commands,
                        &persistence,
                        &subscribers,
                        &now,
                    )
                    .await;
                    finish_callback(&shared).await;
                }
                Ok(ProcessEvent::Exited { exit_code, .. }) => {
                    terminalize(
                        &shared,
                        if exit_code == Some(0) {
                            BackgroundProcessStatus::Completed
                        } else {
                            BackgroundProcessStatus::Failed
                        },
                        exit_code,
                        None,
                        None,
                        &commands,
                        &persistence,
                        &subscribers,
                        &now,
                    )
                    .await;
                    finish_callback(&shared).await;
                }
                Ok(ProcessEvent::Closed { .. }) => {
                    if !shared
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .record
                        .status
                        .is_terminal()
                    {
                        terminalize(
                            &shared,
                            BackgroundProcessStatus::Failed,
                            None,
                            None,
                            Some(RuntimeString::from(
                                "interactive background process closed without an exit status",
                            )),
                            &commands,
                            &persistence,
                            &subscribers,
                            &now,
                        )
                        .await;
                    }
                    break;
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    terminalize(
                        &shared,
                        BackgroundProcessStatus::Failed,
                        None,
                        None,
                        Some(RuntimeString::from(
                            "interactive background process event stream closed",
                        )),
                        &commands,
                        &persistence,
                        &subscribers,
                        &now,
                    )
                    .await;
                    break;
                }
            }
        }
        finished.cancel();
    };
    tokio::join!(
        work,
        drive_stop_requests(&shared, &commands, &persistence, &subscribers, &finished,)
    );
    finish_callback(&shared).await;
}

impl Drop for BackgroundCommandRegistry {
    fn drop(&mut self) {
        self.kill_all();
    }
}

fn retain_monitor(
    shared: &Arc<Mutex<SharedState>>,
    work: impl Future<Output = ()> + Send + 'static,
) -> Arc<BackgroundMonitor> {
    let shared_owner = shared.clone();
    let (completion, receipt) = watch::channel(None);
    let task = tokio::spawn(async move {
        work.await;
        let result = shared_owner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .persistence_error
            .clone()
            .map_or(Ok(()), Err);
        completion.send_replace(Some(result));
    });
    Arc::new(BackgroundMonitor {
        // A removed row can release its buffer once the retained task finishes.
        // Its join handle and completion remain available to Session shutdown.
        shared: Arc::downgrade(shared),
        task: AsyncMutex::new(Some(task)),
        completion: receipt,
    })
}

fn take_complete(
    st: &mut SharedState,
    id: &RuntimeString,
) -> Option<(BackgroundCommandOnComplete, BackgroundCommandComplete)> {
    if st.complete_fired {
        return None;
    }
    st.complete_fired = true;
    let cb = st.on_complete.take()?;
    let result = BackgroundCommandComplete {
        id: id.clone(),
        status: compatibility_status(&st.record),
        exit_code: st.record.exit_code,
    };
    Some((cb, result))
}

async fn run_background_process(
    process_context: RuntimeProcessContext,
    spec: crate::types::sandbox::SpawnSpec,
    shared: Arc<Mutex<SharedState>>,
    commands: Arc<Mutex<Vec<BackgroundCommand>>>,
    persistence: Option<BackgroundProcessPersistenceSink>,
    subscribers: Arc<Mutex<BackgroundProcessSubscribers>>,
    now: BackgroundProcessNow,
) {
    let finished = CancellationToken::new();
    let work = async {
        run_background_process_inner(
            &process_context,
            spec,
            &shared,
            &commands,
            &persistence,
            &subscribers,
            &now,
        )
        .await;
        finished.cancel();
    };
    tokio::join!(
        work,
        drive_stop_requests(&shared, &commands, &persistence, &subscribers, &finished)
    );
    finish_callback(&shared).await;
}

async fn run_background_process_inner(
    process_context: &RuntimeProcessContext,
    spec: crate::types::sandbox::SpawnSpec,
    shared: &Arc<Mutex<SharedState>>,
    commands: &Arc<Mutex<Vec<BackgroundCommand>>>,
    persistence: &Option<BackgroundProcessPersistenceSink>,
    subscribers: &Arc<Mutex<BackgroundProcessSubscribers>>,
    now: &BackgroundProcessNow,
) {
    update_status(
        shared,
        BackgroundProcessStatus::Running,
        commands,
        persistence,
        subscribers,
        now,
    )
    .await;
    let stopped = {
        let state = shared.lock().unwrap_or_else(|e| e.into_inner());
        if state.persistence_error.is_some() {
            return;
        }
        state.stop_requested.clone()
    };
    // Do not poll the spawner after a stop accepted before controller creation.
    let result = tokio::select! {
        biased;
        _ = stopped.cancelled() => return,
        result = async {
        let spawner = process_context.spawner();
        let request =
            RuntimeSpawnRequest::from_spawn_spec(spec, RuntimeProcessSurface::BackgroundShell);
        spawner.spawn(request).await
        } => result,
    };
    let mut child = match result {
        Ok(child) => child,
        Err(error) => {
            push_output(
                shared,
                &RuntimeString::from("\n[spawn error] ")
                    .concat(error.message())
                    .concat(&RuntimeString::from("\n"))
                    .to_node_utf8(),
                commands,
                persistence,
                subscribers,
                now,
            )
            .await;
            terminalize(
                shared,
                BackgroundProcessStatus::Failed,
                None,
                None,
                Some(error.message().clone()),
                commands,
                persistence,
                subscribers,
                now,
            )
            .await;
            return;
        }
    };
    let controller = child.controller();
    let stop_signal = {
        let mut state = shared.lock().unwrap_or_else(|e| e.into_inner());
        state.controller = Some(controller.clone());
        state
            .record
            .stop_requested
            .then(|| match state.record.termination_signal.as_ref() {
                Some(signal) if signal == "SIGKILL" => RuntimeSignal::Kill,
                Some(signal) if signal == "SIGINT" => RuntimeSignal::Interrupt,
                _ => RuntimeSignal::Terminate,
            })
    };
    if let Some(signal) = stop_signal {
        controller.signal(signal);
    }
    let stdout = child.take_stdout().expect("background stdout piped");
    let stderr = child.take_stderr().expect("background stderr piped");
    let stop_output = CancellationToken::new();
    let output = async {
        tokio::join!(
            pump(
                stdout,
                Arc::clone(shared),
                commands.clone(),
                persistence.clone(),
                subscribers.clone(),
                now.clone(),
                stop_output.clone()
            ),
            pump(
                stderr,
                Arc::clone(shared),
                commands.clone(),
                persistence.clone(),
                subscribers.clone(),
                now.clone(),
                stop_output.clone()
            )
        );
        let mut state = shared.lock().unwrap_or_else(|e| e.into_inner());
        state.output_closed = true;
        state.output_changed.notify_waiters();
    };
    tokio::pin!(output);
    let status = {
        let terminal = child.wait_terminal();
        tokio::pin!(terminal);
        tokio::select! {
            status = &mut terminal => {
                {
                    let mut state = shared.lock().unwrap_or_else(|e| e.into_inner());
                    state.exit_observed = Some(status.as_ref().ok().and_then(|status| status.code()));
                    state.output_changed.notify_waiters();
                }
                if tokio::time::timeout(TRAILING_OUTPUT_GRACE, &mut output).await.is_err() {
                stop_output.cancel();
                // Finish publishing bytes already read before closing the pipes.
                output.as_mut().await;
            }
                status
            },
            _ = &mut output => terminal.await,
        }
    };
    match status {
        Ok(status) => {
            terminalize(
                shared,
                if status.code() == Some(0) {
                    BackgroundProcessStatus::Completed
                } else {
                    BackgroundProcessStatus::Failed
                },
                status.code(),
                status
                    .signal()
                    .map(|value| RuntimeString::from(value.to_string())),
                None,
                commands,
                persistence,
                subscribers,
                now,
            )
            .await;
        }
        Err(error) => {
            terminalize(
                shared,
                BackgroundProcessStatus::Failed,
                None,
                None,
                Some(error.message().clone()),
                commands,
                persistence,
                subscribers,
                now,
            )
            .await
        }
    }
    child.disarm();
}

async fn pump<R>(
    mut pipe: R,
    shared: Arc<Mutex<SharedState>>,
    commands: Arc<Mutex<Vec<BackgroundCommand>>>,
    persistence: Option<BackgroundProcessPersistenceSink>,
    subscribers: Arc<Mutex<BackgroundProcessSubscribers>>,
    now: BackgroundProcessNow,
    stop_reading: CancellationToken,
) where
    R: AsyncReadExt + Unpin,
{
    let mut buf = [0u8; 65536];
    loop {
        let read = tokio::select! {
            biased;
            _ = stop_reading.cancelled() => break,
            read = pipe.read(&mut buf) => read,
        };
        match read {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                push_output(
                    &shared,
                    &buf[..n],
                    &commands,
                    &persistence,
                    &subscribers,
                    &now,
                )
                .await
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn terminalize(
    shared: &Arc<Mutex<SharedState>>,
    status: BackgroundProcessStatus,
    exit_code: Option<i32>,
    termination_signal: Option<RuntimeString>,
    failure: Option<RuntimeString>,
    commands: &Arc<Mutex<Vec<BackgroundCommand>>>,
    persistence: &Option<BackgroundProcessPersistenceSink>,
    subscribers: &Arc<Mutex<BackgroundProcessSubscribers>>,
    now: &BackgroundProcessNow,
) {
    mutate_record(
        shared,
        commands,
        persistence,
        subscribers,
        Some(|state: &mut SharedState| {
            state.controller = None;
            state.output_closed = true;
            state.exit_observed = Some(exit_code);
            if state.record.status.is_terminal() {
                if state.record.status == BackgroundProcessStatus::Stopped {
                    state.record.exit_code = exit_code;
                    return Some(BackgroundEmission::Silent(state.record.clone()));
                }
                return None;
            }
            let now_ms = now();
            state.controller = None;
            state.record.status = status;
            state.record.updated_at_ms = now_ms;
            state.record.completed_at_ms = Some(now_ms);
            state.record.exit_code = exit_code;
            state.record.termination_signal = termination_signal;
            state.record.failure = failure.or_else(|| {
                (status == BackgroundProcessStatus::Failed && exit_code != Some(0))
                    .then(|| RuntimeString::from("background process exited unsuccessfully"))
            });
            Some(BackgroundEmission::Updated(state.record.clone(), true))
        }),
    )
    .await;
}

async fn update_status(
    shared: &Arc<Mutex<SharedState>>,
    status: BackgroundProcessStatus,
    commands: &Arc<Mutex<Vec<BackgroundCommand>>>,
    persistence: &Option<BackgroundProcessPersistenceSink>,
    subscribers: &Arc<Mutex<BackgroundProcessSubscribers>>,
    now: &BackgroundProcessNow,
) {
    mutate_record(
        shared,
        commands,
        persistence,
        subscribers,
        Some(|state: &mut SharedState| {
            if state.record.status.is_terminal() {
                return None;
            }
            state.record.status = status;
            state.record.updated_at_ms = now();
            Some(BackgroundEmission::Updated(state.record.clone(), false))
        }),
    )
    .await;
}

async fn push_output(
    shared: &Arc<Mutex<SharedState>>,
    chunk: &[u8],
    commands: &Arc<Mutex<Vec<BackgroundCommand>>>,
    persistence: &Option<BackgroundProcessPersistenceSink>,
    subscribers: &Arc<Mutex<BackgroundProcessSubscribers>>,
    now: &BackgroundProcessNow,
) {
    mutate_record(
        shared,
        commands,
        persistence,
        subscribers,
        Some(|state: &mut SharedState| {
            let after_offset = state.record.output_next_offset;
            state.buffer.push(chunk);
            let had_output_file = state.output_file.is_some();
            if let Some(file) = state.output_file.as_mut() {
                if file.write_all(chunk).and_then(|_| file.flush()).is_err() {
                    state.output_file = None;
                    state.record.output_available = false;
                }
            }
            state.record.updated_at_ms = now();
            state.record.output_next_offset = state.buffer.end() as u64;
            state.record.output_start_offset = if state.output_file.is_some() {
                0
            } else {
                state.buffer.retained_start() as u64
            };
            state.record.dropped_bytes = state.record.output_start_offset;
            if had_output_file && state.output_file.is_some() {
                state.record.output_available = true;
            }
            Some(BackgroundEmission::Output(
                state.record.clone(),
                after_offset,
                NodeChunkUtf8::decode_chunk(chunk),
            ))
        }),
    )
    .await;
}

async fn finish_callback(shared: &Arc<Mutex<SharedState>>) {
    let interaction = shared
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .interaction
        .clone();
    let _interaction = interaction.lock().await;
    let id = shared
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .record
        .shell_id
        .clone();
    let fired = {
        let mut state = shared.lock().unwrap_or_else(|e| e.into_inner());
        take_complete(&mut state, &id)
    };
    if let Some((callback, complete)) = fired {
        callback(complete);
    }
}

fn find_shared(
    commands: &Arc<Mutex<Vec<BackgroundCommand>>>,
    id: &RuntimeString,
) -> Option<Arc<Mutex<SharedState>>> {
    commands
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .find_map(|command| {
            let matches = command
                .shared
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .record
                .shell_id
                == *id;
            matches.then(|| Arc::clone(&command.shared))
        })
}

fn snapshot_locked(commands: &Arc<Mutex<Vec<BackgroundCommand>>>) -> BackgroundProcessSnapshot {
    BackgroundProcessSnapshot {
        processes: commands
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|command| {
                command
                    .shared
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .record
                    .clone()
            })
            .collect(),
    }
}

fn compatibility_status(record: &BackgroundProcessRecord) -> BackgroundCommandStatus {
    match record.status {
        BackgroundProcessStatus::Pending
        | BackgroundProcessStatus::Running
        | BackgroundProcessStatus::Paused => BackgroundCommandStatus::Running,
        BackgroundProcessStatus::Stopped => BackgroundCommandStatus::Killed,
        BackgroundProcessStatus::Completed | BackgroundProcessStatus::Failed => {
            BackgroundCommandStatus::Exited
        }
    }
}

async fn prepare(
    sink: &Option<BackgroundProcessPersistenceSink>,
) -> Result<PreparedBackgroundProcessAppend<'_>, RuntimeString> {
    match sink {
        Some(sink) => sink.prepare().await,
        None => Ok(PreparedBackgroundProcessAppend::new(|_| {})),
    }
}

enum BackgroundEmission {
    Updated(BackgroundProcessRecord, bool),
    Output(BackgroundProcessRecord, u64, RuntimeString),
    Silent(BackgroundProcessRecord),
}

impl BackgroundEmission {
    fn record(&self) -> &BackgroundProcessRecord {
        match self {
            Self::Updated(record, _) | Self::Output(record, _, _) | Self::Silent(record) => record,
        }
    }

    fn emit(
        self,
        commands: &Arc<Mutex<Vec<BackgroundCommand>>>,
        subscribers: &Arc<Mutex<BackgroundProcessSubscribers>>,
    ) {
        match self {
            Self::Updated(record, notification) => {
                emit_item(
                    subscribers,
                    BackgroundProcessEventSubtype::TaskUpdated,
                    record.clone(),
                );
                if notification {
                    emit_item(
                        subscribers,
                        BackgroundProcessEventSubtype::TaskNotification,
                        record,
                    );
                }
                emit_snapshot(commands, subscribers);
            }
            Self::Output(record, after_offset, output) => emit(
                subscribers,
                BackgroundProcessEvent {
                    subtype: BackgroundProcessEventSubtype::TaskProgress,
                    shell_id: Some(record.shell_id.clone()),
                    process: Some(record.clone()),
                    processes: None,
                    output: Some(output),
                    after_offset: Some(after_offset),
                    next_offset: Some(record.output_next_offset),
                    dropped_bytes: Some(record.dropped_bytes),
                },
            ),
            Self::Silent(_) => {}
        }
    }
}

/// Readers and terminalization share one asynchronous sequence. Stop bypasses
/// its capacity wait to signal immediately, leaving one record for this owner.
async fn mutate_record(
    shared: &Arc<Mutex<SharedState>>,
    commands: &Arc<Mutex<Vec<BackgroundCommand>>>,
    persistence: &Option<BackgroundProcessPersistenceSink>,
    subscribers: &Arc<Mutex<BackgroundProcessSubscribers>>,
    change: Option<impl FnOnce(&mut SharedState) -> Option<BackgroundEmission> + Send>,
) {
    let sequence = shared
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .sequence
        .clone();
    let _sequence = sequence.lock().await;
    let mut change = change;
    loop {
        if change.is_none()
            && shared
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .pending_stop
                .is_none()
        {
            return;
        }
        let prepared = match prepare(persistence).await {
            Ok(prepared) => prepared,
            Err(error) => {
                let (controller, stop, id) = {
                    let mut state = shared.lock().unwrap_or_else(|e| e.into_inner());
                    state.persistence_error = Some(error.clone());
                    if let Some(receipt) = &state.stop_receipt {
                        receipt.send_replace(Some(Err(error)));
                    }
                    (
                        state.controller.clone(),
                        state.external_stop.clone(),
                        state.record.shell_id.clone(),
                    )
                };
                if let Some(controller) = controller {
                    controller.signal(RuntimeSignal::Kill);
                } else if let Some(stop) = stop {
                    stop(&id, Signal::Kill);
                }
                return;
            }
        };
        let (emission, stopped) = {
            let mut state = shared.lock().unwrap_or_else(|e| e.into_inner());
            // Recheck after capacity arrives: no stale Running/output snapshot
            // may overtake a stop that was accepted while this writer waited.
            if let Some(record) = state.pending_stop.take() {
                prepared.commit(record.clone());
                if let Some(receipt) = &state.stop_receipt {
                    receipt.send_replace(Some(Ok(())));
                }
                (Some(BackgroundEmission::Updated(record, true)), true)
            } else {
                let emission = change.take().and_then(|change| change(&mut state));
                if let Some(emission) = &emission {
                    prepared.commit(emission.record().clone());
                }
                (emission, false)
            }
        };
        if let Some(emission) = emission {
            shared
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .output_changed
                .notify_waiters();
            emission.emit(commands, subscribers);
        }
        if !stopped {
            return;
        }
    }
}

fn request_stop(
    shared: &Arc<Mutex<SharedState>>,
    signal: Signal,
    now: &BackgroundProcessNow,
) -> BackgroundProcessStopResult {
    let (accepted, record, controller, external_stop, notify) = {
        let mut state = shared.lock().unwrap_or_else(|e| e.into_inner());
        let accepted = !state.record.status.is_terminal();
        if accepted {
            let now_ms = now();
            state.record.status = BackgroundProcessStatus::Stopped;
            state.record.updated_at_ms = now_ms;
            state.record.completed_at_ms = Some(now_ms);
            state.record.stop_requested = true;
            state.record.termination_signal = Some(signal_label(signal).into());
            state.pending_stop = Some(state.record.clone());
            let failure = state.persistence_error.clone().map(Err);
            let (receipt, _) = watch::channel(failure);
            state.stop_receipt = Some(receipt);
            state.stop_requested.cancel();
            if signal != Signal::Kill {
                state.fallback_at = Some(
                    tokio::time::Instant::now() + Duration::from_secs(KILL_FALLBACK_DELAY_SECS),
                );
            }
        }
        let signal_again = accepted || (state.record.stop_requested && signal == Signal::Kill);
        (
            accepted,
            state.record.clone(),
            signal_again.then(|| state.controller.clone()).flatten(),
            signal_again.then(|| state.external_stop.clone()).flatten(),
            state.stop_notify.clone(),
        )
    };
    // Physical stop is independent of the bounded writer and async sequence.
    if let Some(controller) = controller {
        controller.signal(runtime_signal(signal));
    } else if let Some(stop) = external_stop {
        stop(&record.shell_id, signal);
    }
    if accepted {
        notify.notify_one();
    }
    BackgroundProcessStopResult {
        found: true,
        accepted,
        process: Some(record),
    }
}

async fn wait_stop_receipt(shared: &Arc<Mutex<SharedState>>) -> Result<(), RuntimeString> {
    let receipt = shared
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .stop_receipt
        .as_ref()
        .map(watch::Sender::subscribe);
    let Some(mut receipt) = receipt else {
        return Ok(());
    };
    loop {
        if let Some(result) = receipt.borrow().clone() {
            return result;
        }
        receipt.changed().await.map_err(|_| {
            RuntimeString::from("background stop owner ended without a persistence receipt")
        })?;
    }
}

async fn drive_stop_requests(
    shared: &Arc<Mutex<SharedState>>,
    commands: &Arc<Mutex<Vec<BackgroundCommand>>>,
    persistence: &Option<BackgroundProcessPersistenceSink>,
    subscribers: &Arc<Mutex<BackgroundProcessSubscribers>>,
    finished: &CancellationToken,
) {
    let (notify, stopped) = {
        let state = shared.lock().unwrap_or_else(|e| e.into_inner());
        (state.stop_notify.clone(), state.stop_requested.clone())
    };
    let drain = async {
        loop {
            tokio::select! {
                biased;
                _ = notify.notified() => {},
                _ = finished.cancelled() => {},
            }
            let pending = shared
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .pending_stop
                .is_some();
            if pending {
                mutate_record(
                    shared,
                    commands,
                    persistence,
                    subscribers,
                    None::<fn(&mut SharedState) -> Option<BackgroundEmission>>,
                )
                .await;
            }
            if finished.is_cancelled() {
                break;
            }
        }
    };
    let fallback = async {
        tokio::select! {
            biased;
            _ = stopped.cancelled() => {},
            _ = finished.cancelled() => return,
        }
        let deadline = shared.lock().unwrap_or_else(|e| e.into_inner()).fallback_at;
        if let Some(deadline) = deadline {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {
                    let (controller, stop, id) = {
                        let state = shared.lock().unwrap_or_else(|e| e.into_inner());
                        (state.controller.clone(), state.external_stop.clone(), state.record.shell_id.clone())
                    };
                    if let Some(controller) = controller {
                        controller.signal(RuntimeSignal::Kill);
                    } else if let Some(stop) = stop {
                        stop(&id, Signal::Kill);
                    }
                }
                _ = finished.cancelled() => {},
            }
        }
    };
    tokio::join!(drain, fallback);
}

fn emit(subscribers: &Arc<Mutex<BackgroundProcessSubscribers>>, event: BackgroundProcessEvent) {
    let sinks = subscribers
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .sinks
        .values()
        .cloned()
        .collect::<Vec<_>>();
    for sink in sinks {
        let event = event.clone();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sink(event)));
    }
}

fn emit_item(
    subscribers: &Arc<Mutex<BackgroundProcessSubscribers>>,
    subtype: BackgroundProcessEventSubtype,
    process: BackgroundProcessRecord,
) {
    emit(
        subscribers,
        BackgroundProcessEvent {
            subtype,
            shell_id: Some(process.shell_id.clone()),
            process: Some(process),
            processes: None,
            output: None,
            after_offset: None,
            next_offset: None,
            dropped_bytes: None,
        },
    );
}

fn emit_snapshot(
    commands: &Arc<Mutex<Vec<BackgroundCommand>>>,
    subscribers: &Arc<Mutex<BackgroundProcessSubscribers>>,
) {
    emit(
        subscribers,
        BackgroundProcessEvent {
            subtype: BackgroundProcessEventSubtype::BackgroundTasksChanged,
            shell_id: None,
            process: None,
            processes: Some(snapshot_locked(commands).processes),
            output: None,
            after_offset: None,
            next_offset: None,
            dropped_bytes: None,
        },
    );
}

fn output_path(dir: Option<&RuntimePath>, id: &RuntimeString) -> Option<std::path::PathBuf> {
    let dir = dir?.as_runtime_string().try_to_utf8().ok()?;
    let id = id.try_to_utf8().ok()?;
    let safe = id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    Some(std::path::Path::new(&dir).join(format!("{safe}.log")))
}

fn create_output_file(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
}

fn read_output_file(
    path: &std::path::Path,
    after_offset: u64,
    max_bytes: usize,
) -> std::io::Result<(Vec<u8>, u64, u64)> {
    let mut file = std::fs::File::open(path)?;
    let len = file.metadata()?.len();
    let start = after_offset.min(len);
    file.seek(SeekFrom::Start(start))?;
    let mut bytes = vec![0; max_bytes.min(len.saturating_sub(start) as usize)];
    file.read_exact(&mut bytes)?;
    let next = start.saturating_add(bytes.len() as u64);
    Ok((bytes, next, 0))
}

fn output_file_covers_record(path: &std::path::Path, record: &BackgroundProcessRecord) -> bool {
    path.metadata()
        .map(|metadata| metadata.is_file() && metadata.len() >= record.output_next_offset)
        .unwrap_or(false)
}

fn system_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn runtime_signal(signal: Signal) -> RuntimeSignal {
    match signal {
        Signal::Int => RuntimeSignal::Interrupt,
        Signal::Kill => RuntimeSignal::Kill,
        Signal::Hup | Signal::Quit | Signal::Usr1 | Signal::Usr2 | Signal::Term => {
            RuntimeSignal::Terminate
        }
    }
}

fn signal_label(signal: Signal) -> &'static str {
    match signal {
        Signal::Term => "SIGTERM",
        Signal::Kill => "SIGKILL",
        Signal::Int => "SIGINT",
        Signal::Hup => "SIGHUP",
        Signal::Quit => "SIGQUIT",
        Signal::Usr1 => "SIGUSR1",
        Signal::Usr2 => "SIGUSR2",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_process::ParentEnvSnapshot;
    use crate::schemas::background_process::RECOVERY_INTERRUPTED_REASON;
    use crate::types::sandbox::{
        ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    struct IdSandbox;

    impl SandboxBackend for IdSandbox {
        fn kind(&self) -> SandboxKind {
            SandboxKind::Process
        }

        fn is_available(&self) -> bool {
            true
        }

        fn transform(&self, req: &ExecRequest, _policy: &SandboxPolicy) -> SpawnSpec {
            let file = req.argv.first().cloned().unwrap_or_default();
            let args = req
                .argv
                .get(1..)
                .map(<[RuntimeString]>::to_vec)
                .unwrap_or_default();
            let env = req.env.clone().unwrap_or_else(ParentEnvSnapshot::capture);
            SpawnSpec {
                file,
                args,
                cwd: req.cwd.clone(),
                env,
            }
        }
    }

    struct CaptureSandbox {
        captured: Arc<Mutex<Option<(ExecRequest, SandboxPolicy)>>>,
    }

    impl SandboxBackend for CaptureSandbox {
        fn kind(&self) -> SandboxKind {
            SandboxKind::Process
        }

        fn is_available(&self) -> bool {
            true
        }

        fn transform(&self, req: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
            *self.captured.lock().unwrap() = Some((req.clone(), policy.clone()));
            SpawnSpec {
                file: req.argv[0].clone(),
                args: req.argv[1..].to_vec(),
                cwd: req.cwd.clone(),
                env: req.env.clone().unwrap_or_default(),
            }
        }
    }

    pub(super) fn cwd() -> RuntimePath {
        RuntimePath::from(
            std::env::current_dir()
                .unwrap()
                .to_string_lossy()
                .into_owned(),
        )
    }

    fn runtime_env(entries: &[(&str, &str)]) -> RuntimeEnv {
        let mut env = RuntimeEnv::new();
        for (key, value) in entries {
            env.insert((*key).into(), (*value).into());
        }
        env
    }

    pub(super) fn start_options(
        command: &str,
        on_complete: Option<BackgroundCommandOnComplete>,
    ) -> StartBackgroundCommandOptions {
        StartBackgroundCommandOptions {
            command: command.into(),
            cwd: cwd(),
            sandbox: Arc::new(IdSandbox),
            env: None,
            max_output_bytes: 1024,
            writable_roots: None,
            unconfined_writes: false,
            network_allowed: None,
            managed_network: None,
            on_complete,
        }
    }

    async fn wait_for_terminal(
        registry: &BackgroundCommandRegistry,
        id: &RuntimeString,
    ) -> BackgroundProcessRecord {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let record = registry
                .snapshot()
                .processes
                .into_iter()
                .find(|record| record.shell_id == *id)
                .expect("process exists");
            if record.status.is_terminal() {
                return record;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "process did not finish"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn background_registry_fires_once_with_exit_code() {
        let registry = BackgroundCommandRegistry::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_cb = Arc::clone(&calls);
        let (tx, rx) = tokio::sync::oneshot::channel();
        registry
            .start(
                crate::runtime_process::test_runtime_process_context(),
                StartBackgroundCommandOptions {
                    command: "exit 3".into(),
                    cwd: cwd(),
                    sandbox: Arc::new(IdSandbox),
                    env: None,
                    max_output_bytes: 1024,
                    writable_roots: None,
                    unconfined_writes: false,
                    network_allowed: None,
                    managed_network: None,
                    on_complete: Some(Box::new(move |result| {
                        calls_cb.fetch_add(1, Ordering::SeqCst);
                        let _ = tx.send((result.status, result.exit_code));
                    })),
                },
            )
            .await
            .unwrap();

        let (status, exit_code) = rx.await.unwrap();
        assert_eq!(status, BackgroundCommandStatus::Exited);
        assert_eq!(exit_code, Some(3));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        registry.kill_all();
    }

    #[tokio::test]
    async fn background_registry_read_new_drains_incrementally() {
        let registry = BackgroundCommandRegistry::new();
        // A filesystem flag gates the second chunk so the two increments stay
        // deterministic under arbitrary scheduler load.
        let flag = std::env::temp_dir().join(format!(
            "klaus-bg-read-new-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let flag_shell = flag.to_string_lossy().into_owned();
        let id = registry
            .start(
                crate::runtime_process::test_runtime_process_context(),
                StartBackgroundCommandOptions {
                    command: format!(
                    "printf first; until [ -e '{flag_shell}' ]; do sleep 0.05; done; printf second"
                )
                    .into(),
                    cwd: cwd(),
                    sandbox: Arc::new(IdSandbox),
                    env: None,
                    max_output_bytes: 1024,
                    writable_roots: None,
                    unconfined_writes: false,
                    network_allowed: None,
                    managed_network: None,
                    on_complete: None,
                },
            )
            .await
            .unwrap();

        let mut first = registry.read_new(&id);
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !first.output.contains("first") {
            assert!(
                std::time::Instant::now() < deadline,
                "first chunk did not arrive: {first:?}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
            first = registry.read_new(&id);
        }
        assert!(first.found);
        assert!(!first.output.contains("second"));

        std::fs::write(&flag, b"go").unwrap();
        let mut second = registry.read_new(&id);
        while !second.output.contains("second") {
            assert!(
                std::time::Instant::now() < deadline,
                "second chunk did not arrive: {second:?}"
            );
            assert!(!second.output.contains("first"));
            tokio::time::sleep(Duration::from_millis(25)).await;
            second = registry.read_new(&id);
        }
        assert!(second.found);
        assert!(!second.output.contains("first"));
        registry.kill_all();
        let _ = std::fs::remove_file(&flag);
    }

    #[tokio::test]
    async fn background_registry_kill_marks_killed_once() {
        let registry = BackgroundCommandRegistry::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_cb = Arc::clone(&calls);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let id = registry
            .start(
                crate::runtime_process::test_runtime_process_context(),
                StartBackgroundCommandOptions {
                    command: "sleep 30".into(),
                    cwd: cwd(),
                    sandbox: Arc::new(IdSandbox),
                    env: None,
                    max_output_bytes: 1024,
                    writable_roots: None,
                    unconfined_writes: false,
                    network_allowed: None,
                    managed_network: None,
                    on_complete: Some(Box::new(move |result| {
                        calls_cb.fetch_add(1, Ordering::SeqCst);
                        let _ = tx.send(result.status);
                    })),
                },
            )
            .await
            .unwrap();

        tokio::task::yield_now().await;
        assert!(registry.kill(&id));
        let status = rx.await.unwrap();
        assert_eq!(status, BackgroundCommandStatus::Killed);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        registry.kill_all();
    }

    #[tokio::test]
    async fn background_registry_spawn_request_uses_command_runtime_contract() {
        let captured = Arc::new(Mutex::new(None));
        let sandbox = CaptureSandbox {
            captured: Arc::clone(&captured),
        };
        let env = runtime_env(&[("K", "V")]);

        let registry = BackgroundCommandRegistry::new();
        let id = registry
            .start(
                crate::runtime_process::test_runtime_process_context(),
                StartBackgroundCommandOptions {
                    command: "echo hello".into(),
                    cwd: "/tmp".into(),
                    sandbox: Arc::new(sandbox),
                    env: Some(env.clone()),
                    max_output_bytes: 1024,
                    writable_roots: Some(vec!["/tmp/project".into()]),
                    unconfined_writes: false,
                    network_allowed: Some(true),
                    managed_network: None,
                    on_complete: None,
                },
            )
            .await
            .unwrap();

        assert!(!id.is_empty());
        let (request, policy) = captured.lock().unwrap().clone().unwrap();
        assert_eq!(request.cwd, RuntimePath::from("/tmp"));
        assert_eq!(request.env, Some(env));
        assert_eq!(
            policy.writable_roots,
            vec![RuntimeString::from("/tmp/project")]
        );
        assert!(policy.network_allowed);
        registry.remove(&id);
    }

    #[tokio::test]
    async fn model_cursor_and_host_offsets_are_independent() {
        let registry = BackgroundCommandRegistry::new();
        let id = registry
            .start(
                crate::runtime_process::test_runtime_process_context(),
                start_options("printf abcdef", None),
            )
            .await
            .unwrap();
        wait_for_terminal(&registry, &id).await;

        let host_first = registry.read_output(&id, Some(0), Some(3));
        let host_second = registry.read_output(&id, Some(3), Some(3));
        let model_first = registry.read_new(&id);
        let model_second = registry.read_new(&id);

        assert_eq!(host_first.output, "abc");
        assert_eq!(host_first.next_offset, 3);
        assert_eq!(host_second.output, "def");
        assert_eq!(host_second.next_offset, 6);
        assert_eq!(model_first.output, "abcdef");
        assert!(model_second.output.is_empty());
    }

    #[tokio::test]
    async fn event_sequence_reports_authoritative_snapshots_and_one_terminal_notice() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let sink_events = events.clone();
        let registry = BackgroundCommandRegistry::new();
        let unsubscribe = registry.subscribe(Arc::new(move |event| {
            sink_events.lock().unwrap().push(event)
        }));
        let id = registry
            .start(
                crate::runtime_process::test_runtime_process_context(),
                start_options("printf event-output", None),
            )
            .await
            .unwrap();
        let terminal = wait_for_terminal(&registry, &id).await;
        let captured = events.lock().unwrap().clone();
        let subtypes = captured
            .iter()
            .map(|event| event.subtype)
            .collect::<Vec<_>>();

        assert_eq!(
            subtypes.first(),
            Some(&BackgroundProcessEventSubtype::TaskStarted)
        );
        assert!(subtypes.contains(&BackgroundProcessEventSubtype::TaskProgress));
        assert_eq!(
            subtypes
                .iter()
                .filter(|subtype| **subtype == BackgroundProcessEventSubtype::TaskNotification)
                .count(),
            1
        );
        let final_snapshot = captured
            .iter()
            .rev()
            .find(|event| event.subtype == BackgroundProcessEventSubtype::BackgroundTasksChanged)
            .and_then(|event| event.processes.as_ref())
            .expect("final authoritative snapshot");
        assert_eq!(final_snapshot, &vec![terminal]);
        unsubscribe();
        let retained = captured.len();
        let second_id = registry
            .start(
                crate::runtime_process::test_runtime_process_context(),
                start_options("exit 0", None),
            )
            .await
            .unwrap();
        wait_for_terminal(&registry, &second_id).await;
        assert_eq!(events.lock().unwrap().len(), retained);
    }

    #[tokio::test]
    async fn durable_output_sidecar_is_offset_readable_and_reported_available() {
        let output_dir = std::env::temp_dir().join(format!(
            "klaus-background-sidecar-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let registry = BackgroundCommandRegistry::with_options(
            None,
            None,
            None,
            Some(RuntimePath::from(output_dir.to_string_lossy().into_owned())),
            Vec::new(),
        );
        let id = registry
            .start(
                crate::runtime_process::test_runtime_process_context(),
                start_options("printf durable", None),
            )
            .await
            .unwrap();
        let terminal = wait_for_terminal(&registry, &id).await;
        let tail = registry.read_output(&id, Some(3), Some(4));

        assert!(terminal.output_available);
        assert_eq!(tail.output, "able");
        assert_eq!(tail.next_offset, 7);
        let _ = std::fs::remove_dir_all(output_dir);
    }

    #[tokio::test]
    async fn terminal_mapping_covers_completed_failed_and_stopped() {
        let completed = BackgroundCommandRegistry::new();
        let completed_id = completed
            .start(
                crate::runtime_process::test_runtime_process_context(),
                start_options("exit 0", None),
            )
            .await
            .unwrap();
        assert_eq!(
            wait_for_terminal(&completed, &completed_id).await.status,
            BackgroundProcessStatus::Completed
        );

        let failed = BackgroundCommandRegistry::new();
        let failed_id = failed
            .start(
                crate::runtime_process::test_runtime_process_context(),
                start_options("exit 7", None),
            )
            .await
            .unwrap();
        let failed_record = wait_for_terminal(&failed, &failed_id).await;
        assert_eq!(failed_record.status, BackgroundProcessStatus::Failed);
        assert_eq!(failed_record.exit_code, Some(7));

        let stopped = BackgroundCommandRegistry::new();
        let stopped_id = stopped
            .start(
                crate::runtime_process::test_runtime_process_context(),
                start_options("sleep 30", None),
            )
            .await
            .unwrap();
        let stop = stopped.stop_with_signal(&stopped_id, Signal::Kill);
        assert!(stop.accepted);
        assert_eq!(
            stop.process.unwrap().status,
            BackgroundProcessStatus::Stopped
        );
        assert_eq!(
            wait_for_terminal(&stopped, &stopped_id).await.status,
            BackgroundProcessStatus::Stopped
        );
    }

    #[tokio::test]
    async fn cold_recovery_normalizes_nonterminal_records_and_downgrades_missing_output() {
        struct CapturingPersistence(Arc<Mutex<Vec<BackgroundProcessRecord>>>);

        #[async_trait::async_trait]
        impl BackgroundProcessPersistence for CapturingPersistence {
            async fn prepare(&self) -> Result<PreparedBackgroundProcessAppend<'_>, RuntimeString> {
                Ok(PreparedBackgroundProcessAppend::new(|record| {
                    self.0.lock().unwrap().push(record);
                }))
            }
        }

        let persisted = Arc::new(Mutex::new(Vec::new()));
        let record = BackgroundProcessRecord {
            shell_id: "recover-shell".into(),
            command: "sleep 30".into(),
            cwd: "/tmp".into(),
            status: BackgroundProcessStatus::Running,
            started_at_ms: 1,
            updated_at_ms: 2,
            completed_at_ms: None,
            exit_code: None,
            termination_signal: None,
            failure: None,
            stop_requested: false,
            output_start_offset: 0,
            output_next_offset: 4,
            dropped_bytes: 0,
            output_available: true,
        };
        let registry = BackgroundCommandRegistry::with_options(
            Some(Arc::new(|| 99)),
            Some(Arc::new(CapturingPersistence(persisted.clone()))),
            None,
            Some(cwd()),
            vec![record],
        );
        registry.flush().await.unwrap();
        let recovered = registry.snapshot().processes.remove(0);

        assert_eq!(recovered.status, BackgroundProcessStatus::Stopped);
        assert_eq!(recovered.completed_at_ms, Some(99));
        assert_eq!(
            recovered.failure,
            Some(RuntimeString::from(RECOVERY_INTERRUPTED_REASON))
        );
        assert!(!recovered.output_available);
        assert_eq!(persisted.lock().unwrap().as_slice(), &[recovered]);
    }
}

#[cfg(all(test, unix))]
#[path = "background_writer_tests.rs"]
mod writer_tests;
