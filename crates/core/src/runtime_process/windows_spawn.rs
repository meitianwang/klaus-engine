//! Project-owned Windows suspended-process transaction shadow.
//!
//! This module deliberately has no production call site yet.  It closes the
//! ownership gap between a pure [`WindowsExecImage`](super::windows::WindowsExecImage)
//! and the later runtime cutover: every process starts suspended, owns one
//! pre-created threadpool wait, and either commits atomically into the process
//! map or enters bounded hidden cleanup while it is still descendant-free.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, PoisonError, Weak};
use std::time::{Duration, Instant};

use tokio::sync::watch;

#[cfg(windows)]
use std::io::Write;
#[cfg(windows)]
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle};
#[cfg(windows)]
use windows_sys::Win32::Foundation::{SetHandleInformation, HANDLE, HANDLE_FLAG_INHERIT};
#[cfg(windows)]
use windows_sys::Win32::Security::SECURITY_ATTRIBUTES;
#[cfg(windows)]
use windows_sys::Win32::System::Pipes::CreatePipe;

use super::windows::{WindowsCwdImage, WindowsEnvironmentImage, WindowsExecImage};
use super::WindowsJobPolicy;

pub(super) const CREATE_SUSPENDED: u32 = 0x0000_0004;
const HIDDEN_CLEANUP_SETTLE: Duration = Duration::from_millis(2_000);
#[cfg(windows)]
const PRIVATE_CONTROL_MAX_BYTES: usize = klaus_sandbox_win::protocol::MAX_FRAME_BYTES + 4;

#[cfg(windows)]
pub(super) struct WindowsInheritedControlPipe {
    child_read: OwnedHandle,
    parent_write: Arc<Mutex<std::fs::File>>,
}

#[cfg(windows)]
impl WindowsInheritedControlPipe {
    /// Create the pipe and seat `payload` in it as the child's first message.
    ///
    /// `payload` is written verbatim. It arrives already framed by
    /// `klaus_sandbox_win::protocol::write_frame`, which is also what every control
    /// message that follows on this pipe uses, so the wrapper reads one consistent
    /// stream of protocol frames. Adding a length prefix here framed it twice: the
    /// wrapper read the outer length, then found the inner length header where the
    /// JSON body should start and refused the request with "frame is not valid JSON:
    /// expected value at line 1 column 1". Unit tests on both sides passed throughout,
    /// because neither side was ever run against the other.
    pub(super) fn create(payload: &[u8]) -> std::io::Result<Self> {
        if payload.is_empty() || payload.len() > PRIVATE_CONTROL_MAX_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid Windows inherited control payload length",
            ));
        }
        let mut child_read = 0 as HANDLE;
        let mut parent_write = 0 as HANDLE;
        let attributes = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: std::ptr::null_mut(),
            bInheritHandle: 1,
        };
        // The first frame is seated before CreateProcess can start its reader.
        // A default-sized anonymous pipe blocks WriteFile once that buffer fills;
        // a large inherited environment then deadlocks the spawn transaction.
        // Reserve the bounded complete frame plus control headroom, so a private
        // diagnostic's single cooperative cancellation cannot block during startup.
        let capacity = u32::try_from(payload.len().saturating_add(4096)).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "control pipe capacity overflow",
            )
        })?;
        if unsafe { CreatePipe(&mut child_read, &mut parent_write, &attributes, capacity) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
        let child_read = unsafe { OwnedHandle::from_raw_handle(child_read as RawHandle) };
        let mut parent_write = unsafe { std::fs::File::from_raw_handle(parent_write as RawHandle) };
        if unsafe {
            SetHandleInformation(
                parent_write.as_raw_handle() as HANDLE,
                HANDLE_FLAG_INHERIT,
                0,
            )
        } == 0
        {
            return Err(std::io::Error::last_os_error());
        }
        parent_write.write_all(payload)?;
        parent_write.flush()?;
        Ok(Self {
            child_read,
            parent_write: Arc::new(Mutex::new(parent_write)),
        })
    }

    pub(super) fn child_handle(&self) -> usize {
        self.child_read.as_raw_handle() as usize
    }

    pub(super) fn parent_writer(&self) -> Arc<Mutex<std::fs::File>> {
        Arc::clone(&self.parent_write)
    }
}

pub(super) const fn startup_info_size(
    has_extended_attributes: bool,
    startup_info_size: usize,
    startup_info_ex_size: usize,
) -> usize {
    if has_extended_attributes {
        startup_info_ex_size
    } else {
        startup_info_size
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct WindowsRegistrationIdentity {
    generation: u64,
    pid: u32,
}

impl WindowsRegistrationIdentity {
    pub(super) const fn pid(self) -> u32 {
        self.pid
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WindowsStdioDisposition {
    Pipe,
    Inherit,
    Null,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct WindowsStdioPlan {
    stdin: WindowsStdioDisposition,
    stdout: WindowsStdioDisposition,
    stderr: WindowsStdioDisposition,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct WindowsConPtySize {
    rows: u16,
    columns: u16,
}

impl WindowsConPtySize {
    pub(super) const fn new(rows: u16, columns: u16) -> Self {
        Self { rows, columns }
    }
}

impl WindowsStdioPlan {
    pub(super) const fn new(
        stdin: WindowsStdioDisposition,
        stdout: WindowsStdioDisposition,
        stderr: WindowsStdioDisposition,
    ) -> Self {
        Self {
            stdin,
            stdout,
            stderr,
        }
    }

    #[cfg(test)]
    pub(super) const fn pipes() -> Self {
        Self::new(
            WindowsStdioDisposition::Pipe,
            WindowsStdioDisposition::Pipe,
            WindowsStdioDisposition::Pipe,
        )
    }

    #[cfg(test)]
    pub(super) const fn all_inherit() -> Self {
        Self::new(
            WindowsStdioDisposition::Inherit,
            WindowsStdioDisposition::Inherit,
            WindowsStdioDisposition::Inherit,
        )
    }

    const fn dispositions(self) -> [WindowsStdioDisposition; 3] {
        [self.stdin, self.stdout, self.stderr]
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct WindowsSpawnPlan {
    creation_flags: u32,
    job_policy: WindowsJobPolicy,
    stdio: WindowsStdioPlan,
    target_sentinel_source: Option<KernelHandle>,
    retain_target_identity: bool,
    conpty: Option<WindowsConPtySize>,
    additional_inherited_handle: Option<KernelHandle>,
}

impl WindowsSpawnPlan {
    pub(super) const fn new(
        creation_flags: u32,
        job_policy: WindowsJobPolicy,
        stdio: WindowsStdioPlan,
        target_sentinel_source: Option<usize>,
    ) -> Self {
        Self {
            creation_flags,
            job_policy,
            stdio,
            target_sentinel_source: match target_sentinel_source {
                Some(raw) => Some(KernelHandle(raw)),
                None => None,
            },
            retain_target_identity: false,
            conpty: None,
            additional_inherited_handle: None,
        }
    }

    pub(super) const fn with_target_identity_lease(mut self, retain: bool) -> Self {
        self.retain_target_identity = retain;
        self
    }

    pub(super) const fn with_conpty(mut self, size: WindowsConPtySize) -> Self {
        self.conpty = Some(size);
        self
    }

    pub(super) const fn with_additional_inherited_handle(mut self, handle: usize) -> Self {
        self.additional_inherited_handle = Some(KernelHandle(handle));
        self
    }

    #[cfg(test)]
    pub(super) const fn for_test(
        creation_flags: u32,
        job_policy: WindowsJobPolicy,
        stdio: WindowsStdioPlan,
        target_sentinel_source: Option<usize>,
    ) -> Self {
        Self::new(creation_flags, job_policy, stdio, target_sentinel_source)
    }

    const fn effective_creation_flags(self) -> u32 {
        self.creation_flags | CREATE_SUSPENDED
    }

    fn requires_private_job(self) -> bool {
        matches!(
            self.job_policy,
            WindowsJobPolicy::BrowserSidecarPrivate
                | WindowsJobPolicy::LibuvGlobalWithControlTracker
        )
    }

    fn requires_global_job(self) -> bool {
        matches!(
            self.job_policy,
            WindowsJobPolicy::LibuvGlobal | WindowsJobPolicy::LibuvGlobalWithControlTracker
        )
    }

    fn requires_target_sentinel(self) -> bool {
        self.job_policy == WindowsJobPolicy::LibuvGlobalWithControlTracker
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) enum WindowsSpawnStage {
    AcquireExternalSpawnPermit,
    ReserveInternalControlPermit,
    AcquireRepeatableControlPermit,
    CloseExternalAdmission,
    CloseInternalAdmission,
    ReserveProcessMap,
    ReserveCleanupFence,
    AllocateStableWaitCell,
    PreallocateRegistration,
    PreallocateReturnBundle,
    CreateThreadpoolWait,
    InitializeCompletionWorker,
    InitializeCleanupWorker,
    PrepareStdio,
    PrepareConPty,
    CreatePrivateJob,
    ConfigurePrivateJob,
    AllocateJobCompletionCell,
    InsertJobCompletionCell,
    DuplicateTargetSentinel,
    BuildJobListAttribute,
    BuildHandleListAttribute,
    BuildPseudoConsoleAttribute,
    CreateProcessW,
    FillTargetIdentity,
    SetThreadpoolWait,
    CloseChildStdio,
    DeleteAttributeList,
    AssignLibuvGlobalJob,
    WrapParentStdio,
    ValidateReturnBundle,
    AcquireProcessMap,
    AcquireLifecycle,
    ResumeThread,
    PublishProcessMap,
    PublishSurface,
    ClosePrimaryThread,
    ImmediateWaitQuery,
    GetExitCodeProcess,
    ObserveTerminal,
    EnqueueCleanup,
    PromoteHiddenRegistration,
    TerminateProcess,
    TerminatePrivateJob,
    ResizePseudoConsole,
    ClosePseudoConsole,
    HiddenBoundedWait,
    DisarmThreadpoolWait,
    QuiesceThreadpoolWait,
    CloseThreadpoolWait,
    LookupJobCompletionCell,
    ObserveJobActiveZero,
    QueryPrivateJobAccounting,
    ShutdownCompletionWorker,
    WakeCleanupWorker,
    ShutdownCleanupWorker,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct KernelFailure {
    stage: WindowsSpawnStage,
    message: String,
}

impl KernelFailure {
    fn new(stage: WindowsSpawnStage, message: impl Into<String>) -> Self {
        Self {
            stage,
            message: message.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum WindowsSpawnError {
    PreSpawn {
        stage: WindowsSpawnStage,
        message: String,
    },
    CreateProcess {
        message: String,
    },
    PostCreate {
        stage: WindowsSpawnStage,
        message: String,
    },
    RetainedHidden {
        identity: WindowsRegistrationIdentity,
        original_stage: WindowsSpawnStage,
        original_message: String,
        terminate_result: WindowsTerminateResult,
        observation: WindowsWaitObservation,
    },
}

impl fmt::Display for WindowsSpawnError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PreSpawn { stage, message } => {
                write!(
                    formatter,
                    "Windows pre-spawn stage {stage:?} failed: {message}"
                )
            }
            Self::CreateProcess { message } => {
                write!(formatter, "CreateProcessW failed: {message}")
            }
            Self::PostCreate { stage, message } => write!(
                formatter,
                "Windows suspended transaction stage {stage:?} failed: {message}"
            ),
            Self::RetainedHidden {
                identity,
                original_stage,
                original_message,
                terminate_result,
                observation,
            } => write!(
                formatter,
                "Windows hidden process {identity:?} retained after {original_stage:?} ({original_message}); terminate={terminate_result:?}, observation={observation:?}"
            ),
        }
    }
}

impl std::error::Error for WindowsSpawnError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WindowsWaitObservation {
    Signaled,
    Timeout,
    Failed(u32),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WindowsTerminateResult {
    Delivered,
    AlreadyTerminal,
    Failed(u32),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WindowsJobResources {
    None,
    Global,
    PrivateAtomic,
    GlobalAndPrivateAtomic,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct KernelHandle(usize);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct PseudoConsoleHandle(usize);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HandleKind {
    Process,
    PrimaryThread,
    ChildStdio,
    ParentStdio,
    PrivateJob,
    TargetSentinel,
    PseudoConsolePipe,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct ThreadpoolWaitHandle(usize);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct AttributeListHandle(usize);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CreatedProcess {
    pid: u32,
    process: KernelHandle,
    primary_thread: KernelHandle,
}

struct PreparedStdioRaw {
    child: [Option<KernelHandle>; 3],
    parent: [Option<KernelHandle>; 3],
}

struct PreparedConPtyRaw {
    console: PseudoConsoleHandle,
    input_read: KernelHandle,
    output_write: KernelHandle,
    input_write: KernelHandle,
    output_read: KernelHandle,
}

trait WindowsSpawnImage {
    fn application(&self) -> &[u16];
    fn command_line(&self) -> &[u16];
    fn cwd(&self) -> Option<&WindowsCwdImage>;
    fn environment(&self) -> &WindowsEnvironmentImage;
}

impl WindowsSpawnImage for WindowsExecImage {
    fn application(&self) -> &[u16] {
        self.application()
    }

    fn command_line(&self) -> &[u16] {
        self.command_line()
    }

    fn cwd(&self) -> Option<&WindowsCwdImage> {
        self.cwd()
    }

    fn environment(&self) -> &WindowsEnvironmentImage {
        self.environment()
    }
}

trait WindowsSpawnKernel: Send + Sync + 'static {
    fn attach_supervisor(&self, shared: Weak<WindowsSpawnShared>) -> Result<(), KernelFailure>;
    fn uses_automatic_cleanup_worker(&self) -> bool;
    fn shutdown_completion_worker(&self) -> Result<(), KernelFailure>;
    fn checkpoint(&self, stage: WindowsSpawnStage) -> Result<(), KernelFailure>;
    fn record_infallible(&self, stage: WindowsSpawnStage);

    fn create_threadpool_wait(
        &self,
        cell: &Arc<WindowsWaitCell>,
    ) -> Result<ThreadpoolWaitHandle, KernelFailure>;
    fn set_threadpool_wait(
        &self,
        wait: ThreadpoolWaitHandle,
        process: KernelHandle,
        cell: &Arc<WindowsWaitCell>,
    );
    fn disarm_threadpool_wait(&self, wait: ThreadpoolWaitHandle);
    fn quiesce_threadpool_wait(&self, wait: ThreadpoolWaitHandle);
    fn close_threadpool_wait(&self, wait: ThreadpoolWaitHandle);

    fn prepare_stdio(&self, plan: WindowsStdioPlan) -> Result<PreparedStdioRaw, KernelFailure>;
    fn prepare_conpty(&self, size: WindowsConPtySize) -> Result<PreparedConPtyRaw, KernelFailure>;
    fn resize_pseudoconsole(
        &self,
        console: PseudoConsoleHandle,
        size: WindowsConPtySize,
    ) -> Result<(), u32>;
    fn close_pseudoconsole(&self, console: PseudoConsoleHandle);
    fn create_private_job(&self) -> Result<KernelHandle, KernelFailure>;
    fn configure_private_job(
        &self,
        job: KernelHandle,
        completion_id: u64,
    ) -> Result<(), KernelFailure>;
    fn duplicate_target_sentinel(
        &self,
        source: KernelHandle,
    ) -> Result<KernelHandle, KernelFailure>;
    fn build_attribute_list(
        &self,
        job: Option<KernelHandle>,
        pseudoconsole: Option<PseudoConsoleHandle>,
        inherited_handles: &[KernelHandle],
        needs_handle_list: bool,
    ) -> Result<AttributeListHandle, KernelFailure>;
    fn delete_attribute_list(&self, attributes: AttributeListHandle);

    fn create_process(
        &self,
        image: &dyn WindowsSpawnImage,
        flags: u32,
        stdio: &[Option<KernelHandle>; 3],
        attributes: Option<AttributeListHandle>,
        inherit_handles: bool,
        uses_pseudoconsole: bool,
    ) -> Result<CreatedProcess, KernelFailure>;
    fn assign_global_job(&self, process: KernelHandle) -> Result<(), KernelFailure>;
    fn wrap_parent_stdio(&self, handles: &[KernelHandle]) -> Result<(), KernelFailure>;
    fn resume_thread(
        &self,
        thread: KernelHandle,
        cell: &Arc<WindowsWaitCell>,
    ) -> Result<(), KernelFailure>;
    fn wait_for_single_object(
        &self,
        process: KernelHandle,
        timeout: Duration,
    ) -> WindowsWaitObservation;
    fn get_exit_code_process(&self, process: KernelHandle) -> Result<Option<u32>, u32>;
    fn terminate_process(&self, process: KernelHandle, exit_code: u32) -> WindowsTerminateResult;
    fn terminate_job(&self, job: KernelHandle, exit_code: u32) -> WindowsTerminateResult;
    fn query_job_active_processes(&self, job: KernelHandle) -> Result<u32, u32>;
    fn close_handle(&self, handle: KernelHandle, kind: HandleKind) -> bool;
    fn bind_identity(
        &self,
        process: KernelHandle,
        thread: KernelHandle,
        wait: ThreadpoolWaitHandle,
        identity: WindowsRegistrationIdentity,
    );
    fn record_callback_phase(&self, phase: &'static str);
}

struct OwnedKernelHandle {
    raw: Option<KernelHandle>,
    kind: HandleKind,
    kernel: Arc<dyn WindowsSpawnKernel>,
}

impl OwnedKernelHandle {
    fn new(raw: KernelHandle, kind: HandleKind, kernel: Arc<dyn WindowsSpawnKernel>) -> Self {
        Self {
            raw: Some(raw),
            kind,
            kernel,
        }
    }

    fn raw(&self) -> KernelHandle {
        self.raw.expect("owned Windows handle already closed")
    }

    fn close_invariant(&mut self) {
        if let Some(raw) = self.raw.take() {
            assert!(
                self.kernel.close_handle(raw, self.kind),
                "CloseHandle failed for valid owned {:?}",
                self.kind
            );
        }
    }

    #[cfg(windows)]
    fn into_file(mut self) -> std::fs::File {
        use std::os::windows::io::{FromRawHandle, RawHandle};

        let raw = self
            .raw
            .take()
            .expect("owned Windows handle already closed");
        // SAFETY: this method consumes the unique project-owned parent-pipe
        // handle. Clearing `self.raw` transfers the single CloseHandle owner
        // to `File`; the transaction kernel no longer observes that handle.
        unsafe { std::fs::File::from_raw_handle(raw.0 as RawHandle) }
    }
}

impl Drop for OwnedKernelHandle {
    fn drop(&mut self) {
        self.close_invariant();
    }
}

struct OwnedThreadpoolWait {
    raw: Option<ThreadpoolWaitHandle>,
    kernel: Arc<dyn WindowsSpawnKernel>,
    armed: bool,
}

impl OwnedThreadpoolWait {
    fn new(raw: ThreadpoolWaitHandle, kernel: Arc<dyn WindowsSpawnKernel>) -> Self {
        Self {
            raw: Some(raw),
            kernel,
            armed: false,
        }
    }

    fn raw(&self) -> ThreadpoolWaitHandle {
        self.raw.expect("owned threadpool wait already closed")
    }

    fn arm(&mut self, process: KernelHandle, cell: &Arc<WindowsWaitCell>) {
        self.kernel.set_threadpool_wait(self.raw(), process, cell);
        self.armed = true;
    }

    fn close_quiesced(&mut self) {
        if let Some(raw) = self.raw.take() {
            if self.armed {
                self.kernel.disarm_threadpool_wait(raw);
                self.kernel.quiesce_threadpool_wait(raw);
            }
            self.kernel.close_threadpool_wait(raw);
            self.armed = false;
        }
    }
}

impl Drop for OwnedThreadpoolWait {
    fn drop(&mut self) {
        self.close_quiesced();
    }
}

struct OwnedAttributeList {
    raw: Option<AttributeListHandle>,
    kernel: Arc<dyn WindowsSpawnKernel>,
}

impl OwnedAttributeList {
    fn new(raw: AttributeListHandle, kernel: Arc<dyn WindowsSpawnKernel>) -> Self {
        Self {
            raw: Some(raw),
            kernel,
        }
    }

    fn raw(&self) -> AttributeListHandle {
        self.raw.expect("owned attribute list already deleted")
    }

    fn delete(&mut self) {
        if let Some(raw) = self.raw.take() {
            self.kernel.delete_attribute_list(raw);
        }
    }
}

impl Drop for OwnedAttributeList {
    fn drop(&mut self) {
        self.delete();
    }
}

struct PreparedStdio {
    child: [Option<OwnedKernelHandle>; 3],
    parent: [Option<OwnedKernelHandle>; 3],
    pseudoconsole: Option<WindowsPseudoConsoleController>,
}

struct OwnedPseudoConsole {
    raw: Option<PseudoConsoleHandle>,
    input_read: Option<OwnedKernelHandle>,
    output_write: Option<OwnedKernelHandle>,
    kernel: Arc<dyn WindowsSpawnKernel>,
}

impl OwnedPseudoConsole {
    fn close(&mut self) {
        if let Some(raw) = self.raw.take() {
            self.kernel
                .record_infallible(WindowsSpawnStage::ClosePseudoConsole);
            self.kernel.close_pseudoconsole(raw);
        }
        self.input_read = None;
        self.output_write = None;
    }

    fn raw(&self) -> PseudoConsoleHandle {
        self.raw.expect("owned pseudoconsole already closed")
    }
}

impl Drop for OwnedPseudoConsole {
    fn drop(&mut self) {
        self.close();
    }
}

struct WindowsPseudoConsoleState {
    owned: Mutex<Option<OwnedPseudoConsole>>,
}

impl WindowsPseudoConsoleState {
    fn close(&self) {
        self.owned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
    }
}

#[derive(Clone)]
struct WindowsPseudoConsoleController {
    state: Arc<WindowsPseudoConsoleState>,
}

impl WindowsPseudoConsoleController {
    fn from_raw(
        raw: PreparedConPtyRaw,
        kernel: Arc<dyn WindowsSpawnKernel>,
    ) -> (Self, [Option<OwnedKernelHandle>; 3]) {
        let console = OwnedPseudoConsole {
            raw: Some(raw.console),
            input_read: Some(OwnedKernelHandle::new(
                raw.input_read,
                HandleKind::PseudoConsolePipe,
                kernel.clone(),
            )),
            output_write: Some(OwnedKernelHandle::new(
                raw.output_write,
                HandleKind::PseudoConsolePipe,
                kernel.clone(),
            )),
            kernel: kernel.clone(),
        };
        let parent = [
            Some(OwnedKernelHandle::new(
                raw.input_write,
                HandleKind::ParentStdio,
                kernel.clone(),
            )),
            Some(OwnedKernelHandle::new(
                raw.output_read,
                HandleKind::ParentStdio,
                kernel,
            )),
            None,
        ];
        (
            Self {
                state: Arc::new(WindowsPseudoConsoleState {
                    owned: Mutex::new(Some(console)),
                }),
            },
            parent,
        )
    }

    fn raw(&self) -> PseudoConsoleHandle {
        self.state
            .owned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .expect("pseudoconsole closed before process creation")
            .raw()
    }

    fn resize(&self, size: WindowsConPtySize) -> std::io::Result<()> {
        let owned = self
            .state
            .owned
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let Some(owned) = owned.as_ref() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Windows pseudoconsole is closed",
            ));
        };
        owned
            .kernel
            .record_infallible(WindowsSpawnStage::ResizePseudoConsole);
        owned
            .kernel
            .resize_pseudoconsole(owned.raw(), size)
            .map_err(|error| std::io::Error::from_raw_os_error(error as i32))
    }

    fn close(&self) {
        self.state.close();
    }
}

struct FixedParentHandleList {
    handles: [KernelHandle; 3],
    len: usize,
}

impl FixedParentHandleList {
    fn as_slice(&self) -> &[KernelHandle] {
        &self.handles[..self.len]
    }
}

impl PreparedStdio {
    fn from_raw(raw: PreparedStdioRaw, kernel: Arc<dyn WindowsSpawnKernel>) -> Self {
        Self {
            child: raw.child.map(|handle| {
                handle.map(|handle| {
                    OwnedKernelHandle::new(handle, HandleKind::ChildStdio, kernel.clone())
                })
            }),
            parent: raw.parent.map(|handle| {
                handle.map(|handle| {
                    OwnedKernelHandle::new(handle, HandleKind::ParentStdio, kernel.clone())
                })
            }),
            pseudoconsole: None,
        }
    }

    fn from_conpty_raw(raw: PreparedConPtyRaw, kernel: Arc<dyn WindowsSpawnKernel>) -> Self {
        let (pseudoconsole, parent) = WindowsPseudoConsoleController::from_raw(raw, kernel);
        Self {
            child: [None, None, None],
            parent,
            pseudoconsole: Some(pseudoconsole),
        }
    }

    fn child_raw(&self) -> [Option<KernelHandle>; 3] {
        std::array::from_fn(|index| self.child[index].as_ref().map(OwnedKernelHandle::raw))
    }

    fn fixed_parent_raw(&self) -> FixedParentHandleList {
        let mut handles = [KernelHandle(0); 3];
        let mut len = 0;
        for handle in self.parent.iter().flatten() {
            handles[len] = handle.raw();
            len += 1;
        }
        FixedParentHandleList { handles, len }
    }

    fn close_child_ends(&mut self) {
        for handle in &mut self.child {
            if let Some(handle) = handle {
                handle.close_invariant();
            }
            *handle = None;
        }
    }

    fn move_parent_into(&mut self, destination: &mut WindowsParentStdio) {
        assert!(destination.handles.is_empty());
        for handle in &mut self.parent {
            if let Some(handle) = handle.take() {
                destination.handles.push(handle);
            }
        }
    }
}

struct WindowsParentStdio {
    handles: Vec<OwnedKernelHandle>,
}

impl WindowsParentStdio {
    fn preallocated() -> Self {
        Self {
            handles: Vec::with_capacity(3),
        }
    }
}

struct JobCompletionCell {
    id: u64,
    active_zero_seen: std::sync::atomic::AtomicBool,
    worker_failed: std::sync::atomic::AtomicBool,
    registration: OnceLock<Weak<WindowsRegistration>>,
}

impl JobCompletionCell {
    fn new(id: u64) -> Self {
        Self {
            id,
            active_zero_seen: std::sync::atomic::AtomicBool::new(false),
            worker_failed: std::sync::atomic::AtomicBool::new(false),
            registration: OnceLock::new(),
        }
    }

    fn bind_registration(&self, registration: &Arc<WindowsRegistration>) {
        assert!(
            self.registration.set(Arc::downgrade(registration)).is_ok(),
            "Windows job completion cell bound twice"
        );
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RegistrationPhase {
    Unpublished,
    Published,
    HiddenActionPending,
    HiddenSettling,
    HiddenRetained,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WindowsShutdownActionKind {
    None,
    ProcessTree,
    PrivateJob,
}

impl RegistrationPhase {
    const fn callback_label(self) -> &'static str {
        match self {
            Self::Unpublished => "Unpublished",
            Self::Published => "Published",
            Self::HiddenActionPending => "ActionPending",
            Self::HiddenSettling => "Settling",
            Self::HiddenRetained => "RetainedInvariant",
        }
    }
}

struct RegistrationLifecycle {
    phase: RegistrationPhase,
    process: Option<OwnedKernelHandle>,
    primary_thread: Option<OwnedKernelHandle>,
    wait: Option<OwnedThreadpoolWait>,
    pseudoconsole: Option<WindowsPseudoConsoleController>,
    private_job: Option<OwnedKernelHandle>,
    completion_cell: Option<Arc<JobCompletionCell>>,
    terminal: Option<u32>,
    terminal_events: usize,
    cleanup_enqueues: usize,
    cleanup_enqueued: bool,
    cleanup_reservation: Option<CleanupReservation>,
    wait_closed: bool,
    identity_leases: usize,
    terminal_target_identity: Option<WindowsTargetIdentityLease>,
    terminal_control_action: Option<WindowsControlAction>,
    terminate_consumed: bool,
    terminate_result: Option<WindowsTerminateResult>,
}

struct WindowsRegistration {
    identity: OnceLock<WindowsRegistrationIdentity>,
    cell: Arc<WindowsWaitCell>,
    terminal_tx: watch::Sender<Option<u32>>,
    lifecycle: Mutex<RegistrationLifecycle>,
    shutdown_action: WindowsShutdownActionKind,
    termination_machine: Weak<WindowsTerminationMachine>,
    /// A live generation owns the supervisor independently of facade handles.
    /// Final removal explicitly drops this lease after the map entry is gone.
    supervisor_lease: Mutex<Option<Arc<WindowsSpawnShared>>>,
}

impl WindowsRegistration {
    fn identity(&self) -> WindowsRegistrationIdentity {
        *self
            .identity
            .get()
            .expect("Windows registration identity not initialized")
    }
}

struct WindowsWaitCell {
    kernel: Arc<dyn WindowsSpawnKernel>,
    shared: Weak<WindowsSpawnShared>,
    identity: OnceLock<WindowsRegistrationIdentity>,
    registration: OnceLock<Weak<WindowsRegistration>>,
}

impl WindowsWaitCell {
    fn new(kernel: Arc<dyn WindowsSpawnKernel>, shared: Weak<WindowsSpawnShared>) -> Self {
        Self {
            kernel,
            shared,
            identity: OnceLock::new(),
            registration: OnceLock::new(),
        }
    }

    fn fill_identity(&self, identity: WindowsRegistrationIdentity) {
        assert!(
            self.identity.set(identity).is_ok(),
            "Windows wait-cell target identity initialized twice"
        );
    }

    fn identity(&self) -> WindowsRegistrationIdentity {
        *self
            .identity
            .get()
            .expect("Windows wait callback armed without target identity")
    }

    fn bind_registration(&self, registration: Arc<WindowsRegistration>) {
        assert!(
            self.registration.set(Arc::downgrade(&registration)).is_ok(),
            "Windows wait-cell registration initialized twice"
        );
    }

    fn dispatch(&self) {
        let registration = self
            .registration
            .get()
            .expect("signaled suspended target has no registration")
            .upgrade()
            .expect("Windows wait callback lost its live registration");
        let shared = self
            .shared
            .upgrade()
            .expect("Windows wait callback outlived its supervisor");
        shared.observe_callback(&registration, &self.kernel);
    }
}

struct WindowsSpawnShared {
    kernel: Arc<dyn WindowsSpawnKernel>,
    registrations: Mutex<HashMap<WindowsRegistrationIdentity, Arc<WindowsRegistration>>>,
    cleanup_queue: Mutex<VecDeque<Arc<WindowsRegistration>>>,
    completion_cells: Mutex<HashMap<u64, Arc<JobCompletionCell>>>,
    next_generation: AtomicU64,
    next_completion_id: AtomicU64,
    reservation_gate: Mutex<()>,
    map_reservations: AtomicUsize,
    cleanup_reservations: AtomicUsize,
    completion_reservations: AtomicUsize,
    cleanup_worker: OnceLock<Arc<CleanupWorkerRuntime>>,
    worker_invariant: Mutex<Option<WindowsWorkerInvariant>>,
    admission: Mutex<WindowsAdmissionState>,
    admission_epoch_tx: watch::Sender<u64>,
    next_action_receipt: AtomicU64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WindowsWorkerInvariant {
    worker: &'static str,
    message: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WindowsAdmissionPhase {
    Running,
    ExternalClosed,
    InternalClosed,
    Closed,
}

struct WindowsAdmissionState {
    phase: WindowsAdmissionPhase,
    external_permits: usize,
    internal_outstanding: usize,
    epoch: u64,
}

struct WindowsExternalSpawnPermit {
    shared: Arc<WindowsSpawnShared>,
    active: bool,
}

struct WindowsInternalControlPermit {
    shared: Arc<WindowsSpawnShared>,
    active: bool,
}

struct WindowsArmedControlReservation {
    permit: Option<WindowsInternalControlPermit>,
}

impl WindowsArmedControlReservation {
    fn convert(mut self) -> WindowsInternalControlPermit {
        self.permit
            .take()
            .expect("Windows armed control reservation converted twice")
    }
}

impl Drop for WindowsExternalSpawnPermit {
    fn drop(&mut self) {
        if self.active {
            self.active = false;
            self.shared.release_external_permit();
        }
    }
}

impl Drop for WindowsInternalControlPermit {
    fn drop(&mut self) {
        if self.active {
            self.active = false;
            self.shared.release_internal_permit();
        }
    }
}

#[derive(Default)]
struct CleanupWorkerState {
    wake_epoch: u64,
    shutdown: bool,
    stopped: bool,
}

pub(super) struct CleanupWorkerRuntime {
    state: Mutex<CleanupWorkerState>,
    changed: Condvar,
}

impl CleanupWorkerRuntime {
    fn new() -> Self {
        Self {
            state: Mutex::new(CleanupWorkerState::default()),
            changed: Condvar::new(),
        }
    }

    fn wake(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.wake_epoch = state
            .wake_epoch
            .checked_add(1)
            .expect("Windows cleanup-worker wake epoch exhausted");
        self.changed.notify_one();
    }

    fn request_shutdown(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.shutdown = true;
        self.changed.notify_one();
    }

    fn stopped(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .stopped
    }

    #[cfg(test)]
    pub(super) fn wait_stopped(&self, timeout: Duration) -> bool {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        self.changed
            .wait_timeout_while(state, timeout, |state| !state.stopped)
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .0
            .stopped
    }
}

struct SpawnCapacityPermit {
    shared: Arc<WindowsSpawnShared>,
    generation: u64,
    map_reserved: bool,
    cleanup_reserved: bool,
    completion_reserved: bool,
}

impl SpawnCapacityPermit {
    fn consume_map(&mut self) {
        assert!(self.map_reserved, "Windows map reservation consumed twice");
        self.map_reserved = false;
        let previous = self.shared.map_reservations.fetch_sub(1, Ordering::AcqRel);
        assert!(previous > 0, "Windows map reservation underflow");
    }

    fn consume_completion(&mut self) {
        assert!(
            self.completion_reserved,
            "Windows completion reservation consumed twice"
        );
        self.completion_reserved = false;
        let previous = self
            .shared
            .completion_reservations
            .fetch_sub(1, Ordering::AcqRel);
        assert!(previous > 0, "Windows completion reservation underflow");
    }

    fn take_cleanup_reservation(&mut self) -> CleanupReservation {
        assert!(
            self.cleanup_reserved,
            "Windows cleanup reservation transferred twice"
        );
        self.cleanup_reserved = false;
        CleanupReservation {
            shared: self.shared.clone(),
            active: true,
        }
    }
}

impl Drop for SpawnCapacityPermit {
    fn drop(&mut self) {
        if self.map_reserved {
            let previous = self.shared.map_reservations.fetch_sub(1, Ordering::AcqRel);
            assert!(previous > 0, "Windows map reservation underflow");
        }
        if self.cleanup_reserved {
            let previous = self
                .shared
                .cleanup_reservations
                .fetch_sub(1, Ordering::AcqRel);
            assert!(previous > 0, "Windows cleanup reservation underflow");
        }
        if self.completion_reserved {
            let previous = self
                .shared
                .completion_reservations
                .fetch_sub(1, Ordering::AcqRel);
            assert!(previous > 0, "Windows completion reservation underflow");
        }
    }
}

struct CleanupReservation {
    shared: Arc<WindowsSpawnShared>,
    active: bool,
}

struct PendingCompletionMapEntry {
    shared: Arc<WindowsSpawnShared>,
    id: Option<u64>,
}

impl PendingCompletionMapEntry {
    fn commit(&mut self) {
        self.id = None;
    }
}

impl Drop for PendingCompletionMapEntry {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.shared
                .completion_cells
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&id);
        }
    }
}

impl CleanupReservation {
    fn consume(mut self) {
        assert!(self.active, "Windows cleanup reservation consumed twice");
        self.active = false;
        let previous = self
            .shared
            .cleanup_reservations
            .fetch_sub(1, Ordering::AcqRel);
        assert!(previous > 0, "Windows cleanup reservation underflow");
    }
}

impl Drop for CleanupReservation {
    fn drop(&mut self) {
        if self.active {
            let previous = self
                .shared
                .cleanup_reservations
                .fetch_sub(1, Ordering::AcqRel);
            assert!(previous > 0, "Windows cleanup reservation underflow");
        }
    }
}

impl WindowsSpawnShared {
    fn advance_admission_epoch(&self, state: &mut WindowsAdmissionState) {
        state.epoch = state
            .epoch
            .checked_add(1)
            .expect("Windows admission epoch exhausted");
        self.admission_epoch_tx.send_replace(state.epoch);
    }

    fn acquire_external_permit(
        self: &Arc<Self>,
    ) -> Result<WindowsExternalSpawnPermit, KernelFailure> {
        self.kernel
            .checkpoint(WindowsSpawnStage::AcquireExternalSpawnPermit)?;
        let mut state = self
            .admission
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if state.phase != WindowsAdmissionPhase::Running {
            return Err(KernelFailure::new(
                WindowsSpawnStage::AcquireExternalSpawnPermit,
                "Windows external spawn admission is closed",
            ));
        }
        state.external_permits = state.external_permits.checked_add(1).ok_or_else(|| {
            KernelFailure::new(
                WindowsSpawnStage::AcquireExternalSpawnPermit,
                "Windows external spawn permit count exhausted",
            )
        })?;
        self.advance_admission_epoch(&mut state);
        Ok(WindowsExternalSpawnPermit {
            shared: self.clone(),
            active: true,
        })
    }

    fn reserve_internal_control(
        self: &Arc<Self>,
    ) -> Result<WindowsArmedControlReservation, KernelFailure> {
        self.kernel
            .checkpoint(WindowsSpawnStage::ReserveInternalControlPermit)?;
        let permit = self
            .acquire_internal_outstanding(WindowsSpawnStage::ReserveInternalControlPermit, false)?;
        Ok(WindowsArmedControlReservation {
            permit: Some(permit),
        })
    }

    fn acquire_repeatable_control(
        self: &Arc<Self>,
    ) -> Result<WindowsInternalControlPermit, KernelFailure> {
        self.kernel
            .checkpoint(WindowsSpawnStage::AcquireRepeatableControlPermit)?;
        self.acquire_internal_outstanding(WindowsSpawnStage::AcquireRepeatableControlPermit, true)
    }

    fn acquire_internal_outstanding(
        self: &Arc<Self>,
        stage: WindowsSpawnStage,
        require_running: bool,
    ) -> Result<WindowsInternalControlPermit, KernelFailure> {
        let mut state = self
            .admission
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let admitted = if require_running {
            state.phase == WindowsAdmissionPhase::Running
        } else {
            matches!(
                state.phase,
                WindowsAdmissionPhase::Running | WindowsAdmissionPhase::ExternalClosed
            )
        };
        if !admitted {
            return Err(KernelFailure::new(
                stage,
                "Windows internal control admission is closed",
            ));
        }
        state.internal_outstanding =
            state.internal_outstanding.checked_add(1).ok_or_else(|| {
                KernelFailure::new(stage, "Windows internal control permit count exhausted")
            })?;
        self.advance_admission_epoch(&mut state);
        Ok(WindowsInternalControlPermit {
            shared: self.clone(),
            active: true,
        })
    }

    fn release_external_permit(&self) {
        let mut state = self
            .admission
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        assert!(
            state.external_permits > 0,
            "Windows external permit underflow"
        );
        state.external_permits -= 1;
        self.advance_admission_epoch(&mut state);
    }

    fn release_internal_permit(&self) {
        let mut state = self
            .admission
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        assert!(
            state.internal_outstanding > 0,
            "Windows internal permit underflow"
        );
        state.internal_outstanding -= 1;
        self.advance_admission_epoch(&mut state);
    }

    fn close_external_admission(&self) {
        let mut state = self
            .admission
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if state.phase == WindowsAdmissionPhase::Running {
            self.kernel
                .record_infallible(WindowsSpawnStage::CloseExternalAdmission);
            state.phase = WindowsAdmissionPhase::ExternalClosed;
            self.advance_admission_epoch(&mut state);
        }
    }

    fn try_close_internal_admission(&self) -> bool {
        let mut state = self
            .admission
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if state.phase == WindowsAdmissionPhase::ExternalClosed
            && state.external_permits == 0
            && state.internal_outstanding == 0
        {
            self.kernel
                .record_infallible(WindowsSpawnStage::CloseInternalAdmission);
            state.phase = WindowsAdmissionPhase::InternalClosed;
            self.advance_admission_epoch(&mut state);
            true
        } else {
            state.phase == WindowsAdmissionPhase::InternalClosed
        }
    }

    fn refresh_job_accounting_locked(&self, lifecycle: &mut RegistrationLifecycle) {
        let Some(cell) = lifecycle.completion_cell.as_ref() else {
            return;
        };
        if cell.active_zero_seen.load(Ordering::Acquire) {
            return;
        }
        let job = lifecycle
            .private_job
            .as_ref()
            .expect("job completion cell has no private Job")
            .raw();
        match self.kernel.query_job_active_processes(job) {
            Ok(0) => {
                self.kernel
                    .record_infallible(WindowsSpawnStage::ObserveJobActiveZero);
                cell.active_zero_seen.store(true, Ordering::Release);
            }
            Ok(_) => {}
            Err(error) => {
                panic!("QueryInformationJobObject failed for owned private Job: {error}")
            }
        }
    }

    fn observe_job_active_zero(&self, completion_id: u64) -> bool {
        let cell = self
            .completion_cells
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&completion_id)
            .cloned();
        let Some(cell) = cell else {
            return false;
        };
        self.kernel
            .record_infallible(WindowsSpawnStage::LookupJobCompletionCell);
        let Some(registration) = cell.registration.get().and_then(Weak::upgrade) else {
            return false;
        };

        // A completion-port packet is useful only for the exact generation
        // whose cell was cloned from the ID map.  The packet latch and its
        // readiness wake/finalization are serialized with accounting-query
        // retirement by that generation's lifecycle mutex.
        let mut lifecycle = registration
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let exact_cell = lifecycle
            .completion_cell
            .as_ref()
            .is_some_and(|bound| Arc::ptr_eq(bound, &cell));
        if !exact_cell {
            return false;
        }
        if !cell.active_zero_seen.swap(true, Ordering::AcqRel) {
            self.kernel
                .record_infallible(WindowsSpawnStage::ObserveJobActiveZero);
        }
        let ready = registration_resources_ready(&lifecycle);
        let completion_id = lifecycle.completion_cell.as_ref().map(|cell| cell.id);
        if ready {
            close_registration_resources(&mut lifecycle);
        }
        drop(lifecycle);
        if ready {
            let mut registrations = self
                .registrations
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if registrations
                .get(&registration.identity())
                .is_some_and(|bound| Arc::ptr_eq(bound, &registration))
            {
                registrations.remove(&registration.identity());
            }
            drop(registrations);
            if let Some(completion_id) = completion_id {
                let mut cells = self
                    .completion_cells
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if cells
                    .get(&completion_id)
                    .is_some_and(|bound| Arc::ptr_eq(bound, &cell))
                {
                    cells.remove(&completion_id);
                }
            }
            registration
                .supervisor_lease
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
        }
        true
    }

    fn observe_callback(
        &self,
        registration: &Arc<WindowsRegistration>,
        kernel: &Arc<dyn WindowsSpawnKernel>,
    ) {
        let mut lifecycle = registration
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        kernel.record_callback_phase(lifecycle.phase.callback_label());
        match lifecycle.phase {
            RegistrationPhase::Unpublished | RegistrationPhase::HiddenActionPending => return,
            RegistrationPhase::Published
            | RegistrationPhase::HiddenSettling
            | RegistrationPhase::HiddenRetained => {}
        }
        let hidden = lifecycle.phase != RegistrationPhase::Published;
        self.observe_terminal_locked(registration, &mut lifecycle, hidden);
    }

    fn observe_terminal_locked(
        &self,
        registration: &Arc<WindowsRegistration>,
        lifecycle: &mut RegistrationLifecycle,
        hidden: bool,
    ) {
        if lifecycle.terminal.is_some() {
            return;
        }
        let process = lifecycle
            .process
            .as_ref()
            .expect("terminal observer lost process handle")
            .raw();
        let exit_code = match self.kernel.get_exit_code_process(process) {
            Ok(Some(exit_code)) => exit_code,
            Ok(None) => return,
            Err(error) => panic!("GetExitCodeProcess failed for owned signaled target: {error}"),
        };
        self.kernel
            .record_infallible(WindowsSpawnStage::ObserveTerminal);
        lifecycle.terminal = Some(exit_code);
        if let Some(pseudoconsole) = &lifecycle.pseudoconsole {
            pseudoconsole.close();
        }
        registration.terminal_tx.send_replace(Some(exit_code));
        if !hidden {
            lifecycle.terminal_events += 1;
        }
        if !lifecycle.cleanup_enqueued {
            lifecycle.cleanup_enqueued = true;
            lifecycle.cleanup_enqueues += 1;
            self.kernel
                .record_infallible(WindowsSpawnStage::EnqueueCleanup);
            let reservation = lifecycle
                .cleanup_reservation
                .take()
                .expect("terminal registration lost its cleanup reservation");
            self.cleanup_queue
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push_back(registration.clone());
            reservation.consume();
            if let Some(worker) = self.cleanup_worker.get() {
                self.kernel
                    .record_infallible(WindowsSpawnStage::WakeCleanupWorker);
                worker.wake();
            }
        }
    }

    fn record_worker_invariant(&self, worker: &'static str, message: impl Into<String>) {
        let mut invariant = self
            .worker_invariant
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if invariant.is_none() {
            *invariant = Some(WindowsWorkerInvariant {
                worker,
                message: message.into(),
            });
        }
        for cell in self
            .completion_cells
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
        {
            cell.worker_failed.store(true, Ordering::Release);
        }
    }

    fn drain_cleanup(&self) {
        loop {
            let registration = self
                .cleanup_queue
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .pop_front();
            let Some(registration) = registration else {
                break;
            };
            let mut lifecycle = registration
                .lifecycle
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let terminal_target_identity = lifecycle.terminal_target_identity.take();
            let terminal_control_action = lifecycle.terminal_control_action.take();
            // Never quiesce while holding lifecycle: an already queued
            // callback may be waiting for this exact mutex.
            let mut wait = lifecycle.wait.take();
            drop(lifecycle);
            // A taskkill helper releases the target process-object identity
            // after its terminal status is cached and before its own wait
            // object is quiesced/closed. This prevents numeric PID reuse for
            // the complete lifetime of the control actor.
            drop(terminal_target_identity);
            drop(terminal_control_action);
            if let Some(wait) = &mut wait {
                wait.close_quiesced();
            }
            drop(wait);
            let mut lifecycle = registration
                .lifecycle
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            lifecycle.wait_closed = true;
            self.refresh_job_accounting_locked(&mut lifecycle);
            let ready = registration_resources_ready(&lifecycle);
            if ready {
                close_registration_resources(&mut lifecycle);
            }
            let completion_id = lifecycle.completion_cell.as_ref().map(|cell| cell.id);
            drop(lifecycle);
            if ready {
                self.registrations
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(&registration.identity());
                if let Some(completion_id) = completion_id {
                    self.completion_cells
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .remove(&completion_id);
                }
                registration
                    .supervisor_lease
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take();
            }
        }
    }

    fn release_identity_lease(&self, identity: WindowsRegistrationIdentity) {
        let mut registrations = self
            .registrations
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let Some(registration) = registrations.get(&identity).cloned() else {
            return;
        };
        let mut lifecycle = registration
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        assert!(
            lifecycle.identity_leases > 0,
            "Windows identity lease underflow"
        );
        lifecycle.identity_leases -= 1;
        self.refresh_job_accounting_locked(&mut lifecycle);
        let ready = registration_resources_ready(&lifecycle);
        let completion_id = lifecycle.completion_cell.as_ref().map(|cell| cell.id);
        if ready {
            close_registration_resources(&mut lifecycle);
        }
        drop(lifecycle);
        if ready {
            registrations.remove(&identity);
            if let Some(completion_id) = completion_id {
                self.completion_cells
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(&completion_id);
            }
            registration
                .supervisor_lease
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
        }
    }
}

impl Drop for WindowsSpawnShared {
    fn drop(&mut self) {
        if let Some(worker) = self.cleanup_worker.get() {
            self.kernel
                .record_infallible(WindowsSpawnStage::ShutdownCleanupWorker);
            worker.request_shutdown();
        }
    }
}

fn start_cleanup_worker(shared: &Arc<WindowsSpawnShared>) -> Result<(), KernelFailure> {
    shared
        .kernel
        .checkpoint(WindowsSpawnStage::InitializeCleanupWorker)?;
    let runtime = Arc::new(CleanupWorkerRuntime::new());
    assert!(
        shared.cleanup_worker.set(runtime.clone()).is_ok(),
        "Windows cleanup worker initialized twice"
    );
    let weak = Arc::downgrade(shared);
    std::thread::Builder::new()
        .name("klaus-win-process-cleanup".to_string())
        .spawn(move || cleanup_worker_main(weak, runtime))
        .map_err(|error| {
            KernelFailure::new(
                WindowsSpawnStage::InitializeCleanupWorker,
                format!("failed to start Windows cleanup worker: {error}"),
            )
        })?;
    Ok(())
}

fn cleanup_worker_main(shared: Weak<WindowsSpawnShared>, runtime: Arc<CleanupWorkerRuntime>) {
    let mut observed_epoch = 0;
    loop {
        let mut state = runtime.state.lock().unwrap_or_else(PoisonError::into_inner);
        while !state.shutdown && state.wake_epoch == observed_epoch {
            state = runtime
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        if state.shutdown {
            state.stopped = true;
            runtime.changed.notify_all();
            return;
        }
        observed_epoch = state.wake_epoch;
        drop(state);

        let Some(shared) = shared.upgrade() else {
            let mut state = runtime.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.stopped = true;
            runtime.changed.notify_all();
            return;
        };
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| shared.drain_cleanup()))
            .is_err()
        {
            shared.record_worker_invariant("cleanup", "Windows cleanup worker panicked");
            let mut state = runtime.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.stopped = true;
            runtime.changed.notify_all();
            return;
        }
    }
}

fn registration_resources_ready(lifecycle: &RegistrationLifecycle) -> bool {
    let job_complete = lifecycle
        .completion_cell
        .as_ref()
        .is_none_or(|cell| cell.active_zero_seen.load(Ordering::Acquire));
    lifecycle.terminal.is_some()
        && lifecycle.wait_closed
        && lifecycle.identity_leases == 0
        && job_complete
}

fn close_registration_resources(lifecycle: &mut RegistrationLifecycle) {
    if let Some(pseudoconsole) = &lifecycle.pseudoconsole {
        pseudoconsole.close();
    }
    lifecycle.pseudoconsole = None;
    if let Some(thread) = &mut lifecycle.primary_thread {
        thread.close_invariant();
    }
    lifecycle.primary_thread = None;
    if let Some(process) = &mut lifecycle.process {
        process.close_invariant();
    }
    lifecycle.process = None;
    if let Some(job) = &mut lifecycle.private_job {
        job.close_invariant();
    }
    lifecycle.private_job = None;
}

#[derive(Clone)]
pub(super) struct WindowsSpawnSupervisor {
    shared: Arc<WindowsSpawnShared>,
}

impl WindowsSpawnSupervisor {
    fn with_kernel(kernel: Arc<dyn WindowsSpawnKernel>) -> Result<Self, KernelFailure> {
        let (admission_epoch_tx, _) = watch::channel(0);
        let shared = Arc::new(WindowsSpawnShared {
            kernel,
            registrations: Mutex::new(HashMap::new()),
            cleanup_queue: Mutex::new(VecDeque::new()),
            completion_cells: Mutex::new(HashMap::new()),
            next_generation: AtomicU64::new(1),
            next_completion_id: AtomicU64::new(1),
            reservation_gate: Mutex::new(()),
            map_reservations: AtomicUsize::new(0),
            cleanup_reservations: AtomicUsize::new(0),
            completion_reservations: AtomicUsize::new(0),
            cleanup_worker: OnceLock::new(),
            worker_invariant: Mutex::new(None),
            admission: Mutex::new(WindowsAdmissionState {
                phase: WindowsAdmissionPhase::Running,
                external_permits: 0,
                internal_outstanding: 0,
                epoch: 0,
            }),
            admission_epoch_tx,
            next_action_receipt: AtomicU64::new(1),
        });
        shared.kernel.attach_supervisor(Arc::downgrade(&shared))?;
        if shared.kernel.uses_automatic_cleanup_worker() {
            start_cleanup_worker(&shared)?;
        }
        Ok(Self { shared })
    }

    #[cfg(windows)]
    pub(super) fn initialize_windows() -> Result<Self, WindowsSpawnError> {
        let kernel = ProductionWindowsKernel::initialize().map_err(pre_spawn_error)?;
        Self::with_kernel(Arc::new(kernel)).map_err(pre_spawn_error)
    }

    #[cfg(test)]
    pub(super) fn with_kernel_for_test(kernel: Arc<FakeWindowsKernel>) -> Self {
        Self::with_kernel(kernel).expect("fake Windows supervisor attachment")
    }

    pub(super) fn spawn(
        &self,
        image: &WindowsExecImage,
        plan: WindowsSpawnPlan,
    ) -> Result<WindowsSpawnBundle, WindowsSpawnError> {
        let _external = self
            .shared
            .acquire_external_permit()
            .map_err(pre_spawn_error)?;
        self.spawn_image(image, plan, None, None)
    }

    pub(super) fn spawn_control(
        &self,
        image: &WindowsExecImage,
        plan: WindowsSpawnPlan,
        action: WindowsControlAction,
    ) -> Result<WindowsSpawnBundle, WindowsSpawnError> {
        self.spawn_image(image, plan, None, Some(action))
    }

    #[cfg(any(windows, test))]
    pub(super) async fn shutdown(&self) -> Result<(), WindowsSpawnError> {
        self.shared.close_external_admission();
        let mut admission_epoch = self.shared.admission_epoch_tx.subscribe();
        loop {
            let (phase, external_permits) = {
                let state = self
                    .shared
                    .admission
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                (state.phase, state.external_permits)
            };
            if phase == WindowsAdmissionPhase::Closed {
                return Ok(());
            }
            if external_permits == 0 {
                break;
            }
            admission_epoch.changed().await.map_err(|_| {
                pre_spawn_error(KernelFailure::new(
                    WindowsSpawnStage::CloseExternalAdmission,
                    "Windows admission epoch closed before external spawns drained",
                ))
            })?;
        }

        self.shared.drain_cleanup();
        if let Some(invariant) = self
            .shared
            .worker_invariant
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
        {
            return Err(pre_spawn_error(KernelFailure::new(
                WindowsSpawnStage::ShutdownCleanupWorker,
                format!(
                    "{} worker invariant: {}",
                    invariant.worker, invariant.message
                ),
            )));
        }

        let mut live = self
            .shared
            .registrations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .copied()
            .collect::<Vec<_>>();
        live.sort_by_key(|identity| (identity.generation, identity.pid));
        if !live.is_empty() {
            return Err(pre_spawn_error(KernelFailure::new(
                WindowsSpawnStage::CloseInternalAdmission,
                format!("Windows process registrations remain live: {live:?}"),
            )));
        }

        if !self.shared.try_close_internal_admission() {
            let state = self
                .shared
                .admission
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            return Err(pre_spawn_error(KernelFailure::new(
                WindowsSpawnStage::CloseInternalAdmission,
                format!(
                    "Windows internal controls remain live: external={}, internal={}",
                    state.external_permits, state.internal_outstanding
                ),
            )));
        }

        let reservation_counts = (
            self.shared.map_reservations.load(Ordering::Acquire),
            self.shared.cleanup_reservations.load(Ordering::Acquire),
            self.shared.completion_reservations.load(Ordering::Acquire),
        );
        let cleanup_len = self
            .shared
            .cleanup_queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len();
        let completion_len = self
            .shared
            .completion_cells
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len();
        if reservation_counts != (0, 0, 0) || cleanup_len != 0 || completion_len != 0 {
            return Err(pre_spawn_error(KernelFailure::new(
                WindowsSpawnStage::ShutdownCleanupWorker,
                format!(
                    "Windows shutdown retained reservations={reservation_counts:?}, cleanup={cleanup_len}, completions={completion_len}"
                ),
            )));
        }

        self.shared
            .kernel
            .shutdown_completion_worker()
            .map_err(pre_spawn_error)?;

        if let Some(worker) = self.shared.cleanup_worker.get() {
            self.shared
                .kernel
                .record_infallible(WindowsSpawnStage::ShutdownCleanupWorker);
            worker.request_shutdown();
            let deadline = Instant::now() + HIDDEN_CLEANUP_SETTLE;
            while !worker.stopped() {
                if Instant::now() >= deadline {
                    return Err(pre_spawn_error(KernelFailure::new(
                        WindowsSpawnStage::ShutdownCleanupWorker,
                        "Windows cleanup worker did not stop before the shutdown deadline",
                    )));
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }

        let mut state = self
            .shared
            .admission
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if state.phase == WindowsAdmissionPhase::InternalClosed {
            state.phase = WindowsAdmissionPhase::Closed;
            self.shared.advance_admission_epoch(&mut state);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn spawn_test_image(
        &self,
        plan: WindowsSpawnPlan,
    ) -> Result<WindowsSpawnBundle, WindowsSpawnError> {
        let _external = self
            .shared
            .acquire_external_permit()
            .map_err(pre_spawn_error)?;
        self.spawn_image(&TestWindowsImage::new(), plan, None, None)
    }

    #[cfg(test)]
    pub(super) fn spawn_test_control_image(
        &self,
        plan: WindowsSpawnPlan,
        target_identity: WindowsTargetIdentityLease,
    ) -> Result<WindowsSpawnBundle, WindowsSpawnError> {
        self.spawn_image(&TestWindowsImage::new(), plan, Some(target_identity), None)
    }

    #[cfg(test)]
    pub(super) fn spawn_test_action_control_image(
        &self,
        plan: WindowsSpawnPlan,
        action: WindowsControlAction,
    ) -> Result<WindowsSpawnBundle, WindowsSpawnError> {
        self.spawn_image(&TestWindowsImage::new(), plan, None, Some(action))
    }

    #[cfg(test)]
    pub(super) fn begin_shutdown_actions_for_test(&self) -> Vec<WindowsControlStart> {
        self.shared.close_external_admission();
        {
            let admission = self
                .shared
                .admission
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            assert_eq!(
                admission.external_permits, 0,
                "Windows shutdown scan crossed a live external spawn permit"
            );
        }
        let mut machines: Vec<_> = self
            .shared
            .registrations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .filter(|registration| registration.shutdown_action != WindowsShutdownActionKind::None)
            .map(|registration| {
                (
                    registration.identity(),
                    registration
                        .termination_machine
                        .upgrade()
                        .unwrap_or_else(|| std::process::abort()),
                )
            })
            .collect();
        machines.sort_by_key(|(identity, _)| (identity.generation, identity.pid));
        machines
            .into_iter()
            .map(|(_, machine)| machine.begin())
            .collect()
    }

    #[cfg(test)]
    pub(super) fn admission_counts_for_test(&self) -> (usize, usize) {
        let state = self
            .shared
            .admission
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        (state.external_permits, state.internal_outstanding)
    }

    fn spawn_image(
        &self,
        image: &dyn WindowsSpawnImage,
        plan: WindowsSpawnPlan,
        terminal_target_identity: Option<WindowsTargetIdentityLease>,
        terminal_control_action: Option<WindowsControlAction>,
    ) -> Result<WindowsSpawnBundle, WindowsSpawnError> {
        let kernel = self.shared.kernel.clone();
        let mut capacity = reserve_capacity(&self.shared, &kernel, plan.requires_private_job())?;
        kernel
            .checkpoint(WindowsSpawnStage::AllocateStableWaitCell)
            .map_err(pre_spawn_error)?;
        let wait_cell = Arc::new(WindowsWaitCell::new(
            kernel.clone(),
            Arc::downgrade(&self.shared),
        ));
        let raw_wait = kernel
            .create_threadpool_wait(&wait_cell)
            .map_err(pre_spawn_error)?;
        let wait = OwnedThreadpoolWait::new(raw_wait, kernel.clone());

        let mut stdio = match plan.conpty {
            Some(size) => PreparedStdio::from_conpty_raw(
                kernel.prepare_conpty(size).map_err(pre_spawn_error)?,
                kernel.clone(),
            ),
            None => PreparedStdio::from_raw(
                kernel.prepare_stdio(plan.stdio).map_err(pre_spawn_error)?,
                kernel.clone(),
            ),
        };

        let mut private_job = None;
        let mut completion_cell = None;
        let mut pending_completion_entry = None;
        if plan.requires_private_job() {
            let raw_job = kernel.create_private_job().map_err(pre_spawn_error)?;
            let owned_job = OwnedKernelHandle::new(raw_job, HandleKind::PrivateJob, kernel.clone());
            let completion_id = self
                .shared
                .next_completion_id
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                    value.checked_add(1)
                })
                .map_err(|_| WindowsSpawnError::PreSpawn {
                    stage: WindowsSpawnStage::AllocateJobCompletionCell,
                    message: "Windows job completion ID exhausted".to_string(),
                })?;
            kernel
                .checkpoint(WindowsSpawnStage::AllocateJobCompletionCell)
                .map_err(pre_spawn_error)?;
            let cell = Arc::new(JobCompletionCell::new(completion_id));
            kernel
                .configure_private_job(raw_job, completion_id)
                .map_err(pre_spawn_error)?;
            kernel
                .checkpoint(WindowsSpawnStage::InsertJobCompletionCell)
                .map_err(pre_spawn_error)?;
            let previous = self
                .shared
                .completion_cells
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(completion_id, cell.clone());
            assert!(
                previous.is_none(),
                "never-reused Windows completion ID collided"
            );
            capacity.consume_completion();
            pending_completion_entry = Some(PendingCompletionMapEntry {
                shared: self.shared.clone(),
                id: Some(completion_id),
            });
            private_job = Some(owned_job);
            completion_cell = Some(cell);
        }

        let mut sentinel = if plan.requires_target_sentinel() {
            let source =
                plan.target_sentinel_source
                    .ok_or_else(|| WindowsSpawnError::PreSpawn {
                        stage: WindowsSpawnStage::DuplicateTargetSentinel,
                        message: "control tracker requires a target identity sentinel".to_string(),
                    })?;
            let duplicate = kernel
                .duplicate_target_sentinel(source)
                .map_err(pre_spawn_error)?;
            Some(OwnedKernelHandle::new(
                duplicate,
                HandleKind::TargetSentinel,
                kernel.clone(),
            ))
        } else {
            None
        };

        let mut inherited_handles: Vec<_> = stdio.child_raw().into_iter().flatten().collect();
        if let Some(sentinel) = &sentinel {
            inherited_handles.push(sentinel.raw());
        }
        if let Some(handle) = plan.additional_inherited_handle {
            inherited_handles.push(handle);
        }
        let needs_handle_list = !inherited_handles.is_empty();
        let pseudoconsole = stdio
            .pseudoconsole
            .as_ref()
            .map(WindowsPseudoConsoleController::raw);
        let mut attributes =
            if private_job.is_some() || pseudoconsole.is_some() || needs_handle_list {
                Some(OwnedAttributeList::new(
                    kernel
                        .build_attribute_list(
                            private_job.as_ref().map(OwnedKernelHandle::raw),
                            pseudoconsole,
                            &inherited_handles,
                            needs_handle_list,
                        )
                        .map_err(pre_spawn_error)?,
                    kernel.clone(),
                ))
            } else {
                None
            };

        // Registration identity/resource slots, cleanup capacity, and the
        // fixed three-endpoint return allocation all exist before the OS can
        // create a process. Post-create code only fills one-time slots or
        // moves already-owned handles.
        kernel
            .checkpoint(WindowsSpawnStage::PreallocateRegistration)
            .map_err(pre_spawn_error)?;
        let target_identity = plan
            .retain_target_identity
            .then(|| WindowsTargetIdentityLease::preallocated(self.shared.clone()));
        let control_reservation = if plan.retain_target_identity {
            Some(
                self.shared
                    .reserve_internal_control()
                    .map_err(pre_spawn_error)?,
            )
        } else {
            None
        };
        let termination_machine = Arc::new(WindowsTerminationMachine::new(
            self.shared.clone(),
            control_reservation,
        ));
        let shutdown_action = if plan.retain_target_identity {
            WindowsShutdownActionKind::ProcessTree
        } else if plan.job_policy == WindowsJobPolicy::BrowserSidecarPrivate {
            WindowsShutdownActionKind::PrivateJob
        } else {
            WindowsShutdownActionKind::None
        };
        let (terminal_tx, _terminal_rx) = watch::channel(None);
        let registration = Arc::new(WindowsRegistration {
            identity: OnceLock::new(),
            cell: wait_cell.clone(),
            terminal_tx,
            lifecycle: Mutex::new(RegistrationLifecycle {
                phase: RegistrationPhase::Unpublished,
                process: None,
                primary_thread: None,
                wait: Some(wait),
                pseudoconsole: stdio.pseudoconsole.clone(),
                private_job,
                completion_cell,
                terminal: None,
                terminal_events: 0,
                cleanup_enqueues: 0,
                cleanup_enqueued: false,
                cleanup_reservation: Some(capacity.take_cleanup_reservation()),
                wait_closed: false,
                identity_leases: 1,
                terminal_target_identity,
                terminal_control_action,
                terminate_consumed: false,
                terminate_result: None,
            }),
            shutdown_action,
            termination_machine: Arc::downgrade(&termination_machine),
            supervisor_lease: Mutex::new(Some(self.shared.clone())),
        });
        if let Some(cell) = registration
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .completion_cell
            .clone()
        {
            cell.bind_registration(&registration);
        }
        wait_cell.bind_registration(registration.clone());
        kernel
            .checkpoint(WindowsSpawnStage::PreallocateReturnBundle)
            .map_err(pre_spawn_error)?;
        let mut return_stdio = WindowsParentStdio::preallocated();
        let fixed_parent_raw = stdio.fixed_parent_raw();

        let created = match kernel.create_process(
            image,
            plan.effective_creation_flags(),
            &stdio.child_raw(),
            attributes.as_ref().map(OwnedAttributeList::raw),
            !inherited_handles.is_empty(),
            plan.conpty.is_some(),
        ) {
            Ok(created) => created,
            Err(error) => {
                return Err(WindowsSpawnError::CreateProcess {
                    message: error.message,
                });
            }
        };

        let identity = WindowsRegistrationIdentity {
            generation: capacity.generation,
            pid: created.pid,
        };
        kernel.record_infallible(WindowsSpawnStage::FillTargetIdentity);
        wait_cell.fill_identity(identity);
        assert!(
            registration.identity.set(identity).is_ok(),
            "Windows registration identity initialized twice"
        );
        if let Some(target_identity) = &target_identity {
            target_identity.bind(identity);
        }
        kernel.bind_identity(created.process, created.primary_thread, raw_wait, identity);
        {
            let mut lifecycle = registration
                .lifecycle
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            lifecycle.process = Some(OwnedKernelHandle::new(
                created.process,
                HandleKind::Process,
                kernel.clone(),
            ));
            lifecycle.primary_thread = Some(OwnedKernelHandle::new(
                created.primary_thread,
                HandleKind::PrimaryThread,
                kernel.clone(),
            ));
            lifecycle
                .wait
                .as_mut()
                .expect("preallocated Windows wait missing after CreateProcessW")
                .arm(created.process, &wait_cell);
        }
        if let Some(entry) = &mut pending_completion_entry {
            entry.commit();
        }

        stdio.close_child_ends();
        kernel.record_infallible(WindowsSpawnStage::CloseChildStdio);
        if let Some(attributes) = &mut attributes {
            attributes.delete();
            kernel.record_infallible(WindowsSpawnStage::DeleteAttributeList);
        }
        drop(attributes);
        if let Some(sentinel) = &mut sentinel {
            sentinel.close_invariant();
        }
        drop(sentinel);

        if plan.requires_global_job() {
            if let Err(error) = kernel.assign_global_job(created.process) {
                return Err(self.hidden_cleanup(
                    registration,
                    &mut stdio,
                    &mut return_stdio,
                    &mut capacity,
                    error,
                ));
            }
        }

        if let Err(error) = kernel.wrap_parent_stdio(fixed_parent_raw.as_slice()) {
            return Err(self.hidden_cleanup(
                registration,
                &mut stdio,
                &mut return_stdio,
                &mut capacity,
                error,
            ));
        }
        stdio.move_parent_into(&mut return_stdio);
        if let Err(error) = kernel.checkpoint(WindowsSpawnStage::ValidateReturnBundle) {
            return Err(self.hidden_cleanup(
                registration,
                &mut stdio,
                &mut return_stdio,
                &mut capacity,
                error,
            ));
        }

        kernel.record_infallible(WindowsSpawnStage::AcquireProcessMap);
        let mut registrations = self
            .shared
            .registrations
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        kernel.record_infallible(WindowsSpawnStage::AcquireLifecycle);
        let mut lifecycle = registration
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        assert_eq!(lifecycle.phase, RegistrationPhase::Unpublished);
        if let Err(error) = kernel.resume_thread(
            lifecycle
                .primary_thread
                .as_ref()
                .expect("primary thread missing before ResumeThread")
                .raw(),
            &wait_cell,
        ) {
            drop(lifecycle);
            drop(registrations);
            return Err(self.hidden_cleanup(
                registration,
                &mut stdio,
                &mut return_stdio,
                &mut capacity,
                error,
            ));
        }

        if let Some(target_identity) = &target_identity {
            lifecycle.identity_leases = lifecycle
                .identity_leases
                .checked_add(1)
                .expect("Windows target identity lease count exhausted");
            target_identity.activate();
            termination_machine.bind_target_identity(target_identity.clone());
        }

        let previous = registrations.insert(identity, registration.clone());
        assert!(
            previous.is_none(),
            "Windows process identity collided at commit"
        );
        capacity.consume_map();
        kernel.record_infallible(WindowsSpawnStage::PublishProcessMap);
        lifecycle.phase = RegistrationPhase::Published;
        if let Some(action) = lifecycle.terminal_control_action.as_ref() {
            action.mark_settling();
        }
        lifecycle
            .primary_thread
            .as_mut()
            .expect("primary thread missing after ResumeThread")
            .close_invariant();
        lifecycle.primary_thread = None;

        match kernel.wait_for_single_object(created.process, Duration::ZERO) {
            WindowsWaitObservation::Signaled => {
                self.shared
                    .observe_terminal_locked(&registration, &mut lifecycle, false);
            }
            WindowsWaitObservation::Timeout => {}
            WindowsWaitObservation::Failed(error) => {
                panic!("WaitForSingleObject(process, 0) failed after commit: {error}")
            }
        }
        drop(lifecycle);
        drop(registrations);

        Ok(WindowsSpawnBundle {
            shared: self.shared.clone(),
            registration,
            stdio: return_stdio,
            stdio_plan: plan.stdio,
            released: false,
            job_resources: match plan.job_policy {
                WindowsJobPolicy::None => WindowsJobResources::None,
                WindowsJobPolicy::LibuvGlobal => WindowsJobResources::Global,
                WindowsJobPolicy::BrowserSidecarPrivate => WindowsJobResources::PrivateAtomic,
                WindowsJobPolicy::LibuvGlobalWithControlTracker => {
                    WindowsJobResources::GlobalAndPrivateAtomic
                }
            },
            target_identity,
            pseudoconsole: stdio.pseudoconsole.take(),
            termination_machine,
        })
    }

    fn hidden_cleanup(
        &self,
        registration: Arc<WindowsRegistration>,
        stdio: &mut PreparedStdio,
        return_stdio: &mut WindowsParentStdio,
        capacity: &mut SpawnCapacityPermit,
        original: KernelFailure,
    ) -> WindowsSpawnError {
        for handle in &mut stdio.parent {
            *handle = None;
        }
        return_stdio.handles.clear();
        let identity = registration.identity();
        let process;
        let terminate_result;
        {
            self.shared
                .kernel
                .record_infallible(WindowsSpawnStage::AcquireProcessMap);
            let mut registrations = self
                .shared
                .registrations
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            self.shared
                .kernel
                .record_infallible(WindowsSpawnStage::AcquireLifecycle);
            let mut lifecycle = registration
                .lifecycle
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            assert_eq!(lifecycle.phase, RegistrationPhase::Unpublished);
            lifecycle.phase = RegistrationPhase::HiddenActionPending;
            lifecycle.identity_leases = 0;
            process = lifecycle
                .process
                .as_ref()
                .expect("hidden registration lost process handle")
                .raw();
            let previous = registrations.insert(identity, registration.clone());
            assert!(previous.is_none(), "hidden Windows identity collided");
            capacity.consume_map();
            self.shared
                .kernel
                .record_infallible(WindowsSpawnStage::PromoteHiddenRegistration);
            assert!(!lifecycle.terminate_consumed);
            lifecycle.terminate_consumed = true;
            terminate_result = self.shared.kernel.terminate_process(process, 1);
            lifecycle.terminate_result = Some(terminate_result);
            lifecycle.phase = RegistrationPhase::HiddenSettling;
        }

        let observation = self
            .shared
            .kernel
            .wait_for_single_object(process, HIDDEN_CLEANUP_SETTLE);
        if observation == WindowsWaitObservation::Signaled {
            let mut lifecycle = registration
                .lifecycle
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            self.shared
                .observe_terminal_locked(&registration, &mut lifecycle, true);
            drop(lifecycle);
            self.shared.drain_cleanup();
            if self
                .shared
                .registrations
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .contains_key(&identity)
            {
                WindowsSpawnError::RetainedHidden {
                    identity,
                    original_stage: original.stage,
                    original_message: original.message,
                    terminate_result,
                    observation,
                }
            } else {
                WindowsSpawnError::PostCreate {
                    stage: original.stage,
                    message: original.message,
                }
            }
        } else {
            let mut lifecycle = registration
                .lifecycle
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            lifecycle.phase = RegistrationPhase::HiddenRetained;
            WindowsSpawnError::RetainedHidden {
                identity,
                original_stage: original.stage,
                original_message: original.message,
                terminate_result,
                observation,
            }
        }
    }

    #[cfg(test)]
    pub(super) fn contains_registration(&self, identity: WindowsRegistrationIdentity) -> bool {
        self.shared
            .registrations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(&identity)
    }

    #[cfg(test)]
    pub(super) fn retry_hidden_cleanup_for_test(&self, identity: WindowsRegistrationIdentity) {
        let registration = self
            .shared
            .registrations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&identity)
            .cloned()
            .expect("retained hidden registration");
        let lifecycle = registration
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        assert!(matches!(lifecycle.phase, RegistrationPhase::HiddenRetained));
        assert!(lifecycle.terminate_consumed);
    }

    #[cfg(test)]
    pub(super) fn dispatch_wait_for_test(&self, identity: WindowsRegistrationIdentity) {
        let registration = self
            .shared
            .registrations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&identity)
            .cloned()
            .expect("registered Windows process");
        registration.cell.dispatch();
    }

    #[cfg(test)]
    pub(super) fn drain_cleanup_for_test(&self) {
        self.shared.drain_cleanup();
    }

    #[cfg(test)]
    pub(super) fn observe_job_active_zero_for_test(&self, completion_id: u64) -> bool {
        self.shared.observe_job_active_zero(completion_id)
    }

    #[cfg(test)]
    pub(super) fn set_next_generation_for_test(&self, generation: u64) {
        self.shared
            .next_generation
            .store(generation, Ordering::Release);
    }

    #[cfg(test)]
    pub(super) fn reservation_counts_for_test(&self) -> (usize, usize, usize) {
        (
            self.shared.map_reservations.load(Ordering::Acquire),
            self.shared.cleanup_reservations.load(Ordering::Acquire),
            self.shared.completion_reservations.load(Ordering::Acquire),
        )
    }

    #[cfg(test)]
    pub(super) fn cleanup_runtime_for_test(&self) -> Arc<CleanupWorkerRuntime> {
        self.shared
            .cleanup_worker
            .get()
            .expect("automatic cleanup worker")
            .clone()
    }

    #[cfg(test)]
    pub(super) fn worker_invariant_message_for_test(&self) -> Option<String> {
        self.shared
            .worker_invariant
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|invariant| format!("{}:{}", invariant.worker, invariant.message))
    }

    #[cfg(test)]
    pub(super) fn fail_completion_worker_for_test(&self) {
        self.shared
            .record_worker_invariant("completion", "injected GetQueuedCompletionStatus failure");
    }
}

fn reserve_capacity(
    shared: &Arc<WindowsSpawnShared>,
    kernel: &Arc<dyn WindowsSpawnKernel>,
    needs_completion: bool,
) -> Result<SpawnCapacityPermit, WindowsSpawnError> {
    kernel
        .checkpoint(WindowsSpawnStage::ReserveProcessMap)
        .map_err(pre_spawn_error)?;
    let generation = shared
        .next_generation
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
            value.checked_add(1)
        })
        .map_err(|_| WindowsSpawnError::PreSpawn {
            stage: WindowsSpawnStage::ReserveProcessMap,
            message: "Windows process generation exhausted".to_string(),
        })?;

    let _reservation_guard = shared
        .reservation_gate
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let map_additional = shared.map_reservations.load(Ordering::Acquire) + 1;
    shared
        .registrations
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .try_reserve(map_additional)
        .map_err(|error| WindowsSpawnError::PreSpawn {
            stage: WindowsSpawnStage::ReserveProcessMap,
            message: error.to_string(),
        })?;
    if needs_completion {
        let completion_additional = shared.completion_reservations.load(Ordering::Acquire) + 1;
        shared
            .completion_cells
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .try_reserve(completion_additional)
            .map_err(|error| WindowsSpawnError::PreSpawn {
                stage: WindowsSpawnStage::ReserveProcessMap,
                message: error.to_string(),
            })?;
    }
    kernel
        .checkpoint(WindowsSpawnStage::ReserveCleanupFence)
        .map_err(pre_spawn_error)?;
    let cleanup_additional = shared.cleanup_reservations.load(Ordering::Acquire) + 1;
    shared
        .cleanup_queue
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .try_reserve(cleanup_additional)
        .map_err(|error| WindowsSpawnError::PreSpawn {
            stage: WindowsSpawnStage::ReserveCleanupFence,
            message: error.to_string(),
        })?;
    shared.map_reservations.fetch_add(1, Ordering::Release);
    shared.cleanup_reservations.fetch_add(1, Ordering::Release);
    if needs_completion {
        shared
            .completion_reservations
            .fetch_add(1, Ordering::Release);
    }
    Ok(SpawnCapacityPermit {
        shared: shared.clone(),
        generation,
        map_reserved: true,
        cleanup_reserved: true,
        completion_reserved: needs_completion,
    })
}

fn pre_spawn_error(error: KernelFailure) -> WindowsSpawnError {
    WindowsSpawnError::PreSpawn {
        stage: error.stage,
        message: error.message,
    }
}

pub(super) struct WindowsSpawnBundle {
    shared: Arc<WindowsSpawnShared>,
    registration: Arc<WindowsRegistration>,
    stdio: WindowsParentStdio,
    stdio_plan: WindowsStdioPlan,
    released: bool,
    job_resources: WindowsJobResources,
    target_identity: Option<WindowsTargetIdentityLease>,
    pseudoconsole: Option<WindowsPseudoConsoleController>,
    termination_machine: Arc<WindowsTerminationMachine>,
}

impl fmt::Debug for WindowsSpawnBundle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WindowsSpawnBundle")
            .field("identity", &self.registration.identity())
            .field("job_resources", &self.job_resources)
            .field("stdio_handle_count", &self.stdio.handles.len())
            .field("has_pseudoconsole", &self.pseudoconsole.is_some())
            .finish()
    }
}

impl WindowsSpawnBundle {
    pub(super) fn identity(&self) -> WindowsRegistrationIdentity {
        self.registration.identity()
    }

    pub(super) const fn job_resources(&self) -> WindowsJobResources {
        self.job_resources
    }

    pub(super) fn controller(&self) -> WindowsProcessController {
        WindowsProcessController {
            shared: self.shared.clone(),
            registration: self.registration.clone(),
            target_identity: self.target_identity.clone(),
            termination_machine: self.termination_machine.clone(),
        }
    }

    pub(super) fn waiter(&self) -> WindowsProcessWaiter {
        WindowsProcessWaiter {
            terminal_rx: self.registration.terminal_tx.subscribe(),
        }
    }

    /// Authenticate a browser named-pipe client against the already-published
    /// root and its private Job without treating a numeric PID as identity.
    #[cfg(windows)]
    pub(super) fn verify_browser_client_identity(&self, client_pid: u32) -> Result<bool, String> {
        use windows_sys::Win32::Foundation::{
            CloseHandle, CompareObjectHandles, GetLastError, SetLastError, ERROR_SUCCESS,
            WAIT_TIMEOUT,
        };
        use windows_sys::Win32::System::JobObjects::IsProcessInJob;
        use windows_sys::Win32::System::Threading::{
            GetProcessId, OpenProcess, WaitForSingleObject, PROCESS_QUERY_LIMITED_INFORMATION,
            PROCESS_SYNCHRONIZE, PROCESS_TERMINATE,
        };

        let lifecycle = self
            .registration
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let root = lifecycle
            .process
            .as_ref()
            .ok_or_else(|| "browser root identity is no longer live".to_string())?
            .raw()
            .0 as windows_sys::Win32::Foundation::HANDLE;
        let job = lifecycle
            .private_job
            .as_ref()
            .ok_or_else(|| "browser private Job identity is unavailable".to_string())?
            .raw()
            .0 as windows_sys::Win32::Foundation::HANDLE;
        let client = unsafe {
            OpenProcess(
                PROCESS_SYNCHRONIZE | PROCESS_TERMINATE | PROCESS_QUERY_LIMITED_INFORMATION,
                0,
                client_pid,
            )
        };
        if client.is_null() {
            return Err("browser pipe client process could not be opened".to_string());
        }
        struct ClientHandle(windows_sys::Win32::Foundation::HANDLE);
        impl Drop for ClientHandle {
            fn drop(&mut self) {
                unsafe {
                    CloseHandle(self.0);
                }
            }
        }
        let client = ClientHandle(client);
        if unsafe { GetProcessId(client.0) } != client_pid {
            return Err("browser pipe client process identity changed".to_string());
        }
        let mut in_job = 0;
        if unsafe { IsProcessInJob(client.0, job, &mut in_job) } == 0 || in_job == 0 {
            return Err("browser pipe client is outside the private Job".to_string());
        }
        if unsafe { WaitForSingleObject(client.0, 0) } != WAIT_TIMEOUT {
            return Err("browser pipe client terminated during adoption".to_string());
        }
        unsafe {
            SetLastError(ERROR_SUCCESS);
        }
        let aliases_root = unsafe { CompareObjectHandles(root, client.0) } != 0;
        if !aliases_root && unsafe { GetLastError() } != ERROR_SUCCESS {
            return Err("browser pipe client handle comparison failed".to_string());
        }
        Ok(aliases_root)
    }

    #[cfg(windows)]
    pub(super) fn take_parent_files(&mut self) -> Result<WindowsParentFiles, String> {
        if self.pseudoconsole.is_some() {
            return Err("Windows ConPTY bundle cannot be projected as standard pipes".to_string());
        }
        let expected = self
            .stdio_plan
            .dispositions()
            .into_iter()
            .filter(|disposition| *disposition == WindowsStdioDisposition::Pipe)
            .count();
        if self.stdio.handles.len() != expected {
            return Err(format!(
                "Windows parent stdio shape mismatch: expected {expected} pipe handles, received {}",
                self.stdio.handles.len()
            ));
        }

        let mut handles = self.stdio.handles.drain(..);
        let mut take = |disposition| match disposition {
            WindowsStdioDisposition::Pipe => handles
                .next()
                .map(OwnedKernelHandle::into_file)
                .map(tokio::fs::File::from_std),
            WindowsStdioDisposition::Inherit | WindowsStdioDisposition::Null => None,
        };
        let [stdin, stdout, stderr] = self.stdio_plan.dispositions();
        Ok(WindowsParentFiles {
            stdin: take(stdin),
            stdout: take(stdout),
            stderr: take(stderr),
        })
    }

    #[cfg(windows)]
    pub(super) fn take_parent_pty(&mut self) -> Result<WindowsConPtyMaster, String> {
        let pseudoconsole = self
            .pseudoconsole
            .take()
            .ok_or_else(|| "Windows process bundle has no pseudoconsole".to_string())?;
        if self.stdio.handles.len() != 2 {
            return Err(format!(
                "Windows ConPTY parent shape mismatch: expected 2 handles, received {}",
                self.stdio.handles.len()
            ));
        }
        let mut handles = self.stdio.handles.drain(..);
        let input = handles
            .next()
            .expect("validated ConPTY input handle")
            .into_file();
        let output = handles
            .next()
            .expect("validated ConPTY output handle")
            .into_file();
        Ok(WindowsConPtyMaster {
            reader: tokio::fs::File::from_std(output),
            writer: tokio::fs::File::from_std(input),
            pseudoconsole,
        })
    }

    #[cfg(test)]
    pub(super) fn completion_id(&self) -> Option<u64> {
        self.registration
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .completion_cell
            .as_ref()
            .map(|cell| cell.id)
    }

    pub(super) fn try_wait(&mut self) -> Option<u32> {
        self.registration
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .terminal
    }

    #[cfg(test)]
    pub(super) fn terminal_event_count(&self) -> usize {
        self.registration
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .terminal_events
    }

    #[cfg(test)]
    pub(super) fn cleanup_enqueue_count(&self) -> usize {
        self.registration
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cleanup_enqueues
    }
}

#[cfg(windows)]
pub(super) struct WindowsParentFiles {
    pub(super) stdin: Option<tokio::fs::File>,
    pub(super) stdout: Option<tokio::fs::File>,
    pub(super) stderr: Option<tokio::fs::File>,
}

#[cfg(windows)]
pub(super) struct WindowsConPtyMaster {
    reader: tokio::fs::File,
    writer: tokio::fs::File,
    pseudoconsole: WindowsPseudoConsoleController,
}

#[cfg(windows)]
pub(super) struct WindowsConPtyReader {
    reader: tokio::fs::File,
}

#[cfg(windows)]
pub(super) struct WindowsConPtyWriter {
    writer: tokio::fs::File,
    pseudoconsole: WindowsPseudoConsoleController,
}

#[cfg(windows)]
impl WindowsConPtyMaster {
    pub(super) fn split(self) -> (WindowsConPtyReader, WindowsConPtyWriter) {
        (
            WindowsConPtyReader {
                reader: self.reader,
            },
            WindowsConPtyWriter {
                writer: self.writer,
                pseudoconsole: self.pseudoconsole,
            },
        )
    }
}

#[cfg(windows)]
impl WindowsConPtyWriter {
    pub(super) fn resize(&self, rows: u16, columns: u16) -> std::io::Result<()> {
        self.pseudoconsole
            .resize(WindowsConPtySize::new(rows, columns))
    }
}

#[cfg(windows)]
impl tokio::io::AsyncRead for WindowsConPtyMaster {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        tokio::io::AsyncRead::poll_read(std::pin::Pin::new(&mut self.reader), context, buffer)
    }
}

#[cfg(windows)]
impl tokio::io::AsyncWrite for WindowsConPtyMaster {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
        buffer: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        tokio::io::AsyncWrite::poll_write(std::pin::Pin::new(&mut self.writer), context, buffer)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        tokio::io::AsyncWrite::poll_flush(std::pin::Pin::new(&mut self.writer), context)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        tokio::io::AsyncWrite::poll_shutdown(std::pin::Pin::new(&mut self.writer), context)
    }
}

#[cfg(windows)]
impl tokio::io::AsyncRead for WindowsConPtyReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        tokio::io::AsyncRead::poll_read(std::pin::Pin::new(&mut self.reader), context, buffer)
    }
}

#[cfg(windows)]
impl tokio::io::AsyncWrite for WindowsConPtyWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
        buffer: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        tokio::io::AsyncWrite::poll_write(std::pin::Pin::new(&mut self.writer), context, buffer)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        tokio::io::AsyncWrite::poll_flush(std::pin::Pin::new(&mut self.writer), context)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        tokio::io::AsyncWrite::poll_shutdown(std::pin::Pin::new(&mut self.writer), context)
    }
}

#[derive(Clone)]
pub(super) struct WindowsProcessController {
    shared: Arc<WindowsSpawnShared>,
    registration: Arc<WindowsRegistration>,
    target_identity: Option<WindowsTargetIdentityLease>,
    termination_machine: Arc<WindowsTerminationMachine>,
}

impl WindowsProcessController {
    pub(super) fn try_wait(&self) -> Option<u32> {
        self.registration
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .terminal
    }

    pub(super) fn target_identity_lease(&self) -> Option<WindowsTargetIdentityLease> {
        self.target_identity.clone()
    }

    pub(super) fn begin_control(&self, repeatable: bool) -> WindowsControlStart {
        if repeatable {
            let permit = match self.shared.acquire_repeatable_control() {
                Ok(permit) => permit,
                Err(_) => return WindowsControlStart::Disarmed,
            };
            let machine = Arc::new(WindowsTerminationMachine::new(self.shared.clone(), None));
            let WindowsControlStart::Execute(mut action) = machine.begin() else {
                std::process::abort();
            };
            action.permit = Some(permit);
            action.bind_target_identity(self.target_identity.clone());
            WindowsControlStart::Execute(action)
        } else {
            self.termination_machine.begin()
        }
    }

    pub(super) fn disarm(&self) {
        self.termination_machine.disarm();
    }

    #[cfg(test)]
    pub(super) fn termination_state_for_test(&self) -> (&'static str, Option<u64>) {
        let state = self
            .termination_machine
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match *state {
            WindowsTerminationState::Armed { .. } => ("Armed", None),
            WindowsTerminationState::ActionStarted { receipt } => ("ActionStarted", Some(receipt)),
            WindowsTerminationState::Settling { receipt, deadline } => {
                let _absolute_deadline = deadline;
                ("Settling", Some(receipt))
            }
            WindowsTerminationState::Consumed { receipt } => ("Consumed", Some(receipt)),
            WindowsTerminationState::Disarmed => ("Disarmed", None),
        }
    }

    pub(super) fn spawn_control(
        &self,
        image: &WindowsExecImage,
        plan: WindowsSpawnPlan,
        action: WindowsControlAction,
    ) -> Result<(), WindowsSpawnError> {
        let helper = self.spawn_control_bundle(image, plan, action)?;
        // The helper registration, not this facade bundle, owns completion.
        // Dropping the bundle releases only its ordinary identity lease; the
        // registration retains the target lease until terminal cleanup.
        drop(helper);
        Ok(())
    }

    /// Commit a control helper while retaining its fixed parent pipes and wait
    /// receipt for a synchronous broker operation. The helper registration
    /// still owns the target lease through tracker completion.
    pub(super) fn spawn_control_bundle(
        &self,
        image: &WindowsExecImage,
        plan: WindowsSpawnPlan,
        action: WindowsControlAction,
    ) -> Result<WindowsSpawnBundle, WindowsSpawnError> {
        WindowsSpawnSupervisor {
            shared: self.shared.clone(),
        }
        .spawn_control(image, plan, action)
    }

    /// Deliver a handle-bound child termination without reopening the numeric
    /// PID. Tree termination remains a separate taskkill/control-tracker path.
    pub(super) fn terminate_child(&self) -> WindowsTerminateResult {
        let lifecycle = self
            .registration
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if lifecycle.terminal.is_some() {
            return WindowsTerminateResult::AlreadyTerminal;
        }
        let Some(process) = lifecycle.process.as_ref() else {
            return WindowsTerminateResult::AlreadyTerminal;
        };
        self.shared.kernel.terminate_process(process.raw(), 1)
    }

    /// Terminate the registration's private Job through its retained handle.
    /// Unlike direct-child termination this remains meaningful after the Job's
    /// leader exits, because descendants can keep the completion lease alive.
    pub(super) fn terminate_private_job(&self) -> WindowsTerminateResult {
        let mut lifecycle = self
            .registration
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if lifecycle.terminal_control_action.is_some() {
            return WindowsTerminateResult::AlreadyTerminal;
        }
        let action = match self.termination_machine.begin() {
            WindowsControlStart::Execute(action) => action,
            WindowsControlStart::Join { .. } => {
                return lifecycle
                    .terminate_result
                    .unwrap_or(WindowsTerminateResult::AlreadyTerminal);
            }
            WindowsControlStart::Disarmed => return WindowsTerminateResult::AlreadyTerminal,
        };
        let Some(job) = lifecycle.private_job.as_ref() else {
            return WindowsTerminateResult::AlreadyTerminal;
        };
        assert!(
            !lifecycle.terminate_consumed,
            "Windows private Job termination consumed twice"
        );
        let result = self.shared.kernel.terminate_job(job.raw(), 1);
        lifecycle.terminate_consumed = true;
        lifecycle.terminate_result = Some(result);
        action.mark_settling();
        lifecycle.terminal_control_action = Some(action);
        result
    }
}

#[derive(Clone)]
pub(super) struct WindowsTargetIdentityLease {
    inner: Arc<WindowsTargetIdentityLeaseInner>,
}

struct WindowsTargetIdentityLeaseInner {
    shared: Arc<WindowsSpawnShared>,
    identity: OnceLock<WindowsRegistrationIdentity>,
    active: std::sync::atomic::AtomicBool,
}

impl WindowsTargetIdentityLease {
    fn preallocated(shared: Arc<WindowsSpawnShared>) -> Self {
        Self {
            inner: Arc::new(WindowsTargetIdentityLeaseInner {
                shared,
                identity: OnceLock::new(),
                active: std::sync::atomic::AtomicBool::new(false),
            }),
        }
    }

    fn bind(&self, identity: WindowsRegistrationIdentity) {
        assert!(
            self.inner.identity.set(identity).is_ok(),
            "Windows target identity lease bound twice"
        );
    }

    fn activate(&self) {
        assert!(
            self.inner.identity.get().is_some(),
            "Windows target identity lease activated before binding"
        );
        assert!(
            !self.inner.active.swap(true, Ordering::AcqRel),
            "Windows target identity lease activated twice"
        );
    }

    pub(super) fn pid(&self) -> u32 {
        self.inner
            .identity
            .get()
            .expect("Windows target identity lease has no committed identity")
            .pid()
    }
}

impl Drop for WindowsTargetIdentityLeaseInner {
    fn drop(&mut self) {
        if self.active.load(Ordering::Acquire) {
            self.shared.release_identity_lease(
                *self
                    .identity
                    .get()
                    .expect("active Windows target identity lease lost identity"),
            );
        }
    }
}

enum WindowsTerminationState {
    Armed {
        reservation: Option<WindowsArmedControlReservation>,
        target_identity: Option<WindowsTargetIdentityLease>,
    },
    ActionStarted {
        receipt: u64,
    },
    Settling {
        receipt: u64,
        deadline: Instant,
    },
    Consumed {
        receipt: u64,
    },
    Disarmed,
}

struct WindowsTerminationMachine {
    shared: Arc<WindowsSpawnShared>,
    state: Mutex<WindowsTerminationState>,
}

impl WindowsTerminationMachine {
    fn new(
        shared: Arc<WindowsSpawnShared>,
        reservation: Option<WindowsArmedControlReservation>,
    ) -> Self {
        Self {
            shared,
            state: Mutex::new(WindowsTerminationState::Armed {
                reservation,
                target_identity: None,
            }),
        }
    }

    fn bind_target_identity(&self, lease: WindowsTargetIdentityLease) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        match &mut *state {
            WindowsTerminationState::Armed {
                target_identity, ..
            } if target_identity.is_none() => {
                *target_identity = Some(lease);
            }
            _ => std::process::abort(),
        }
    }

    fn begin(self: &Arc<Self>) -> WindowsControlStart {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        match &mut *state {
            WindowsTerminationState::Armed {
                reservation,
                target_identity,
            } => {
                let permit = reservation
                    .take()
                    .map(WindowsArmedControlReservation::convert);
                let target_identity = target_identity.take();
                let receipt = self
                    .shared
                    .next_action_receipt
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                        value.checked_add(1)
                    })
                    .unwrap_or_else(|_| std::process::abort());
                *state = WindowsTerminationState::ActionStarted { receipt };
                WindowsControlStart::Execute(WindowsControlAction {
                    machine: self.clone(),
                    receipt,
                    permit,
                    target_identity,
                    complete_on_drop: true,
                })
            }
            WindowsTerminationState::ActionStarted { receipt }
            | WindowsTerminationState::Settling { receipt, .. }
            | WindowsTerminationState::Consumed { receipt } => {
                WindowsControlStart::Join { receipt: *receipt }
            }
            WindowsTerminationState::Disarmed => WindowsControlStart::Disarmed,
        }
    }

    fn disarm(&self) {
        let (reservation, target_identity) = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            match &mut *state {
                WindowsTerminationState::Armed {
                    reservation,
                    target_identity,
                } => {
                    let reservation = reservation.take();
                    let target_identity = target_identity.take();
                    *state = WindowsTerminationState::Disarmed;
                    (reservation, target_identity)
                }
                WindowsTerminationState::ActionStarted { .. }
                | WindowsTerminationState::Settling { .. }
                | WindowsTerminationState::Consumed { .. }
                | WindowsTerminationState::Disarmed => (None, None),
            }
        };
        drop(reservation);
        drop(target_identity);
    }

    fn complete(&self, receipt: u64) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        match *state {
            WindowsTerminationState::ActionStarted { receipt: active } if active == receipt => {
                *state = WindowsTerminationState::Consumed { receipt };
            }
            WindowsTerminationState::Settling {
                receipt: active, ..
            } if active == receipt => {
                *state = WindowsTerminationState::Consumed { receipt };
            }
            WindowsTerminationState::Consumed { receipt: active } if active == receipt => {}
            _ => std::process::abort(),
        }
    }
}

pub(super) enum WindowsControlStart {
    Execute(WindowsControlAction),
    Join { receipt: u64 },
    Disarmed,
}

pub(super) struct WindowsControlAction {
    machine: Arc<WindowsTerminationMachine>,
    receipt: u64,
    permit: Option<WindowsInternalControlPermit>,
    target_identity: Option<WindowsTargetIdentityLease>,
    complete_on_drop: bool,
}

impl WindowsControlAction {
    pub(super) fn pid(&self) -> Option<u32> {
        self.target_identity
            .as_ref()
            .map(WindowsTargetIdentityLease::pid)
    }

    fn bind_target_identity(&mut self, target_identity: Option<WindowsTargetIdentityLease>) {
        assert!(
            self.target_identity.is_none(),
            "Windows control action target identity bound twice"
        );
        self.target_identity = target_identity;
    }

    fn mark_settling(&self) {
        let deadline = Instant::now()
            .checked_add(HIDDEN_CLEANUP_SETTLE)
            .unwrap_or_else(|| std::process::abort());
        let mut state = self
            .machine
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match *state {
            WindowsTerminationState::ActionStarted { receipt } if receipt == self.receipt => {
                *state = WindowsTerminationState::Settling { receipt, deadline };
            }
            WindowsTerminationState::Settling { receipt, .. } if receipt == self.receipt => {}
            _ => std::process::abort(),
        }
    }

    #[cfg(test)]
    pub(super) const fn receipt_for_test(&self) -> u64 {
        self.receipt
    }
}

impl Drop for WindowsControlAction {
    fn drop(&mut self) {
        self.target_identity.take();
        self.permit.take();
        if self.complete_on_drop {
            self.complete_on_drop = false;
            self.machine.complete(self.receipt);
        }
    }
}

pub(super) struct WindowsProcessWaiter {
    terminal_rx: watch::Receiver<Option<u32>>,
}

impl WindowsProcessWaiter {
    pub(super) async fn wait(mut self) -> Result<u32, String> {
        loop {
            if let Some(code) = *self.terminal_rx.borrow_and_update() {
                return Ok(code);
            }
            self.terminal_rx.changed().await.map_err(|_| {
                "Windows process terminal channel closed before exit observation".to_string()
            })?;
        }
    }
}

impl Drop for WindowsSpawnBundle {
    fn drop(&mut self) {
        if !self.released {
            self.released = true;
            self.shared
                .release_identity_lease(self.registration.identity());
        }
    }
}

#[cfg(test)]
struct TestWindowsImage {
    application: Vec<u16>,
    command_line: Vec<u16>,
    environment: WindowsEnvironmentImage,
}

#[cfg(test)]
impl TestWindowsImage {
    fn new() -> Self {
        Self {
            application: "C:\\fixture.exe\0".encode_utf16().collect(),
            command_line: "\"C:\\fixture.exe\"\0".encode_utf16().collect(),
            environment: WindowsEnvironmentImage::NativeParentInherit,
        }
    }
}

#[cfg(test)]
impl WindowsSpawnImage for TestWindowsImage {
    fn application(&self) -> &[u16] {
        &self.application
    }

    fn command_line(&self) -> &[u16] {
        &self.command_line
    }

    fn cwd(&self) -> Option<&WindowsCwdImage> {
        None
    }

    fn environment(&self) -> &WindowsEnvironmentImage {
        &self.environment
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WindowsSpawnFault {
    At(WindowsSpawnStage),
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum HiddenCleanupOutcome {
    Terminal { exit_code: u32 },
    StillLive,
    Failed(u32),
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CreateProcessOutcome {
    Running,
    FastExitOnResume { exit_code: u32 },
}

#[cfg(test)]
struct FakeProcess {
    pid: u32,
    identity: Option<WindowsRegistrationIdentity>,
    primary_thread: KernelHandle,
    outcome: CreateProcessOutcome,
    exit_code: Option<u32>,
    descendants: usize,
}

#[cfg(test)]
struct FakeState {
    events: Vec<WindowsSpawnStage>,
    fault: Option<WindowsSpawnStage>,
    next_handle: usize,
    open_handles: HashMap<KernelHandle, (HandleKind, Option<WindowsRegistrationIdentity>)>,
    open_waits: HashMap<ThreadpoolWaitHandle, Option<WindowsRegistrationIdentity>>,
    open_attributes: std::collections::HashSet<AttributeListHandle>,
    open_pseudoconsoles: std::collections::HashSet<PseudoConsoleHandle>,
    wait_cells: HashMap<ThreadpoolWaitHandle, Weak<WindowsWaitCell>>,
    process_waits: HashMap<KernelHandle, ThreadpoolWaitHandle>,
    processes: HashMap<KernelHandle, FakeProcess>,
    threads: HashMap<KernelHandle, KernelHandle>,
    last_creation_flags: u32,
    last_inherit_handles: bool,
    created_pid: u32,
    create_outcome: CreateProcessOutcome,
    hidden_cleanup: HiddenCleanupOutcome,
    max_hidden_wait: Option<Duration>,
    queue_callback_inside_resume: bool,
    resume_callback: Option<std::thread::JoinHandle<()>>,
    callback_phases: Vec<&'static str>,
    callback_inside_quiesce: bool,
    resumed: bool,
    fallible_after_resume: Option<WindowsSpawnStage>,
    supervisor: Option<Weak<WindowsSpawnShared>>,
    job_active_processes: u32,
    job_query_blocked: bool,
    job_query_entered: bool,
    automatic_cleanup_worker: bool,
}

#[cfg(test)]
impl Default for FakeState {
    fn default() -> Self {
        Self {
            events: Vec::new(),
            fault: None,
            next_handle: 1,
            open_handles: HashMap::new(),
            open_waits: HashMap::new(),
            open_attributes: std::collections::HashSet::new(),
            open_pseudoconsoles: std::collections::HashSet::new(),
            wait_cells: HashMap::new(),
            process_waits: HashMap::new(),
            processes: HashMap::new(),
            threads: HashMap::new(),
            last_creation_flags: 0,
            last_inherit_handles: false,
            created_pid: 0,
            create_outcome: CreateProcessOutcome::Running,
            hidden_cleanup: HiddenCleanupOutcome::Terminal { exit_code: 1 },
            max_hidden_wait: None,
            queue_callback_inside_resume: false,
            resume_callback: None,
            callback_phases: Vec::new(),
            callback_inside_quiesce: false,
            resumed: false,
            fallible_after_resume: None,
            supervisor: None,
            job_active_processes: 1,
            job_query_blocked: false,
            job_query_entered: false,
            automatic_cleanup_worker: false,
        }
    }
}

#[cfg(test)]
pub(super) struct FakeWindowsKernel {
    state: Mutex<FakeState>,
    changed: Condvar,
}

#[cfg(test)]
impl FakeWindowsKernel {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(FakeState::default()),
            changed: Condvar::new(),
        }
    }

    pub(super) fn with_automatic_cleanup_worker() -> Self {
        let state = FakeState {
            automatic_cleanup_worker: true,
            ..FakeState::default()
        };
        Self {
            state: Mutex::new(state),
            changed: Condvar::new(),
        }
    }

    pub(super) fn with_fault(fault: WindowsSpawnFault) -> Self {
        let WindowsSpawnFault::At(stage) = fault;
        let state = FakeState {
            fault: Some(stage),
            ..FakeState::default()
        };
        Self {
            state: Mutex::new(state),
            changed: Condvar::new(),
        }
    }

    fn stage(&self, stage: WindowsSpawnStage, fallible: bool) -> Result<(), KernelFailure> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.events.push(stage);
        self.changed.notify_all();
        if fallible && state.resumed && state.fallible_after_resume.is_none() {
            state.fallible_after_resume = Some(stage);
        }
        if fallible && state.fault == Some(stage) {
            return Err(KernelFailure::new(stage, "injected Windows kernel failure"));
        }
        Ok(())
    }

    fn allocate_handle(&self, kind: HandleKind) -> KernelHandle {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let raw = KernelHandle(state.next_handle);
        state.next_handle += 1;
        state.open_handles.insert(raw, (kind, None));
        raw
    }

    pub(super) fn events(&self) -> Vec<WindowsSpawnStage> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .events
            .clone()
    }

    pub(super) fn count(&self, stage: WindowsSpawnStage) -> usize {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .events
            .iter()
            .filter(|event| **event == stage)
            .count()
    }

    pub(super) fn open_handle_count(&self) -> usize {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.open_handles.len()
            + state.open_waits.len()
            + state.open_attributes.len()
            + state.open_pseudoconsoles.len()
    }

    pub(super) fn open_handle_count_for_identity(
        &self,
        identity: WindowsRegistrationIdentity,
    ) -> usize {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .open_handles
            .values()
            .filter(|(_, owner)| *owner == Some(identity))
            .count()
            + state
                .open_waits
                .values()
                .filter(|owner| **owner == Some(identity))
                .count()
    }

    pub(super) fn last_creation_flags(&self) -> u32 {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .last_creation_flags
    }

    pub(super) fn last_inherit_handles(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .last_inherit_handles
    }

    pub(super) fn created_pid(&self) -> u32 {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .created_pid
    }

    pub(super) fn set_hidden_cleanup(&self, outcome: HiddenCleanupOutcome) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .hidden_cleanup = outcome;
    }

    pub(super) fn max_hidden_wait(&self) -> Option<Duration> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .max_hidden_wait
    }

    pub(super) fn created_descendant_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .processes
            .values()
            .map(|process| process.descendants)
            .sum()
    }

    pub(super) fn set_create_outcome(&self, outcome: CreateProcessOutcome) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .create_outcome = outcome;
    }

    pub(super) fn queue_callback_inside_resume(&self) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .queue_callback_inside_resume = true;
    }

    pub(super) fn join_resume_callback(&self) {
        let callback = self
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .resume_callback
            .take();
        if let Some(callback) = callback {
            callback.join().expect("fake Windows callback thread");
        }
    }

    pub(super) fn callback_phase_observations(&self) -> Vec<&'static str> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .callback_phases
            .clone()
    }

    pub(super) fn callback_inside_quiesce(&self) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .callback_inside_quiesce = true;
    }

    pub(super) fn dispatch_bound_wait(&self, identity: WindowsRegistrationIdentity) {
        let cell = {
            let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let wait = state
                .open_waits
                .iter()
                .find_map(|(wait, owner)| (*owner == Some(identity)).then_some(*wait))
                .expect("bound wait identity");
            state
                .wait_cells
                .get(&wait)
                .expect("bound wait cell")
                .upgrade()
                .expect("live bound wait cell")
        };
        cell.dispatch();
    }

    pub(super) fn drain_bound_cleanup(&self, identity: WindowsRegistrationIdentity) {
        let shared = {
            let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let wait = state
                .open_waits
                .iter()
                .find_map(|(wait, owner)| (*owner == Some(identity)).then_some(*wait))
                .expect("bound wait identity");
            state
                .wait_cells
                .get(&wait)
                .expect("bound wait cell")
                .upgrade()
                .expect("live bound wait cell")
                .shared
                .upgrade()
                .expect("live supervisor lease")
        };
        shared.drain_cleanup();
    }

    pub(super) fn fallible_stage_after_resume(&self) -> Option<WindowsSpawnStage> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .fallible_after_resume
    }

    pub(super) fn signal_process(&self, identity: WindowsRegistrationIdentity, exit_code: u32) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let process = state
            .processes
            .values_mut()
            .find(|process| process.identity == Some(identity))
            .expect("fake process identity");
        process.exit_code = Some(exit_code);
    }

    pub(super) fn set_job_active_processes(&self, active_processes: u32) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .job_active_processes = active_processes;
    }

    pub(super) fn block_job_accounting_query(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.job_query_blocked = true;
        state.job_query_entered = false;
    }

    pub(super) fn wait_for_blocked_job_accounting_query(&self, timeout: Duration) -> bool {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        self.changed
            .wait_timeout_while(state, timeout, |state| !state.job_query_entered)
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .0
            .job_query_entered
    }

    pub(super) fn release_job_accounting_query(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.job_query_blocked = false;
        self.changed.notify_all();
    }

    pub(super) fn wait_for_count(
        &self,
        stage: WindowsSpawnStage,
        count: usize,
        timeout: Duration,
    ) -> bool {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        self.changed
            .wait_timeout_while(state, timeout, |state| {
                state.events.iter().filter(|event| **event == stage).count() < count
            })
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .0
            .events
            .iter()
            .filter(|event| **event == stage)
            .count()
            >= count
    }
}

#[cfg(test)]
impl WindowsSpawnKernel for FakeWindowsKernel {
    fn attach_supervisor(&self, shared: Weak<WindowsSpawnShared>) -> Result<(), KernelFailure> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .supervisor = Some(shared);
        Ok(())
    }

    fn uses_automatic_cleanup_worker(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .automatic_cleanup_worker
    }

    fn shutdown_completion_worker(&self) -> Result<(), KernelFailure> {
        self.stage(WindowsSpawnStage::ShutdownCompletionWorker, true)
    }

    fn checkpoint(&self, stage: WindowsSpawnStage) -> Result<(), KernelFailure> {
        self.stage(stage, true)
    }

    fn record_infallible(&self, stage: WindowsSpawnStage) {
        self.stage(stage, false).expect("infallible fake stage");
    }

    fn create_threadpool_wait(
        &self,
        cell: &Arc<WindowsWaitCell>,
    ) -> Result<ThreadpoolWaitHandle, KernelFailure> {
        self.stage(WindowsSpawnStage::CreateThreadpoolWait, true)?;
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let wait = ThreadpoolWaitHandle(state.next_handle);
        state.next_handle += 1;
        state.open_waits.insert(wait, None);
        state.wait_cells.insert(wait, Arc::downgrade(cell));
        Ok(wait)
    }

    fn set_threadpool_wait(
        &self,
        wait: ThreadpoolWaitHandle,
        process: KernelHandle,
        _cell: &Arc<WindowsWaitCell>,
    ) {
        self.record_infallible(WindowsSpawnStage::SetThreadpoolWait);
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .process_waits
            .insert(process, wait);
    }

    fn disarm_threadpool_wait(&self, _wait: ThreadpoolWaitHandle) {
        self.record_infallible(WindowsSpawnStage::DisarmThreadpoolWait);
    }

    fn quiesce_threadpool_wait(&self, _wait: ThreadpoolWaitHandle) {
        self.record_infallible(WindowsSpawnStage::QuiesceThreadpoolWait);
        let callback = {
            let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.callback_inside_quiesce.then(|| {
                state
                    .wait_cells
                    .get(&_wait)
                    .expect("quiesced wait cell")
                    .upgrade()
                    .expect("live quiesced wait cell")
            })
        };
        if let Some(callback) = callback {
            callback.dispatch();
        }
    }

    fn close_threadpool_wait(&self, wait: ThreadpoolWaitHandle) {
        self.record_infallible(WindowsSpawnStage::CloseThreadpoolWait);
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(state.open_waits.remove(&wait).is_some());
        state.wait_cells.remove(&wait);
    }

    fn prepare_stdio(&self, plan: WindowsStdioPlan) -> Result<PreparedStdioRaw, KernelFailure> {
        self.stage(WindowsSpawnStage::PrepareStdio, true)?;
        let mut child = [None; 3];
        let mut parent = [None; 3];
        for (index, disposition) in plan.dispositions().into_iter().enumerate() {
            match disposition {
                WindowsStdioDisposition::Pipe => {
                    child[index] = Some(self.allocate_handle(HandleKind::ChildStdio));
                    parent[index] = Some(self.allocate_handle(HandleKind::ParentStdio));
                }
                WindowsStdioDisposition::Null => {
                    child[index] = Some(self.allocate_handle(HandleKind::ChildStdio));
                }
                WindowsStdioDisposition::Inherit => {}
            }
        }
        Ok(PreparedStdioRaw { child, parent })
    }

    fn prepare_conpty(&self, _size: WindowsConPtySize) -> Result<PreparedConPtyRaw, KernelFailure> {
        self.stage(WindowsSpawnStage::PrepareConPty, true)?;
        let input_read = self.allocate_handle(HandleKind::PseudoConsolePipe);
        let output_write = self.allocate_handle(HandleKind::PseudoConsolePipe);
        let input_write = self.allocate_handle(HandleKind::ParentStdio);
        let output_read = self.allocate_handle(HandleKind::ParentStdio);
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let console = PseudoConsoleHandle(state.next_handle);
        state.next_handle += 1;
        assert!(state.open_pseudoconsoles.insert(console));
        Ok(PreparedConPtyRaw {
            console,
            input_read,
            output_write,
            input_write,
            output_read,
        })
    }

    fn resize_pseudoconsole(
        &self,
        console: PseudoConsoleHandle,
        _size: WindowsConPtySize,
    ) -> Result<(), u32> {
        assert!(
            self.state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .open_pseudoconsoles
                .contains(&console),
            "resize of closed fake pseudoconsole"
        );
        Ok(())
    }

    fn close_pseudoconsole(&self, console: PseudoConsoleHandle) {
        assert!(
            self.state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .open_pseudoconsoles
                .remove(&console),
            "fake pseudoconsole closed twice"
        );
    }

    fn create_private_job(&self) -> Result<KernelHandle, KernelFailure> {
        self.stage(WindowsSpawnStage::CreatePrivateJob, true)?;
        Ok(self.allocate_handle(HandleKind::PrivateJob))
    }

    fn configure_private_job(
        &self,
        _job: KernelHandle,
        _completion_id: u64,
    ) -> Result<(), KernelFailure> {
        self.stage(WindowsSpawnStage::ConfigurePrivateJob, true)
    }

    fn duplicate_target_sentinel(
        &self,
        _source: KernelHandle,
    ) -> Result<KernelHandle, KernelFailure> {
        self.stage(WindowsSpawnStage::DuplicateTargetSentinel, true)?;
        Ok(self.allocate_handle(HandleKind::TargetSentinel))
    }

    fn build_attribute_list(
        &self,
        job: Option<KernelHandle>,
        pseudoconsole: Option<PseudoConsoleHandle>,
        _inherited_handles: &[KernelHandle],
        needs_handle_list: bool,
    ) -> Result<AttributeListHandle, KernelFailure> {
        if job.is_some() {
            self.stage(WindowsSpawnStage::BuildJobListAttribute, true)?;
        }
        if needs_handle_list {
            self.stage(WindowsSpawnStage::BuildHandleListAttribute, true)?;
        }
        if pseudoconsole.is_some() {
            self.stage(WindowsSpawnStage::BuildPseudoConsoleAttribute, true)?;
        }
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let attributes = AttributeListHandle(state.next_handle);
        state.next_handle += 1;
        state.open_attributes.insert(attributes);
        Ok(attributes)
    }

    fn delete_attribute_list(&self, attributes: AttributeListHandle) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(state.open_attributes.remove(&attributes));
    }

    fn create_process(
        &self,
        image: &dyn WindowsSpawnImage,
        flags: u32,
        _stdio: &[Option<KernelHandle>; 3],
        _attributes: Option<AttributeListHandle>,
        inherit_handles: bool,
        _uses_pseudoconsole: bool,
    ) -> Result<CreatedProcess, KernelFailure> {
        self.stage(WindowsSpawnStage::CreateProcessW, true)?;
        assert!(!image.application().is_empty());
        assert!(!image.command_line().is_empty());
        let _ = image.cwd();
        let _ = image.environment();
        let process = self.allocate_handle(HandleKind::Process);
        let primary_thread = self.allocate_handle(HandleKind::PrimaryThread);
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let pid = 4_000 + u32::try_from(process.0).expect("fake handle fits u32");
        state.last_creation_flags = flags;
        state.last_inherit_handles = inherit_handles;
        state.created_pid = pid;
        let outcome = state.create_outcome;
        state.processes.insert(
            process,
            FakeProcess {
                pid,
                identity: None,
                primary_thread,
                outcome,
                exit_code: None,
                descendants: 0,
            },
        );
        state.threads.insert(primary_thread, process);
        Ok(CreatedProcess {
            pid,
            process,
            primary_thread,
        })
    }

    fn assign_global_job(&self, _process: KernelHandle) -> Result<(), KernelFailure> {
        self.stage(WindowsSpawnStage::AssignLibuvGlobalJob, true)
    }

    fn wrap_parent_stdio(&self, _handles: &[KernelHandle]) -> Result<(), KernelFailure> {
        self.stage(WindowsSpawnStage::WrapParentStdio, true)
    }

    fn resume_thread(
        &self,
        thread: KernelHandle,
        cell: &Arc<WindowsWaitCell>,
    ) -> Result<(), KernelFailure> {
        self.stage(WindowsSpawnStage::ResumeThread, true)?;
        let callback = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.resumed = true;
            let process_handle = *state.threads.get(&thread).expect("fake primary thread");
            let process = state
                .processes
                .get_mut(&process_handle)
                .expect("fake process");
            if let CreateProcessOutcome::FastExitOnResume { exit_code } = process.outcome {
                process.exit_code = Some(exit_code);
            }
            state.queue_callback_inside_resume
        };
        if callback {
            let cell = cell.clone();
            let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
            let callback = std::thread::spawn(move || {
                started_tx.send(()).expect("callback start barrier");
                cell.dispatch();
            });
            started_rx.recv().expect("callback start barrier");
            self.state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .resume_callback = Some(callback);
        }
        Ok(())
    }

    fn wait_for_single_object(
        &self,
        process: KernelHandle,
        timeout: Duration,
    ) -> WindowsWaitObservation {
        if timeout.is_zero() {
            self.record_infallible(WindowsSpawnStage::ImmediateWaitQuery);
        } else {
            self.record_infallible(WindowsSpawnStage::HiddenBoundedWait);
        }
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if !timeout.is_zero() {
            state.max_hidden_wait = Some(
                state
                    .max_hidden_wait
                    .map_or(timeout, |current| current.max(timeout)),
            );
            let outcome = state.hidden_cleanup;
            let process = state.processes.get_mut(&process).expect("fake process");
            let observation = match outcome {
                HiddenCleanupOutcome::Terminal { exit_code } => {
                    process.exit_code = Some(exit_code);
                    WindowsWaitObservation::Signaled
                }
                HiddenCleanupOutcome::StillLive => WindowsWaitObservation::Timeout,
                HiddenCleanupOutcome::Failed(error) => WindowsWaitObservation::Failed(error),
            };
            let identity = process.identity;
            let supervisor = state.supervisor.clone();
            drop(state);
            if observation == WindowsWaitObservation::Signaled {
                if let (Some(identity), Some(shared)) =
                    (identity, supervisor.and_then(|shared| shared.upgrade()))
                {
                    let registration = shared
                        .registrations
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .get(&identity)
                        .cloned();
                    if let Some(registration) = registration {
                        let completion_id = registration
                            .lifecycle
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .completion_cell
                            .as_ref()
                            .map(|cell| cell.id);
                        if let Some(completion_id) = completion_id {
                            shared.observe_job_active_zero(completion_id);
                        }
                    }
                }
            }
            return observation;
        }
        if state
            .processes
            .get(&process)
            .expect("fake process")
            .exit_code
            .is_some()
        {
            WindowsWaitObservation::Signaled
        } else {
            WindowsWaitObservation::Timeout
        }
    }

    fn get_exit_code_process(&self, process: KernelHandle) -> Result<Option<u32>, u32> {
        self.record_infallible(WindowsSpawnStage::GetExitCodeProcess);
        Ok(self
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .processes
            .get(&process)
            .expect("fake process")
            .exit_code)
    }

    fn terminate_process(&self, process: KernelHandle, _exit_code: u32) -> WindowsTerminateResult {
        self.record_infallible(WindowsSpawnStage::TerminateProcess);
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let outcome = state.hidden_cleanup;
        let process = state.processes.get_mut(&process).expect("fake process");
        if process.exit_code.is_some() {
            return WindowsTerminateResult::AlreadyTerminal;
        }
        match outcome {
            HiddenCleanupOutcome::Terminal { exit_code } => {
                process.exit_code = Some(exit_code);
                WindowsTerminateResult::Delivered
            }
            HiddenCleanupOutcome::StillLive => WindowsTerminateResult::Delivered,
            HiddenCleanupOutcome::Failed(error) => WindowsTerminateResult::Failed(error),
        }
    }

    fn terminate_job(&self, _job: KernelHandle, _exit_code: u32) -> WindowsTerminateResult {
        self.record_infallible(WindowsSpawnStage::TerminatePrivateJob);
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let outcome = state.hidden_cleanup;
        if state
            .processes
            .values()
            .all(|process| process.exit_code.is_some())
        {
            return WindowsTerminateResult::AlreadyTerminal;
        }
        match outcome {
            HiddenCleanupOutcome::Terminal { exit_code } => {
                for process in state.processes.values_mut() {
                    process.exit_code.get_or_insert(exit_code);
                }
                WindowsTerminateResult::Delivered
            }
            HiddenCleanupOutcome::StillLive => WindowsTerminateResult::Delivered,
            HiddenCleanupOutcome::Failed(error) => WindowsTerminateResult::Failed(error),
        }
    }

    fn query_job_active_processes(&self, _job: KernelHandle) -> Result<u32, u32> {
        self.record_infallible(WindowsSpawnStage::QueryPrivateJobAccounting);
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.job_query_blocked {
            state.job_query_entered = true;
            self.changed.notify_all();
            while state.job_query_blocked {
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner);
            }
        }
        Ok(state.job_active_processes)
    }

    fn close_handle(&self, handle: KernelHandle, kind: HandleKind) -> bool {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let removed = state.open_handles.remove(&handle);
        assert_eq!(removed.map(|entry| entry.0), Some(kind));
        if kind == HandleKind::PrimaryThread {
            state.events.push(WindowsSpawnStage::ClosePrimaryThread);
        }
        true
    }

    fn bind_identity(
        &self,
        process: KernelHandle,
        thread: KernelHandle,
        wait: ThreadpoolWaitHandle,
        identity: WindowsRegistrationIdentity,
    ) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .processes
            .get_mut(&process)
            .expect("fake process")
            .identity = Some(identity);
        for handle in [process, thread] {
            state
                .open_handles
                .get_mut(&handle)
                .expect("open fake handle")
                .1 = Some(identity);
        }
        *state.open_waits.get_mut(&wait).expect("open fake wait") = Some(identity);
    }

    fn record_callback_phase(&self, phase: &'static str) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .callback_phases
            .push(phase);
    }
}

#[cfg(windows)]
use production::ProductionWindowsKernel;

/// Real Win32 ownership boundary. The surrounding module keeps it shadowed
/// until the atomic runtime-process cutover, but this layer performs the real
/// handle, attribute-list, Job, wait, and CreateProcessW operations; it never
/// delegates to a higher-level command API.
#[cfg(windows)]
mod production {
    use std::ffi::c_void;
    use std::mem::{size_of, zeroed};
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::ptr::{null, null_mut};
    use std::sync::atomic::AtomicBool;

    use super::*;

    type Handle = *mut c_void;
    type Bool = i32;
    type Dword = u32;

    const FALSE: Bool = 0;
    const TRUE: Bool = 1;
    const INVALID_HANDLE_VALUE: Handle = (-1_isize) as Handle;
    const ERROR_ACCESS_DENIED: Dword = 5;
    const ERROR_INSUFFICIENT_BUFFER: Dword = 122;
    const WAIT_OBJECT_0: Dword = 0;
    const WAIT_TIMEOUT: Dword = 258;
    const WAIT_FAILED: Dword = Dword::MAX;
    const STILL_ACTIVE: Dword = 259;
    const INFINITE: Dword = Dword::MAX;

    const CREATE_UNICODE_ENVIRONMENT: Dword = 0x0000_0400;
    const EXTENDED_STARTUPINFO_PRESENT: Dword = 0x0008_0000;
    const STARTF_USESTDHANDLES: Dword = 0x0000_0100;
    const HANDLE_FLAG_INHERIT: Dword = 0x0000_0001;
    const DUPLICATE_SAME_ACCESS: Dword = 0x0000_0002;
    const GENERIC_READ: Dword = 0x8000_0000;
    const GENERIC_WRITE: Dword = 0x4000_0000;
    const FILE_SHARE_READ: Dword = 0x0000_0001;
    const FILE_SHARE_WRITE: Dword = 0x0000_0002;
    const OPEN_EXISTING: Dword = 3;
    const STD_INPUT_HANDLE: Dword = (-10_i32) as Dword;
    const STD_OUTPUT_HANDLE: Dword = (-11_i32) as Dword;
    const STD_ERROR_HANDLE: Dword = (-12_i32) as Dword;

    const JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION: Dword = 0x0000_0400;
    const JOB_OBJECT_LIMIT_BREAKAWAY_OK: Dword = 0x0000_0800;
    const JOB_OBJECT_LIMIT_SILENT_BREAKAWAY_OK: Dword = 0x0000_1000;
    const JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE: Dword = 0x0000_2000;
    const JOB_OBJECT_EXTENDED_LIMIT_INFORMATION: Dword = 9;
    const JOB_OBJECT_ASSOCIATE_COMPLETION_PORT_INFORMATION: Dword = 7;
    const JOB_OBJECT_BASIC_ACCOUNTING_INFORMATION: Dword = 1;
    const JOB_OBJECT_MSG_ACTIVE_PROCESS_ZERO: Dword = 4;
    const COMPLETION_WORKER_SHUTDOWN_KEY: usize = usize::MAX;

    // ProcThreadAttributeValue(number, thread=false, input=true,
    // additive=false), from the Windows SDK headers.
    const PROC_THREAD_ATTRIBUTE_HANDLE_LIST: usize = 0x0002_0002;
    const PROC_THREAD_ATTRIBUTE_JOB_LIST: usize = 0x0002_000d;
    const PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE: usize = 0x0002_0016;
    const PSEUDOCONSOLE_RESIZE_QUIRK: Dword = 0x2;

    #[repr(C)]
    struct Coord {
        x: i16,
        y: i16,
    }

    #[repr(C)]
    struct SecurityAttributes {
        length: Dword,
        security_descriptor: *mut c_void,
        inherit_handle: Bool,
    }

    #[repr(C)]
    struct StartupInfoW {
        cb: Dword,
        reserved: *mut u16,
        desktop: *mut u16,
        title: *mut u16,
        x: Dword,
        y: Dword,
        x_size: Dword,
        y_size: Dword,
        x_count_chars: Dword,
        y_count_chars: Dword,
        fill_attribute: Dword,
        flags: Dword,
        show_window: u16,
        reserved2_size: u16,
        reserved2: *mut u8,
        stdin: Handle,
        stdout: Handle,
        stderr: Handle,
    }

    #[repr(C)]
    struct StartupInfoExW {
        startup_info: StartupInfoW,
        attribute_list: *mut c_void,
    }

    #[repr(C)]
    struct ProcessInformation {
        process: Handle,
        thread: Handle,
        process_id: Dword,
        thread_id: Dword,
    }

    #[repr(C)]
    struct IoCounters {
        read_operation_count: u64,
        write_operation_count: u64,
        other_operation_count: u64,
        read_transfer_count: u64,
        write_transfer_count: u64,
        other_transfer_count: u64,
    }

    #[repr(C)]
    struct JobObjectBasicLimitInformation {
        per_process_user_time_limit: i64,
        per_job_user_time_limit: i64,
        limit_flags: Dword,
        minimum_working_set_size: usize,
        maximum_working_set_size: usize,
        active_process_limit: Dword,
        affinity: usize,
        priority_class: Dword,
        scheduling_class: Dword,
    }

    #[repr(C)]
    struct JobObjectExtendedLimitInformation {
        basic_limit_information: JobObjectBasicLimitInformation,
        io_info: IoCounters,
        process_memory_limit: usize,
        job_memory_limit: usize,
        peak_process_memory_used: usize,
        peak_job_memory_used: usize,
    }

    #[repr(C)]
    struct JobObjectAssociateCompletionPort {
        completion_key: *mut c_void,
        completion_port: Handle,
    }

    #[repr(C)]
    struct JobObjectBasicAccountingInformation {
        total_user_time: i64,
        total_kernel_time: i64,
        this_period_total_user_time: i64,
        this_period_total_kernel_time: i64,
        total_page_fault_count: Dword,
        total_processes: Dword,
        active_processes: Dword,
        total_terminated_processes: Dword,
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetLastError() -> Dword;
        fn CloseHandle(object: Handle) -> Bool;
        fn GetCurrentProcess() -> Handle;
        fn GetStdHandle(std_handle: Dword) -> Handle;
        fn GetHandleInformation(object: Handle, flags: *mut Dword) -> Bool;
        fn SetHandleInformation(object: Handle, mask: Dword, flags: Dword) -> Bool;
        fn DuplicateHandle(
            source_process: Handle,
            source: Handle,
            target_process: Handle,
            target: *mut Handle,
            access: Dword,
            inherit: Bool,
            options: Dword,
        ) -> Bool;
        fn CreatePipe(
            read_pipe: *mut Handle,
            write_pipe: *mut Handle,
            attributes: *mut SecurityAttributes,
            size: Dword,
        ) -> Bool;
        fn CreateFileW(
            name: *const u16,
            access: Dword,
            share: Dword,
            attributes: *mut SecurityAttributes,
            creation: Dword,
            flags: Dword,
            template: Handle,
        ) -> Handle;
        fn CreatePseudoConsole(
            size: Coord,
            input: Handle,
            output: Handle,
            flags: Dword,
            console: *mut Handle,
        ) -> i32;
        fn ResizePseudoConsole(console: Handle, size: Coord) -> i32;
        fn ClosePseudoConsole(console: Handle);

        fn CreateThreadpoolWait(
            callback: Option<
                unsafe extern "system" fn(*mut c_void, *mut c_void, *mut c_void, Dword),
            >,
            context: *mut c_void,
            callback_environment: *mut c_void,
        ) -> *mut c_void;
        fn SetThreadpoolWait(wait: *mut c_void, object: Handle, timeout: *const c_void);
        fn WaitForThreadpoolWaitCallbacks(wait: *mut c_void, cancel_pending: Bool);
        fn CloseThreadpoolWait(wait: *mut c_void);

        fn InitializeProcThreadAttributeList(
            attributes: *mut c_void,
            count: Dword,
            flags: Dword,
            size: *mut usize,
        ) -> Bool;
        fn UpdateProcThreadAttribute(
            attributes: *mut c_void,
            flags: Dword,
            attribute: usize,
            value: *mut c_void,
            size: usize,
            previous_value: *mut c_void,
            return_size: *mut usize,
        ) -> Bool;
        fn DeleteProcThreadAttributeList(attributes: *mut c_void);

        fn CreateProcessW(
            application_name: *const u16,
            command_line: *mut u16,
            process_attributes: *mut SecurityAttributes,
            thread_attributes: *mut SecurityAttributes,
            inherit_handles: Bool,
            creation_flags: Dword,
            environment: *mut c_void,
            current_directory: *const u16,
            startup_info: *mut StartupInfoW,
            process_information: *mut ProcessInformation,
        ) -> Bool;
        fn ResumeThread(thread: Handle) -> Dword;
        fn WaitForSingleObject(object: Handle, milliseconds: Dword) -> Dword;
        fn GetExitCodeProcess(process: Handle, exit_code: *mut Dword) -> Bool;
        fn TerminateProcess(process: Handle, exit_code: Dword) -> Bool;

        fn CreateJobObjectW(attributes: *mut SecurityAttributes, name: *const u16) -> Handle;
        fn SetInformationJobObject(
            job: Handle,
            information_class: Dword,
            information: *mut c_void,
            information_length: Dword,
        ) -> Bool;
        fn AssignProcessToJobObject(job: Handle, process: Handle) -> Bool;
        fn TerminateJobObject(job: Handle, exit_code: Dword) -> Bool;
        fn QueryInformationJobObject(
            job: Handle,
            information_class: Dword,
            information: *mut c_void,
            information_length: Dword,
            return_length: *mut Dword,
        ) -> Bool;
        fn CreateIoCompletionPort(
            file: Handle,
            existing_port: Handle,
            completion_key: usize,
            concurrent_threads: Dword,
        ) -> Handle;
        fn GetQueuedCompletionStatus(
            completion_port: Handle,
            bytes_transferred: *mut Dword,
            completion_key: *mut usize,
            overlapped: *mut *mut c_void,
            milliseconds: Dword,
        ) -> Bool;
        fn PostQueuedCompletionStatus(
            completion_port: Handle,
            bytes_transferred: Dword,
            completion_key: usize,
            overlapped: *mut c_void,
        ) -> Bool;
    }

    pub(super) struct ProductionWindowsKernel {
        completion_port: Arc<CompletionPortRuntime>,
        global_job: Mutex<Option<KernelHandle>>,
        attribute_allocations: Mutex<HashMap<AttributeListHandle, AttributeAllocation>>,
        completion_worker: Mutex<Option<std::thread::JoinHandle<()>>>,
        completion_worker_attached: AtomicBool,
        completion_worker_failed: Arc<AtomicBool>,
    }

    struct AttributeAllocation {
        list_storage: Box<[usize]>,
        job_values: Box<[usize]>,
        handle_values: Box<[usize]>,
    }

    struct CompletionPortRuntime {
        port: KernelHandle,
    }

    // SAFETY: the completion port is an opaque kernel handle and this object
    // only exposes thread-safe completion-port operations.
    unsafe impl Send for CompletionPortRuntime {}
    // SAFETY: see the Send justification above.
    unsafe impl Sync for CompletionPortRuntime {}

    impl CompletionPortRuntime {
        fn request_shutdown(&self) -> Result<(), KernelFailure> {
            // SAFETY: the runtime retains the live port until the worker drops
            // its final Arc after observing this sentinel.
            let posted = unsafe {
                PostQueuedCompletionStatus(
                    to_handle(self.port),
                    0,
                    COMPLETION_WORKER_SHUTDOWN_KEY,
                    null_mut(),
                )
            };
            if posted == FALSE {
                Err(last_failure(
                    WindowsSpawnStage::ShutdownCompletionWorker,
                    "PostQueuedCompletionStatus(completion shutdown)",
                ))
            } else {
                Ok(())
            }
        }
    }

    impl Drop for CompletionPortRuntime {
        fn drop(&mut self) {
            close_raw(self.port);
        }
    }

    // SAFETY: every raw handle is only used through Win32's thread-safe handle
    // APIs, while mutable ownership tables are protected by mutexes.
    unsafe impl Send for ProductionWindowsKernel {}
    // SAFETY: see the Send justification above.
    unsafe impl Sync for ProductionWindowsKernel {}

    impl ProductionWindowsKernel {
        pub(super) fn initialize() -> Result<Self, KernelFailure> {
            // SAFETY: INVALID_HANDLE_VALUE requests a new completion port and
            // the remaining arguments follow CreateIoCompletionPort's creation
            // contract.
            let port = unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, null_mut(), 0, 0) };
            if is_invalid(port) {
                return Err(last_failure(
                    WindowsSpawnStage::ConfigurePrivateJob,
                    "CreateIoCompletionPort",
                ));
            }
            Ok(Self {
                completion_port: Arc::new(CompletionPortRuntime {
                    port: from_handle(port),
                }),
                global_job: Mutex::new(None),
                attribute_allocations: Mutex::new(HashMap::new()),
                completion_worker: Mutex::new(None),
                completion_worker_attached: AtomicBool::new(false),
                completion_worker_failed: Arc::new(AtomicBool::new(false)),
            })
        }

        fn ensure_global_job(&self) -> Result<KernelHandle, KernelFailure> {
            let mut slot = self
                .global_job
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(job) = *slot {
                return Ok(job);
            }
            let job = create_job(WindowsSpawnStage::AssignLibuvGlobalJob)?;
            let flags = JOB_OBJECT_LIMIT_BREAKAWAY_OK
                | JOB_OBJECT_LIMIT_SILENT_BREAKAWAY_OK
                | JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION
                | JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            if let Err(error) =
                configure_job_limits(job, flags, WindowsSpawnStage::AssignLibuvGlobalJob)
            {
                close_raw(job);
                return Err(error);
            }
            *slot = Some(job);
            Ok(job)
        }
    }

    impl Drop for ProductionWindowsKernel {
        fn drop(&mut self) {
            if self.completion_worker_attached.load(Ordering::Acquire) {
                // The worker owns a separate Arc to the port runtime. Dropping
                // this kernel on that worker can therefore request shutdown
                // and detach its own JoinHandle without self-joining or
                // closing the port underneath the current dequeue.
                let _ = self.completion_port.request_shutdown();
                self.completion_worker
                    .get_mut()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take();
            }
            for (attributes, _) in self
                .attribute_allocations
                .get_mut()
                .unwrap_or_else(PoisonError::into_inner)
                .drain()
            {
                // SAFETY: each key is the live allocation pointer registered
                // by build_attribute_list and has not yet been deleted.
                unsafe { DeleteProcThreadAttributeList(to_ptr(attributes.0)) };
            }
            if let Some(job) = self
                .global_job
                .get_mut()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
            {
                close_raw(job);
            }
        }
    }

    impl WindowsSpawnKernel for ProductionWindowsKernel {
        fn attach_supervisor(&self, shared: Weak<WindowsSpawnShared>) -> Result<(), KernelFailure> {
            if self
                .completion_worker_attached
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return Err(KernelFailure::new(
                    WindowsSpawnStage::InitializeCompletionWorker,
                    "Windows completion worker attached twice",
                ));
            }
            let port = self.completion_port.clone();
            let failed = self.completion_worker_failed.clone();
            let worker = std::thread::Builder::new()
                .name("klaus-win-job-completion".to_string())
                .spawn(move || completion_worker_main(port, shared, failed))
                .map_err(|error| {
                    self.completion_worker_attached
                        .store(false, Ordering::Release);
                    KernelFailure::new(
                        WindowsSpawnStage::InitializeCompletionWorker,
                        format!("failed to start Windows completion worker: {error}"),
                    )
                })?;
            *self
                .completion_worker
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(worker);
            Ok(())
        }

        fn uses_automatic_cleanup_worker(&self) -> bool {
            true
        }

        fn shutdown_completion_worker(&self) -> Result<(), KernelFailure> {
            let mut slot = self
                .completion_worker
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let Some(worker) = slot.as_ref() else {
                return Ok(());
            };
            if worker.thread().id() == std::thread::current().id() {
                return Err(KernelFailure::new(
                    WindowsSpawnStage::ShutdownCompletionWorker,
                    "Windows completion worker cannot join itself",
                ));
            }
            self.completion_port.request_shutdown()?;
            let worker = slot
                .take()
                .expect("completion worker observed immediately above");
            drop(slot);
            let joined = worker.join().map_err(|_| {
                KernelFailure::new(
                    WindowsSpawnStage::ShutdownCompletionWorker,
                    "Windows completion worker panicked during shutdown",
                )
            });
            self.completion_worker_attached
                .store(false, Ordering::Release);
            joined
        }

        fn checkpoint(&self, stage: WindowsSpawnStage) -> Result<(), KernelFailure> {
            if self.completion_worker_failed.load(Ordering::Acquire) {
                Err(KernelFailure::new(
                    stage,
                    "Windows completion worker is unavailable",
                ))
            } else {
                Ok(())
            }
        }

        fn record_infallible(&self, _stage: WindowsSpawnStage) {}

        fn create_threadpool_wait(
            &self,
            cell: &Arc<WindowsWaitCell>,
        ) -> Result<ThreadpoolWaitHandle, KernelFailure> {
            // SAFETY: the Arc-held cell address is stable until the owned wait
            // has been disarmed, quiesced, and closed.
            let wait = unsafe {
                CreateThreadpoolWait(
                    Some(threadpool_wait_callback),
                    Arc::as_ptr(cell).cast_mut().cast(),
                    null_mut(),
                )
            };
            if wait.is_null() {
                Err(last_failure(
                    WindowsSpawnStage::CreateThreadpoolWait,
                    "CreateThreadpoolWait",
                ))
            } else {
                Ok(ThreadpoolWaitHandle(wait as usize))
            }
        }

        fn set_threadpool_wait(
            &self,
            wait: ThreadpoolWaitHandle,
            process: KernelHandle,
            _cell: &Arc<WindowsWaitCell>,
        ) {
            // SAFETY: both handles are live owned transaction resources and a
            // null timeout requests an indefinite asynchronous wait.
            unsafe { SetThreadpoolWait(to_ptr(wait.0), to_handle(process), null()) };
        }

        fn disarm_threadpool_wait(&self, wait: ThreadpoolWaitHandle) {
            // SAFETY: null object and timeout disarm this live wait.
            unsafe { SetThreadpoolWait(to_ptr(wait.0), null_mut(), null()) };
        }

        fn quiesce_threadpool_wait(&self, wait: ThreadpoolWaitHandle) {
            // SAFETY: the wait was disarmed first; TRUE also cancels a pending
            // callback before this function returns.
            unsafe { WaitForThreadpoolWaitCallbacks(to_ptr(wait.0), TRUE) };
        }

        fn close_threadpool_wait(&self, wait: ThreadpoolWaitHandle) {
            // SAFETY: the wait has been disarmed and quiesced exactly once.
            unsafe { CloseThreadpoolWait(to_ptr(wait.0)) };
        }

        fn prepare_stdio(&self, plan: WindowsStdioPlan) -> Result<PreparedStdioRaw, KernelFailure> {
            let mut child = [None; 3];
            let mut parent = [None; 3];
            for (index, disposition) in plan.dispositions().into_iter().enumerate() {
                let prepared = match disposition {
                    WindowsStdioDisposition::Pipe => prepare_pipe(index),
                    WindowsStdioDisposition::Inherit => {
                        duplicate_inherited_stdio(index).map(|handle| (handle, None))
                    }
                    WindowsStdioDisposition::Null => {
                        open_null(index).map(|handle| (Some(handle), None))
                    }
                };
                match prepared {
                    Ok((child_handle, parent_handle)) => {
                        child[index] = child_handle;
                        parent[index] = parent_handle;
                    }
                    Err(error) => {
                        close_raw_stdio(&mut child, &mut parent);
                        return Err(error);
                    }
                }
            }
            Ok(PreparedStdioRaw { child, parent })
        }

        fn prepare_conpty(
            &self,
            size: WindowsConPtySize,
        ) -> Result<PreparedConPtyRaw, KernelFailure> {
            let coord = conpty_coord(size)
                .map_err(|message| KernelFailure::new(WindowsSpawnStage::PrepareConPty, message))?;
            let mut input_read = null_mut();
            let mut input_write = null_mut();
            let mut output_read = null_mut();
            let mut output_write = null_mut();
            let mut attributes = SecurityAttributes {
                length: size_of::<SecurityAttributes>() as Dword,
                security_descriptor: null_mut(),
                inherit_handle: FALSE,
            };
            // SAFETY: all four outputs are valid and each successful pipe is
            // closed below on any later failure.
            if unsafe { CreatePipe(&mut input_read, &mut input_write, &mut attributes, 0) } == FALSE
            {
                return Err(last_failure(
                    WindowsSpawnStage::PrepareConPty,
                    "CreatePipe(ConPTY input)",
                ));
            }
            if unsafe { CreatePipe(&mut output_read, &mut output_write, &mut attributes, 0) }
                == FALSE
            {
                close_handle_value(input_read);
                close_handle_value(input_write);
                return Err(last_failure(
                    WindowsSpawnStage::PrepareConPty,
                    "CreatePipe(ConPTY output)",
                ));
            }
            let mut console = null_mut();
            // SAFETY: the pipe endpoints remain live in OwnedPseudoConsole for
            // the entire pseudoconsole lifetime.
            let result = unsafe {
                CreatePseudoConsole(
                    coord,
                    input_read,
                    output_write,
                    PSEUDOCONSOLE_RESIZE_QUIRK,
                    &mut console,
                )
            };
            if result != 0 || is_invalid(console) {
                close_handle_value(input_read);
                close_handle_value(input_write);
                close_handle_value(output_read);
                close_handle_value(output_write);
                return Err(KernelFailure::new(
                    WindowsSpawnStage::PrepareConPty,
                    format!(
                        "CreatePseudoConsole failed with HRESULT 0x{:08x}",
                        result as u32
                    ),
                ));
            }
            Ok(PreparedConPtyRaw {
                console: PseudoConsoleHandle(console as usize),
                input_read: from_handle(input_read),
                output_write: from_handle(output_write),
                input_write: from_handle(input_write),
                output_read: from_handle(output_read),
            })
        }

        fn resize_pseudoconsole(
            &self,
            console: PseudoConsoleHandle,
            size: WindowsConPtySize,
        ) -> Result<(), u32> {
            let coord = conpty_coord(size).map_err(|_| 87_u32)?;
            // SAFETY: the state mutex keeps the live pseudoconsole handle
            // owned for this complete synchronous resize.
            let result = unsafe { ResizePseudoConsole(to_ptr(console.0), coord) };
            if result == 0 {
                Ok(())
            } else {
                Err(result as u32)
            }
        }

        fn close_pseudoconsole(&self, console: PseudoConsoleHandle) {
            // SAFETY: OwnedPseudoConsole calls this exactly once; HPCON values
            // are closed by ClosePseudoConsole rather than CloseHandle.
            unsafe { ClosePseudoConsole(to_ptr(console.0)) };
        }

        fn create_private_job(&self) -> Result<KernelHandle, KernelFailure> {
            create_job(WindowsSpawnStage::CreatePrivateJob)
        }

        fn configure_private_job(
            &self,
            job: KernelHandle,
            completion_id: u64,
        ) -> Result<(), KernelFailure> {
            configure_job_limits(
                job,
                JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
                WindowsSpawnStage::ConfigurePrivateJob,
            )?;
            let completion_key = usize::try_from(completion_id).map_err(|_| {
                KernelFailure::new(
                    WindowsSpawnStage::ConfigurePrivateJob,
                    "Windows job completion ID does not fit completion key",
                )
            })?;
            if completion_key == COMPLETION_WORKER_SHUTDOWN_KEY {
                return Err(KernelFailure::new(
                    WindowsSpawnStage::ConfigurePrivateJob,
                    "Windows job completion ID collides with worker shutdown key",
                ));
            }
            let mut association = JobObjectAssociateCompletionPort {
                completion_key: completion_key as *mut c_void,
                completion_port: to_handle(self.completion_port.port),
            };
            // SAFETY: association points to the documented fixed-size
            // JOBOBJECT_ASSOCIATE_COMPLETION_PORT structure.
            let ok = unsafe {
                SetInformationJobObject(
                    to_handle(job),
                    JOB_OBJECT_ASSOCIATE_COMPLETION_PORT_INFORMATION,
                    (&mut association as *mut JobObjectAssociateCompletionPort).cast(),
                    size_of::<JobObjectAssociateCompletionPort>() as Dword,
                )
            };
            if ok == FALSE {
                Err(last_failure(
                    WindowsSpawnStage::ConfigurePrivateJob,
                    "SetInformationJobObject(AssociateCompletionPort)",
                ))
            } else {
                Ok(())
            }
        }

        fn duplicate_target_sentinel(
            &self,
            source: KernelHandle,
        ) -> Result<KernelHandle, KernelFailure> {
            duplicate_handle(source, true, WindowsSpawnStage::DuplicateTargetSentinel)
        }

        fn build_attribute_list(
            &self,
            job: Option<KernelHandle>,
            pseudoconsole: Option<PseudoConsoleHandle>,
            inherited_handles: &[KernelHandle],
            needs_handle_list: bool,
        ) -> Result<AttributeListHandle, KernelFailure> {
            let count = Dword::from(job.is_some())
                + Dword::from(pseudoconsole.is_some())
                + Dword::from(needs_handle_list);
            if count == 0 {
                return Err(KernelFailure::new(
                    WindowsSpawnStage::BuildJobListAttribute,
                    "empty process attribute list",
                ));
            }
            let mut byte_len = 0_usize;
            // SAFETY: the first documented sizing call accepts a null list.
            let first =
                unsafe { InitializeProcThreadAttributeList(null_mut(), count, 0, &mut byte_len) };
            // A successful sizing call is tolerated, but zero storage is not.
            let sizing_error = unsafe { GetLastError() };
            if byte_len == 0 || (first == FALSE && sizing_error != ERROR_INSUFFICIENT_BUFFER) {
                return Err(last_failure(
                    WindowsSpawnStage::BuildJobListAttribute,
                    "InitializeProcThreadAttributeList(size)",
                ));
            }
            let words = byte_len
                .checked_add(size_of::<usize>() - 1)
                .and_then(|value| value.checked_div(size_of::<usize>()))
                .ok_or_else(|| {
                    KernelFailure::new(
                        WindowsSpawnStage::BuildJobListAttribute,
                        "process attribute-list size overflow",
                    )
                })?;
            let mut list_storage = vec![0_usize; words].into_boxed_slice();
            let mut job_values: Box<[usize]> = job
                .into_iter()
                .map(|handle| handle.0)
                .collect::<Vec<_>>()
                .into_boxed_slice();
            let mut handle_values: Box<[usize]> = inherited_handles
                .iter()
                .map(|handle| handle.0)
                .collect::<Vec<_>>()
                .into_boxed_slice();
            let pointer = list_storage.as_mut_ptr().cast::<c_void>();
            // SAFETY: storage is aligned, byte_len-sized, and remains stable in
            // its Box until DeleteProcThreadAttributeList.
            if unsafe { InitializeProcThreadAttributeList(pointer, count, 0, &mut byte_len) }
                == FALSE
            {
                return Err(last_failure(
                    WindowsSpawnStage::BuildJobListAttribute,
                    "InitializeProcThreadAttributeList",
                ));
            }
            let result = (|| {
                if !job_values.is_empty() {
                    // SAFETY: the value is one live Job handle and the list has
                    // an allocated JOB_LIST slot.
                    if unsafe {
                        UpdateProcThreadAttribute(
                            pointer,
                            0,
                            PROC_THREAD_ATTRIBUTE_JOB_LIST,
                            job_values.as_mut_ptr().cast(),
                            job_values.len() * size_of::<Handle>(),
                            null_mut(),
                            null_mut(),
                        )
                    } == FALSE
                    {
                        return Err(last_failure(
                            WindowsSpawnStage::BuildJobListAttribute,
                            "UpdateProcThreadAttribute(JOB_LIST)",
                        ));
                    }
                }
                if needs_handle_list {
                    if inherited_handles.is_empty() {
                        return Err(KernelFailure::new(
                            WindowsSpawnStage::BuildHandleListAttribute,
                            "HANDLE_LIST requires at least one inherited handle",
                        ));
                    }
                    // SAFETY: the value slice remains live for the synchronous
                    // update and every listed handle was prepared inheritable.
                    if unsafe {
                        UpdateProcThreadAttribute(
                            pointer,
                            0,
                            PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
                            handle_values.as_mut_ptr().cast(),
                            handle_values.len() * size_of::<Handle>(),
                            null_mut(),
                            null_mut(),
                        )
                    } == FALSE
                    {
                        return Err(last_failure(
                            WindowsSpawnStage::BuildHandleListAttribute,
                            "UpdateProcThreadAttribute(HANDLE_LIST)",
                        ));
                    }
                }
                if let Some(pseudoconsole) = pseudoconsole {
                    // PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE takes the HPCON
                    // value itself as lpValue, not a pointer to a handle slot.
                    if unsafe {
                        UpdateProcThreadAttribute(
                            pointer,
                            0,
                            PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE,
                            to_ptr(pseudoconsole.0),
                            size_of::<Handle>(),
                            null_mut(),
                            null_mut(),
                        )
                    } == FALSE
                    {
                        return Err(last_failure(
                            WindowsSpawnStage::BuildPseudoConsoleAttribute,
                            "UpdateProcThreadAttribute(PSEUDOCONSOLE)",
                        ));
                    }
                }
                Ok(())
            })();
            if let Err(error) = result {
                // SAFETY: the list was initialized above and is not published.
                unsafe { DeleteProcThreadAttributeList(pointer) };
                return Err(error);
            }
            let handle = AttributeListHandle(pointer as usize);
            let previous = self
                .attribute_allocations
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(
                    handle,
                    AttributeAllocation {
                        list_storage,
                        job_values,
                        handle_values,
                    },
                );
            assert!(
                previous.is_none(),
                "attribute allocation address reused while live"
            );
            Ok(handle)
        }

        fn delete_attribute_list(&self, attributes: AttributeListHandle) {
            let storage = self
                .attribute_allocations
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&attributes)
                .expect("owned process attribute list missing");
            // SAFETY: the key is the pointer of this exact still-live Box.
            unsafe { DeleteProcThreadAttributeList(to_ptr(attributes.0)) };
            drop(storage);
        }

        fn create_process(
            &self,
            image: &dyn WindowsSpawnImage,
            flags: u32,
            stdio: &[Option<KernelHandle>; 3],
            attributes: Option<AttributeListHandle>,
            inherit_handles: bool,
            uses_pseudoconsole: bool,
        ) -> Result<CreatedProcess, KernelFailure> {
            let mut command_line = image.command_line().to_vec();
            let mut startup: StartupInfoExW = unsafe { zeroed() };
            let has_extended_attributes = attributes.is_some();
            startup.startup_info.cb = startup_info_size(
                has_extended_attributes,
                size_of::<StartupInfoW>(),
                size_of::<StartupInfoExW>(),
            ) as Dword;
            startup.attribute_list = attributes.map_or(null_mut(), |value| to_ptr(value.0));
            let any_stdio = stdio.iter().any(Option::is_some);
            if uses_pseudoconsole {
                startup.startup_info.flags |= STARTF_USESTDHANDLES;
                startup.startup_info.stdin = INVALID_HANDLE_VALUE;
                startup.startup_info.stdout = INVALID_HANDLE_VALUE;
                startup.startup_info.stderr = INVALID_HANDLE_VALUE;
            } else if any_stdio {
                startup.startup_info.flags |= STARTF_USESTDHANDLES;
                startup.startup_info.stdin = option_handle(stdio[0]);
                startup.startup_info.stdout = option_handle(stdio[1]);
                startup.startup_info.stderr = option_handle(stdio[2]);
            }
            let environment = match image.environment() {
                WindowsEnvironmentImage::NativeParentInherit => null_mut(),
                WindowsEnvironmentImage::Explicit(block) => block.as_ptr().cast_mut().cast(),
            };
            let cwd = image.cwd().map_or(null(), |cwd| cwd.argument().as_ptr());
            let mut process_information: ProcessInformation = unsafe { zeroed() };
            let mut creation_flags = flags | CREATE_SUSPENDED | CREATE_UNICODE_ENVIRONMENT;
            if has_extended_attributes {
                creation_flags |= EXTENDED_STARTUPINFO_PRESENT;
            }
            // SAFETY: every pointer refers to transaction-owned, NUL-terminated
            // storage for the entire synchronous CreateProcessW call.
            let created = unsafe {
                CreateProcessW(
                    image.application().as_ptr(),
                    command_line.as_mut_ptr(),
                    null_mut(),
                    null_mut(),
                    if inherit_handles { TRUE } else { FALSE },
                    creation_flags,
                    environment,
                    cwd,
                    &mut startup.startup_info as *mut StartupInfoW,
                    &mut process_information,
                )
            };
            if created == FALSE {
                return Err(last_failure(
                    WindowsSpawnStage::CreateProcessW,
                    "CreateProcessW",
                ));
            }
            Ok(CreatedProcess {
                pid: process_information.process_id,
                process: from_handle(process_information.process),
                primary_thread: from_handle(process_information.thread),
            })
        }

        fn assign_global_job(&self, process: KernelHandle) -> Result<(), KernelFailure> {
            let job = self.ensure_global_job()?;
            // SAFETY: both handles are live and the process is still suspended.
            if unsafe { AssignProcessToJobObject(to_handle(job), to_handle(process)) } == FALSE {
                let error = unsafe { GetLastError() };
                if error != ERROR_ACCESS_DENIED {
                    return Err(KernelFailure::new(
                        WindowsSpawnStage::AssignLibuvGlobalJob,
                        format!("AssignProcessToJobObject failed with win32:{error}"),
                    ));
                }
            }
            Ok(())
        }

        fn wrap_parent_stdio(&self, handles: &[KernelHandle]) -> Result<(), KernelFailure> {
            for handle in handles {
                let mut flags = 0;
                // SAFETY: each handle is a live parent endpoint. This checked
                // ownership probe is the last fallible boundary before the
                // return bundle takes those exact handles.
                if unsafe { GetHandleInformation(to_handle(*handle), &mut flags) } == FALSE {
                    return Err(last_failure(
                        WindowsSpawnStage::WrapParentStdio,
                        "GetHandleInformation(parent stdio)",
                    ));
                }
                if flags & HANDLE_FLAG_INHERIT != 0 {
                    return Err(KernelFailure::new(
                        WindowsSpawnStage::WrapParentStdio,
                        "parent stdio endpoint remained inheritable",
                    ));
                }
            }
            Ok(())
        }

        fn resume_thread(
            &self,
            thread: KernelHandle,
            _cell: &Arc<WindowsWaitCell>,
        ) -> Result<(), KernelFailure> {
            // SAFETY: this transaction owns the still-suspended primary thread.
            if unsafe { ResumeThread(to_handle(thread)) } == Dword::MAX {
                Err(last_failure(
                    WindowsSpawnStage::ResumeThread,
                    "ResumeThread",
                ))
            } else {
                Ok(())
            }
        }

        fn wait_for_single_object(
            &self,
            process: KernelHandle,
            timeout: Duration,
        ) -> WindowsWaitObservation {
            let milliseconds = u32::try_from(timeout.as_millis()).unwrap_or(INFINITE - 1);
            // SAFETY: the registration owns this process handle.
            match unsafe { WaitForSingleObject(to_handle(process), milliseconds) } {
                WAIT_OBJECT_0 => WindowsWaitObservation::Signaled,
                WAIT_TIMEOUT => WindowsWaitObservation::Timeout,
                WAIT_FAILED => WindowsWaitObservation::Failed(unsafe { GetLastError() }),
                unexpected => WindowsWaitObservation::Failed(unexpected),
            }
        }

        fn get_exit_code_process(&self, process: KernelHandle) -> Result<Option<u32>, u32> {
            let mut exit_code = 0;
            // SAFETY: the registration owns this process handle.
            if unsafe { GetExitCodeProcess(to_handle(process), &mut exit_code) } == FALSE {
                Err(unsafe { GetLastError() })
            } else if exit_code == STILL_ACTIVE {
                Ok(None)
            } else {
                Ok(Some(exit_code))
            }
        }

        fn terminate_process(
            &self,
            process: KernelHandle,
            exit_code: u32,
        ) -> WindowsTerminateResult {
            // SAFETY: the hidden transaction owns this descendant-free target.
            if unsafe { TerminateProcess(to_handle(process), exit_code) } != FALSE {
                return WindowsTerminateResult::Delivered;
            }
            let error = unsafe { GetLastError() };
            if error == ERROR_ACCESS_DENIED
                && matches!(self.get_exit_code_process(process), Ok(Some(_)))
            {
                WindowsTerminateResult::AlreadyTerminal
            } else {
                WindowsTerminateResult::Failed(error)
            }
        }

        fn terminate_job(&self, job: KernelHandle, exit_code: u32) -> WindowsTerminateResult {
            // SAFETY: the registration lifecycle retains the private Job
            // handle for the complete synchronous termination call.
            if unsafe { TerminateJobObject(to_handle(job), exit_code) } != FALSE {
                WindowsTerminateResult::Delivered
            } else {
                WindowsTerminateResult::Failed(unsafe { GetLastError() })
            }
        }

        fn query_job_active_processes(&self, job: KernelHandle) -> Result<u32, u32> {
            let mut accounting: JobObjectBasicAccountingInformation = unsafe { zeroed() };
            // SAFETY: accounting is the documented buffer for
            // JobObjectBasicAccountingInformation and the Job handle is live
            // under the registration lifecycle mutex.
            if unsafe {
                QueryInformationJobObject(
                    to_handle(job),
                    JOB_OBJECT_BASIC_ACCOUNTING_INFORMATION,
                    (&mut accounting as *mut JobObjectBasicAccountingInformation).cast(),
                    size_of::<JobObjectBasicAccountingInformation>() as Dword,
                    null_mut(),
                )
            } == FALSE
            {
                Err(unsafe { GetLastError() })
            } else {
                Ok(accounting.active_processes)
            }
        }

        fn close_handle(&self, handle: KernelHandle, _kind: HandleKind) -> bool {
            // SAFETY: OwnedKernelHandle invokes this once for a valid handle.
            unsafe { CloseHandle(to_handle(handle)) != FALSE }
        }

        fn bind_identity(
            &self,
            _process: KernelHandle,
            _thread: KernelHandle,
            _wait: ThreadpoolWaitHandle,
            _identity: WindowsRegistrationIdentity,
        ) {
        }

        fn record_callback_phase(&self, _phase: &'static str) {}
    }

    unsafe extern "system" fn threadpool_wait_callback(
        _instance: *mut c_void,
        context: *mut c_void,
        _wait: *mut c_void,
        wait_result: Dword,
    ) {
        if wait_result != WAIT_OBJECT_0 || context.is_null() {
            std::process::abort();
        }
        // SAFETY: the transaction retains this stable Arc allocation until it
        // disarms and quiesces the wait.
        let cell = unsafe { &*context.cast::<WindowsWaitCell>() };
        if catch_unwind(AssertUnwindSafe(|| cell.dispatch())).is_err() {
            std::process::abort();
        }
    }

    fn completion_worker_main(
        port: Arc<CompletionPortRuntime>,
        shared: Weak<WindowsSpawnShared>,
        failed: Arc<AtomicBool>,
    ) {
        loop {
            let mut message = 0;
            let mut completion_key = 0;
            let mut overlapped = null_mut();
            // SAFETY: port is the process-owned completion port retained by
            // ProductionWindowsKernel until the shutdown sentinel is joined.
            let received = unsafe {
                GetQueuedCompletionStatus(
                    to_handle(port.port),
                    &mut message,
                    &mut completion_key,
                    &mut overlapped,
                    INFINITE,
                )
            };
            if received == FALSE {
                failed.store(true, Ordering::Release);
                if let Some(shared) = shared.upgrade() {
                    shared.record_worker_invariant(
                        "completion",
                        "GetQueuedCompletionStatus failed for the Windows Job completion worker",
                    );
                }
                return;
            }
            if completion_key == COMPLETION_WORKER_SHUTDOWN_KEY {
                return;
            }
            if message != JOB_OBJECT_MSG_ACTIVE_PROCESS_ZERO {
                continue;
            }
            let Ok(completion_id) = u64::try_from(completion_key) else {
                continue;
            };
            let Some(shared) = shared.upgrade() else {
                return;
            };
            // A missing never-reused key is a stale packet. Lookup clones the
            // cell through the map; the raw completion packet never carries a
            // Rust pointer and therefore cannot wake a later generation.
            shared.observe_job_active_zero(completion_id);
        }
    }

    fn prepare_pipe(
        index: usize,
    ) -> Result<(Option<KernelHandle>, Option<KernelHandle>), KernelFailure> {
        let mut attributes = SecurityAttributes {
            length: size_of::<SecurityAttributes>() as Dword,
            security_descriptor: null_mut(),
            inherit_handle: TRUE,
        };
        let mut reader = null_mut();
        let mut writer = null_mut();
        // SAFETY: pointers are valid outputs and attributes requests
        // inheritable child endpoints.
        if unsafe { CreatePipe(&mut reader, &mut writer, &mut attributes, 0) } == FALSE {
            return Err(last_failure(WindowsSpawnStage::PrepareStdio, "CreatePipe"));
        }
        let (child, parent) = if index == 0 {
            (reader, writer)
        } else {
            (writer, reader)
        };
        // SAFETY: parent is one of the two freshly created pipe handles.
        if unsafe { SetHandleInformation(parent, HANDLE_FLAG_INHERIT, 0) } == FALSE {
            close_handle_value(reader);
            close_handle_value(writer);
            return Err(last_failure(
                WindowsSpawnStage::PrepareStdio,
                "SetHandleInformation(parent pipe)",
            ));
        }
        Ok((Some(from_handle(child)), Some(from_handle(parent))))
    }

    fn conpty_coord(size: WindowsConPtySize) -> Result<Coord, String> {
        if size.rows == 0 || size.columns == 0 {
            return Err("ConPTY rows and columns must be nonzero".to_string());
        }
        Ok(Coord {
            x: i16::try_from(size.columns)
                .map_err(|_| "ConPTY columns exceed Windows COORD".to_string())?,
            y: i16::try_from(size.rows)
                .map_err(|_| "ConPTY rows exceed Windows COORD".to_string())?,
        })
    }

    fn duplicate_inherited_stdio(index: usize) -> Result<Option<KernelHandle>, KernelFailure> {
        let selector = [STD_INPUT_HANDLE, STD_OUTPUT_HANDLE, STD_ERROR_HANDLE][index];
        // SAFETY: selector is one of the three standard-handle constants.
        let source = unsafe { GetStdHandle(selector) };
        if is_invalid(source) {
            return Ok(None);
        }
        duplicate_handle(from_handle(source), true, WindowsSpawnStage::PrepareStdio).map(Some)
    }

    fn open_null(index: usize) -> Result<KernelHandle, KernelFailure> {
        const NUL: [u16; 4] = [b'N' as u16, b'U' as u16, b'L' as u16, 0];
        let mut attributes = SecurityAttributes {
            length: size_of::<SecurityAttributes>() as Dword,
            security_descriptor: null_mut(),
            inherit_handle: TRUE,
        };
        let access = if index == 0 {
            GENERIC_READ
        } else {
            GENERIC_WRITE
        };
        // SAFETY: NUL is a static NUL-terminated device name and the security
        // attributes request an inheritable child handle.
        let handle = unsafe {
            CreateFileW(
                NUL.as_ptr(),
                access,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                &mut attributes,
                OPEN_EXISTING,
                0,
                null_mut(),
            )
        };
        if is_invalid(handle) {
            Err(last_failure(
                WindowsSpawnStage::PrepareStdio,
                "CreateFileW(NUL)",
            ))
        } else {
            Ok(from_handle(handle))
        }
    }

    fn duplicate_handle(
        source: KernelHandle,
        inheritable: bool,
        stage: WindowsSpawnStage,
    ) -> Result<KernelHandle, KernelFailure> {
        let mut duplicate = null_mut();
        // SAFETY: pseudo process handles are valid in both source and target
        // positions and source is caller-attested/live.
        let process = unsafe { GetCurrentProcess() };
        if unsafe {
            DuplicateHandle(
                process,
                to_handle(source),
                process,
                &mut duplicate,
                0,
                if inheritable { TRUE } else { FALSE },
                DUPLICATE_SAME_ACCESS,
            )
        } == FALSE
        {
            Err(last_failure(stage, "DuplicateHandle"))
        } else {
            Ok(from_handle(duplicate))
        }
    }

    fn create_job(stage: WindowsSpawnStage) -> Result<KernelHandle, KernelFailure> {
        // SAFETY: null attributes/name requests an unnamed non-inheritable Job.
        let job = unsafe { CreateJobObjectW(null_mut(), null()) };
        if is_invalid(job) {
            Err(last_failure(stage, "CreateJobObjectW"))
        } else {
            Ok(from_handle(job))
        }
    }

    fn configure_job_limits(
        job: KernelHandle,
        limit_flags: Dword,
        stage: WindowsSpawnStage,
    ) -> Result<(), KernelFailure> {
        let mut limits: JobObjectExtendedLimitInformation = unsafe { zeroed() };
        limits.basic_limit_information.limit_flags = limit_flags;
        // SAFETY: limits is the exact documented structure for information
        // class JobObjectExtendedLimitInformation.
        if unsafe {
            SetInformationJobObject(
                to_handle(job),
                JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
                (&mut limits as *mut JobObjectExtendedLimitInformation).cast(),
                size_of::<JobObjectExtendedLimitInformation>() as Dword,
            )
        } == FALSE
        {
            Err(last_failure(
                stage,
                "SetInformationJobObject(ExtendedLimitInformation)",
            ))
        } else {
            Ok(())
        }
    }

    fn close_raw_stdio(
        child: &mut [Option<KernelHandle>; 3],
        parent: &mut [Option<KernelHandle>; 3],
    ) {
        for handle in child.iter_mut().chain(parent.iter_mut()) {
            if let Some(handle) = handle.take() {
                close_raw(handle);
            }
        }
    }

    fn option_handle(handle: Option<KernelHandle>) -> Handle {
        handle.map_or(null_mut(), to_handle)
    }

    fn from_handle(handle: Handle) -> KernelHandle {
        KernelHandle(handle as usize)
    }

    fn to_handle(handle: KernelHandle) -> Handle {
        handle.0 as Handle
    }

    fn to_ptr(value: usize) -> *mut c_void {
        value as *mut c_void
    }

    fn is_invalid(handle: Handle) -> bool {
        handle.is_null() || handle == INVALID_HANDLE_VALUE
    }

    fn close_handle_value(handle: Handle) {
        if !is_invalid(handle) {
            // SAFETY: caller passes a live, unowned local handle.
            let closed = unsafe { CloseHandle(handle) };
            assert!(closed != FALSE, "CloseHandle failed for local Win32 handle");
        }
    }

    fn close_raw(handle: KernelHandle) {
        close_handle_value(to_handle(handle));
    }

    fn last_failure(stage: WindowsSpawnStage, operation: &str) -> KernelFailure {
        // SAFETY: GetLastError has no preconditions and is read immediately
        // after the failing Win32 call.
        let error = unsafe { GetLastError() };
        KernelFailure::new(stage, format!("{operation} failed with win32:{error}"))
    }
}

#[cfg(all(test, windows))]
mod inherited_control_pipe_tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn inherited_control_pipe_seats_large_frame_before_reader_exists() {
        // The timeout is the regression: the old default buffer never returned
        // from create, so its future could not even reach a process-spawn timeout.
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let payload = vec![0x5a; PRIVATE_CONTROL_MAX_BYTES];
        let expected = payload.clone();
        let writer = std::thread::spawn(move || {
            tx.send(WindowsInheritedControlPipe::create(&payload))
                .unwrap();
        });
        let pipe = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the entire initial frame must fit before a reader exists")
            .expect("create inherited control pipe");
        writer.join().unwrap();
        let WindowsInheritedControlPipe {
            child_read,
            parent_write,
        } = pipe;
        let mut reader = std::fs::File::from(child_read);
        let mut received = vec![0; expected.len()];
        reader.read_exact(&mut received).unwrap();
        assert_eq!(received, expected);
        drop(parent_write);
        assert_eq!(reader.read(&mut [0]).unwrap(), 0);
    }
}
