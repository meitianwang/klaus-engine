//! Defect: ConPTY startup passed an HPCON address, and Pty::open leaked its pipe copies.
//! Root cause: the launch attribute needs the HPCON value; CreatePseudoConsole does
//! not consume the caller's original pipe handles.
//! Regression for the 2026-09-09 Windows sandbox launch/PTY work item.
//!
//! The public launch, Pty and supervision APIs are exercised. The exact returned
//! job is checked, but this is not a deterministic reproduction of the old
//! CreateProcessW-to-AssignProcessToJobObject race. Each case runs in an owned
//! worker: its cleanup job stays alive until observations have been collected.
//!
//! The pipe regression requires real output EOF after every public Pty::drop;
//! explicitly stopping the reader is cleanup, never evidence of EOF. It covers
//! the caller-owned output writer leak, not the separately recorded Windows
//! ConPTY process-handle issue (microsoft/terminal#17903). Process-wide handle
//! counts, OS-version skips, and comparisons with another implementation are
//! deliberately absent. Input-pipe disconnection is not asserted here.
//!
//! The descendant regression covers the next teardown defect: Target::wait
//! reported root exit before its job was empty. A retained child-process handle
//! must already be signaled when wait returns, while Target and Pty remain alive.
//!
//! The partial-frame supervision case is positive coverage of the new public
//! supervise API. Baseline 97d815fb did not export it; missing-API compilation is
//! not product red evidence. The unchanged public wait case supplies that evidence.
//!
//! Defect: stopping a blocked host write also retired the sole ConPTY reader,
//! leaving ClosePseudoConsole pending after the target had stopped. Keep reading
//! and discarding console output during shutdown. Work item: 2026-09-09 supervision.
//!
//! The output-backpressure case keeps a worker-owned stdout pipe open and unread.
//! It observes a full, stable buffer containing target output before sending a
//! terminate frame, then checks actual supervision drainage and repeated shutdown.

#![cfg(windows)]

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc,
};
use std::thread;
use std::time::{Duration, Instant};

use klaus_sandbox_win::identity;
use klaus_sandbox_win::process::{self, LaunchIdentity, LaunchSpec};
use klaus_sandbox_win::protocol::{self, ControlMessage, TerminalSize};
use klaus_sandbox_win::pty::Pty;
use serde::{Deserialize, Serialize};
use windows_sys::Win32::Foundation::{
    CloseHandle, DuplicateHandle, GetLastError, DUPLICATE_SAME_ACCESS, HANDLE,
    INVALID_HANDLE_VALUE, WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows_sys::Win32::Security::{FreeSid, SECURITY_ATTRIBUTES};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, IsProcessInJob,
    JobObjectBasicAccountingInformation, JobObjectBasicProcessIdList,
    JobObjectExtendedLimitInformation, QueryInformationJobObject, SetInformationJobObject,
    TerminateJobObject, JOBOBJECT_BASIC_ACCOUNTING_INFORMATION,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
};
use windows_sys::Win32::System::Pipes::{
    CreatePipe, GetNamedPipeInfo, PeekNamedPipe, PIPE_SERVER_END,
};
use windows_sys::Win32::System::SystemInformation::GetSystemDirectoryW;
use windows_sys::Win32::System::Threading::{
    CreateProcessW, DeleteProcThreadAttributeList, GetCurrentProcess, GetExitCodeProcess,
    GetProcessId, InitializeProcThreadAttributeList, OpenProcess, ResumeThread, TerminateProcess,
    UpdateProcThreadAttribute, WaitForSingleObject, CREATE_NO_WINDOW, CREATE_SUSPENDED,
    CREATE_UNICODE_ENVIRONMENT, EXTENDED_STARTUPINFO_PRESENT, LPPROC_THREAD_ATTRIBUTE_LIST,
    PROCESS_INFORMATION, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE,
    PROC_THREAD_ATTRIBUTE_HANDLE_LIST, STARTF_USESTDHANDLES, STARTUPINFOEXW,
};

// These API families are deliberately local test bindings: the core's
// windows-sys feature set does not otherwise need profile management or console UI.
#[link(name = "userenv")]
unsafe extern "system" {
    #[link_name = "CreateAppContainerProfile"]
    fn create_app_container_profile(
        name: *const u16,
        display: *const u16,
        description: *const u16,
        capabilities: *const std::ffi::c_void,
        count: u32,
        sid: *mut *mut std::ffi::c_void,
    ) -> i32;
    #[link_name = "DeleteAppContainerProfile"]
    fn delete_app_container_profile(name: *const u16) -> i32;
}
#[link(name = "kernel32")]
unsafe extern "system" {
    #[link_name = "SetErrorMode"]
    pub(super) fn set_error_mode(mode: u32) -> u32;
    #[link_name = "GetStdHandle"]
    fn get_std_handle(kind: u32) -> HANDLE;
    #[link_name = "SetStdHandle"]
    fn set_std_handle(kind: u32, handle: HANDLE) -> i32;
}

type ProbeResult<T> = Result<T, String>;
pub(super) const ROLE: &str = "KLAUS_SANDBOX_LAUNCH_TEST_WORKER";
pub(super) const PROFILE: &str = "KLAUS_SANDBOX_LAUNCH_TEST_PROFILE";
pub(super) const REPORT: &str = "KLAUS_SANDBOX_LAUNCH_TEST_REPORT";
const WORKER_MS: u32 = 20_000;
const DRAIN_MS: u32 = 3_000;
const BACKPRESSURE_CASE: &str = "output-backpressure";
const BACKPRESSURE_MARKER: &[u8] = b"KLAUS_OUTPUT_BACKPRESSURE";

fn wide(value: &str) -> Vec<u16> {
    value.encode_utf16().chain(Some(0)).collect()
}
fn checked(ok: i32, stage: &str) -> ProbeResult<()> {
    if ok == 0 {
        Err(format!("{stage}: Win32 error {}", unsafe {
            GetLastError()
        }))
    } else {
        Ok(())
    }
}
fn stage(value: &str) {
    eprintln!("launch-regression pid={} stage={value}", std::process::id());
}

struct Handle(HANDLE);
impl Drop for Handle {
    fn drop(&mut self) {
        if !self.0.is_null() && self.0 != INVALID_HANDLE_VALUE {
            unsafe { CloseHandle(self.0) };
        }
    }
}
fn duplicate(handle: HANDLE, inherit: bool) -> ProbeResult<Handle> {
    let mut copy = ptr::null_mut();
    checked(
        unsafe {
            DuplicateHandle(
                GetCurrentProcess(),
                handle,
                GetCurrentProcess(),
                &mut copy,
                0,
                i32::from(inherit),
                DUPLICATE_SAME_ACCESS,
            )
        },
        "DuplicateHandle",
    )?;
    Ok(Handle(copy))
}
fn duplicate_file(handle: HANDLE) -> ProbeResult<File> {
    let copy = duplicate(handle, false)?;
    let file = unsafe { File::from_raw_handle(copy.0) };
    std::mem::forget(copy);
    Ok(file)
}

pub(super) struct ProfileOwner(pub(super) Option<String>);
impl ProfileOwner {
    pub(super) fn create() -> ProbeResult<Self> {
        let name = format!("klaus-launch-test-{}", uuid::Uuid::new_v4());
        let text = wide(&name);
        let mut sid = ptr::null_mut();
        let code = unsafe {
            create_app_container_profile(
                text.as_ptr(),
                text.as_ptr(),
                text.as_ptr(),
                ptr::null(),
                0,
                &mut sid,
            )
        };
        if !sid.is_null() {
            unsafe { FreeSid(sid) };
        }
        // Only S_OK establishes ownership. Never remove a pre-existing profile.
        if code != 0 {
            return Err(format!("create owned profile: HRESULT {code:#x}"));
        }
        Ok(Self(Some(name)))
    }
    pub(super) fn close(&mut self) -> ProbeResult<()> {
        if let Some(name) = &self.0 {
            let code = unsafe { delete_app_container_profile(wide(name).as_ptr()) };
            if code < 0 {
                return Err(format!("delete owned profile: HRESULT {code:#x}"));
            }
            self.0 = None;
        }
        Ok(())
    }
}
impl Drop for ProfileOwner {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

struct Attributes(LPPROC_THREAD_ATTRIBUTE_LIST);
impl Drop for Attributes {
    fn drop(&mut self) {
        unsafe { DeleteProcThreadAttributeList(self.0) };
    }
}

pub(super) struct Worker {
    process: Handle,
    job: Handle,
    cleaned: bool,
}
impl Worker {
    fn spawn(case: &str, test: &str, root: &Path, profile: &str) -> ProbeResult<Self> {
        Self::spawn_inner(case, test, root, profile, false)
    }
    pub(super) fn spawn_ignored(
        case: &str,
        test: &str,
        root: &Path,
        profile: &str,
    ) -> ProbeResult<Self> {
        Self::spawn_inner(case, test, root, profile, true)
    }
    fn spawn_inner(
        case: &str,
        test: &str,
        root: &Path,
        profile: &str,
        ignored: bool,
    ) -> ProbeResult<Self> {
        let job = Handle(unsafe { CreateJobObjectW(ptr::null(), ptr::null()) });
        if job.0.is_null() {
            return Err(format!("CreateJobObjectW: Win32 error {}", unsafe {
                GetLastError()
            }));
        }
        let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        checked(
            unsafe {
                SetInformationJobObject(
                    job.0,
                    JobObjectExtendedLimitInformation,
                    (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                    std::mem::size_of_val(&limits) as u32,
                )
            },
            "configure cleanup job",
        )?;
        let log = File::create(root.join("worker.log")).map_err(|e| e.to_string())?;
        let output = duplicate(log.as_raw_handle(), true)?;
        let security = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: ptr::null_mut(),
            bInheritHandle: 1,
        };
        let (mut read, mut write) = (ptr::null_mut(), ptr::null_mut());
        checked(
            unsafe { CreatePipe(&mut read, &mut write, &security, 0) },
            "worker stdin pipe",
        )?;
        let input = Handle(read);
        let input_writer = Handle(write);
        // Only these copies are inherited. The cleanup job is not inheritable.
        let inherited = [input.0, output.0];
        let mut bytes = 0;
        unsafe { InitializeProcThreadAttributeList(ptr::null_mut(), 1, 0, &mut bytes) };
        let mut storage = vec![0usize; bytes.div_ceil(std::mem::size_of::<usize>())];
        let raw = storage.as_mut_ptr().cast();
        checked(
            unsafe { InitializeProcThreadAttributeList(raw, 1, 0, &mut bytes) },
            "worker attributes",
        )?;
        let attrs = Attributes(raw);
        checked(
            unsafe {
                UpdateProcThreadAttribute(
                    attrs.0,
                    0,
                    PROC_THREAD_ATTRIBUTE_HANDLE_LIST as usize,
                    inherited.as_ptr().cast(),
                    std::mem::size_of_val(&inherited),
                    ptr::null_mut(),
                    ptr::null(),
                )
            },
            "worker handle whitelist",
        )?;
        let exe = std::env::current_exe().map_err(|e| e.to_string())?;
        let mut arguments = vec![
            exe.to_string_lossy().into_owned(),
            "--exact".into(),
            test.into(),
            "--nocapture".into(),
            "--test-threads=1".into(),
        ];
        if ignored {
            arguments.push("--ignored".into());
        }
        let mut command = wide(&process::command_line(&arguments));
        let mut environment: BTreeMap<String, String> = std::env::vars().collect();
        environment.insert(ROLE.into(), case.into());
        environment.insert(PROFILE.into(), profile.into());
        environment.insert(
            REPORT.into(),
            root.join("report.json").to_string_lossy().into_owned(),
        );
        let environment = process::environment_block(&environment);
        let mut startup: STARTUPINFOEXW = unsafe { std::mem::zeroed() };
        startup.StartupInfo.cb = std::mem::size_of::<STARTUPINFOEXW>() as u32;
        startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
        startup.StartupInfo.hStdInput = input.0;
        startup.StartupInfo.hStdOutput = output.0;
        startup.StartupInfo.hStdError = output.0;
        startup.lpAttributeList = attrs.0;
        let mut info: PROCESS_INFORMATION = unsafe { std::mem::zeroed() };
        checked(
            unsafe {
                CreateProcessW(
                    wide(&exe.to_string_lossy()).as_ptr(),
                    command.as_mut_ptr(),
                    ptr::null(),
                    ptr::null(),
                    1,
                    CREATE_NO_WINDOW
                        | CREATE_SUSPENDED
                        | CREATE_UNICODE_ENVIRONMENT
                        | EXTENDED_STARTUPINFO_PRESENT,
                    environment.as_ptr().cast(),
                    ptr::null(),
                    &startup.StartupInfo,
                    &mut info,
                )
            },
            "create suspended worker",
        )?;
        let worker = Self {
            process: Handle(info.hProcess),
            job,
            cleaned: false,
        };
        let thread = Handle(info.hThread);
        if let Err(error) = checked(
            unsafe { AssignProcessToJobObject(worker.job.0, worker.process.0) },
            "assign suspended worker to cleanup job",
        ) {
            let terminate_requested = unsafe { TerminateProcess(worker.process.0, 1) };
            if case == BACKPRESSURE_CASE {
                // This API cannot return a partially started worker. Keep its
                // process/job owners until controller exit; no drain was proved.
                std::mem::forget(worker);
                return Err(format!(
                    "{error}; owned_worker_pid={}; terminate_requested={terminate_requested}; owners_retained_until_controller_exit=true; drain_confirmed=false",
                    info.dwProcessId
                ));
            }
            return Err(error);
        }
        if unsafe { ResumeThread(thread.0) } == u32::MAX {
            let error = format!("resume worker: Win32 error {}", unsafe { GetLastError() });
            if case == BACKPRESSURE_CASE {
                std::mem::forget(worker);
                return Err(format!(
                    "{error}; owned_worker_pid={}; owners_retained_until_controller_exit=true; drain_confirmed=false",
                    info.dwProcessId
                ));
            }
            return Err(error);
        }
        drop(input_writer);
        Ok(worker)
    }
    pub(super) fn wait(&self) -> ProbeResult<u32> {
        wait(self.process.0, WORKER_MS, "worker")?;
        let mut exit = 0;
        checked(
            unsafe { GetExitCodeProcess(self.process.0, &mut exit) },
            "worker exit code",
        )?;
        Ok(exit)
    }
    pub(super) fn cleanup(&mut self) -> ProbeResult<()> {
        if self.cleaned {
            return Ok(());
        }
        let deadline = Instant::now() + Duration::from_millis(u64::from(DRAIN_MS));
        checked(
            unsafe { TerminateJobObject(self.job.0, 1) },
            "terminate cleanup job",
        )?;
        wait(self.process.0, DRAIN_MS, "worker cleanup")?;
        loop {
            let mut accounting: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION =
                unsafe { std::mem::zeroed() };
            checked(
                unsafe {
                    QueryInformationJobObject(
                        self.job.0,
                        JobObjectBasicAccountingInformation,
                        (&mut accounting as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
                        std::mem::size_of_val(&accounting) as u32,
                        ptr::null_mut(),
                    )
                },
                "cleanup job accounting",
            )?;
            if accounting.ActiveProcesses == 0 {
                self.cleaned = true;
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "cleanup job still has {} processes",
                    accounting.ActiveProcesses
                ));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}
fn wait(handle: HANDLE, milliseconds: u32, label: &str) -> ProbeResult<()> {
    match unsafe { WaitForSingleObject(handle, milliseconds) } {
        WAIT_OBJECT_0 => Ok(()),
        WAIT_TIMEOUT => Err(format!("{label}: timed out after {milliseconds} ms")),
        _ => Err(format!(
            "{label}: wait failed with Win32 error {}",
            unsafe { GetLastError() }
        )),
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum DrainEnd {
    Eof,
    BrokenPipe,
    Stopped,
}

#[derive(Debug, Serialize, Deserialize)]
struct DrainOutcome {
    bytes: usize,
    end: DrainEnd,
}

struct Drain {
    stop: Arc<AtomicBool>,
    result: mpsc::Receiver<ProbeResult<DrainOutcome>>,
    thread: thread::JoinHandle<()>,
}
impl Drain {
    fn start(pty: &Pty) -> ProbeResult<Self> {
        let mut pipe = duplicate_file(pty.output_handle() as HANDLE)?;
        let stop = Arc::new(AtomicBool::new(false));
        let reader_stop = Arc::clone(&stop);
        let (send, result) = mpsc::channel();
        let thread = thread::spawn(move || {
            let result = (|| {
                let mut total = 0;
                loop {
                    let mut available = 0;
                    if unsafe {
                        PeekNamedPipe(
                            pipe.as_raw_handle(),
                            ptr::null_mut(),
                            0,
                            ptr::null_mut(),
                            &mut available,
                            ptr::null_mut(),
                        )
                    } == 0
                    {
                        let error = unsafe { GetLastError() };
                        if error == 109 {
                            return Ok(DrainOutcome {
                                bytes: total,
                                end: DrainEnd::BrokenPipe,
                            });
                        }
                        return Err(format!("PTY PeekNamedPipe: Win32 error {error}"));
                    }
                    if available > 0 {
                        let mut buffer = [0u8; 4096];
                        let count = usize::min(available as usize, buffer.len());
                        let read = match pipe.read(&mut buffer[..count]) {
                            Ok(read) => read,
                            Err(error) if error.raw_os_error() == Some(109) => {
                                return Ok(DrainOutcome {
                                    bytes: total,
                                    end: DrainEnd::BrokenPipe,
                                });
                            }
                            Err(error) => return Err(error.to_string()),
                        };
                        total += read;
                        if read == 0 {
                            return Ok(DrainOutcome {
                                bytes: total,
                                end: DrainEnd::Eof,
                            });
                        }
                        if total > 65_536 {
                            return Err("PTY emitted more than 64 KiB".into());
                        }
                    } else if reader_stop.load(Ordering::Acquire) {
                        return Ok(DrainOutcome {
                            bytes: total,
                            end: DrainEnd::Stopped,
                        });
                    } else {
                        thread::sleep(Duration::from_millis(5));
                    }
                }
            })();
            // Release our duplicate before publishing completion. The caller
            // still joins the reader before recording a completed round.
            drop(pipe);
            let _ = send.send(result);
        });
        Ok(Self {
            stop,
            result,
            thread,
        })
    }
    fn finish(self) -> ProbeResult<DrainOutcome> {
        self.complete(false)
    }
    fn finish_eof(self) -> ProbeResult<DrainOutcome> {
        self.complete(true)
    }
    fn complete(self, require_eof: bool) -> ProbeResult<DrainOutcome> {
        if !require_eof {
            self.stop.store(true, Ordering::Release);
        }
        // For EOF verification the stop flag remains false for the entire
        // deadline. A subsequent stop only releases our reader on the red path.
        let mut received = self.result.recv_timeout(Duration::from_secs(2));
        let eof_timeout = require_eof && matches!(received, Err(mpsc::RecvTimeoutError::Timeout));
        self.stop.store(true, Ordering::Release);
        if eof_timeout {
            received = self.result.recv_timeout(Duration::from_secs(2));
        }
        // Attempt bounded joining even when reading failed or EOF timed out.
        // The controller's job remains the final cleanup bound on every path.
        let joined = (|| {
            let deadline = Instant::now() + Duration::from_secs(2);
            while !self.thread.is_finished() {
                if Instant::now() >= deadline {
                    return Err("PTY reader did not finish after stop".to_string());
                }
                thread::sleep(Duration::from_millis(5));
            }
            self.thread
                .join()
                .map_err(|_| "PTY reader panicked".to_string())
        })();
        if eof_timeout {
            return Err(format!(
                "PTY output did not reach EOF within 2000 ms; stopped_reader={received:?}; reader_join={joined:?}"
            ));
        }
        joined?;
        let outcome = received.map_err(|e| format!("PTY reader did not stop: {e}"))??;
        if require_eof && matches!(outcome.end, DrainEnd::Stopped) {
            return Err("stopping a PTY reader is not output EOF".into());
        }
        Ok(outcome)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct PipeClosure {
    round: usize,
    output: Option<DrainOutcome>,
    error: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Observation {
    case: String,
    error: Option<String>,
    exit_code: Option<u32>,
    exact_job_member: Option<bool>,
    pipe_closures: Vec<PipeClosure>,
    root_exit_before_wait: Option<u32>,
    descendant_pid: Option<u32>,
    descendant_running_before_wait: Option<bool>,
    descendant_signaled_after_wait: Option<bool>,
    active_processes_before_wait: Option<u32>,
    active_processes_after_wait: Option<u32>,
    control_prefix_bytes: Option<usize>,
    supervision_elapsed_ms: Option<u128>,
    control_write_error: Option<i32>,
    control_disconnected_after_supervision: Option<bool>,
    output_backpressure: Option<BackpressureObservation>,
}

#[derive(Debug, Serialize, Deserialize)]
struct BackpressureSample {
    elapsed_ms: u128,
    available_bytes: u32,
    target_wait: u32,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct BackpressureProbe {
    pipe_flags: u32,
    incoming_capacity_bytes: u32,
    outgoing_capacity_bytes: u32,
    pipe_capacity_bytes: u32,
    samples: Vec<BackpressureSample>,
    marker_observed: bool,
    saturated_for_ms: Option<u128>,
    observation_error: Option<String>,
    terminate_frame: Option<ProbeResult<()>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ShutdownObservation {
    result: ProbeResult<()>,
    quiescent: bool,
    elapsed_ms: u128,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct BackpressureObservation {
    probe: Option<BackpressureProbe>,
    first_result: Option<ProbeResult<u32>>,
    first_quiescent: bool,
    first_elapsed_ms: u128,
    target_signaled_before_pipe_release: bool,
    target_exit_before_pipe_release: Option<u32>,
    stdout_restored: Option<ProbeResult<()>>,
    observer_join: Option<ProbeResult<()>>,
    reader_released_after_first_observation: bool,
    cleanup_attempts: Vec<ShutdownObservation>,
    repeated_shutdown: Option<ShutdownObservation>,
    supervisor_retained_until_worker_exit: bool,
}
fn system_directory() -> ProbeResult<String> {
    let mut buffer = vec![0u16; 32_768];
    let length = unsafe { GetSystemDirectoryW(buffer.as_mut_ptr(), buffer.len() as u32) } as usize;
    if length == 0 || length >= buffer.len() {
        return Err("GetSystemDirectoryW failed".into());
    }
    String::from_utf16(&buffer[..length]).map_err(|e| e.to_string())
}
fn open_pty() -> ProbeResult<Pty> {
    Pty::open(TerminalSize { rows: 24, cols: 80 }).map_err(|e| e.to_string())
}
fn launch_observation(observation: &mut Observation) -> ProbeResult<()> {
    stage("derive-owned-profile");
    let profile = std::env::var(PROFILE).map_err(|e| e.to_string())?;
    let sid = identity::derive_sid(&profile).map_err(|e| e.to_string())?;
    let cwd = system_directory()?;
    stage("open-pty");
    let pty = open_pty()?;
    let drain = Drain::start(&pty)?;
    stage("launch-interactive-cmd");
    let command = process::command_line(&[
        Path::new(&cwd)
            .join("cmd.exe")
            .to_string_lossy()
            .into_owned(),
        "/d".into(),
        "/q".into(),
    ]);
    let launched = process::launch(LaunchSpec {
        command_line: command,
        working_directory: cwd,
        environment: process::environment_block(&std::env::vars().collect()),
        identity: LaunchIdentity::AppContainer {
            container: &sid,
            capabilities: &[],
        },
        inherit: Vec::new(),
        stdin: 0,
        stdout: 0,
        stderr: 0,
        pseudoconsole: Some(pty.console()),
    })
    .map_err(|e| e.to_string());
    let result = match &launched {
        Ok(target) => (|| {
            stage("query-exact-job");
            let mut member = 0;
            checked(
                unsafe {
                    IsProcessInJob(
                        target.process_handle() as HANDLE,
                        target.tree_handle() as HANDLE,
                        &mut member,
                    )
                },
                "query exact target job",
            )?;
            observation.exact_job_member = Some(member != 0);
            if unsafe { WaitForSingleObject(target.process_handle() as HANDLE, 0) } != WAIT_TIMEOUT
            {
                return Err("interactive target exited before the host sent exit".into());
            }
            stage("write-exit-seven");
            let mut input = duplicate_file(pty.input_handle() as HANDLE)?;
            input
                .write_all(b"exit /b 7\r\n")
                .map_err(|e| e.to_string())?;
            wait(
                target.process_handle() as HANDLE,
                5_000,
                "interactive cmd exit",
            )?;
            observation.exit_code = Some(target.wait().map_err(|e| e.to_string())?.exit_code);
            Ok(())
        })(),
        Err(error) => Err(error.clone()),
    };
    // Even a failed launch must reach public Pty::drop. Keep draining its final
    // frame, then stop explicitly: old leaked writer copies prevent pipe EOF.
    let mut cleanup = Ok(());
    if let Ok(target) = &launched {
        process::terminate_tree(target.tree_handle());
        cleanup = wait(
            target.process_handle() as HANDLE,
            DRAIN_MS,
            "target cleanup",
        );
    }
    drop(launched);
    stage("close-pty");
    drop(pty);
    stage("close-pty-complete");
    let drained = drain.finish();
    result?;
    cleanup?;
    drained?;
    if observation.exact_job_member != Some(true) {
        return Err("target absent from returned job".into());
    }
    if observation.exit_code != Some(7) {
        return Err(format!("expected exit 7, got {:?}", observation.exit_code));
    }
    Ok(())
}

fn active_job_processes(job: HANDLE) -> ProbeResult<u32> {
    let mut accounting: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION = unsafe { std::mem::zeroed() };
    checked(
        unsafe {
            QueryInformationJobObject(
                job,
                JobObjectBasicAccountingInformation,
                (&mut accounting as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
                std::mem::size_of_val(&accounting) as u32,
                ptr::null_mut(),
            )
        },
        "query exact target job accounting",
    )?;
    Ok(accounting.ActiveProcesses)
}

fn retain_descendant(target: &process::Target) -> ProbeResult<(u32, Handle)> {
    // This is the documented variable-length JOBOBJECT_BASIC_PROCESS_ID_LIST
    // layout, bounded to sixteen entries from this one owned job only.
    #[repr(C)]
    struct JobProcessIds {
        assigned: u32,
        count: u32,
        pids: [usize; 16],
    }
    let mut list: JobProcessIds = unsafe { std::mem::zeroed() };
    checked(
        unsafe {
            QueryInformationJobObject(
                target.tree_handle() as HANDLE,
                JobObjectBasicProcessIdList,
                (&mut list as *mut JobProcessIds).cast(),
                std::mem::size_of_val(&list) as u32,
                ptr::null_mut(),
            )
        },
        "query owned descendant IDs",
    )?;
    if list.assigned != list.count || list.count as usize > list.pids.len() {
        return Err("owned job PID list was truncated".into());
    }
    let root_pid = unsafe { GetProcessId(target.process_handle() as HANDLE) };
    if root_pid == 0 {
        return Err(format!("GetProcessId(root): Win32 error {}", unsafe {
            GetLastError()
        }));
    }
    let descendants: Vec<_> = list.pids[..list.count as usize]
        .iter()
        .copied()
        .filter(|pid| *pid != root_pid as usize)
        .collect();
    let [pid] = descendants.as_slice() else {
        return Err(format!(
            "fixture must leave exactly one child in the target job, got {descendants:?}"
        ));
    };
    let pid = u32::try_from(*pid).map_err(|_| "invalid child PID".to_string())?;
    let child = Handle(unsafe {
        OpenProcess(
            PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
            0,
            pid,
        )
    });
    if child.0.is_null() {
        return Err(format!(
            "OpenProcess(owned child): Win32 error {}",
            unsafe { GetLastError() }
        ));
    }
    let mut member = 0;
    checked(
        unsafe { IsProcessInJob(child.0, target.tree_handle() as HANDLE, &mut member) },
        "query retained child's exact job",
    )?;
    if member == 0 {
        return Err("retained child is absent from the target job".into());
    }
    Ok((pid, child))
}

fn wait_descendant_observation(observation: &mut Observation) -> ProbeResult<()> {
    let profile = std::env::var(PROFILE).map_err(|e| e.to_string())?;
    let sid = identity::derive_sid(&profile).map_err(|e| e.to_string())?;
    let cwd = system_directory()?;
    let cmd = Path::new(&cwd).join("cmd.exe");
    let cmd = cmd.to_string_lossy();
    let mut pty = open_pty()?;
    let drain = Drain::start(&pty)?;
    // START /B creates a real descendant on the same console. The root's /C
    // command exits without reading stdin; the child's /K command remains at
    // its prompt because our PTY input stays open and we never send input.
    let command = format!(r#""{cmd}" /d /q /s /c "start "" /b "{cmd}" /d /q /k & exit /b 7""#);
    stage("launch-root-with-owned-descendant");
    let launched = process::launch(LaunchSpec {
        command_line: command,
        working_directory: cwd,
        environment: process::environment_block(&std::env::vars().collect()),
        identity: LaunchIdentity::AppContainer {
            container: &sid,
            capabilities: &[],
        },
        inherit: Vec::new(),
        stdin: 0,
        stdout: 0,
        stderr: 0,
        pseudoconsole: Some(pty.console()),
    })
    .map_err(|e| e.to_string());
    pty.close_launch_handles();
    let result = match &launched {
        Ok(target) => (|| {
            wait(
                target.process_handle() as HANDLE,
                5_000,
                "root exit before public wait",
            )?;
            let mut root_code = 0;
            checked(
                unsafe { GetExitCodeProcess(target.process_handle() as HANDLE, &mut root_code) },
                "root exit code before public wait",
            )?;
            observation.root_exit_before_wait = Some(root_code);
            if root_code != 7 {
                return Err(format!("fixture root must exit 7, got {root_code}"));
            }
            let (pid, child) = retain_descendant(target)?;
            observation.descendant_pid = Some(pid);
            observation.exact_job_member = Some(true);
            let before = unsafe { WaitForSingleObject(child.0, 0) };
            observation.descendant_running_before_wait = Some(before == WAIT_TIMEOUT);
            observation.active_processes_before_wait =
                Some(active_job_processes(target.tree_handle() as HANDLE)?);
            if before != WAIT_TIMEOUT || observation.active_processes_before_wait == Some(0) {
                return Err("fixture child must still be running before public wait".into());
            }
            stage("public-wait-with-retained-descendant");
            observation.exit_code = Some(target.wait().map_err(|e| e.to_string())?.exit_code);
            // Observe before Target/Pty drop or any test cleanup can kill the
            // descendant. The retained process handle keeps this assertion bound
            // to the original child, independently of any later PID reuse.
            let after = unsafe { WaitForSingleObject(child.0, 0) };
            observation.descendant_signaled_after_wait = Some(after == WAIT_OBJECT_0);
            observation.active_processes_after_wait =
                Some(active_job_processes(target.tree_handle() as HANDLE)?);
            if observation.exit_code != Some(7)
                || after != WAIT_OBJECT_0
                || observation.active_processes_after_wait != Some(0)
            {
                return Err(
                    "public wait returned before descendant teardown or changed root exit".into(),
                );
            }
            Ok(())
        })(),
        Err(error) => Err(error.clone()),
    };
    // Cleanup runs after the observations, including on the deliberately red
    // baseline. Never let closing our job handle make the assertion pass.
    let cleanup = (|| {
        if let Ok(target) = &launched {
            process::terminate_tree(target.tree_handle());
            let deadline = Instant::now() + Duration::from_millis(u64::from(DRAIN_MS));
            while active_job_processes(target.tree_handle() as HANDLE)? != 0 {
                if Instant::now() >= deadline {
                    return Err("owned target job did not drain during cleanup".to_string());
                }
                thread::sleep(Duration::from_millis(5));
            }
        }
        Ok(())
    })();
    drop(launched);
    drop(pty);
    let drained = drain.finish();
    if result.is_err() || cleanup.is_err() || drained.is_err() {
        return Err(format!(
            "wait_result={result:?}; target_cleanup={cleanup:?}; reader_cleanup={drained:?}"
        ));
    }
    Ok(())
}

fn pty_output_cycle() -> ProbeResult<DrainOutcome> {
    let pty = open_pty()?;
    let drain = Drain::start(&pty)?;
    // Keep the duplicated output reader alive across public Pty::drop. A leaked
    // caller-owned writer leaves the empty pipe connected instead of reaching EOF.
    drop(pty);
    drain.finish_eof()
}
fn pipe_observation(observation: &mut Observation) -> ProbeResult<()> {
    for round in 0..4 {
        stage(&format!("pty-output-eof-round-{round}"));
        let result = pty_output_cycle();
        eprintln!("pty_output_eof round={round} result={result:?}");
        let (output, error) = match result {
            Ok(output) => (Some(output), None),
            Err(error) => (None, Some(error)),
        };
        observation.pipe_closures.push(PipeClosure {
            round,
            output,
            error,
        });
    }
    if observation.pipe_closures.len() != 4
        || observation.pipe_closures.iter().any(|round| {
            round.error.is_some()
                || !matches!(
                    round.output.as_ref().map(|output| &output.end),
                    Some(DrainEnd::Eof | DrainEnd::BrokenPipe)
                )
        })
    {
        return Err(format!(
            "every public Pty::open/drop must close its output pipe: {:?}",
            observation.pipe_closures
        ));
    }
    Ok(())
}

fn partial_control_observation(observation: &mut Observation) -> ProbeResult<()> {
    let (mut read, mut write) = (ptr::null_mut(), ptr::null_mut());
    checked(
        unsafe { CreatePipe(&mut read, &mut write, ptr::null(), 4096) },
        "owned supervision control pipe",
    )?;
    let control = unsafe { File::from_raw_handle(read) };
    let mut writer = unsafe { File::from_raw_handle(write) };
    let mut frame = Vec::new();
    protocol::write_frame(
        &mut frame,
        &ControlMessage::Resize(TerminalSize { rows: 25, cols: 81 }),
    )
    .map_err(|e| e.to_string())?;
    // Leave half of the four-byte frame header pending. This writer is kept
    // alive throughout supervision; closing it must not wake the product reader.
    writer.write_all(&frame[..2]).map_err(|e| e.to_string())?;
    observation.control_prefix_bytes = Some(2);

    let profile = std::env::var(PROFILE).map_err(|e| e.to_string())?;
    let sid = identity::derive_sid(&profile).map_err(|e| e.to_string())?;
    let cwd = system_directory()?;
    let cmd = Path::new(&cwd).join("cmd.exe");
    let mut pty = open_pty()?;
    stage("launch-self-exiting-supervised-target");
    let target = process::launch(LaunchSpec {
        command_line: format!(r#""{}" /d /q /c "exit /b 7""#, cmd.display()),
        working_directory: cwd,
        environment: process::environment_block(&std::env::vars().collect()),
        identity: LaunchIdentity::AppContainer {
            container: &sid,
            capabilities: &[],
        },
        inherit: Vec::new(),
        stdin: 0,
        stdout: 0,
        stderr: 0,
        pseudoconsole: Some(pty.console()),
    })
    .map_err(|e| e.to_string());
    pty.close_launch_handles();
    let target = target?;

    // The production supervisor owns the only PTY reader and all target/control
    // resources from here. Do not start the fixture's Drain on the same output.
    stage("supervise-with-partial-frame-and-open-writer");
    let started = Instant::now();
    let mut supervisor = klaus_sandbox_win::wrapper::Supervisor::new(target, Some(pty), control);
    let result = supervisor.run().map_err(|e| e.to_string());
    let quiescent = supervisor.is_quiescent();
    let repeated_shutdown = supervisor.shutdown().map_err(|e| e.to_string());
    let elapsed = started.elapsed();
    observation.supervision_elapsed_ms = Some(elapsed.as_millis());
    observation.exit_code = result.as_ref().ok().copied();

    // Observe before closing the writer or letting controller cleanup run. At
    // most two bytes are pending in this 4096-byte owned pipe, so the one-byte
    // probe cannot block on a full buffer if a reader was incorrectly retained.
    let written = writer.write(b"x");
    observation.control_write_error = written.as_ref().err().and_then(|e| e.raw_os_error());
    // Rust File::write uses NtWriteFile and classifies both ERROR_BROKEN_PIPE
    // and ERROR_NO_DATA as BrokenPipe. Keep the raw result for diagnostics, but
    // require the File API's disconnection category rather than Win32's one code.
    observation.control_disconnected_after_supervision = Some(
        written
            .as_ref()
            .is_err_and(|error| error.kind() == std::io::ErrorKind::BrokenPipe),
    );
    eprintln!(
        "partial_control_supervision result={result:?} quiescent={quiescent} repeated_shutdown={repeated_shutdown:?} elapsed_ms={} writer_probe={written:?}",
        elapsed.as_millis()
    );
    let valid = result == Ok(7)
        && quiescent
        && repeated_shutdown == Ok(())
        && elapsed <= Duration::from_secs(5)
        && observation.control_disconnected_after_supervision == Some(true);
    // Only this explicit post-observation drop can close our writer endpoint.
    drop(writer);
    if !valid {
        return Err(format!(
            "supervision must return 7 within 5000 ms and close its control reader: result={result:?}, elapsed_ms={}, writer_probe={written:?}",
            elapsed.as_millis()
        ));
    }
    Ok(())
}

/// This process is an exact, isolated worker. The controller's stdout is untouched.
struct WorkerStdout {
    original: HANDLE,
    pipe: Option<File>,
}
impl WorkerStdout {
    const KIND: u32 = (-11_i32) as u32;

    fn redirect(pipe: File) -> ProbeResult<Self> {
        let original = unsafe { get_std_handle(Self::KIND) };
        if original.is_null() || original == INVALID_HANDLE_VALUE {
            return Err("worker has no valid stdout to restore".into());
        }
        checked(
            unsafe { set_std_handle(Self::KIND, pipe.as_raw_handle()) },
            "redirect owned worker stdout",
        )?;
        Ok(Self {
            original,
            pipe: Some(pipe),
        })
    }

    fn restore(&mut self) -> ProbeResult<()> {
        if self.pipe.is_some() {
            checked(
                unsafe { set_std_handle(Self::KIND, self.original) },
                "restore owned worker stdout",
            )?;
            self.pipe.take();
        }
        Ok(())
    }
}
impl Drop for WorkerStdout {
    fn drop(&mut self) {
        if self.restore().is_err() {
            // Never leave the worker's standard-handle table naming a closed
            // handle. The outer job owns the final worker-exit cleanup bound.
            std::mem::forget(self.pipe.take());
        }
    }
}

fn observe_backpressure_then_terminate(
    pipe: File,
    mut control_writer: File,
    terminate_frame: Vec<u8>,
    target: Arc<OwnedHandle>,
    stop: Arc<AtomicBool>,
) -> (File, BackpressureProbe) {
    let mut probe = BackpressureProbe::default();
    let observed = (|| {
        checked(
            unsafe {
                GetNamedPipeInfo(
                    pipe.as_raw_handle(),
                    &mut probe.pipe_flags,
                    &mut probe.outgoing_capacity_bytes,
                    &mut probe.incoming_capacity_bytes,
                    ptr::null_mut(),
                )
            },
            "query owned stdout buffer capacity",
        )?;
        // Capacities describe the named pipe's server side. The read endpoint
        // receives incoming data on a server end, outgoing data on a client end.
        probe.pipe_capacity_bytes = if probe.pipe_flags & PIPE_SERVER_END != 0 {
            probe.incoming_capacity_bytes
        } else {
            probe.outgoing_capacity_bytes
        };
        if probe.pipe_capacity_bytes == 0 || probe.pipe_capacity_bytes > 65_536 {
            return Err(format!(
                "owned stdout has no bounded fixed capacity: {}",
                probe.pipe_capacity_bytes
            ));
        }
        let started = Instant::now();
        let mut previous_available = None;
        let mut full_since = None;
        let mut preview = [0u8; 1024];
        stage("observe-unread-owned-stdout");
        // Only PeekNamedPipe touches this read endpoint. No read or duplicate
        // reader can consume bytes while this observation is in progress. The
        // outer worker deadline also bounds a stuck native synchronous call.
        while started.elapsed() <= Duration::from_secs(5) && probe.samples.len() < 251 {
            if stop.load(Ordering::Acquire) {
                return Err("supervision returned before backpressure was observed".into());
            }
            let (mut copied, mut available) = (0, 0);
            checked(
                unsafe {
                    PeekNamedPipe(
                        pipe.as_raw_handle(),
                        preview.as_mut_ptr().cast(),
                        preview.len() as u32,
                        &mut copied,
                        &mut available,
                        ptr::null_mut(),
                    )
                },
                "peek unread owned stdout",
            )?;
            probe.marker_observed |= preview[..copied as usize]
                .windows(BACKPRESSURE_MARKER.len())
                .any(|bytes| bytes == BACKPRESSURE_MARKER);
            let target_wait = unsafe { WaitForSingleObject(target.as_raw_handle(), 0) };
            probe.samples.push(BackpressureSample {
                elapsed_ms: started.elapsed().as_millis(),
                available_bytes: available,
                target_wait,
            });
            if target_wait != WAIT_TIMEOUT {
                return Err(format!(
                    "continuous-output target stopped before termination: wait={target_wait}"
                ));
            }
            if available >= probe.pipe_capacity_bytes && probe.marker_observed {
                if previous_available != Some(available) {
                    full_since = Some(Instant::now());
                }
                let saturated = full_since.get_or_insert_with(Instant::now).elapsed();
                if saturated >= Duration::from_millis(100) {
                    probe.saturated_for_ms = Some(saturated.as_millis());
                    stage(&format!(
                        "stdout-backpressure-observed available={available} capacity={} saturated_ms={}",
                        probe.pipe_capacity_bytes,
                        saturated.as_millis()
                    ));
                    return Ok(());
                }
            } else {
                full_since = None;
            }
            previous_available = Some(available);
            thread::sleep(Duration::from_millis(20));
        }
        Err("no stable, full stdout buffer containing target output within 5000 ms".into())
    })();
    probe.observation_error = observed.err();
    // One pre-encoded frame smaller than this otherwise-empty 4096-byte pipe
    // cannot fill it. Failure still attempts control termination for cleanup.
    probe.terminate_frame = Some(
        control_writer
            .write_all(&terminate_frame)
            .map_err(|e| e.to_string()),
    );
    stage("terminate-frame-attempted-with-stdout-reader-still-open");
    // Returning the File keeps the endpoint open in the thread result even after
    // this thread exits. It is released only after the first supervision receipt.
    (pipe, probe)
}

fn output_backpressure_observation(observation: &mut Observation) -> ProbeResult<()> {
    let record = observation.output_backpressure.insert(Default::default());
    let (mut read, mut write) = (ptr::null_mut(), ptr::null_mut());
    checked(
        unsafe { CreatePipe(&mut read, &mut write, ptr::null(), 4096) },
        "owned backpressure stdout pipe",
    )?;
    let output_reader = unsafe { File::from_raw_handle(read) };
    let output_writer = unsafe { File::from_raw_handle(write) };
    checked(
        unsafe { CreatePipe(&mut read, &mut write, ptr::null(), 4096) },
        "owned backpressure control pipe",
    )?;
    let control = unsafe { File::from_raw_handle(read) };
    let control_writer = unsafe { File::from_raw_handle(write) };
    let mut terminate_frame = Vec::new();
    protocol::write_frame(&mut terminate_frame, &ControlMessage::Terminate)
        .map_err(|e| e.to_string())?;
    if terminate_frame.len() >= 4096 {
        return Err("termination frame exceeds the owned control-pipe bound".into());
    }

    let profile = std::env::var(PROFILE).map_err(|e| e.to_string())?;
    let sid = identity::derive_sid(&profile).map_err(|e| e.to_string())?;
    let cwd = system_directory()?;
    let cmd = Path::new(&cwd).join("cmd.exe");
    let mut pty = open_pty()?;
    stage("launch-continuous-output-target");
    let target = process::launch(LaunchSpec {
        // A billion lines cannot complete during this bounded observation. The
        // command writes no files and uses no network, grants, or outside jobs.
        command_line: format!(
            r#""{}" /d /q /c "for /L %i in (1,1,1000000000) do @echo KLAUS_OUTPUT_BACKPRESSURE_012345678901234567890123456789""#,
            cmd.display()
        ),
        working_directory: cwd,
        environment: process::environment_block(&std::env::vars().collect()),
        identity: LaunchIdentity::AppContainer {
            container: &sid,
            capabilities: &[],
        },
        inherit: Vec::new(),
        stdin: 0,
        stdout: 0,
        stderr: 0,
        pseudoconsole: Some(pty.console()),
    })
    .map_err(|e| e.to_string());
    pty.close_launch_handles();
    let target = target?;
    let target_copy = duplicate(target.process_handle() as HANDLE, false).map(|copy| {
        let owned = unsafe { OwnedHandle::from_raw_handle(copy.0) };
        std::mem::forget(copy);
        Arc::new(owned)
    });
    // Establish the product owner before any remaining fallible setup. Its Drop
    // retains pending resources; the isolated worker's exit is the outer bound.
    let mut supervisor = klaus_sandbox_win::wrapper::Supervisor::new(target, Some(pty), control);
    record.supervisor_retained_until_worker_exit = true;
    let target_copy = target_copy?;
    let mut stdout = WorkerStdout::redirect(output_writer)?;
    let stop = Arc::new(AtomicBool::new(false));
    let observer_stop = Arc::clone(&stop);
    let observer_target = Arc::clone(&target_copy);
    let observer = thread::Builder::new()
        .name("owned-stdout-backpressure-observer".into())
        .spawn(move || {
            observe_backpressure_then_terminate(
                output_reader,
                control_writer,
                terminate_frame,
                observer_target,
                observer_stop,
            )
        })
        .map_err(|e| e.to_string())?;
    stage("supervise-with-unread-owned-stdout");
    let started = Instant::now();
    let result = supervisor.run();
    record.first_quiescent = supervisor.is_quiescent();
    record.first_elapsed_ms = started.elapsed().as_millis();
    record.target_signaled_before_pipe_release =
        unsafe { WaitForSingleObject(target_copy.as_raw_handle(), 0) } == WAIT_OBJECT_0;
    let mut target_exit = 0;
    if record.target_signaled_before_pipe_release
        && unsafe { GetExitCodeProcess(target_copy.as_raw_handle(), &mut target_exit) } != 0
    {
        record.target_exit_before_pipe_release = Some(target_exit);
    }
    // Success and a cancellation timeout are both observable outcomes. Neither
    // an error nor a successful cancellation call substitutes for quiescence.
    let allowed_result = match &result {
        Ok(code) => *code == klaus_sandbox_win::wrapper::EXIT_TERMINATED && record.first_quiescent,
        Err(klaus_sandbox_win::wrapper::WrapperError::SupervisionIo(error)) => matches!(
            error.kind(),
            std::io::ErrorKind::TimedOut | std::io::ErrorKind::Interrupted
        ),
        Err(_) => false,
    };
    record.first_result = Some(result.map_err(|e| e.to_string()));
    record.stdout_restored = Some(stdout.restore());
    stop.store(true, Ordering::Release);

    let join_deadline = Instant::now() + Duration::from_secs(1);
    while !observer.is_finished() && Instant::now() < join_deadline {
        thread::sleep(Duration::from_millis(5));
    }
    if observer.is_finished() {
        match observer.join() {
            Ok((reader, probe)) => {
                record.probe = Some(probe);
                record.observer_join = Some(Ok(()));
                // Releasing only our own unread endpoint permits any retained
                // blocked write to finish before bounded explicit cleanup retries.
                drop(reader);
                record.reader_released_after_first_observation = true;
            }
            Err(_) => record.observer_join = Some(Err("stdout observer panicked".into())),
        }
    } else {
        record.observer_join = Some(Err("stdout observer did not join within 1000 ms".into()));
        std::mem::forget(observer);
    }
    for _ in 0..2 {
        if supervisor.is_quiescent() {
            break;
        }
        let started = Instant::now();
        let result = supervisor.shutdown().map_err(|e| e.to_string());
        record.cleanup_attempts.push(ShutdownObservation {
            result,
            quiescent: supervisor.is_quiescent(),
            elapsed_ms: started.elapsed().as_millis(),
        });
    }
    let started = Instant::now();
    let repeated = supervisor.shutdown().map_err(|e| e.to_string());
    let quiescent = supervisor.is_quiescent();
    record.repeated_shutdown = Some(ShutdownObservation {
        result: repeated,
        quiescent,
        elapsed_ms: started.elapsed().as_millis(),
    });
    record.supervisor_retained_until_worker_exit = !quiescent;
    if !quiescent {
        std::mem::forget(supervisor);
    }
    let valid = allowed_result
        && record.first_elapsed_ms <= 8_000
        && record.target_signaled_before_pipe_release
        && record.target_exit_before_pipe_release == Some(1)
        && record.stdout_restored == Some(Ok(()))
        && record.observer_join == Some(Ok(()))
        && record.reader_released_after_first_observation
        && record.probe.as_ref().is_some_and(|probe| {
            probe.observation_error.is_none()
                && probe.marker_observed
                && probe.saturated_for_ms.is_some_and(|elapsed| elapsed >= 100)
                && probe.terminate_frame == Some(Ok(()))
        })
        && record.repeated_shutdown.as_ref().is_some_and(|repeat| {
            repeat.result == Ok(()) && repeat.quiescent && repeat.elapsed_ms <= 2_000
        });
    stage("backpressure-observation-and-bounded-cleanup-complete");
    if !valid {
        return Err(format!(
            "output-backpressure supervision failed: {record:?}"
        ));
    }
    Ok(())
}

fn run(case: &str, name: &str) {
    if std::env::var(ROLE).ok().as_deref() == Some(case) {
        // This affects only the owned worker and descendants, never the controller
        // or system policy. Suppress error dialogs on the intentionally broken baseline.
        unsafe { set_error_mode(0x0001 | 0x0002 | 0x8000) };
        let mut observation = Observation {
            case: case.into(),
            ..Default::default()
        };
        let result = match case {
            "launch" => launch_observation(&mut observation),
            "pipes" => pipe_observation(&mut observation),
            "drain" => wait_descendant_observation(&mut observation),
            "partial-control" => partial_control_observation(&mut observation),
            BACKPRESSURE_CASE => output_backpressure_observation(&mut observation),
            _ => Err("unknown worker case".into()),
        };
        observation.error = result.err();
        stage("write-observation");
        let written = std::env::var(REPORT)
            .map_err(|e| e.to_string())
            .and_then(|path| {
                let bytes = serde_json::to_vec(&observation).map_err(|e| e.to_string())?;
                fs::write(path, bytes).map_err(|e| e.to_string())
            });
        if let Err(error) = written {
            eprintln!("cannot write observation: {error}");
            std::process::exit(2);
        }
        // OS exit closes leaked handles in the red baseline. The controller
        // drains the outer job and removes the profile before asserting anything.
        std::process::exit(i32::from(observation.error.is_some()));
    }
    let result = controller(case, name);
    assert!(result.is_ok(), "{}", result.unwrap_err());
}
pub(super) struct ScratchDirectory(PathBuf);
impl ScratchDirectory {
    pub(super) fn create() -> std::io::Result<Self> {
        let path =
            std::env::temp_dir().join(format!("klaus-launch-regression-{}", uuid::Uuid::new_v4()));
        // A collision is an error: only a successfully created directory is ours.
        fs::create_dir(&path)?;
        Ok(Self(path))
    }
    pub(super) fn path(&self) -> &Path {
        &self.0
    }
    pub(super) fn close(self) -> std::io::Result<()> {
        fs::remove_dir_all(&self.0)
    }
}
impl Drop for ScratchDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn retain_backpressure_fixture(
    root: ScratchDirectory,
    profile: ProfileOwner,
    worker: Option<Worker>,
    reason: String,
) -> String {
    let path = root.path().to_path_buf();
    let profile_name = profile.0.clone();
    let worker_pid = worker
        .as_ref()
        .map(|worker| unsafe { GetProcessId(worker.process.0) });
    // Retain every available owner before fallible evidence output. In
    // particular, ProfileOwner::drop must not delete an unconfirmed boundary.
    std::mem::forget(worker);
    std::mem::forget(profile);
    std::mem::forget(root);
    let record = serde_json::json!({
        "status": "cleanup-unconfirmed",
        "case": BACKPRESSURE_CASE,
        "controller_pid": std::process::id(),
        "worker_pid": worker_pid,
        "worker_owner_returned": worker_pid.is_some(),
        "profile_name": profile_name,
        "fixture_directory": path,
        "reason": reason,
    });
    let written = serde_json::to_vec_pretty(&record)
        .map_err(|e| e.to_string())
        .and_then(|bytes| {
            fs::write(path.join("cleanup-unconfirmed.json"), bytes).map_err(|e| e.to_string())
        });
    format!("{reason}; retained_backpressure_fixture={record}; evidence_write={written:?}")
}

fn controller(case: &str, name: &str) -> ProbeResult<()> {
    let root = ScratchDirectory::create().map_err(|e| e.to_string())?;
    let mut profile = if matches!(
        case,
        "launch" | "drain" | "partial-control" | BACKPRESSURE_CASE
    ) {
        ProfileOwner::create()?
    } else {
        ProfileOwner(None)
    };
    let module = module_path!()
        .split_once("::")
        .map_or(module_path!(), |(_, rest)| rest);
    let test = format!("{module}::{name}");
    stage(&format!("controller-{case}"));
    let spawned = Worker::spawn(case, &test, root.path(), profile.0.as_deref().unwrap_or(""));
    let mut worker = match spawned {
        Ok(worker) => worker,
        Err(error) => {
            if case == BACKPRESSURE_CASE {
                return Err(retain_backpressure_fixture(root, profile, None, error));
            }
            let profile_cleanup = profile.close();
            let removed = root.close().map_err(|e| e.to_string());
            return Err(format!(
                "{error}; profile_cleanup={profile_cleanup:?}; fixture_cleanup={removed:?}"
            ));
        }
    };
    let exit = worker.wait();
    // Capture the structured result before cleanup, even if the worker failed.
    // Console diagnostics may use the system codepage; they are not UTF-8 input.
    let observation = fs::read(root.path().join("report.json"))
        .map_err(|e| e.to_string())
        .and_then(|bytes| serde_json::from_slice::<Observation>(&bytes).map_err(|e| e.to_string()));
    let log = fs::read(root.path().join("worker.log")).map_err(|e| e.to_string());
    let cleanup = worker.cleanup();
    if let (BACKPRESSURE_CASE, Err(error)) = (case, &cleanup) {
        return Err(retain_backpressure_fixture(
            root,
            profile,
            Some(worker),
            format!("worker cleanup failed: {error}; worker_exit={exit:?}"),
        ));
    }
    drop(worker);
    let profile_cleanup = profile.close();
    if let (BACKPRESSURE_CASE, Err(error)) = (case, &profile_cleanup) {
        return Err(retain_backpressure_fixture(
            root,
            profile,
            None,
            format!("worker drained but profile cleanup failed: {error}"),
        ));
    }
    if let Ok(log) = &log {
        eprintln!("{}", String::from_utf8_lossy(log));
        // Reversible bytes survive removal of the owned scratch directory in the
        // controller's captured test output; lossy text is only for display.
        eprintln!("worker_log_raw_bytes={log:?}");
    }
    eprintln!("launch_observation={observation:?}");
    if case == BACKPRESSURE_CASE
        && (exit != Ok(0)
            || log.is_err()
            || !observation
                .as_ref()
                .is_ok_and(|observation| observation.case == case && observation.error.is_none()))
    {
        let path = root.path().to_path_buf();
        std::mem::forget(root);
        return Err(format!(
            "backpressure assertion failed; worker_exit={exit:?}; worker_cleanup={cleanup:?}; profile_cleanup={profile_cleanup:?}; observation={observation:?}; evidence_retained={}",
            path.display()
        ));
    }
    let removed = root.close().map_err(|e| e.to_string());
    eprintln!(
        "owned_worker_cleanup={cleanup:?}; profile_cleanup={profile_cleanup:?}; fixture_cleanup={removed:?}"
    );
    cleanup?;
    profile_cleanup?;
    removed?;
    let code = exit?;
    let observation = observation?;
    if code != 0 || observation.case != case || observation.error.is_some() {
        return Err(format!("worker exit {code}; observation {observation:?}"));
    }
    log?;
    Ok(())
}

#[test]
fn pseudoconsole_launch_preserves_exit_code_and_owns_target() {
    run(
        "launch",
        "pseudoconsole_launch_preserves_exit_code_and_owns_target",
    );
}

#[test]
fn pseudoconsole_drop_closes_its_output_pipe() {
    run("pipes", "pseudoconsole_drop_closes_its_output_pipe");
}

#[test]
fn public_wait_drains_descendants_and_preserves_root_exit() {
    run(
        "drain",
        "public_wait_drains_descendants_and_preserves_root_exit",
    );
}

#[test]
fn public_supervision_closes_a_partial_control_frame_before_returning() {
    run(
        "partial-control",
        "public_supervision_closes_a_partial_control_frame_before_returning",
    );
}

#[test]
fn public_supervision_terminates_with_an_open_unread_stdout_pipe() {
    run(
        BACKPRESSURE_CASE,
        "public_supervision_terminates_with_an_open_unread_stdout_pipe",
    );
}
