//! Starting the target inside the sandbox, and holding its process tree.
//!
//! # The job object is what makes "kill it" mean anything
//!
//! Killing a process does not kill what it started. A command that spawns a build
//! tool that spawns a compiler leaves two survivors when only the first is
//! terminated, and those survivors keep the sandbox's ACEs in use so teardown
//! cannot clean up either. So the target is assigned to a job object before it
//! runs a single instruction, with `KILL_ON_JOB_CLOSE`: closing the job handle
//! kills the whole tree, and the handle closes even if the wrapper dies badly,
//! because process exit closes handles.
//!
//! `JOB_OBJECT_LIMIT_BREAKAWAY_OK` is deliberately **not** set. With it, a child
//! can ask to leave the job and Windows obliges, which would let any descendant
//! step outside the tree that teardown knows about.
//!
//! # Handle inheritance is a whitelist, never a flag
//!
//! `bInheritHandles = TRUE` on its own hands the child *every* inheritable handle
//! this process holds. In a wrapper that is a long list: the engine's own pipes,
//! log files, whatever the parent left inheritable. A sandboxed command with a
//! handle to something outside its boundary has a way around the boundary, and no
//! ACL will stop it, because access was already granted when the handle was
//! opened. So every launch passes an explicit `PROC_THREAD_ATTRIBUTE_HANDLE_LIST`
//! naming only the stdio handles the target is meant to have.

/// How the target's standard streams are wired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StdioMode {
    /// Plain pipes: the wrapper relays bytes between the target and the host.
    Pipes,
    /// A pseudoconsole, for a target that expects a terminal.
    Pty,
}

/// What a finished target reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Outcome {
    pub exit_code: u32,
}

#[cfg(windows)]
mod imp {
    use std::collections::BTreeMap;
    use std::ptr;
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    use windows_sys::Win32::Foundation::{
        CloseHandle, GetLastError, ERROR_INVALID_DATA, ERROR_INVALID_PARAMETER, ERROR_MORE_DATA,
        FALSE, HANDLE, INVALID_HANDLE_VALUE, TRUE, WAIT_OBJECT_0, WAIT_TIMEOUT,
    };
    use windows_sys::Win32::Security::SECURITY_CAPABILITIES;
    use windows_sys::Win32::System::JobObjects::{
        CreateJobObjectW, IsProcessInJob, JobObjectBasicAccountingInformation,
        JobObjectBasicProcessIdList, JobObjectExtendedLimitInformation, QueryInformationJobObject,
        SetInformationJobObject, TerminateJobObject, JOBOBJECT_BASIC_ACCOUNTING_INFORMATION,
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    };
    use windows_sys::Win32::System::Threading::{
        CreateProcessAsUserW, CreateProcessW, DeleteProcThreadAttributeList, GetExitCodeProcess,
        InitializeProcThreadAttributeList, OpenProcess, UpdateProcThreadAttribute,
        WaitForSingleObject, CREATE_UNICODE_ENVIRONMENT, EXTENDED_STARTUPINFO_PRESENT, INFINITE,
        LPPROC_THREAD_ATTRIBUTE_LIST, PROCESS_INFORMATION, PROCESS_QUERY_LIMITED_INFORMATION,
        PROCESS_SYNCHRONIZE, PROC_THREAD_ATTRIBUTE_HANDLE_LIST, PROC_THREAD_ATTRIBUTE_JOB_LIST,
        PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE, PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES,
        STARTF_USESTDHANDLES, STARTUPINFOEXW,
    };

    use super::Outcome;
    use crate::capability::CapabilitySid;
    use crate::identity::AppContainerSid;

    /// Why a target could not be started or supervised.
    #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
    pub enum LaunchError {
        #[error("creating the job object failed: Win32 error {0}")]
        CreateJob(u32),
        #[error("configuring the job object failed: Win32 error {0}")]
        ConfigureJob(u32),
        #[error("preparing process attributes failed: Win32 error {0}")]
        Attributes(u32),
        #[error("starting {program:?} failed: Win32 error {code}")]
        Spawn { program: String, code: u32 },
        #[error("waiting on the target failed: Win32 error {0}")]
        Wait(u32),
        #[error("reading the target's exit code failed: Win32 error {0}")]
        ExitCode(u32),
        #[error("terminating the target's process tree failed: Win32 error {0}")]
        TerminateTree(u32),
        #[error("querying the target's process tree failed: Win32 error {0}")]
        QueryTree(u32),
        #[error("retaining process {pid} for shutdown failed: Win32 error {code}")]
        RetainTreeProcess { pid: u32, code: u32 },
        #[error("the target's process list exceeds the shutdown snapshot limit")]
        ProcessListLimit,
        #[error("the target's shutdown state was poisoned")]
        DrainStatePoisoned,
        #[error(
            "the target's process tree has {active} active and {pending} retained processes still exiting after {timeout_ms} ms"
        )]
        TreeDrainTimeout {
            active: u32,
            pending: usize,
            timeout_ms: u128,
        },
    }

    /// An owned handle, closed on drop.
    ///
    /// `HANDLE` is `isize` in this binding, not a pointer, so "empty" is `0` and
    /// "invalid" is `-1`. Both are checked: `CreateJobObjectW` reports failure as
    /// null while the file APIs report it as `INVALID_HANDLE_VALUE`, and closing
    /// either one is a call into the kernel with a bogus handle.
    pub struct OwnedHandle(HANDLE);

    impl OwnedHandle {
        pub fn raw(&self) -> HANDLE {
            self.0
        }
    }

    impl Drop for OwnedHandle {
        fn drop(&mut self) {
            if self.0 != 0 && self.0 != INVALID_HANDLE_VALUE {
                unsafe { CloseHandle(self.0) };
            }
        }
    }

    unsafe impl Send for OwnedHandle {}
    unsafe impl Sync for OwnedHandle {}

    /// A job object holding a target's whole process tree.
    ///
    /// Dropping it kills the tree, which is the intended teardown: there is no
    /// "detach" path, because a detached descendant is one the sandbox has lost
    /// track of while its ACEs are still in force.
    pub struct ProcessTree {
        job: OwnedHandle,
        // Retain observed process objects across failed shutdown attempts. A retry
        // must not lose them merely because Job accounting has already reached 0.
        draining: Mutex<BTreeMap<u32, OwnedHandle>>,
    }

    impl ProcessTree {
        /// Create the job the target will be assigned to.
        pub fn new() -> Result<Self, LaunchError> {
            let job = unsafe { CreateJobObjectW(ptr::null(), ptr::null()) };
            if job == 0 {
                return Err(LaunchError::CreateJob(unsafe { GetLastError() }));
            }
            let job = OwnedHandle(job);

            let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
            // KILL_ON_JOB_CLOSE and nothing else: no breakaway, so a descendant
            // cannot leave the tree teardown will kill.
            limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            let ok = unsafe {
                SetInformationJobObject(
                    job.raw(),
                    JobObjectExtendedLimitInformation,
                    &limits as *const _ as *const std::ffi::c_void,
                    std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
                )
            };
            if ok == FALSE {
                return Err(LaunchError::ConfigureJob(unsafe { GetLastError() }));
            }
            Ok(Self {
                job,
                draining: Mutex::new(BTreeMap::new()),
            })
        }

        /// Kill everything in the tree now.
        pub fn terminate(&self) {
            if let Err(error) = self.terminate_and_wait(None, Duration::from_secs(1)) {
                eprintln!("klaus sandbox: {error}");
            }
        }

        fn active_processes(&self) -> Result<u32, LaunchError> {
            let mut accounting: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION =
                unsafe { std::mem::zeroed() };
            let ok = unsafe {
                QueryInformationJobObject(
                    self.job.raw(),
                    JobObjectBasicAccountingInformation,
                    (&mut accounting as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
                    std::mem::size_of_val(&accounting) as u32,
                    ptr::null_mut(),
                )
            };
            if ok == FALSE {
                return Err(LaunchError::QueryTree(unsafe { GetLastError() }));
            }
            Ok(accounting.ActiveProcesses)
        }

        fn terminate_checked(&self) -> Result<(), LaunchError> {
            if unsafe { TerminateJobObject(self.job.raw(), 1) } == FALSE {
                return Err(LaunchError::TerminateTree(unsafe { GetLastError() }));
            }
            Ok(())
        }

        fn retain_current_processes(
            &self,
            retained: &mut BTreeMap<u32, OwnedHandle>,
            started: Instant,
            timeout: Duration,
        ) -> Result<(), LaunchError> {
            // The two DWORD header fields precede a variable ULONG_PTR array.
            // usize storage provides the alignment required on both architectures.
            const HEADER_WORDS: usize = 8 / std::mem::size_of::<usize>();
            const MAX_PROCESSES: usize = 65_536;
            let mut capacity = 64;
            loop {
                let mut storage = vec![0usize; HEADER_WORDS + capacity];
                let ok = unsafe {
                    QueryInformationJobObject(
                        self.job.raw(),
                        JobObjectBasicProcessIdList,
                        storage.as_mut_ptr().cast(),
                        std::mem::size_of_val(storage.as_slice()) as u32,
                        ptr::null_mut(),
                    )
                };
                if ok == FALSE {
                    let code = unsafe { GetLastError() };
                    if code != ERROR_MORE_DATA {
                        return Err(LaunchError::QueryTree(code));
                    }
                }
                let header = storage.as_ptr().cast::<u32>();
                let assigned = unsafe { *header } as usize;
                let count = unsafe { *header.add(1) } as usize;
                if count > capacity {
                    return Err(LaunchError::QueryTree(ERROR_INVALID_DATA));
                }
                for value in &storage[HEADER_WORDS..HEADER_WORDS + count] {
                    if started.elapsed() >= timeout {
                        return Err(LaunchError::TreeDrainTimeout {
                            active: assigned as u32,
                            pending: retained.len(),
                            timeout_ms: timeout.as_millis(),
                        });
                    }
                    let pid = u32::try_from(*value)
                        .map_err(|_| LaunchError::QueryTree(ERROR_INVALID_DATA))?;
                    if retained.contains_key(&pid) {
                        continue;
                    }
                    let raw = unsafe {
                        OpenProcess(
                            PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
                            FALSE,
                            pid,
                        )
                    };
                    if raw == 0 {
                        let code = unsafe { GetLastError() };
                        // A process can finish between the snapshot and OpenProcess.
                        if code == ERROR_INVALID_PARAMETER && pid != 0 {
                            continue;
                        }
                        return Err(LaunchError::RetainTreeProcess { pid, code });
                    }
                    let process = OwnedHandle(raw);
                    let mut member = FALSE;
                    if unsafe { IsProcessInJob(raw, self.job.raw(), &mut member) } == FALSE {
                        return Err(LaunchError::QueryTree(unsafe { GetLastError() }));
                    }
                    // Never wait on an unrelated process if a snapshot PID was
                    // reused. This private Job has no breakaway or external writer.
                    if member != FALSE {
                        retained.insert(pid, process);
                    }
                }
                if started.elapsed() >= timeout {
                    return Err(LaunchError::TreeDrainTimeout {
                        active: assigned as u32,
                        pending: retained.len(),
                        timeout_ms: timeout.as_millis(),
                    });
                }
                if ok != FALSE && count == assigned {
                    return Ok(());
                }
                capacity = assigned.max(capacity * 2);
                if capacity > MAX_PROCESSES {
                    return Err(LaunchError::ProcessListLimit);
                }
            }
        }

        /// Terminate the private Job and wait for its observed process objects.
        /// Job accounting may reach zero before a process handle is signaled.
        fn terminate_and_wait(
            &self,
            root: Option<HANDLE>,
            timeout: Duration,
        ) -> Result<(), LaunchError> {
            let mut retained = self
                .draining
                .lock()
                .map_err(|_| LaunchError::DrainStatePoisoned)?;
            let started = Instant::now();
            let snapshot = self.retain_current_processes(&mut retained, started, timeout);
            // Still stop the whole Job if a process cannot be retained. Snapshot
            // failure remains an error; it must not prevent the termination request.
            self.terminate_checked()?;
            snapshot?;
            loop {
                let active = self.active_processes()?;
                let mut pending = 0;
                for process in retained.values().map(OwnedHandle::raw).chain(root) {
                    match unsafe { WaitForSingleObject(process, 0) } {
                        WAIT_OBJECT_0 => {}
                        WAIT_TIMEOUT => pending += 1,
                        _ => return Err(LaunchError::Wait(unsafe { GetLastError() })),
                    }
                }
                let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                    return Err(LaunchError::TreeDrainTimeout {
                        active,
                        pending,
                        timeout_ms: timeout.as_millis(),
                    });
                };
                if active == 0 && pending == 0 {
                    return Ok(());
                }
                std::thread::sleep(remaining.min(Duration::from_millis(10)));
            }
        }
    }

    /// A running target.
    pub struct Target {
        process: OwnedHandle,
        /// Held for the target's life: dropping the job kills the tree.
        tree: ProcessTree,
    }

    impl Target {
        /// Wait for the target, stop remaining descendants, and report its code.
        /// Success confirms Job emptiness and termination of retained processes.
        pub fn wait(&self) -> Result<Outcome, LaunchError> {
            let status = unsafe { WaitForSingleObject(self.process.raw(), INFINITE) };
            if status != WAIT_OBJECT_0 {
                return Err(LaunchError::Wait(unsafe { GetLastError() }));
            }
            self.finished_outcome()
        }

        /// Check the target without blocking on a running root process.
        /// Once it exits, descendant shutdown uses the same bound as `wait`.
        pub fn try_wait(&self) -> Result<Option<Outcome>, LaunchError> {
            match unsafe { WaitForSingleObject(self.process.raw(), 0) } {
                WAIT_TIMEOUT => Ok(None),
                WAIT_OBJECT_0 => self.finished_outcome().map(Some),
                _ => Err(LaunchError::Wait(unsafe { GetLastError() })),
            }
        }

        fn finished_outcome(&self) -> Result<Outcome, LaunchError> {
            let mut code: u32 = 0;
            let ok = unsafe { GetExitCodeProcess(self.process.raw(), &mut code) };
            if ok == FALSE {
                return Err(LaunchError::ExitCode(unsafe { GetLastError() }));
            }
            self.terminate_and_wait()?;
            Ok(Outcome { exit_code: code })
        }

        /// Terminate through the same snapshot and wait path used on normal exit.
        pub fn terminate(&self) -> Result<(), LaunchError> {
            self.terminate_and_wait()
        }

        /// Stop and confirm the entire tree before the caller tears down grants.
        /// This leaves room in the host's cooperative shutdown budget for relays
        /// and permission cleanup; timeout is an error, never successful teardown.
        pub fn terminate_and_wait(&self) -> Result<(), LaunchError> {
            self.tree
                .terminate_and_wait(Some(self.process.raw()), Duration::from_secs(1))
        }

        pub fn process_handle(&self) -> HANDLE {
            self.process.raw()
        }

        /// Borrow the Job handle for inspection while this `Target` remains alive.
        /// Copying the value does not extend its lifetime or transfer ownership.
        pub fn tree_handle(&self) -> HANDLE {
            self.tree.job.raw()
        }
    }

    /// Kill every process in a tree.
    pub fn terminate_tree(job: HANDLE) {
        // Exit code 1: the tree was killed rather than having run to completion.
        unsafe { TerminateJobObject(job, 1) };
    }

    /// Deliver an interrupt to a tree.
    ///
    /// Windows has no per-job signal, and `GenerateConsoleCtrlEvent` addresses a
    /// *console process group*, not a job — and it can only target a group in the
    /// caller's own console, which a sandboxed target with its own pseudoconsole is
    /// not in. There is no API that says "Ctrl+C this job", so the honest
    /// implementation of interrupt for a job is termination: the tree stops, which is
    /// what the host asked for, and the exit code says it was killed rather than
    /// that it finished.
    ///
    /// Pretending otherwise — accepting the message and doing nothing — would leave
    /// a host waiting for a command that never notices it was interrupted.
    pub fn interrupt_tree(job: HANDLE) {
        terminate_tree(job);
    }

    /// Identity is selected before creating the process or any of its children.
    pub enum LaunchIdentity<'a> {
        AppContainer {
            container: &'a AppContainerSid,
            capabilities: &'a [CapabilitySid],
        },
        /// Borrowed primary token, retained by the caller throughout creation.
        RestrictedToken(HANDLE),
    }

    /// Everything a launch needs that is specific to one target.
    pub struct LaunchSpec<'a> {
        /// Fully-quoted command line, as `CreateProcessW` expects it.
        pub command_line: String,
        pub working_directory: String,
        /// `KEY=VALUE\0`-joined, double-NUL terminated, UTF-16.
        pub environment: Vec<u16>,
        pub identity: LaunchIdentity<'a>,
        /// Handles the child may inherit. Everything else stays behind; see the
        /// module note.
        pub inherit: Vec<HANDLE>,
        pub stdin: HANDLE,
        pub stdout: HANDLE,
        pub stderr: HANDLE,
        /// Pseudoconsole to attach, for a target that asked for a terminal.
        ///
        /// When this is set, explicit null standard handles let the console supply
        /// the target's stdio instead of copying the parent's redirected handles.
        pub pseudoconsole: Option<windows_sys::Win32::System::Console::HPCON>,
    }

    fn wide(text: &str) -> Vec<u16> {
        text.encode_utf16().chain(std::iter::once(0)).collect()
    }

    /// Start the target inside a fresh AppContainer-scoped process tree.
    pub fn launch(spec: LaunchSpec<'_>) -> Result<Target, LaunchError> {
        let tree = ProcessTree::new()?;

        let (container, capabilities, token) = match spec.identity {
            LaunchIdentity::AppContainer {
                container,
                capabilities,
            } => (Some(container), capabilities, None),
            LaunchIdentity::RestrictedToken(token) => (None, &[][..], Some(token)),
        };
        let mut capability_entries: Vec<windows_sys::Win32::Security::SID_AND_ATTRIBUTES> =
            capabilities
                .iter()
                .map(
                    |capability| windows_sys::Win32::Security::SID_AND_ATTRIBUTES {
                        Sid: capability.as_psid(),
                        // SE_GROUP_ENABLED: a capability present but not enabled grants
                        // nothing, which would look like a capability bug rather than a
                        // flag bug.
                        Attributes: 0x0000_0004,
                    },
                )
                .collect();
        let mut security_capabilities = SECURITY_CAPABILITIES {
            AppContainerSid: container.map_or(ptr::null_mut(), AppContainerSid::as_psid),
            Capabilities: if capability_entries.is_empty() {
                ptr::null_mut()
            } else {
                capability_entries.as_mut_ptr()
            },
            CapabilityCount: capability_entries.len() as u32,
            Reserved: 0,
        };
        // Attribute values outlive AttributeList, including its destructor.
        let mut jobs = [tree.job.raw()];
        let mut inherit = spec.inherit.clone();

        // Up to four attributes: the AppContainer identity, its job, the handle
        // whitelist, and a pseudoconsole. The count has to be exact —
        // `InitializeProcThreadAttributeList` sizes the buffer from it, and adding an
        // attribute past the declared count corrupts the list.
        let mut attribute_count = 1 + u32::from(container.is_some()); // job plus optional low-box identity
        if !spec.inherit.is_empty() {
            attribute_count += 1;
        }
        if spec.pseudoconsole.is_some() {
            attribute_count += 1;
        }
        let mut size: usize = 0;
        unsafe {
            InitializeProcThreadAttributeList(ptr::null_mut(), attribute_count, 0, &mut size)
        };
        let mut buffer = vec![0u8; size];
        let attributes = buffer.as_mut_ptr() as LPPROC_THREAD_ATTRIBUTE_LIST;
        let ok =
            unsafe { InitializeProcThreadAttributeList(attributes, attribute_count, 0, &mut size) };
        if ok == FALSE {
            return Err(LaunchError::Attributes(unsafe { GetLastError() }));
        }
        // Freed on every exit path below, including the error ones.
        let attributes = AttributeList(attributes);

        if container.is_some() {
            let ok = unsafe {
                UpdateProcThreadAttribute(
                    attributes.0,
                    0,
                    PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES as usize,
                    &mut security_capabilities as *mut _ as *mut std::ffi::c_void,
                    std::mem::size_of::<SECURITY_CAPABILITIES>(),
                    ptr::null_mut(),
                    ptr::null_mut(),
                )
            };
            if ok == FALSE {
                return Err(LaunchError::Attributes(unsafe { GetLastError() }));
            }
        }
        // Bind the tree during process creation. Assigning it after CreateProcessW
        // returns lets the target run and spawn children before the job contains it;
        // those children would survive cancellation and permission teardown.
        // JOB_LIST is supported on Windows 10+, as required by the Windows runtime.
        let ok = unsafe {
            UpdateProcThreadAttribute(
                attributes.0,
                0,
                PROC_THREAD_ATTRIBUTE_JOB_LIST as usize,
                jobs.as_mut_ptr().cast(),
                std::mem::size_of_val(&jobs),
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        if ok == FALSE {
            return Err(LaunchError::Attributes(unsafe { GetLastError() }));
        }

        if !inherit.is_empty() {
            let ok = unsafe {
                UpdateProcThreadAttribute(
                    attributes.0,
                    0,
                    PROC_THREAD_ATTRIBUTE_HANDLE_LIST as usize,
                    inherit.as_mut_ptr() as *mut std::ffi::c_void,
                    std::mem::size_of::<HANDLE>() * inherit.len(),
                    ptr::null_mut(),
                    ptr::null_mut(),
                )
            };
            if ok == FALSE {
                return Err(LaunchError::Attributes(unsafe { GetLastError() }));
            }
        }

        if let Some(console) = spec.pseudoconsole {
            let ok = unsafe {
                UpdateProcThreadAttribute(
                    attributes.0,
                    0,
                    PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE as usize,
                    console as *mut std::ffi::c_void,
                    std::mem::size_of::<windows_sys::Win32::System::Console::HPCON>(),
                    ptr::null_mut(),
                    ptr::null_mut(),
                )
            };
            if ok == FALSE {
                return Err(LaunchError::Attributes(unsafe { GetLastError() }));
            }
        }

        let mut startup: STARTUPINFOEXW = unsafe { std::mem::zeroed() };
        startup.StartupInfo.cb = std::mem::size_of::<STARTUPINFOEXW>() as u32;
        // Explicit null standard handles let ConPTY supply its console connections.
        // Without this flag, Windows duplicates the parent's redirected stdio into
        // a console child even when bInheritHandles is FALSE, bypassing the PTY.
        startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
        if spec.pseudoconsole.is_none() {
            startup.StartupInfo.hStdInput = spec.stdin;
            startup.StartupInfo.hStdOutput = spec.stdout;
            startup.StartupInfo.hStdError = spec.stderr;
        }
        // Naming a desktop is required, not cosmetic: a sandboxed token that
        // inherits the desktop can fail in DLL initialization with
        // STATUS_DLL_INIT_FAILED before the target runs any of its own code, which
        // presents as the command silently producing nothing. See the note in
        // `crate::pty`.
        let mut desktop = wide(crate::pty::desktop_for_launch());
        startup.StartupInfo.lpDesktop = desktop.as_mut_ptr();
        startup.lpAttributeList = attributes.0;

        let mut command_line = wide(&spec.command_line);
        let working_directory = wide(&spec.working_directory);
        let mut environment = spec.environment.clone();
        let mut info: PROCESS_INFORMATION = unsafe { std::mem::zeroed() };

        let inheritance = if inherit.is_empty() { FALSE } else { TRUE };
        let flags = CREATE_UNICODE_ENVIRONMENT | EXTENDED_STARTUPINFO_PRESENT;
        let ok = unsafe {
            match token {
                Some(token) => CreateProcessAsUserW(
                    token,
                    ptr::null(),
                    command_line.as_mut_ptr(),
                    ptr::null(),
                    ptr::null(),
                    inheritance,
                    flags,
                    environment.as_mut_ptr().cast(),
                    working_directory.as_ptr(),
                    &startup.StartupInfo,
                    &mut info,
                ),
                None => CreateProcessW(
                    ptr::null(),
                    command_line.as_mut_ptr(),
                    ptr::null(),
                    ptr::null(),
                    inheritance,
                    flags,
                    environment.as_mut_ptr().cast(),
                    working_directory.as_ptr(),
                    &startup.StartupInfo,
                    &mut info,
                ),
            }
        };
        if ok == FALSE {
            return Err(LaunchError::Spawn {
                program: spec.command_line.clone(),
                code: unsafe { GetLastError() },
            });
        }
        let process = OwnedHandle(info.hProcess);
        let _thread = OwnedHandle(info.hThread);

        Ok(Target { process, tree })
    }

    /// Attribute list wrapper so it is deleted on every path.
    struct AttributeList(LPPROC_THREAD_ATTRIBUTE_LIST);

    impl Drop for AttributeList {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe { DeleteProcThreadAttributeList(self.0) };
            }
        }
    }
}

#[cfg(windows)]
pub use imp::{
    interrupt_tree, launch, terminate_tree, LaunchError, LaunchIdentity, LaunchSpec, OwnedHandle,
    ProcessTree, Target,
};

/// Build the `KEY=VALUE\0...\0\0` block `CreateProcessW` expects.
///
/// Kept out of the Windows-only module so the encoding can be tested anywhere: a
/// malformed environment block is a launch failure with an unhelpful error code, and
/// the terminator rule (double NUL after the last entry) is easy to get wrong.
///
/// # An AppContainer needs the host's environment underneath
///
/// `overrides` is layered on top of the parent's environment rather than replacing
/// it. That is not a convenience, it is what makes a launch work at all: a process
/// started in an AppContainer with a small hand-built environment fails with
/// `ERROR_ENVVAR_NOT_FOUND` (203) before running any of its own code, because the
/// loader resolves per-container redirections (`APPDATA`, `LOCALAPPDATA` and
/// friends, which point at the container's private directories) through variables
/// the parent has and a hand-built block does not.
///
/// This was measured, not guessed. On a real machine, launching `cmd.exe /c exit 7`
/// in an AppContainer succeeded with the full parent environment (77 variables) and
/// with the parent environment plus one extra, and failed with 203 for a 3-variable
/// block — and kept failing as candidates (`SystemDrive`, `ALLUSERSPROFILE`,
/// `ProgramData`, `TEMP`, `TMP`, `PROCESSOR_ARCHITECTURE`, `NUMBER_OF_PROCESSORS`)
/// were added one at a time. No single variable is the missing one; the base set is.
///
/// The policy still decides what the target *sees* for anything it names: an
/// override wins over the inherited value, and a scrubbed variable can be blanked by
/// overriding it with an empty string.
pub fn environment_block(overrides: &std::collections::BTreeMap<String, String>) -> Vec<u16> {
    encode_block(&merge_environment(host_environment(), overrides))
}

/// Layer `overrides` on top of `base`.
///
/// Split out from [`environment_block`] because the merge rule is decidable without
/// a Windows host, while the base it merges into is whatever this process inherited.
fn merge_environment(
    mut base: std::collections::BTreeMap<String, String>,
    overrides: &std::collections::BTreeMap<String, String>,
) -> std::collections::BTreeMap<String, String> {
    for (key, value) in overrides {
        // Replace case-insensitively: Windows treats `Path` and `PATH` as one
        // variable, and leaving both in the block gives the child two entries whose
        // winner is undefined.
        if let Some(existing) = base
            .keys()
            .find(|candidate| candidate.eq_ignore_ascii_case(key))
            .cloned()
        {
            base.remove(&existing);
        }
        base.insert(key.clone(), value.clone());
    }
    base
}

/// Encode a resolved environment as the wide `KEY=VALUE\0...\0\0` block.
fn encode_block(vars: &std::collections::BTreeMap<String, String>) -> Vec<u16> {
    let mut out: Vec<u16> = Vec::new();
    for (key, value) in vars {
        out.extend(key.encode_utf16());
        out.push(u16::from(b'='));
        out.extend(value.encode_utf16());
        out.push(0);
    }
    // Final terminator, after the last entry's own NUL.
    out.push(0);
    out
}

/// This process's environment, as the base an AppContainer launch needs.
fn host_environment() -> std::collections::BTreeMap<String, String> {
    std::env::vars().collect()
}

/// Quote one argument for `CreateProcessW`.
///
/// Windows hands the raw command line to the child, and the C runtime parses it, so
/// quoting is the caller's job. Getting it wrong is not cosmetic: an unquoted
/// argument containing a space becomes two arguments, and a path ending in a
/// backslash before a closing quote escapes that quote and swallows the rest of
/// the line.
pub fn quote_argument(argument: &str) -> String {
    if !argument.is_empty()
        && !argument
            .chars()
            .any(|ch| ch == ' ' || ch == '\t' || ch == '"' || ch == '\n')
    {
        return argument.to_string();
    }
    let mut out = String::with_capacity(argument.len() + 2);
    out.push('"');
    let mut backslashes = 0usize;
    for ch in argument.chars() {
        match ch {
            '\\' => {
                backslashes += 1;
                out.push('\\');
            }
            '"' => {
                // Double the run of backslashes, then escape the quote itself.
                for _ in 0..backslashes {
                    out.push('\\');
                }
                backslashes = 0;
                out.push('\\');
                out.push('"');
            }
            other => {
                backslashes = 0;
                out.push(other);
            }
        }
    }
    // A trailing backslash run must be doubled, or it escapes the closing quote.
    for _ in 0..backslashes {
        out.push('\\');
    }
    out.push('"');
    out
}

/// Join an argv into a command line.
pub fn command_line(argv: &[String]) -> String {
    argv.iter()
        .map(|argument| quote_argument(argument))
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn a_plain_argument_is_left_alone() {
        assert_eq!(quote_argument("cmd.exe"), "cmd.exe");
        assert_eq!(quote_argument("/c"), "/c");
        assert_eq!(quote_argument("C:\\ws\\file.txt"), "C:\\ws\\file.txt");
    }

    #[test]
    fn an_argument_with_spaces_is_quoted() {
        assert_eq!(
            quote_argument("C:\\Program Files\\tool.exe"),
            "\"C:\\Program Files\\tool.exe\""
        );
        assert_eq!(quote_argument("echo hello"), "\"echo hello\"");
    }

    #[test]
    fn an_empty_argument_survives_as_an_empty_pair_of_quotes() {
        // Without this it disappears and every later argument shifts position.
        assert_eq!(quote_argument(""), "\"\"");
    }

    #[test]
    fn a_trailing_backslash_is_doubled_so_it_cannot_escape_the_closing_quote() {
        // The bug: "C:\dir with space\" ends up escaping the quote, and the rest of
        // the command line becomes part of this argument.
        assert_eq!(
            quote_argument("C:\\dir with space\\"),
            "\"C:\\dir with space\\\\\""
        );
    }

    #[test]
    fn embedded_quotes_are_escaped() {
        assert_eq!(quote_argument("say \"hi\""), "\"say \\\"hi\\\"\"");
        // Backslashes before a quote double, then the quote escapes.
        assert_eq!(quote_argument("a\\\"b"), "\"a\\\\\\\"b\"");
    }

    #[test]
    fn a_command_line_round_trips_the_shape_a_shell_would_produce() {
        let argv = vec![
            "C:\\Windows\\System32\\cmd.exe".to_string(),
            "/c".to_string(),
            "echo one two".to_string(),
        ];
        assert_eq!(
            command_line(&argv),
            "C:\\Windows\\System32\\cmd.exe /c \"echo one two\""
        );
    }

    /// Decode a block back to text so a test can read it.
    fn block_text(block: &[u16]) -> String {
        char::decode_utf16(block.iter().copied())
            .map(|ch| ch.unwrap_or('?'))
            .collect()
    }

    /// Split a decoded block into its `KEY=VALUE` entries.
    fn block_entries(block: &[u16]) -> Vec<String> {
        let text = block_text(block);
        assert!(text.ends_with("\0\0"), "block is not double-NUL terminated");
        text.trim_end_matches('\0')
            .split('\0')
            .filter(|entry| !entry.is_empty())
            .map(str::to_string)
            .collect()
    }

    #[test]
    fn an_environment_block_is_nul_separated_and_double_nul_terminated() {
        let vars = BTreeMap::from([
            ("A".to_string(), "1".to_string()),
            ("B".to_string(), "2".to_string()),
        ]);
        assert_eq!(block_text(&encode_block(&vars)), "A=1\0B=2\0\0");
    }

    #[test]
    fn an_empty_environment_still_carries_the_terminator() {
        // A bare NUL, not an empty buffer: CreateProcessW reads until the double NUL,
        // so an empty vector walks off the end of the allocation.
        assert_eq!(encode_block(&BTreeMap::new()), vec![0u16]);
    }

    #[test]
    fn the_host_environment_is_the_base_a_launch_starts_from() {
        // Win32 error 203 (ERROR_ENVVAR_NOT_FOUND) is what an AppContainer launch
        // returns when the block is hand-built and small: the loader resolves the
        // container's own redirections through variables the parent has. Measured on a
        // real machine — the full parent environment worked, a 3-variable block failed,
        // and adding candidate variables one at a time never fixed it.
        let entries = block_entries(&environment_block(&BTreeMap::new()));
        let host: Vec<String> = std::env::vars()
            .map(|(key, value)| format!("{key}={value}"))
            .collect();
        assert!(!host.is_empty(), "a process always has some environment");
        for entry in &host {
            assert!(
                entries.contains(entry),
                "an inherited variable was dropped: {entry:?}"
            );
        }
    }

    #[test]
    fn an_override_wins_over_the_inherited_value_and_the_rest_survives() {
        let base = BTreeMap::from([
            ("KEEP".to_string(), "kept".to_string()),
            ("REPLACED".to_string(), "old".to_string()),
        ]);
        let merged = merge_environment(
            base,
            &BTreeMap::from([("REPLACED".to_string(), "new".to_string())]),
        );
        assert_eq!(merged.get("KEEP").map(String::as_str), Some("kept"));
        assert_eq!(merged.get("REPLACED").map(String::as_str), Some("new"));
        assert_eq!(merged.len(), 2, "the override must replace, not append");
    }

    #[test]
    fn an_override_replaces_a_differently_cased_inherited_name() {
        // Windows environment names are case-insensitive, so leaving both `Path` and
        // `PATH` in the block gives the child two entries with no defined winner.
        let base = BTreeMap::from([("Path".to_string(), "C:\\old".to_string())]);
        let merged = merge_environment(
            base,
            &BTreeMap::from([("PATH".to_string(), "C:\\new".to_string())]),
        );
        assert_eq!(merged.len(), 1, "two spellings of one variable survived");
        assert_eq!(merged.get("PATH").map(String::as_str), Some("C:\\new"));
    }

    #[test]
    fn an_empty_override_blanks_an_inherited_variable() {
        // How a policy hides something it does not want the target to read: the name
        // stays, the value does not.
        let base = BTreeMap::from([("SECRET".to_string(), "value".to_string())]);
        let merged = merge_environment(
            base,
            &BTreeMap::from([("SECRET".to_string(), String::new())]),
        );
        assert_eq!(merged.get("SECRET").map(String::as_str), Some(""));
        assert_eq!(block_text(&encode_block(&merged)), "SECRET=\0\0");
    }

    #[test]
    fn an_override_reaches_the_block_through_the_public_entry_point() {
        // The whole path at once, without depending on what the host's environment
        // happens to hold: one entry for the overridden name, and it is ours.
        let marker = "klaus-sandbox-marker";
        let entries = block_entries(&environment_block(&BTreeMap::from([(
            "PATH".to_string(),
            marker.to_string(),
        )])));
        let paths: Vec<&String> = entries
            .iter()
            .filter(|entry| {
                entry
                    .split_once('=')
                    .is_some_and(|(key, _)| key.eq_ignore_ascii_case("PATH"))
            })
            .collect();
        assert_eq!(paths.len(), 1, "PATH appears more than once: {paths:?}");
        assert_eq!(paths[0], &format!("PATH={marker}"));
    }

    #[test]
    fn environment_values_with_spaces_and_paths_survive_unquoted() {
        // The block is not a command line: quoting here would put literal quotes
        // into the variable's value.
        let vars = BTreeMap::from([
            ("PATH".to_string(), "C:\\Program Files\\a;C:\\b".to_string()),
            ("SystemRoot".to_string(), "C:\\Windows".to_string()),
        ]);
        assert_eq!(
            block_text(&encode_block(&vars)),
            "PATH=C:\\Program Files\\a;C:\\b\0SystemRoot=C:\\Windows\0\0"
        );
    }
}
