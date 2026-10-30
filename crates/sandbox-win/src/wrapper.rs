//! The wrapper: the child process that builds the sandbox and runs the target.
//!
//! # Order is a security property here
//!
//! Every boundary must be in force before the target's first instruction, so setup
//! runs to completion first and the target is launched last. If any step fails, the
//! steps that already succeeded have to come back off — a half-built sandbox that
//! runs the target anyway is worse than no sandbox, because the caller believes one
//! is in place.
//!
//! Teardown is setup reversed, after the target and supervision I/O have stopped.
//! Filesystem grants, loopback exemptions and the execution's profile can outlive
//! this process after incomplete cleanup. Failed steps retain their journal for
//! retry and are reported on stderr before exit. WFP filters are held by a dynamic
//! session (see [`crate::filter`]).
//!
//! # The exit code belongs to the target
//!
//! The engine reads the wrapper's exit code and reports it as the command's. So a
//! wrapper that exits 0 after the target exited 1 turns a failed command into a
//! successful one, and a wrapper that exits 1 because *its own* cleanup hiccuped
//! turns a successful command into a failure. [`exit_code_for`] is the one place
//! that decides, and it keeps those cases apart.

use crate::protocol::SandboxRequest;

/// One step of building the sandbox.
///
/// Modelled as data so the plan can be asserted without a Windows host: which steps
/// a policy calls for, and in what order, is exactly the part that must not drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupStep {
    /// Exclusively create this execution's AppContainer profile and SID.
    EnsureContainer,
    /// Write the filesystem ACEs.
    ApplyFilesystemAcl,
    /// Name this container on the null device, so `NUL` can be opened.
    GrantNullDevice,
    /// Add this container to the machine's loopback exemption list.
    ExemptLoopback,
    /// Install the port-scoped WFP filters.
    InstallNetworkFilters,
    /// Create the pseudoconsole, for a target that asked for a terminal.
    OpenPseudoConsole,
}

/// The steps a request calls for, in the order they must run.
///
/// `EnsureContainer` is always first because every later step is keyed on the SID it
/// produces. The network steps are conditional: withholding the capability is what
/// enforces a denied network (see [`crate::capability`]), so a denied policy installs
/// no filters and takes no exemption — adding either would imply the boundary
/// depends on them having worked.
///
/// `GrantNullDevice` is unconditional. It is not part of any boundary; it undoes a
/// gap in what an AppContainer can reach at all, and a target that cannot open `NUL`
/// fails on most real command lines (see [`crate::null_device`]).
pub fn setup_plan(request: &SandboxRequest) -> Vec<SetupStep> {
    let mut steps = vec![SetupStep::EnsureContainer];
    if !request.filesystem.writable.is_empty()
        || request.filesystem.readable.is_some()
        || !request.filesystem.deny_read.is_empty()
        || !request.filesystem.deny_write.is_empty()
    {
        steps.push(SetupStep::ApplyFilesystemAcl);
    }
    steps.push(SetupStep::GrantNullDevice);
    if crate::capability::needs_loopback_exemption(&request.network) {
        steps.push(SetupStep::ExemptLoopback);
    }
    if !crate::filter::plan_filters(&request.network).is_empty() {
        steps.push(SetupStep::InstallNetworkFilters);
    }
    if request.terminal.is_some() {
        steps.push(SetupStep::OpenPseudoConsole);
    }
    steps
}

/// The steps that have to be undone, given how far setup got.
///
/// Reverse order, and only the steps that actually ran. The execution profile is
/// last: its SID remains available until all temporary permissions are removed.
pub fn teardown_plan(completed: &[SetupStep]) -> Vec<SetupStep> {
    completed.iter().rev().copied().collect()
}

/// What the wrapper should exit with.
///
/// `target` is the code the sandboxed command reported, if it got as far as
/// reporting one. Cleanup problems deliberately do not change it: they are the
/// wrapper's business, not the command's, and reporting them as the command's exit
/// status would make a successful build look like a failed one.
pub fn exit_code_for(target: Option<u32>, setup_failed: bool) -> u32 {
    match (target, setup_failed) {
        // The target ran; its answer is the answer.
        (Some(code), _) => code,
        // Setup failed, so the target never ran. A distinct code, not 1, so this is
        // distinguishable from a command that itself exited 1.
        (None, true) => EXIT_SANDBOX_SETUP_FAILED,
        // No target and no setup failure means the wrapper was told to stop before
        // the target finished.
        (None, false) => EXIT_TERMINATED,
    }
}

/// Setup could not complete, so the target was never started.
pub const EXIT_SANDBOX_SETUP_FAILED: u32 = 189;

/// The host asked for termination before the target finished.
pub const EXIT_TERMINATED: u32 = 190;

/// A diagnostic cannot approve a boundary whose temporary grants were not removed.
pub const EXIT_DIAGNOSTIC_CLEANUP_FAILED: u32 = 191;

#[cfg(windows)]
mod imp {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use crate::acl::{self, Rule, SavedDacl};
    use crate::capability;
    use crate::filter::{self, FilterSession};
    use crate::identity::{self, AppContainerSid, OwnedAppContainerProfile};
    use crate::loopback::{LoopbackError, LoopbackLease};
    use crate::null_device;
    use crate::process::{self, LaunchSpec};
    use crate::protocol::{
        ControlMessage, NativeSandboxLevel, NativeSandboxRequest, SandboxRequest,
    };
    use crate::pty::Pty;

    use super::{exit_code_for, setup_plan, SetupStep};

    // Host shutdown can close stderr while teardown is still running. Reporting
    // must not panic before resource owners have retained or drained their work.
    macro_rules! diagnostic {
        ($($argument:tt)*) => {{
            use std::io::Write as _;
            let _ = writeln!(std::io::stderr().lock(), $($argument)*);
        }};
    }

    /// Why the wrapper could not do its job.
    #[derive(Debug, thiserror::Error)]
    pub enum WrapperError {
        #[error("reading the sandbox request failed: {0}")]
        Request(#[from] crate::protocol::ProtocolError),
        #[error("the sandbox request ended before it arrived")]
        NoRequest,
        #[error("the sandbox request names no command")]
        EmptyCommand,
        #[error("native Windows sandbox: {0}")]
        Native(#[from] anyhow::Error),
        #[error(transparent)]
        Identity(#[from] identity::IdentityError),
        #[error(transparent)]
        Acl(#[from] acl::AclError),
        #[error(transparent)]
        NullDevice(#[from] null_device::NullDeviceError),
        #[error(transparent)]
        Capability(#[from] capability::CapabilityError),
        #[error(transparent)]
        Loopback(#[from] LoopbackError),
        #[error(transparent)]
        Filter(#[from] filter::FilterError),
        #[error(transparent)]
        Pty(#[from] crate::pty::PtyError),
        #[error(transparent)]
        Launch(#[from] process::LaunchError),
        #[error("sandbox supervision I/O failed: {0}")]
        SupervisionIo(#[from] std::io::Error),
    }

    /// Phase evidence stays inside the disposable probe workspace. It carries
    /// no environment, command text, or user paths and disappears with the probe.
    struct DiagnosticTrace {
        file: Option<std::fs::File>,
        started: std::time::Instant,
    }
    impl DiagnosticTrace {
        fn new(request: &SandboxRequest) -> Self {
            let file =
                if request.command.get(1).map(String::as_str) == Some("--windows-sandbox-probe") {
                    std::fs::OpenOptions::new()
                        .create_new(true)
                        .append(true)
                        .open(
                            std::path::Path::new(&request.cwd.as_str())
                                .join(".klaus-diagnostic-trace.jsonl"),
                        )
                        .ok()
                } else {
                    None
                };
            Self {
                file,
                started: std::time::Instant::now(),
            }
        }
        fn record(&mut self, phase: &str) {
            use std::io::Write;
            if let Some(file) = self.file.as_mut() {
                let _ = writeln!(
                    file,
                    "{}",
                    serde_json::json!({"phase": phase, "elapsedMs": self.started.elapsed().as_millis()})
                );
                let _ = file.flush();
            }
        }
    }

    /// Boundaries that are currently in force, and how to take them off.
    ///
    /// Cleanup is explicit and retryable. Drop reports any remaining obligations;
    /// it never deletes a profile or revokes permissions without a drain receipt.
    struct Installed {
        profile: OwnedAppContainerProfile,
        acl_rules: Vec<Rule>,
        /// Per-write ACL journal, including original grants and rollback state.
        /// Paths whose effective access was already sufficient are never revoked.
        saved_dacls: Vec<SavedDacl>,
        null_device_granted: bool,
        loopback_lease: Option<LoopbackLease>,
        /// Dropping this closes the dynamic WFP session, and the kernel removes the
        /// filters. Kept as a field so that happens as part of teardown.
        filters: Option<FilterSession>,
        completed: Vec<SetupStep>,
        cleanup_permitted: bool,
    }

    impl Installed {
        fn new(profile: OwnedAppContainerProfile) -> Self {
            Self {
                profile,
                acl_rules: Vec::new(),
                saved_dacls: Vec::new(),
                null_device_granted: false,
                loopback_lease: None,
                filters: None,
                completed: vec![SetupStep::EnsureContainer],
                cleanup_permitted: true,
            }
        }

        fn container(&self) -> &AppContainerSid {
            self.profile
                .sid()
                .expect("live execution profile has a SID")
        }

        fn cleanup(&mut self) -> Vec<String> {
            if !self.cleanup_permitted {
                return vec![
                    "target or I/O drainage is unconfirmed; retaining installed boundaries".into(),
                ];
            }
            for step in super::teardown_plan(&self.completed) {
                let mut failures = Vec::new();
                match step {
                    SetupStep::ApplyFilesystemAcl => {
                        // Roll back only recorded writes. A descriptor conflict
                        // stops the journal before an ancestor's inheriting write
                        // could overwrite that conflict; report it to the caller.
                        for failure in
                            acl::revoke(&self.acl_rules, &self.saved_dacls, self.container())
                        {
                            failures.push(format!("could not revoke an ACE: {failure}"));
                        }
                    }
                    SetupStep::GrantNullDevice => {
                        if self.null_device_granted {
                            if let Err(error) = null_device::revoke(self.container()) {
                                failures.push(format!(
                                    "could not take back null device access: {error}"
                                ));
                            } else {
                                self.null_device_granted = false;
                            }
                        }
                    }
                    SetupStep::ExemptLoopback => {
                        if let Some(lease) = &mut self.loopback_lease {
                            if let Err(error) = lease.release() {
                                failures.push(format!(
                                    "could not remove the loopback exemption: {error}"
                                ));
                            } else {
                                self.loopback_lease = None;
                            }
                        }
                    }
                    SetupStep::InstallNetworkFilters => {
                        // Closing the session is what removes them; taking the field
                        // here makes that happen at this point in the order rather
                        // than whenever the struct finishes dropping.
                        drop(self.filters.take());
                    }
                    SetupStep::EnsureContainer => {
                        if let Err(error) = self.profile.close() {
                            failures
                                .push(format!("could not delete the execution profile: {error}"));
                        }
                    }
                    SetupStep::OpenPseudoConsole => {}
                }
                if !failures.is_empty() {
                    // Keep this step and all earlier ones. In particular, a failed
                    // permission rollback must retain the profile used by its SID.
                    return failures;
                }
                self.completed.pop();
            }
            Vec::new()
        }

        fn report_pending_cleanup(&self) {
            if self.completed.is_empty() {
                return;
            }
            // The host captures wrapper stderr. Preserve exact ownership and ACL
            // images there; never select a recovery file from the target's env or
            // automatically overwrite a descriptor that may have changed later.
            diagnostic!(
                "KLAUS_SANDBOX_RECOVERY_JSON={}",
                serde_json::json!({
                    "version": 1,
                    "profile": self.profile.name(),
                    "creatorUserSid": self.profile.creator_user_sid(),
                    "profileOwned": self.profile.is_owned(),
                    "targetAndIoDrained": self.cleanup_permitted,
                    "pendingSteps": self.completed.iter().map(|step| format!("{step:?}")).collect::<Vec<_>>(),
                    "nullDeviceGranted": self.null_device_granted,
                    "loopbackLease": self.loopback_lease.as_ref().map(LoopbackLease::recovery_record),
                    "aclJournal": self.saved_dacls.iter().map(SavedDacl::recovery_record).collect::<Vec<_>>(),
                })
            );
        }
    }

    impl Drop for Installed {
        fn drop(&mut self) {
            if !self.cleanup_permitted {
                if let Some(filters) = self.filters.take() {
                    // A failed drain must not remove dynamic filters while this
                    // wrapper still owns a potentially live sandboxed target.
                    std::mem::forget(filters);
                }
            }
            self.report_pending_cleanup();
        }
    }

    /// Read the request, build the sandbox, run the target, report its exit code.
    ///
    /// `channel` is the inherited pipe named on the command line: its first frame is
    /// the request, and everything after it is control traffic. See
    /// [`crate::protocol`] for why the request does not travel on the command line.
    pub fn run(channel: usize) -> u32 {
        match run_inner(channel) {
            Ok(code) => code,
            Err(error) => {
                // Reported on stderr because the engine captures it, and named as a
                // sandbox failure so it cannot be mistaken for the command's own
                // diagnostics.
                diagnostic!("klaus sandbox: {error}");
                exit_code_for(None, true)
            }
        }
    }

    fn run_inner(channel: usize) -> Result<u32, WrapperError> {
        // One frame off the front is the request; the rest of this pipe is control
        // traffic, read later by the supervisor thread. Reading it here first is what
        // makes the hand-off safe: nothing else touches the pipe until the request is
        // fully consumed.
        let mut request_pipe = BorrowedPipe::new(channel);
        let envelope: NativeSandboxRequest =
            crate::protocol::read_frame(&mut request_pipe)?.ok_or(WrapperError::NoRequest)?;
        if let Some(context) = envelope.native {
            context
                .validate()
                .map_err(|error| WrapperError::Native(anyhow::anyhow!(error)))?;
            let config_home = std::path::Path::new(&context.config_home);
            if !config_home.is_absolute() {
                return Err(WrapperError::Native(anyhow::anyhow!(
                    "native Windows configuration home must be absolute"
                )));
            }
            return match context.level {
                NativeSandboxLevel::Unelevated => {
                    run_unelevated(envelope.request, config_home, channel)
                }
                NativeSandboxLevel::Elevated => {
                    crate::elevated::run(&envelope.request, config_home, channel)
                        .map_err(WrapperError::Native)
                }
            };
        }
        let request = envelope.request;
        if request.command.is_empty() {
            return Err(WrapperError::EmptyCommand);
        }
        let capabilities = capability::resolve_for_request(&request)?;

        let mut trace = DiagnosticTrace::new(&request);
        trace.record("identity-start");
        let plan = setup_plan(&request);
        let profile =
            OwnedAppContainerProfile::create(&request.container_name).map_err(|failure| {
                let (error, recovery) = failure.into_parts();
                if let Some(profile) = recovery {
                    // A malformed successful native creation still owns storage, but
                    // no target or permissions exist yet. Preserve its name on failure.
                    let mut installed = Installed::new(profile);
                    for _ in 0..2 {
                        let failures = installed.cleanup();
                        if failures.is_empty() {
                            break;
                        }
                        for failure in failures {
                            diagnostic!(
                                "klaus sandbox: failed profile creation cleanup: {failure}"
                            );
                        }
                    }
                }
                error
            })?;
        trace.record("identity-complete");

        let mut installed = Installed::new(profile);
        let mut supervisor = None;

        // Keep setup failures inside the transaction, so cleanup errors remain
        // observable to the private diagnostic even when setup stopped halfway.
        let outcome = (|| -> Result<u32, WrapperError> {
            for step in plan.iter().skip(1) {
                trace.record(&format!("setup-{step:?}-start"));
                match step {
                    SetupStep::ApplyFilesystemAcl => {
                        let rules = acl::plan_entries(&request.filesystem);
                        // Record each attempted rule before its first write, so
                        // partial failures are recoverable without touching rules
                        // that setup never reached.
                        installed.completed.push(*step);
                        acl::apply_recorded_with_progress(
                            &rules,
                            installed.profile.sid().expect("new profile has a SID"),
                            &capabilities,
                            &mut installed.saved_dacls,
                            |index, rule| {
                                installed.acl_rules.push(rule.clone());
                                let kind = match rule {
                                    acl::Rule::Allow {
                                        grant, inheritable, ..
                                    } => {
                                        format!("{grant:?}-inherit-{inheritable}")
                                    }
                                    acl::Rule::Withhold { .. } => "withhold".to_owned(),
                                };
                                trace.record(&format!("acl-{index}-{kind}-start"));
                            },
                        )?;
                    }
                    SetupStep::GrantNullDevice => {
                        match null_device::grant(installed.container()) {
                            Ok(()) => {
                                installed.null_device_granted = true;
                                installed.completed.push(*step);
                            }
                            // An ordinary caller holds no WRITE_DAC on the device
                            // object, so the ACE cannot exist. Commands that
                            // redirect to NUL will report access denied there;
                            // the rest of the boundary must still install
                            // instead of failing as a whole.
                            Err(error) if error.is_dacl_write_denied() => {
                                installed.completed.push(*step);
                            }
                            Err(error) => return Err(error.into()),
                        }
                    }
                    SetupStep::ExemptLoopback => {
                        match LoopbackLease::acquire(installed.container()) {
                            Ok(lease) => {
                                installed.loopback_lease = Some(lease);
                                installed.completed.push(*step);
                            }
                            Err(error) => {
                                let (error, recovery) = error.into_parts();
                                if let Some(lease) = recovery {
                                    // A failed write/readback may still have changed
                                    // the table. Keep the obligation before returning.
                                    installed.loopback_lease = Some(lease);
                                    installed.completed.push(*step);
                                }
                                return Err(error.into());
                            }
                        }
                    }
                    SetupStep::InstallNetworkFilters => {
                        let plans = filter::plan_filters(&request.network);
                        installed.filters = FilterSession::install(&plans, installed.container())?;
                        installed.completed.push(*step);
                    }
                    SetupStep::OpenPseudoConsole | SetupStep::EnsureContainer => {}
                }
            }

            let pty = match request.terminal {
                Some(size) => Some(Pty::open(crate::pty::clamp_size(size))?),
                None => None,
            };

            trace.record("target-launch");
            launch_and_supervise(
                &request,
                &mut installed,
                &capabilities,
                pty,
                channel,
                &mut trace,
                &mut supervisor,
            )
        })();
        if let Err(error) = &outcome {
            diagnostic!("klaus sandbox: setup or target launch failed: {error}");
        }
        if let Some(supervisor) = supervisor.as_mut() {
            if !supervisor.is_quiescent() {
                // Retry the same owner once. A timeout never authorizes teardown.
                if let Err(error) = supervisor.shutdown() {
                    diagnostic!("klaus sandbox: supervision cleanup retry failed: {error}");
                }
            }
            installed.cleanup_permitted = supervisor.is_quiescent();
        }
        trace.record("cleanup-start");
        let mut cleanup_failures = installed.cleanup();
        for failure in &cleanup_failures {
            diagnostic!("klaus sandbox: {failure}");
        }
        if !cleanup_failures.is_empty() && installed.cleanup_permitted {
            cleanup_failures = installed.cleanup();
            for failure in &cleanup_failures {
                diagnostic!("klaus sandbox: cleanup retry failed: {failure}");
            }
        }
        trace.record(if cleanup_failures.is_empty() {
            "cleanup-complete"
        } else {
            "cleanup-incomplete"
        });
        // Ordinary command exit codes retain their existing contract. The engine's
        // own diagnostic is stricter: it must also prove its temporary grants left.
        if request.command.get(1).map(String::as_str) == Some("--windows-sandbox-probe")
            && !cleanup_failures.is_empty()
        {
            return Ok(super::EXIT_DIAGNOSTIC_CLEANUP_FAILED);
        }
        Ok(exit_code_for(Some(outcome?), false))
    }

    fn run_unelevated(
        mut request: SandboxRequest,
        config_home: &std::path::Path,
        channel: usize,
    ) -> Result<u32, WrapperError> {
        use windows_sys::Win32::System::Console::{
            GetStdHandle, STD_ERROR_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
        };
        if request.command.is_empty() {
            return Err(WrapperError::EmptyCommand);
        }
        let token = crate::unelevated::prepare(&mut request, config_home)?;
        let pty = request
            .terminal
            .map(|size| Pty::open(crate::pty::clamp_size(size)))
            .transpose()?;
        let (stdin, stdout, stderr, inherit) = if pty.is_some() {
            (0, 0, 0, Vec::new())
        } else {
            let input = unsafe { GetStdHandle(STD_INPUT_HANDLE) };
            let output = unsafe { GetStdHandle(STD_OUTPUT_HANDLE) };
            let error = unsafe { GetStdHandle(STD_ERROR_HANDLE) };
            (input, output, error, vec![input, output, error])
        };
        let control = duplicate_file(channel)?;
        let target = process::launch(LaunchSpec {
            command_line: process::command_line(&request.command),
            working_directory: request.cwd.as_str(),
            environment: process::environment_block(&request.env),
            identity: process::LaunchIdentity::RestrictedToken(
                token.as_raw() as windows_sys::Win32::Foundation::HANDLE
            ),
            inherit,
            stdin,
            stdout,
            stderr,
            pseudoconsole: pty.as_ref().map(Pty::console),
        })?;
        let mut supervisor = Supervisor::new(target, pty, control);
        supervisor.run()
    }

    fn launch_and_supervise(
        request: &SandboxRequest,
        installed: &mut Installed,
        capabilities: &[capability::CapabilitySid],
        pty: Option<Pty>,
        channel: usize,
        trace: &mut DiagnosticTrace,
        supervisor: &mut Option<Supervisor>,
    ) -> Result<u32, WrapperError> {
        use windows_sys::Win32::Foundation::HANDLE;
        use windows_sys::Win32::System::Console::GetStdHandle;
        use windows_sys::Win32::System::Console::{
            STD_ERROR_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
        };

        // Without a terminal the target uses the wrapper's own standard handles,
        // which are already the pipes the engine is reading. Relaying bytes through
        // this process instead would add a copy and a place for output to be lost.
        let (stdin, stdout, stderr, inherit) = match &pty {
            None => {
                let stdin = unsafe { GetStdHandle(STD_INPUT_HANDLE) };
                let stdout = unsafe { GetStdHandle(STD_OUTPUT_HANDLE) };
                let stderr = unsafe { GetStdHandle(STD_ERROR_HANDLE) };
                (stdin, stdout, stderr, vec![stdin, stdout, stderr])
            }
            Some(_) => {
                // With a pseudoconsole the console owns the target's stdio, so no
                // standard handles are passed and nothing needs inheriting: the
                // console's own ends were handed to it at creation, and the wrapper
                // relays between the console and its own stdio. Naming handles here
                // would hand the target file descriptors it has no use for.
                let handles: Vec<HANDLE> = Vec::new();
                (0, 0, 0, handles)
            }
        };

        let spec = LaunchSpec {
            command_line: process::command_line(&request.command),
            working_directory: request.cwd.as_str(),
            environment: process::environment_block(&request.env),
            identity: process::LaunchIdentity::AppContainer {
                container: installed.container(),
                capabilities,
            },
            inherit,
            stdin,
            stdout,
            stderr,
            pseudoconsole: pty.as_ref().map(|pty| pty.console()),
        };
        let control = duplicate_file(channel)?;
        let target = process::launch(spec)?;
        installed.cleanup_permitted = false;
        let supervisor = supervisor.insert(Supervisor::new(target, pty, control));
        trace.record("target-started");

        let outcome = supervisor.run();
        installed.cleanup_permitted = supervisor.is_quiescent();
        trace.record("target-supervision-complete");
        outcome
    }

    /// Own the target and all supervision resources through explicit shutdown.
    /// An error is not a drain receipt: callers must inspect `is_quiescent` before
    /// removing any security boundary, and can retry `shutdown` on this same owner.
    #[must_use]
    pub struct Supervisor {
        target: Option<process::Target>,
        pty: Option<Pty>,
        control: Option<std::fs::File>,
        reader: Option<ControlReader>,
        relays: Option<PtyRelays>,
        closer: Option<IoThread>,
        tree_drained: bool,
    }

    impl Supervisor {
        pub fn new(target: process::Target, pty: Option<Pty>, control: std::fs::File) -> Self {
            let mut owner = Self {
                target: Some(target),
                pty,
                control: Some(control),
                reader: None,
                relays: None,
                closer: None,
                tree_drained: false,
            };
            if let Some(pty) = owner.pty.as_mut() {
                pty.close_launch_handles();
            }
            owner
        }

        pub fn run(&mut self) -> Result<u32, WrapperError> {
            let outcome = (|| -> Result<u32, WrapperError> {
                let control = self.control.take().ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "sandbox supervisor cannot be run twice",
                    )
                })?;
                self.reader = Some(ControlReader::start(control)?);
                if let Some(pty) = self.pty.as_mut() {
                    let relays = self.relays.insert(PtyRelays::default());
                    relays.start(pty)?;
                }
                let target = self.target.as_ref().expect("supervisor owns target");
                let reader = self.reader.as_mut().expect("control reader initialized");
                let mut terminated = false;
                loop {
                    if let Some(outcome) = target.try_wait()? {
                        return Ok(if terminated {
                            super::EXIT_TERMINATED
                        } else {
                            outcome.exit_code
                        });
                    }
                    match reader.receive(Duration::from_millis(20)) {
                        Some(ControlMessage::Resize(size)) => {
                            if let Some(pty) = self.pty.as_ref() {
                                if let Err(error) = pty.resize(crate::pty::clamp_size(size)) {
                                    diagnostic!("klaus sandbox: terminal resize failed: {error}");
                                }
                            }
                        }
                        Some(ControlMessage::Interrupt) => target.terminate()?,
                        Some(ControlMessage::Terminate) => {
                            terminated = true;
                            target.terminate()?;
                        }
                        None => {}
                    }
                }
            })();
            let cleanup = self.shutdown();
            if let (Err(operation), Err(cleanup)) = (&outcome, &cleanup) {
                diagnostic!("klaus sandbox: supervision failed: {operation}; cleanup: {cleanup}");
            }
            cleanup?;
            outcome
        }

        /// A positive receipt requires actual process and thread completion.
        /// A joined I/O worker may report an I/O error and still be quiescent.
        pub fn is_quiescent(&self) -> bool {
            self.tree_drained
                && self.control.is_none()
                && self
                    .reader
                    .as_ref()
                    .is_none_or(|reader| reader.worker.is_drained())
                && self.relays.as_ref().is_none_or(PtyRelays::is_drained)
                && self.closer.as_ref().is_none_or(IoThread::is_drained)
                && self.pty.is_none()
        }

        pub fn shutdown(&mut self) -> Result<(), WrapperError> {
            self.control.take();
            let control = self
                .reader
                .as_mut()
                .map_or(Ok(()), |reader| reader.finish(Duration::from_millis(200)));
            // Keep the same process handles on every attempt. The console and
            // relays stay owned when Job or process-object drainage is uncertain.
            if !self.tree_drained {
                self.target
                    .as_ref()
                    .expect("supervisor owns target")
                    .terminate_and_wait()?;
                self.tree_drained = true;
            }
            let relays = match self.relays.as_mut() {
                None => close_pty(&mut self.pty, &mut self.closer, Duration::from_millis(1500)),
                Some(relays) => {
                    relays.finish(&mut self.pty, &mut self.closer, Duration::from_millis(1500))
                }
            };
            control?;
            relays?;
            if !self.is_quiescent() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "sandbox supervision still owns pending resources; retry shutdown",
                )
                .into());
            }
            Ok(())
        }
    }

    impl Drop for Supervisor {
        fn drop(&mut self) {
            if self.is_quiescent() {
                return;
            }
            // Last-resort ownership retention in the one-command wrapper. Drop
            // must not close a pending console or discard the only drain handles.
            // The process exit still lets Windows close its remaining handles.
            diagnostic!(
                "klaus sandbox: retaining unconfirmed target and I/O resources until wrapper exit"
            );
            if let Some(target) = self.target.take() {
                process::terminate_tree(target.tree_handle());
                std::mem::forget(target);
            }
            if let Some(mut reader) = self.reader.take() {
                reader.receiver.take();
                reader.worker.request_stop();
                std::mem::forget(reader);
            }
            if let Some(relays) = self.relays.take() {
                relays.request_stop();
                std::mem::forget(relays);
            }
            if let Some(closer) = self.closer.take() {
                std::mem::forget(closer);
            }
            if let Some(pty) = self.pty.take() {
                std::mem::forget(pty);
            }
        }
    }

    struct ControlReader {
        receiver: Option<std::sync::mpsc::Receiver<ControlMessage>>,
        worker: IoThread,
    }

    impl ControlReader {
        fn start(control: std::fs::File) -> std::io::Result<Self> {
            let (sender, receiver) = std::sync::mpsc::sync_channel(64);
            let worker = IoThread::start("klaus-sandbox-control", move |stop| {
                let mut control = CancellableFile::new(control, Arc::clone(&stop));
                while !stop.load(Ordering::Acquire) {
                    let message = match crate::protocol::read_frame(&mut control) {
                        Ok(Some(message)) => message,
                        // EOF means the host has finished sending, not that it
                        // requested cancellation of a command still doing work.
                        Ok(None) => return Ok(()),
                        Err(error) => {
                            if !stop.load(Ordering::Acquire) {
                                diagnostic!("klaus sandbox: control channel error: {error}");
                            }
                            return Ok(());
                        }
                    };
                    if stop.load(Ordering::Acquire) || sender.send(message).is_err() {
                        return Ok(());
                    }
                }
                Ok(())
            })?;
            Ok(Self {
                receiver: Some(receiver),
                worker,
            })
        }

        fn receive(&mut self, timeout: Duration) -> Option<ControlMessage> {
            let Some(receiver) = self.receiver.as_ref() else {
                std::thread::sleep(timeout);
                return None;
            };
            match receiver.recv_timeout(timeout) {
                Ok(message) => Some(message),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => None,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    self.receiver.take();
                    None
                }
            }
        }

        fn finish(&mut self, timeout: Duration) -> std::io::Result<()> {
            self.receiver.take();
            self.worker.join_with_timeout(timeout, true)
        }
    }

    /// A synchronous I/O worker owns every File it uses, even on a cleanup timeout.
    /// Cancellation is retried until the actual JoinHandle reports completion;
    /// ERROR_NOT_FOUND only means no I/O was pending at that particular instant.
    struct IoThread {
        stop: Arc<AtomicBool>,
        thread: Option<std::thread::JoinHandle<std::io::Result<()>>>,
        name: &'static str,
    }

    impl IoThread {
        fn start(
            name: &'static str,
            run: impl FnOnce(Arc<AtomicBool>) -> std::io::Result<()> + Send + 'static,
        ) -> std::io::Result<Self> {
            let stop = Arc::new(AtomicBool::new(false));
            let thread_stop = Arc::clone(&stop);
            let thread = std::thread::Builder::new()
                .name(name.to_owned())
                .spawn(move || run(thread_stop))?;
            Ok(Self {
                stop,
                thread: Some(thread),
                name,
            })
        }

        fn is_drained(&self) -> bool {
            self.thread.is_none()
        }

        fn request_stop(&self) {
            self.stop.store(true, Ordering::Release);
            self.cancel_pending_io();
        }

        fn cancel_pending_io(&self) {
            use std::os::windows::io::AsRawHandle;
            if let Some(thread) = self.thread.as_ref() {
                // The JoinHandle owns this thread handle until the joined thread
                // has stopped. Cancellation success is not used as exit evidence.
                unsafe {
                    windows_sys::Win32::System::IO::CancelSynchronousIo(
                        thread.as_raw_handle() as isize
                    )
                };
            }
        }

        fn join_with_timeout(&mut self, timeout: Duration, cancel: bool) -> std::io::Result<()> {
            if cancel {
                self.stop.store(true, Ordering::Release);
            }
            let started = Instant::now();
            loop {
                let Some(thread) = self.thread.as_ref() else {
                    return Ok(());
                };
                if thread.is_finished() {
                    return self
                        .thread
                        .take()
                        .expect("finished worker retained")
                        .join()
                        .map_err(|_| std::io::Error::other(format!("{} panicked", self.name)))?;
                }
                if cancel {
                    self.cancel_pending_io();
                }
                let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!(
                            "{} did not finish within {} ms",
                            self.name,
                            timeout.as_millis()
                        ),
                    ));
                };
                std::thread::sleep(remaining.min(Duration::from_millis(5)));
            }
        }
    }

    impl Drop for IoThread {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            self.cancel_pending_io();
            // Normal and failed supervision explicitly join with a shared deadline.
            // An OS I/O operation may refuse cancellation; a remaining worker still
            // owns its Files and cannot operate on a reused Target/Pty raw handle.
        }
    }

    /// Framing and write_all may retry Interrupted errors internally. Check the
    /// stop flag before every retry and use a non-retryable error once cancelled.
    struct CancellableFile {
        file: std::fs::File,
        stop: Arc<AtomicBool>,
    }

    impl CancellableFile {
        fn new(file: std::fs::File, stop: Arc<AtomicBool>) -> Self {
            Self { file, stop }
        }

        fn check_running(&self) -> std::io::Result<()> {
            if self.stop.load(Ordering::Acquire) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "sandbox I/O worker stopped",
                ));
            }
            Ok(())
        }
    }

    impl std::io::Read for CancellableFile {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            self.check_running()?;
            std::io::Read::read(&mut self.file, buffer)
        }
    }

    impl std::io::Write for CancellableFile {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.check_running()?;
            std::io::Write::write(&mut self.file, buffer)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.check_running()?;
            std::io::Write::flush(&mut self.file)
        }
    }

    fn duplicate_file(handle: usize) -> std::io::Result<std::fs::File> {
        use std::os::windows::io::FromRawHandle;
        use windows_sys::Win32::Foundation::{DuplicateHandle, DUPLICATE_SAME_ACCESS};
        use windows_sys::Win32::System::Threading::GetCurrentProcess;
        let process = unsafe { GetCurrentProcess() };
        let mut duplicate = 0;
        if unsafe {
            DuplicateHandle(
                process,
                handle as isize,
                process,
                &mut duplicate,
                0,
                0,
                DUPLICATE_SAME_ACCESS,
            )
        } == 0
        {
            return Err(std::io::Error::last_os_error());
        }
        Ok(unsafe { std::fs::File::from_raw_handle(duplicate as *mut std::ffi::c_void) })
    }

    /// Move bytes through independently owned handles, then confirm both workers
    /// stopped. The output reader remains active while ClosePseudoConsole flushes.
    #[derive(Default)]
    struct PtyRelays {
        to_host: Option<IoThread>,
        to_target: Option<IoThread>,
    }

    impl PtyRelays {
        fn is_drained(&self) -> bool {
            self.to_host.as_ref().is_none_or(IoThread::is_drained)
                && self.to_target.as_ref().is_none_or(IoThread::is_drained)
        }

        fn request_stop(&self) {
            for worker in [&self.to_host, &self.to_target].into_iter().flatten() {
                worker.request_stop();
            }
        }

        fn start(&mut self, pty: &mut Pty) -> std::io::Result<()> {
            use std::io::{Read, Write};
            use windows_sys::Win32::System::Console::{
                GetStdHandle, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
            };
            let host_out = duplicate_file(unsafe { GetStdHandle(STD_OUTPUT_HANDLE) } as usize)?;
            let mut host_in = duplicate_file(unsafe { GetStdHandle(STD_INPUT_HANDLE) } as usize)?;
            let (console_in, mut console_out) = pty.take_io();
            self.to_host = Some(IoThread::start("klaus-sandbox-output", move |stop| {
                let mut host_out = CancellableFile::new(host_out, Arc::clone(&stop));
                let mut buffer = [0u8; 8192];
                let mut output_error = None;
                // Stopping host forwarding must not retire ConPTY's reader
                // while ClosePseudoConsole can still flush output. Cancellation
                // may hit either the blocked host write or this console read.
                loop {
                    match console_out.read(&mut buffer) {
                        Ok(0) => break,
                        Err(error) if error.kind() == std::io::ErrorKind::BrokenPipe => break,
                        Err(error)
                            if stop.load(Ordering::Acquire)
                                && error.raw_os_error()
                                    == Some(
                                        windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED
                                            as i32,
                                    ) =>
                        {
                            continue;
                        }
                        Err(error) => return Err(error),
                        Ok(read) => {
                            if output_error.is_none() && !stop.load(Ordering::Acquire) {
                                if let Err(error) = host_out.write_all(&buffer[..read]) {
                                    // Keep draining ConPTY after the host disappears;
                                    // otherwise console shutdown can block on output.
                                    output_error = Some(error);
                                }
                            }
                        }
                    }
                }
                output_error.map_or(Ok(()), Err)
            })?);
            self.to_target = Some(IoThread::start("klaus-sandbox-input", move |stop| {
                let mut console_in = CancellableFile::new(console_in, Arc::clone(&stop));
                let mut buffer = [0u8; 8192];
                while !stop.load(Ordering::Acquire) {
                    let read = match host_in.read(&mut buffer) {
                        Ok(0) => return Ok(()),
                        Ok(read) => read,
                        Err(_) if stop.load(Ordering::Acquire) => return Ok(()),
                        Err(error) => return Err(error),
                    };
                    if stop.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    if let Err(error) = console_in.write_all(&buffer[..read]) {
                        if stop.load(Ordering::Acquire)
                            || error.kind() == std::io::ErrorKind::BrokenPipe
                        {
                            return Ok(());
                        }
                        return Err(error);
                    }
                }
                Ok(())
            })?);
            Ok(())
        }

        fn finish(
            &mut self,
            pty: &mut Option<Pty>,
            closer: &mut Option<IoThread>,
            timeout: Duration,
        ) -> std::io::Result<()> {
            let started = Instant::now();
            let input = self.to_target.as_mut().map_or(Ok(()), |worker| {
                worker.join_with_timeout(Duration::from_millis(200), true)
            });
            let close_start_error = start_pty_close(pty, closer).err();
            let output = self.to_host.as_mut().map_or(Ok(()), |worker| {
                let result = worker.join_with_timeout(
                    timeout
                        .saturating_sub(started.elapsed())
                        .min(Duration::from_millis(800)),
                    close_start_error.is_some(),
                );
                if result.is_err() {
                    let _ = worker.join_with_timeout(
                        timeout
                            .saturating_sub(started.elapsed())
                            .min(Duration::from_millis(200)),
                        true,
                    );
                }
                result
            });
            let console = closer.as_mut().map_or(Ok(()), |worker| {
                worker.join_with_timeout(timeout.saturating_sub(started.elapsed()), false)
            });
            // Failed starts and timed-out joins remain in the Supervisor. A later
            // attempt must join the original closer, not transfer the console again.
            input?;
            if let Some(error) = close_start_error {
                return Err(error);
            }
            output?;
            console
        }
    }

    fn start_pty_close(
        pty: &mut Option<Pty>,
        closer: &mut Option<IoThread>,
    ) -> std::io::Result<()> {
        if pty.is_none() {
            return Ok(());
        }
        if let Some(worker) = closer.as_ref() {
            if !worker.is_drained() {
                return Ok(());
            }
            // Only a failed transfer can retain both a console and an old worker.
            // Its original JoinHandle must be joined before another attempt starts.
            *closer = None;
        }
        let (sender, receiver) = std::sync::mpsc::channel::<Pty>();
        *closer = Some(IoThread::start("klaus-sandbox-console-close", move |_| {
            if let Ok(pty) = receiver.recv() {
                drop(pty);
            }
            Ok(())
        })?);
        if let Err(error) = sender.send(pty.take().expect("console retained until transfer")) {
            *pty = Some(error.0);
            return Err(std::io::Error::other(
                "console close worker ended before transfer",
            ));
        }
        Ok(())
    }

    fn close_pty(
        pty: &mut Option<Pty>,
        closer: &mut Option<IoThread>,
        timeout: Duration,
    ) -> std::io::Result<()> {
        start_pty_close(pty, closer)?;
        if let Some(worker) = closer.as_mut() {
            worker.join_with_timeout(timeout, false)?;
        }
        Ok(())
    }

    /// Read and write a handle this process does not own.
    ///
    /// The handles involved belong to the parent's pipes or to the pseudoconsole, so
    /// closing them here would cut a channel that is still in use. `File` closes on
    /// drop, so it cannot be used directly; this wrapper borrows the handle for I/O
    /// and leaves it open.
    struct BorrowedPipe {
        file: std::mem::ManuallyDrop<std::fs::File>,
    }

    impl BorrowedPipe {
        fn new(handle: usize) -> Self {
            use std::os::windows::io::FromRawHandle;
            Self {
                file: std::mem::ManuallyDrop::new(unsafe {
                    std::fs::File::from_raw_handle(handle as *mut std::ffi::c_void)
                }),
            }
        }
    }

    impl std::io::Read for BorrowedPipe {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            std::io::Read::read(&mut *self.file, buf)
        }
    }

    impl std::io::Write for BorrowedPipe {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            std::io::Write::write(&mut *self.file, buf)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            std::io::Write::flush(&mut *self.file)
        }
    }
}

#[cfg(windows)]
pub use imp::{run, Supervisor, WrapperError};

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::boundary::{AbsPath, FilesystemBoundary, NetworkBoundary};
    use crate::protocol::TerminalSize;

    fn abs(path: &str) -> AbsPath {
        AbsPath::new(path).expect("test path parses")
    }

    fn request(filesystem: FilesystemBoundary, network: NetworkBoundary) -> SandboxRequest {
        SandboxRequest {
            command: vec!["cmd.exe".into()],
            cwd: abs("C:\\ws"),
            env: BTreeMap::new(),
            filesystem,
            network,
            network_route_capability_sid: None,
            container_name: "klaus-ws".into(),
            terminal: None,
        }
    }

    #[test]
    fn the_container_is_always_established_first() {
        // Every other step is keyed on the SID this produces.
        let plan = setup_plan(&request(
            FilesystemBoundary::default(),
            NetworkBoundary::Denied,
        ));
        assert_eq!(plan.first(), Some(&SetupStep::EnsureContainer));
    }

    #[test]
    fn a_denied_network_installs_neither_filters_nor_an_exemption() {
        // Denied is enforced by withholding the capability; adding either of these
        // would imply the boundary depends on them succeeding.
        let plan = setup_plan(&request(
            FilesystemBoundary {
                writable: vec![abs("C:\\ws")],
                ..Default::default()
            },
            NetworkBoundary::Denied,
        ));
        assert!(!plan.contains(&SetupStep::ExemptLoopback));
        assert!(!plan.contains(&SetupStep::InstallNetworkFilters));
        assert!(plan.contains(&SetupStep::ApplyFilesystemAcl));
    }

    #[test]
    fn an_allowed_network_includes_loopback_without_restrictive_filters() {
        let plan = setup_plan(&request(
            FilesystemBoundary::default(),
            NetworkBoundary::Allowed,
        ));
        assert!(!plan.contains(&SetupStep::InstallNetworkFilters));
        assert!(plan.contains(&SetupStep::ExemptLoopback));
    }

    #[test]
    fn proxy_mode_installs_the_exemption_before_the_filters() {
        // The exemption lifts the AppContainer loopback block; the filters then scope
        // what is reachable. Installing filters first would briefly leave the
        // container able to reach nothing, but more importantly the ordering is what
        // the teardown reversal depends on.
        let plan = setup_plan(&request(
            FilesystemBoundary::default(),
            NetworkBoundary::LoopbackPorts {
                ports: vec![8080],
                allow_local_binding: false,
            },
        ));
        let exempt = plan
            .iter()
            .position(|step| *step == SetupStep::ExemptLoopback)
            .expect("exemption planned");
        let filters = plan
            .iter()
            .position(|step| *step == SetupStep::InstallNetworkFilters)
            .expect("filters planned");
        assert!(exempt < filters);
    }

    #[test]
    fn a_boundary_that_restricts_nothing_plans_no_acl_work() {
        let plan = setup_plan(&request(
            FilesystemBoundary::default(),
            NetworkBoundary::Denied,
        ));
        assert!(!plan.contains(&SetupStep::ApplyFilesystemAcl));
        // The null device is still granted: it is not part of any boundary, it is a
        // gap in what an AppContainer can reach at all.
        assert_eq!(
            plan,
            vec![SetupStep::EnsureContainer, SetupStep::GrantNullDevice]
        );
    }

    #[test]
    fn the_null_device_is_granted_whatever_the_policy_says() {
        // A target that cannot open `NUL` fails on most real command lines, including
        // the `.cmd` shims that front Node tooling, and it fails as "access denied"
        // with nothing pointing at the bit bucket. So this is unconditional.
        for boundary in [
            FilesystemBoundary::default(),
            FilesystemBoundary {
                writable: vec![abs("C:\\ws")],
                ..Default::default()
            },
        ] {
            for network in [
                NetworkBoundary::Denied,
                NetworkBoundary::Allowed,
                NetworkBoundary::LoopbackPorts {
                    ports: vec![8080],
                    allow_local_binding: false,
                },
            ] {
                let plan = setup_plan(&request(boundary.clone(), network));
                assert!(
                    plan.contains(&SetupStep::GrantNullDevice),
                    "null device missing from {plan:?}"
                );
            }
        }
    }

    #[test]
    fn a_deny_only_boundary_still_plans_acl_work() {
        // Denials matter even with no grants: this is the "confine reads of secrets
        // but do not restrict writes" shape.
        let plan = setup_plan(&request(
            FilesystemBoundary {
                deny_read: vec![abs("C:\\users\\me\\.ssh")],
                ..Default::default()
            },
            NetworkBoundary::Denied,
        ));
        assert!(plan.contains(&SetupStep::ApplyFilesystemAcl));
    }

    #[test]
    fn a_terminal_request_plans_a_pseudoconsole_last() {
        let mut spec = request(
            FilesystemBoundary {
                writable: vec![abs("C:\\ws")],
                ..Default::default()
            },
            NetworkBoundary::Denied,
        );
        spec.terminal = Some(TerminalSize { rows: 24, cols: 80 });
        let plan = setup_plan(&spec);
        assert_eq!(plan.last(), Some(&SetupStep::OpenPseudoConsole));
    }

    #[test]
    fn teardown_is_setup_reversed() {
        let completed = vec![
            SetupStep::EnsureContainer,
            SetupStep::ApplyFilesystemAcl,
            SetupStep::GrantNullDevice,
            SetupStep::ExemptLoopback,
            SetupStep::InstallNetworkFilters,
        ];
        assert_eq!(
            teardown_plan(&completed),
            vec![
                SetupStep::InstallNetworkFilters,
                SetupStep::ExemptLoopback,
                SetupStep::GrantNullDevice,
                SetupStep::ApplyFilesystemAcl,
                SetupStep::EnsureContainer,
            ]
        );
    }

    #[test]
    fn teardown_deletes_the_execution_profile_after_its_permissions() {
        let completed = vec![SetupStep::EnsureContainer, SetupStep::ApplyFilesystemAcl];
        assert_eq!(
            teardown_plan(&completed).last(),
            Some(&SetupStep::EnsureContainer)
        );
    }

    #[test]
    fn teardown_only_undoes_what_actually_ran() {
        // Setup that failed partway must not try to undo steps it never reached; a
        // revoke against an ACE that was never written reports a failure that means
        // nothing.
        let completed = vec![SetupStep::EnsureContainer, SetupStep::ApplyFilesystemAcl];
        let teardown = teardown_plan(&completed);
        assert_eq!(
            teardown,
            vec![SetupStep::ApplyFilesystemAcl, SetupStep::EnsureContainer]
        );
        assert!(!teardown.contains(&SetupStep::InstallNetworkFilters));
        assert!(!teardown.contains(&SetupStep::ExemptLoopback));
    }

    #[test]
    fn the_targets_exit_code_is_what_the_wrapper_reports() {
        // Including zero: a successful command must not be turned into a failure, and
        // a failed one must not be turned into a success.
        assert_eq!(exit_code_for(Some(0), false), 0);
        assert_eq!(exit_code_for(Some(1), false), 1);
        assert_eq!(exit_code_for(Some(127), false), 127);
    }

    #[test]
    fn a_cleanup_problem_does_not_change_the_targets_answer() {
        // Cleanup is the wrapper's business. Reporting it as the command's status
        // would make a successful build look failed.
        assert_eq!(exit_code_for(Some(0), true), 0);
    }

    #[test]
    fn a_setup_failure_is_distinguishable_from_a_command_that_exited_one() {
        let code = exit_code_for(None, true);
        assert_eq!(code, EXIT_SANDBOX_SETUP_FAILED);
        assert_ne!(code, 0);
        assert_ne!(code, 1);
    }

    #[test]
    fn termination_before_the_target_finished_has_its_own_code() {
        let code = exit_code_for(None, false);
        assert_eq!(code, EXIT_TERMINATED);
        assert_ne!(code, EXIT_SANDBOX_SETUP_FAILED);
        assert_ne!(code, 0);
    }
}
