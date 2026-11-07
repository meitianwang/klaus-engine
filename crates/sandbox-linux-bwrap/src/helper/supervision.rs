// Copyright 2025 OpenAI. Licensed under the Apache License, Version 2.0.
// Modified for the engine's embedded launcher and exact operating-system arguments.
//! Namespace child lifecycle, protected mount ownership, and cleanup supervision.
use std::ffi::{CString, OsString};
use std::fs::{self, File, Metadata, OpenOptions};
use std::io::{self, Read};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::Duration;

static BWRAP_CHILD_PID: AtomicI32 = AtomicI32::new(0);
static PENDING_FORWARDED_SIGNAL: AtomicI32 = AtomicI32::new(0);
const FORWARDED_SIGNALS: &[libc::c_int] =
    &[libc::SIGHUP, libc::SIGINT, libc::SIGQUIT, libc::SIGTERM];
const SYNTHETIC_MOUNT_MARKER_SYNTHETIC: &[u8] = b"synthetic\n";
const SYNTHETIC_MOUNT_MARKER_EXISTING: &[u8] = b"existing\n";
const PROTECTED_CREATE_MARKER: &[u8] = b"protected-create\n";
#[derive(Debug)]
struct SyntheticMountTargetRegistration {
    target: SyntheticMountTarget,
    marker_file: PathBuf,
    marker_dir: PathBuf,
}

#[derive(Debug)]
struct ProtectedCreateTargetRegistration {
    target: ProtectedCreateTarget,
    marker_file: PathBuf,
    marker_dir: PathBuf,
}

struct ProtectedCreateMonitor {
    stop: Arc<AtomicBool>,
    violation: Arc<AtomicBool>,
    handle: thread::JoinHandle<()>,
}

struct ProtectedCreateWatcher {
    fd: libc::c_int,
    _watches: Vec<libc::c_int>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProtectedCreateRemoval {
    Directory,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FileIdentity {
    dev: u64,
    ino: u64,
}

impl FileIdentity {
    fn from_metadata(metadata: &Metadata) -> Self {
        Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SyntheticMountTargetKind {
    EmptyFile,
    EmptyDirectory,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct SyntheticMountTarget {
    path: PathBuf,
    kind: SyntheticMountTargetKind,
    // If an empty metadata path was already present, remember its inode so
    // cleanup does not delete a real pre-existing file or directory.
    pre_existing_path: Option<FileIdentity>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ProtectedCreateTarget {
    path: PathBuf,
}

impl ProtectedCreateTarget {
    pub(super) fn missing(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
        }
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }
}

impl SyntheticMountTarget {
    pub(super) fn missing(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
            kind: SyntheticMountTargetKind::EmptyFile,
            pre_existing_path: None,
        }
    }

    pub(super) fn missing_empty_directory(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
            kind: SyntheticMountTargetKind::EmptyDirectory,
            pre_existing_path: None,
        }
    }

    pub(super) fn existing_empty_file(path: &Path, metadata: &Metadata) -> Self {
        Self {
            path: path.to_path_buf(),
            kind: SyntheticMountTargetKind::EmptyFile,
            pre_existing_path: Some(FileIdentity::from_metadata(metadata)),
        }
    }

    pub(super) fn existing_empty_directory(path: &Path, metadata: &Metadata) -> Self {
        Self {
            path: path.to_path_buf(),
            kind: SyntheticMountTargetKind::EmptyDirectory,
            pre_existing_path: Some(FileIdentity::from_metadata(metadata)),
        }
    }

    pub(super) fn preserves_pre_existing_path(&self) -> bool {
        self.pre_existing_path.is_some()
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn kind(&self) -> SyntheticMountTargetKind {
        self.kind
    }

    pub(super) fn should_remove_after_bwrap(&self, metadata: &Metadata) -> bool {
        match self.kind {
            SyntheticMountTargetKind::EmptyFile => {
                if !metadata.file_type().is_file() || metadata.len() != 0 {
                    return false;
                }
            }
            SyntheticMountTargetKind::EmptyDirectory => {
                if !metadata.file_type().is_dir() {
                    return false;
                }
            }
        }

        match self.pre_existing_path {
            Some(pre_existing_path) => pre_existing_path != FileIdentity::from_metadata(metadata),
            None => true,
        }
    }
}

pub(super) struct BwrapArgs {
    pub args: Vec<OsString>,
    pub preserved_files: Vec<File>,
    pub synthetic_mount_targets: Vec<SyntheticMountTarget>,
    pub protected_create_targets: Vec<ProtectedCreateTarget>,
}

fn exec_bwrap(args: Vec<OsString>, preserved_files: Vec<File>) -> ! {
    for file in &preserved_files {
        let descriptor = file.as_raw_fd();
        let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
        if flags < 0
            || unsafe { libc::fcntl(descriptor, libc::F_SETFD, flags & !libc::FD_CLOEXEC) } < 0
        {
            panic!(
                "failed to retain namespace descriptor: {}",
                io::Error::last_os_error()
            );
        }
    }
    crate::run(&args)
}
pub(super) fn run_or_exec_bwrap(bwrap_args: BwrapArgs) -> ! {
    if bwrap_args.synthetic_mount_targets.is_empty()
        && bwrap_args.protected_create_targets.is_empty()
    {
        exec_bwrap(bwrap_args.args, bwrap_args.preserved_files);
    }
    run_bwrap_in_child_with_synthetic_mount_cleanup(bwrap_args);
}

fn run_bwrap_in_child_with_synthetic_mount_cleanup(bwrap_args: BwrapArgs) -> ! {
    let BwrapArgs {
        args,
        preserved_files,
        synthetic_mount_targets,
        protected_create_targets,
    } = bwrap_args;
    let setup_signal_mask = ForwardedSignalMask::block();
    let synthetic_mount_registrations = register_synthetic_mount_targets(&synthetic_mount_targets);
    let protected_create_registrations =
        register_protected_create_targets(&protected_create_targets);
    let exec_start_pipe = create_exec_start_pipe(!protected_create_targets.is_empty());
    let parent_pid = unsafe { libc::getpid() };
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        let err = std::io::Error::last_os_error();
        panic!("failed to fork for bubblewrap: {err}");
    }

    if pid == 0 {
        reset_forwarded_signal_handlers_to_default();
        setup_signal_mask.restore();
        let setpgid_res = unsafe { libc::setpgid(0, 0) };
        if setpgid_res < 0 {
            let err = std::io::Error::last_os_error();
            panic!("failed to place bubblewrap child in its own process group: {err}");
        }
        terminate_with_parent(parent_pid);
        wait_for_parent_exec_start(exec_start_pipe[0], exec_start_pipe[1]);
        exec_bwrap(args, preserved_files);
    }

    drop(preserved_files);
    close_child_exec_start_read(exec_start_pipe[0]);
    let protected_create_monitor = ProtectedCreateMonitor::start(&protected_create_targets);
    let signal_forwarders = install_bwrap_signal_forwarders(pid);
    release_child_exec_start(exec_start_pipe[1]);
    setup_signal_mask.restore();
    let status = wait_for_bwrap_child(pid);
    let cleanup_signal_mask = ForwardedSignalMask::block();
    BWRAP_CHILD_PID.store(0, Ordering::SeqCst);
    let protected_create_monitor_violation = protected_create_monitor
        .map(ProtectedCreateMonitor::stop)
        .unwrap_or(false);
    cleanup_synthetic_mount_targets(&synthetic_mount_registrations);
    let protected_create_violation = protected_create_monitor_violation
        || cleanup_protected_create_targets(&protected_create_registrations);
    signal_forwarders.restore();
    cleanup_signal_mask.restore();
    exit_with_wait_status_or_policy_violation(status, protected_create_violation);
}

impl ProtectedCreateMonitor {
    fn start(targets: &[ProtectedCreateTarget]) -> Option<Self> {
        if targets.is_empty() {
            return None;
        }

        let targets = targets.to_vec();
        let stop = Arc::new(AtomicBool::new(false));
        let violation = Arc::new(AtomicBool::new(false));
        let monitor_stop = Arc::clone(&stop);
        let monitor_violation = Arc::clone(&violation);
        let handle = thread::spawn(move || {
            let watcher = ProtectedCreateWatcher::new(&targets);
            while !monitor_stop.load(Ordering::SeqCst) {
                for target in &targets {
                    if remove_protected_create_target_best_effort(target).is_some() {
                        monitor_violation.store(true, Ordering::SeqCst);
                    }
                }
                if let Some(watcher) = &watcher {
                    watcher.wait_for_create_event(&monitor_stop);
                } else {
                    thread::sleep(Duration::from_millis(1));
                }
            }
        });

        Some(Self {
            stop,
            violation,
            handle,
        })
    }

    fn stop(self) -> bool {
        self.stop.store(true, Ordering::SeqCst);
        self.handle
            .join()
            .unwrap_or_else(|_| panic!("protected create monitor thread panicked"));
        self.violation.load(Ordering::SeqCst)
    }
}

impl ProtectedCreateWatcher {
    fn new(targets: &[ProtectedCreateTarget]) -> Option<Self> {
        let fd = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
        if fd < 0 {
            return None;
        }

        let mut watched_parents = Vec::<PathBuf>::new();
        let mut watches = Vec::new();
        for target in targets {
            let Some(parent) = target.path().parent() else {
                continue;
            };
            if watched_parents.iter().any(|watched| watched == parent) {
                continue;
            }
            watched_parents.push(parent.to_path_buf());
            let Ok(parent_cstr) = CString::new(parent.as_os_str().as_bytes()) else {
                continue;
            };
            let mask =
                libc::IN_CREATE | libc::IN_MOVED_TO | libc::IN_DELETE_SELF | libc::IN_MOVE_SELF;
            let watch = unsafe { libc::inotify_add_watch(fd, parent_cstr.as_ptr(), mask) };
            if watch >= 0 {
                watches.push(watch);
            }
        }

        if watches.is_empty() {
            unsafe {
                libc::close(fd);
            }
            return None;
        }

        Some(Self {
            fd,
            _watches: watches,
        })
    }

    fn wait_for_create_event(&self, stop: &AtomicBool) {
        let mut poll_fd = libc::pollfd {
            fd: self.fd,
            events: libc::POLLIN,
            revents: 0,
        };
        while !stop.load(Ordering::SeqCst) {
            let res = unsafe { libc::poll(&mut poll_fd, 1, 10) };
            if res > 0 {
                self.drain_events();
                return;
            }
            if res == 0 {
                return;
            }
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return;
        }
    }

    fn drain_events(&self) {
        let mut buf = [0_u8; 4096];
        loop {
            let read = unsafe { libc::read(self.fd, buf.as_mut_ptr().cast(), buf.len()) };
            if read > 0 {
                continue;
            }
            if read == 0 {
                return;
            }
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return;
        }
    }
}

impl Drop for ProtectedCreateWatcher {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
        }
    }
}

fn create_exec_start_pipe(enabled: bool) -> [libc::c_int; 2] {
    if !enabled {
        return [-1, -1];
    }
    let mut pipe = [-1, -1];
    if unsafe { libc::pipe2(pipe.as_mut_ptr(), libc::O_CLOEXEC) } < 0 {
        let err = std::io::Error::last_os_error();
        panic!("failed to create bubblewrap exec start pipe: {err}");
    }
    pipe
}

fn wait_for_parent_exec_start(read_fd: libc::c_int, write_fd: libc::c_int) {
    if write_fd >= 0 {
        unsafe {
            libc::close(write_fd);
        }
    }
    if read_fd < 0 {
        return;
    }

    let mut byte = [0_u8; 1];
    loop {
        let read = unsafe { libc::read(read_fd, byte.as_mut_ptr().cast(), byte.len()) };
        if read >= 0 {
            break;
        }
        let err = std::io::Error::last_os_error();
        if err.kind() != std::io::ErrorKind::Interrupted {
            break;
        }
    }
    unsafe {
        libc::close(read_fd);
    }
}

fn close_child_exec_start_read(read_fd: libc::c_int) {
    if read_fd >= 0 {
        unsafe {
            libc::close(read_fd);
        }
    }
}

fn release_child_exec_start(write_fd: libc::c_int) {
    if write_fd < 0 {
        return;
    }
    let byte = [0_u8; 1];
    unsafe {
        libc::write(write_fd, byte.as_ptr().cast(), byte.len());
        libc::close(write_fd);
    }
}

struct ForwardedSignalMask {
    previous: libc::sigset_t,
}

struct ForwardedSignalHandlers {
    previous: Vec<(libc::c_int, libc::sigaction)>,
}

impl ForwardedSignalMask {
    fn block() -> Self {
        let mut blocked: libc::sigset_t = unsafe { std::mem::zeroed() };
        let mut previous: libc::sigset_t = unsafe { std::mem::zeroed() };
        unsafe {
            libc::sigemptyset(&mut blocked);
            for signal in FORWARDED_SIGNALS {
                libc::sigaddset(&mut blocked, *signal);
            }
            if libc::sigprocmask(libc::SIG_BLOCK, &blocked, &mut previous) < 0 {
                let err = std::io::Error::last_os_error();
                panic!("failed to block bubblewrap forwarded signals: {err}");
            }
        }
        Self { previous }
    }

    fn restore(&self) {
        let mut restored = self.previous;
        unsafe {
            for signal in FORWARDED_SIGNALS {
                libc::sigdelset(&mut restored, *signal);
            }
            if libc::sigprocmask(libc::SIG_SETMASK, &restored, std::ptr::null_mut()) < 0 {
                let err = std::io::Error::last_os_error();
                panic!("failed to restore bubblewrap forwarded signals: {err}");
            }
        }
    }
}

fn terminate_with_parent(parent_pid: libc::pid_t) {
    let res = unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) };
    if res < 0 {
        let err = std::io::Error::last_os_error();
        panic!("failed to set bubblewrap child parent-death signal: {err}");
    }
    if unsafe { libc::getppid() } != parent_pid {
        unsafe {
            libc::raise(libc::SIGTERM);
        }
    }
}

impl ForwardedSignalHandlers {
    fn restore(self) {
        BWRAP_CHILD_PID.store(0, Ordering::SeqCst);
        PENDING_FORWARDED_SIGNAL.store(0, Ordering::SeqCst);
        for (signal, previous_action) in self.previous {
            unsafe {
                if libc::sigaction(signal, &previous_action, std::ptr::null_mut()) < 0 {
                    let err = std::io::Error::last_os_error();
                    panic!("failed to restore bubblewrap signal handler for {signal}: {err}");
                }
            }
        }
    }
}

fn install_bwrap_signal_forwarders(pid: libc::pid_t) -> ForwardedSignalHandlers {
    BWRAP_CHILD_PID.store(pid, Ordering::SeqCst);
    let mut previous = Vec::with_capacity(FORWARDED_SIGNALS.len());
    for signal in FORWARDED_SIGNALS {
        let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
        let mut previous_action: libc::sigaction = unsafe { std::mem::zeroed() };
        action.sa_sigaction = forward_signal_to_bwrap_child as *const () as libc::sighandler_t;
        unsafe {
            libc::sigemptyset(&mut action.sa_mask);
            if libc::sigaction(*signal, &action, &mut previous_action) < 0 {
                let err = std::io::Error::last_os_error();
                panic!("failed to install bubblewrap signal forwarder for {signal}: {err}");
            }
        }
        previous.push((*signal, previous_action));
    }
    replay_pending_forwarded_signal(pid);
    ForwardedSignalHandlers { previous }
}

extern "C" fn forward_signal_to_bwrap_child(signal: libc::c_int) {
    PENDING_FORWARDED_SIGNAL.store(signal, Ordering::SeqCst);
    let pid = BWRAP_CHILD_PID.load(Ordering::SeqCst);
    if pid > 0 {
        send_signal_to_bwrap_child(pid, signal);
    }
}

fn replay_pending_forwarded_signal(pid: libc::pid_t) {
    let signal = PENDING_FORWARDED_SIGNAL.swap(0, Ordering::SeqCst);
    if signal > 0 {
        send_signal_to_bwrap_child(pid, signal);
    }
}

fn send_signal_to_bwrap_child(pid: libc::pid_t, signal: libc::c_int) {
    unsafe {
        libc::kill(-pid, signal);
        libc::kill(pid, signal);
    }
}

fn reset_forwarded_signal_handlers_to_default() {
    for signal in FORWARDED_SIGNALS {
        unsafe {
            if libc::signal(*signal, libc::SIG_DFL) == libc::SIG_ERR {
                let err = std::io::Error::last_os_error();
                panic!("failed to reset bubblewrap signal handler for {signal}: {err}");
            }
        }
    }
}

fn wait_for_bwrap_child(pid: libc::pid_t) -> libc::c_int {
    loop {
        let mut status: libc::c_int = 0;
        let wait_res = unsafe { libc::waitpid(pid, &mut status as *mut libc::c_int, 0) };
        if wait_res >= 0 {
            return status;
        }
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        panic!("waitpid failed for bubblewrap child: {err}");
    }
}

fn register_synthetic_mount_targets(
    targets: &[SyntheticMountTarget],
) -> Vec<SyntheticMountTargetRegistration> {
    with_synthetic_mount_registry_lock(|| {
        targets
            .iter()
            .map(|target| {
                let marker_dir = synthetic_mount_marker_dir(target.path());
                fs::create_dir_all(&marker_dir).unwrap_or_else(|err| {
                    panic!(
                        "failed to create synthetic bubblewrap mount marker directory {}: {err}",
                        marker_dir.display()
                    )
                });
                let target = if target.preserves_pre_existing_path()
                    && synthetic_mount_marker_dir_has_active_synthetic_owner(&marker_dir)
                {
                    match target.kind() {
                        SyntheticMountTargetKind::EmptyFile => {
                            SyntheticMountTarget::missing(target.path())
                        }
                        SyntheticMountTargetKind::EmptyDirectory => {
                            SyntheticMountTarget::missing_empty_directory(target.path())
                        }
                    }
                } else {
                    target.clone()
                };
                let marker_file = marker_dir.join(std::process::id().to_string());
                fs::write(&marker_file, synthetic_mount_marker_contents(&target)).unwrap_or_else(
                    |err| {
                        panic!(
                            "failed to register synthetic bubblewrap mount target {}: {err}",
                            target.path().display()
                        )
                    },
                );
                SyntheticMountTargetRegistration {
                    target,
                    marker_file,
                    marker_dir,
                }
            })
            .collect()
    })
}

fn register_protected_create_targets(
    targets: &[ProtectedCreateTarget],
) -> Vec<ProtectedCreateTargetRegistration> {
    with_synthetic_mount_registry_lock(|| {
        targets
            .iter()
            .map(|target| {
                let marker_dir = synthetic_mount_marker_dir(target.path());
                fs::create_dir_all(&marker_dir).unwrap_or_else(|err| {
                    panic!(
                        "failed to create protected create marker directory {}: {err}",
                        marker_dir.display()
                    )
                });
                let marker_file = marker_dir.join(std::process::id().to_string());
                fs::write(&marker_file, PROTECTED_CREATE_MARKER).unwrap_or_else(|err| {
                    panic!(
                        "failed to register protected create target {}: {err}",
                        target.path().display()
                    )
                });
                ProtectedCreateTargetRegistration {
                    target: target.clone(),
                    marker_file,
                    marker_dir,
                }
            })
            .collect()
    })
}

fn synthetic_mount_marker_contents(target: &SyntheticMountTarget) -> &'static [u8] {
    if target.preserves_pre_existing_path() {
        SYNTHETIC_MOUNT_MARKER_EXISTING
    } else {
        SYNTHETIC_MOUNT_MARKER_SYNTHETIC
    }
}

fn synthetic_mount_marker_dir_has_active_synthetic_owner(marker_dir: &Path) -> bool {
    synthetic_mount_marker_dir_has_active_process_matching(marker_dir, |path| {
        match fs::read(path) {
            Ok(contents) => contents == SYNTHETIC_MOUNT_MARKER_SYNTHETIC,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
            Err(err) => panic!(
                "failed to read synthetic bubblewrap mount marker {}: {err}",
                path.display()
            ),
        }
    })
}

fn synthetic_mount_marker_dir_has_active_process(marker_dir: &Path) -> bool {
    synthetic_mount_marker_dir_has_active_process_matching(marker_dir, |_| true)
}

fn synthetic_mount_marker_dir_has_active_process_matching(
    marker_dir: &Path,
    matches_marker: impl Fn(&Path) -> bool,
) -> bool {
    let entries = match fs::read_dir(marker_dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return false,
        Err(err) => panic!(
            "failed to read synthetic bubblewrap mount marker directory {}: {err}",
            marker_dir.display()
        ),
    };
    for entry in entries {
        let entry = entry.unwrap_or_else(|err| {
            panic!(
                "failed to read synthetic bubblewrap mount marker in {}: {err}",
                marker_dir.display()
            )
        });
        let path = entry.path();
        let Some(pid) = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.parse::<libc::pid_t>().ok())
        else {
            continue;
        };
        if !process_is_active(pid) {
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => panic!(
                    "failed to remove stale synthetic bubblewrap mount marker {}: {err}",
                    path.display()
                ),
            }
            continue;
        }
        let matches_marker = matches_marker(&path);
        if matches_marker {
            return true;
        }
    }
    false
}

fn cleanup_synthetic_mount_targets(targets: &[SyntheticMountTargetRegistration]) {
    with_synthetic_mount_registry_lock(|| {
        for target in targets.iter().rev() {
            match fs::remove_file(&target.marker_file) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => panic!(
                    "failed to unregister synthetic bubblewrap mount target {}: {err}",
                    target.target.path().display()
                ),
            }
        }

        for target in targets.iter().rev() {
            if synthetic_mount_marker_dir_has_active_process(&target.marker_dir) {
                continue;
            }
            remove_synthetic_mount_target(&target.target);
            match fs::remove_dir(&target.marker_dir) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) if err.kind() == std::io::ErrorKind::DirectoryNotEmpty => {}
                Err(err) => panic!(
                    "failed to remove synthetic bubblewrap mount marker directory {}: {err}",
                    target.marker_dir.display()
                ),
            }
        }
    });
}

fn cleanup_protected_create_targets(targets: &[ProtectedCreateTargetRegistration]) -> bool {
    with_synthetic_mount_registry_lock(|| {
        for target in targets.iter().rev() {
            match fs::remove_file(&target.marker_file) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => panic!(
                    "failed to unregister protected create target {}: {err}",
                    target.target.path().display()
                ),
            }
        }

        let mut violation = false;
        for target in targets.iter().rev() {
            violation |= remove_protected_create_target(&target.target);
            match fs::remove_dir(&target.marker_dir) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) if err.kind() == std::io::ErrorKind::DirectoryNotEmpty => {}
                Err(err) => panic!(
                    "failed to remove protected create marker directory {}: {err}",
                    target.marker_dir.display()
                ),
            }
        }
        violation
    })
}

fn remove_protected_create_target(target: &ProtectedCreateTarget) -> bool {
    for attempt in 0..100 {
        match try_remove_protected_create_target(target) {
            Ok(removal) => return removal.is_some(),
            Err(err) if err.kind() == std::io::ErrorKind::DirectoryNotEmpty && attempt < 99 => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(err) => {
                panic!(
                    "failed to remove protected create target {}: {err}",
                    target.path().display()
                );
            }
        }
    }
    unreachable!("protected create removal retry loop should return or panic")
}

fn remove_protected_create_target_best_effort(
    target: &ProtectedCreateTarget,
) -> Option<ProtectedCreateRemoval> {
    for _ in 0..100 {
        match try_remove_protected_create_target(target) {
            Ok(removal) => return removal,
            Err(err) if err.kind() == std::io::ErrorKind::DirectoryNotEmpty => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(_) => return Some(ProtectedCreateRemoval::Other),
        }
    }
    Some(ProtectedCreateRemoval::Other)
}

fn try_remove_protected_create_target(
    target: &ProtectedCreateTarget,
) -> std::io::Result<Option<ProtectedCreateRemoval>> {
    let path = target.path();
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };

    let removal = if metadata.is_dir() {
        ProtectedCreateRemoval::Directory
    } else {
        ProtectedCreateRemoval::Other
    };
    let result = if removal == ProtectedCreateRemoval::Directory {
        match fs::remove_dir_all(path) {
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                make_directory_tree_writable(path)?;
                fs::remove_dir_all(path)
            }
            result => result,
        }
    } else {
        fs::remove_file(path)
    };
    match result {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    }
    eprintln!(
        "sandbox blocked creation of protected workspace metadata path {}",
        path.display()
    );
    Ok(Some(removal))
}

fn make_directory_tree_writable(path: &Path) -> std::io::Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };
    if !metadata.is_dir() {
        return Ok(());
    }

    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_PATH | libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)?;
    let directory_path = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()));
    fs::set_permissions(&directory_path, fs::Permissions::from_mode(0o700))?;
    for entry in fs::read_dir(directory_path)? {
        make_directory_tree_writable(&entry?.path())?;
    }
    Ok(())
}

fn remove_synthetic_mount_target(target: &SyntheticMountTarget) {
    let path = target.path();
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return,
        Err(err) => panic!(
            "failed to inspect synthetic bubblewrap mount target {}: {err}",
            path.display()
        ),
    };
    if !target.should_remove_after_bwrap(&metadata) {
        return;
    }
    match target.kind() {
        SyntheticMountTargetKind::EmptyFile => match fs::remove_file(path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => panic!(
                "failed to remove synthetic bubblewrap mount target {}: {err}",
                path.display()
            ),
        },
        SyntheticMountTargetKind::EmptyDirectory => match fs::remove_dir(path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) if err.kind() == std::io::ErrorKind::DirectoryNotEmpty => {}
            Err(err) => panic!(
                "failed to remove synthetic bubblewrap mount target {}: {err}",
                path.display()
            ),
        },
    }
}

fn process_is_active(pid: libc::pid_t) -> bool {
    let result = unsafe { libc::kill(pid, 0) };
    if result == 0 {
        return true;
    }
    let err = std::io::Error::last_os_error();
    !matches!(err.raw_os_error(), Some(libc::ESRCH))
}

fn with_synthetic_mount_registry_lock<T>(f: impl FnOnce() -> T) -> T {
    let registry_root = synthetic_mount_registry_root();
    fs::create_dir_all(&registry_root).unwrap_or_else(|err| {
        panic!(
            "failed to create synthetic bubblewrap mount registry {}: {err}",
            registry_root.display()
        )
    });
    let lock_path = registry_root.join("lock");
    let lock_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .unwrap_or_else(|err| {
            panic!(
                "failed to open synthetic bubblewrap mount registry lock {}: {err}",
                lock_path.display()
            )
        });
    if unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX) } < 0 {
        let err = std::io::Error::last_os_error();
        panic!(
            "failed to lock synthetic bubblewrap mount registry {}: {err}",
            lock_path.display()
        );
    }
    let result = f();
    if unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_UN) } < 0 {
        let err = std::io::Error::last_os_error();
        panic!(
            "failed to unlock synthetic bubblewrap mount registry {}: {err}",
            lock_path.display()
        );
    }
    result
}

fn synthetic_mount_marker_dir(path: &Path) -> PathBuf {
    synthetic_mount_registry_root().join(format!("{:016x}", hash_path(path)))
}

pub(crate) fn synthetic_mount_registry_root() -> PathBuf {
    static REGISTRY_ROOT: OnceLock<PathBuf> = OnceLock::new();

    REGISTRY_ROOT
        .get_or_init(|| {
            let effective_uid = unsafe { libc::geteuid() };
            let temp_dir = std::env::temp_dir();
            let temp_dir = temp_dir.canonicalize().unwrap_or_else(|err| {
                panic!(
                    "failed to resolve synthetic mount registry temp directory {}: {err}",
                    temp_dir.display()
                )
            });
            let registry_root = temp_dir.join(format!(
                "klaus-bwrap-synthetic-mount-targets-{effective_uid}"
            ));
            // A registry symlink can redirect bookkeeping into a writable root
            // that does not overlap TMPDIR, bypassing its read-only mount.
            assert!(
                !registry_root.is_symlink(),
                "synthetic mount registry must not be a symlink: {}",
                registry_root.display()
            );
            registry_root
        })
        .clone()
}

fn hash_path(path: &Path) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in path.as_os_str().as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn exit_with_wait_status(status: libc::c_int) -> ! {
    if libc::WIFEXITED(status) {
        std::process::exit(libc::WEXITSTATUS(status));
    }

    if libc::WIFSIGNALED(status) {
        let signal = libc::WTERMSIG(status);
        unsafe {
            libc::signal(signal, libc::SIG_DFL);
            libc::kill(libc::getpid(), signal);
        }
        std::process::exit(128 + signal);
    }

    std::process::exit(1);
}

fn exit_with_wait_status_or_policy_violation(
    status: libc::c_int,
    protected_create_violation: bool,
) -> ! {
    if protected_create_violation && libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0 {
        std::process::exit(1);
    }

    exit_with_wait_status(status);
}

pub(super) fn proc_mount_supported() -> bool {
    let executable = if Path::new("/usr/bin/true").exists() {
        "/usr/bin/true"
    } else {
        "/bin/true"
    };
    let arguments = [
        "klaus-bwrap",
        "--unshare-user",
        "--unshare-pid",
        "--unshare-ipc",
        "--unshare-net",
        "--ro-bind",
        "/",
        "/",
        "--dev",
        "/dev",
        "--proc",
        "/proc",
        "--",
        executable,
    ];
    let output = run_bwrap_in_child_capture_stderr(BwrapArgs {
        args: arguments.into_iter().map(OsString::from).collect(),
        preserved_files: vec![],
        synthetic_mount_targets: vec![],
        protected_create_targets: vec![],
    });
    !is_proc_mount_failure(&output)
}
fn run_bwrap_in_child_capture_stderr(bwrap_args: BwrapArgs) -> String {
    let BwrapArgs {
        args,
        preserved_files,
        synthetic_mount_targets,
        protected_create_targets,
    } = bwrap_args;
    let setup_signal_mask = ForwardedSignalMask::block();
    let synthetic_mount_registrations = register_synthetic_mount_targets(&synthetic_mount_targets);
    let protected_create_registrations =
        register_protected_create_targets(&protected_create_targets);

    let mut pipe_fds = [0; 2];
    let pipe_res = unsafe { libc::pipe2(pipe_fds.as_mut_ptr(), libc::O_CLOEXEC) };
    if pipe_res < 0 {
        let err = std::io::Error::last_os_error();
        panic!("failed to create stderr pipe for bubblewrap: {err}");
    }
    let read_fd = pipe_fds[0];
    let write_fd = pipe_fds[1];

    let pid = unsafe { libc::fork() };
    if pid < 0 {
        let err = std::io::Error::last_os_error();
        panic!("failed to fork for bubblewrap: {err}");
    }

    if pid == 0 {
        reset_forwarded_signal_handlers_to_default();
        setup_signal_mask.restore();
        // Child: redirect stderr to the pipe, then run bubblewrap.
        unsafe {
            close_fd_or_panic(read_fd, "close read end in bubblewrap child");
            if libc::dup2(write_fd, libc::STDERR_FILENO) < 0 {
                let err = std::io::Error::last_os_error();
                panic!("failed to redirect stderr for bubblewrap: {err}");
            }
            close_fd_or_panic(write_fd, "close write end in bubblewrap child");
        }

        exec_bwrap(args, preserved_files);
    }

    let signal_forwarders = install_bwrap_signal_forwarders(pid);
    setup_signal_mask.restore();
    // Parent: close the write end and read stderr while the child runs.
    close_fd_or_panic(write_fd, "close write end in bubblewrap parent");

    // SAFETY: `read_fd` is a valid owned fd in the parent.
    let mut read_file = unsafe { File::from_raw_fd(read_fd) };
    let mut stderr_bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        let count = read_file
            .read(&mut buffer)
            .unwrap_or_else(|error| panic!("failed to read namespace stderr: {error}"));
        if count == 0 {
            break;
        }
        stderr_bytes.extend_from_slice(&buffer[..count]);
        if stderr_bytes.len() > 8192 {
            stderr_bytes.drain(..stderr_bytes.len() - 8192);
        }
    }

    let status = wait_for_bwrap_child(pid);
    let cleanup_signal_mask = ForwardedSignalMask::block();
    BWRAP_CHILD_PID.store(0, Ordering::SeqCst);
    cleanup_synthetic_mount_targets(&synthetic_mount_registrations);
    cleanup_protected_create_targets(&protected_create_registrations);
    signal_forwarders.restore();
    cleanup_signal_mask.restore();
    if libc::WIFSIGNALED(status) {
        exit_with_wait_status(status);
    }

    String::from_utf8_lossy(&stderr_bytes).into_owned()
}

/// Close an owned file descriptor and panic with context on failure.
///
/// We use explicit close() checks here (instead of ignoring return codes)
/// because this code runs in low-level sandbox setup paths where fd leaks or
/// close errors can mask the root cause of later failures.
fn close_fd_or_panic(fd: libc::c_int, context: &str) {
    let close_res = unsafe { libc::close(fd) };
    if close_res < 0 {
        let err = std::io::Error::last_os_error();
        panic!("{context}: {err}");
    }
}

fn is_proc_mount_failure(stderr: &str) -> bool {
    stderr.contains("Can't mount proc")
        && stderr.contains("/newroot/proc")
        && (stderr.contains("Invalid argument")
            || stderr.contains("Operation not permitted")
            || stderr.contains("Permission denied"))
}
