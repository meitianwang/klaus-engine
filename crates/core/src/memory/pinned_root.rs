//! Handle-backed filesystem capability for restricted memory-maintenance forks.
//!
//! Admission pins the canonical root component by component without following
//! links. Mutable files are subsequently addressed through stable handles.
//! Read/search use private handle-backed snapshots, so helpers such as poppler and
//! ripgrep retain their normal behavior without reopening the admitted root.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{File, Metadata, OpenOptions};
use std::io::{self, Seek, SeekFrom};
use std::panic::{self, AssertUnwindSafe};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};

use crate::runtime_values::{RuntimePath, RuntimeString};
// NodeBufferUtf8 feeds only the unix native-path decode below.
#[cfg(unix)]
use crate::runtime_values::NodeBufferUtf8;

#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd};
#[cfg(unix)]
use std::os::unix::ffi::{OsStrExt, OsStringExt};
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

/// Canonical absolute memory root whose complete path remains pinned.
#[derive(Debug)]
pub struct PinnedMemoryRoot {
    logical_root: PathBuf,
    canonical_root: PathBuf,
    chain: Vec<File>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OpenDisposition {
    Existing,
    OpenOrCreate,
    CreateNew,
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

#[cfg(windows)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FileIdentity {
    volume: u32,
    index: u64,
}

impl PinnedMemoryRoot {
    pub fn pin(path: impl AsRef<Path>) -> io::Result<Arc<Self>> {
        let logical_root = absolute_logical_root(path.as_ref())?;
        let (canonical_root, chain) = pin_existing_directory(&logical_root)?;
        Ok(Arc::new(Self {
            logical_root,
            canonical_root,
            chain,
        }))
    }

    /// Pin an existing root or create only its missing suffix relative to the
    /// nearest pinned existing ancestor. No missing component is ever created
    /// through a re-resolved pathname.
    pub fn pin_or_create(path: impl AsRef<Path>) -> io::Result<Arc<Self>> {
        let logical_root = absolute_logical_root(path.as_ref())?;
        match std::fs::symlink_metadata(&logical_root) {
            Ok(_) => {
                let (canonical_root, chain) = pin_existing_directory(&logical_root)?;
                return Ok(Arc::new(Self {
                    logical_root,
                    canonical_root,
                    chain,
                }));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }

        let mut existing = logical_root.clone();
        let mut missing = Vec::new();
        loop {
            match std::fs::symlink_metadata(&existing) {
                Ok(_) => break,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    let name = existing.file_name().ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::NotFound,
                            "memory root has no existing ancestor",
                        )
                    })?;
                    missing.push(name.to_os_string());
                    if !existing.pop() {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
        missing.reverse();
        let (canonical_ancestor, mut chain) = pin_existing_directory(&existing)?;
        create_missing_chain(&mut chain, &canonical_ancestor, &missing)?;
        let canonical_root = missing
            .iter()
            .fold(canonical_ancestor, |path, component| path.join(component));
        Ok(Arc::new(Self {
            logical_root,
            canonical_root,
            chain,
        }))
    }

    pub fn logical_root(&self) -> &Path {
        &self.logical_root
    }

    /// [`Self::pin_or_create`] through a process-wide registry: one pin per
    /// logical root, shared by every holder that asks for it.
    ///
    /// A pin costs one open directory handle PER PATH COMPONENT, held for as long
    /// as the holder lives. Pinning per session made that per-session: with
    /// `~/.klaus/projects/<slug>/memory` it is seven handles each, and a measured
    /// desktop run held twenty of them for the SAME directory — 141 of the
    /// process's 256-descriptor budget spent re-pinning one path, which is what
    /// starved everything else in the process (EMFILE).
    ///
    /// Sharing is sound because a pin is immutable and already `Arc`-shared
    /// within one holder; what it must not do is outlive the directory it names,
    /// so a cached pin is reused ONLY while the logical path still resolves to the
    /// same physical directory. A root that was replaced (deleted, renamed,
    /// swapped for a symlink) is re-pinned rather than reused, which is the same
    /// identity rule `pin_existing_directory` closes its own loop with.
    pub fn pin_or_create_shared(path: impl AsRef<Path>) -> io::Result<Arc<Self>> {
        let logical_root = absolute_logical_root(path.as_ref())?;
        let mut registry = shared_pins()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Weak entries whose last holder is gone carry no descriptors; drop them
        // so the registry cannot grow across a long-lived process.
        registry.retain(|_, pinned| pinned.strong_count() > 0);
        if let Some(pinned) = registry.get(&logical_root).and_then(Weak::upgrade) {
            if pinned.root_handle_still_names_its_path() {
                return Ok(pinned);
            }
        }
        let pinned = Self::pin_or_create(&logical_root)?;
        registry.insert(logical_root, Arc::downgrade(&pinned));
        Ok(pinned)
    }

    /// Whether this pin's retained root handle is still what its logical path
    /// resolves to. False once the directory has been replaced, and false when
    /// the path no longer resolves at all.
    fn root_handle_still_names_its_path(&self) -> bool {
        let Some(root) = self.chain.last() else {
            return false;
        };
        match open_directory_path_no_follow(&self.logical_root) {
            Ok(current) => same_directory_identity(&current, root).unwrap_or(false),
            Err(_) => false,
        }
    }

    pub fn canonical_root(&self) -> &Path {
        &self.canonical_root
    }

    pub fn validate_root_namespace(&self) -> io::Result<()> {
        if self.root_handle_still_names_its_path() {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "pinned resource root was replaced",
            ))
        }
    }

    /// Enumerate names from an admitted directory handle without following
    /// symbolic links or copying the contents of its files.
    pub fn directory_entries(
        &self,
        path: impl AsRef<Path>,
    ) -> io::Result<Vec<(std::ffi::OsString, bool)>> {
        self.validate_root_namespace()?;
        let relative = self.relative(path.as_ref())?;
        pinned_directory_entries(self, &relative)
    }

    pub fn open_read(&self, path: impl AsRef<Path>) -> io::Result<PinnedFileLease> {
        self.open_file(path.as_ref(), false, OpenDisposition::Existing)
    }

    /// Open an existing write target without creating any pathname component.
    /// WriteTool uses this for stale-file admission before the LSP gate.
    pub fn open_write_existing(&self, path: impl AsRef<Path>) -> io::Result<PinnedFileLease> {
        self.open_file(path.as_ref(), true, OpenDisposition::Existing)
    }

    /// Open-or-create is reserved for internal state such as the consolidation
    /// lock, whose acquisition itself owns the leaf creation boundary.
    pub fn open_write(&self, path: impl AsRef<Path>) -> io::Result<PinnedFileLease> {
        self.open_file(path.as_ref(), true, OpenDisposition::OpenOrCreate)
    }

    /// Stage a previously missing write target at the final mutation boundary.
    /// Unix keeps the new inode anonymous until an atomic no-replace commit;
    /// Windows CreateNew holds a no-delete leaf handle. Either form fails closed
    /// if another actor created the target after admission.
    pub fn create_write_new(&self, path: impl AsRef<Path>) -> io::Result<PinnedFileLease> {
        self.open_file(path.as_ref(), true, OpenDisposition::CreateNew)
    }

    pub fn open_edit(&self, path: impl AsRef<Path>) -> io::Result<PinnedFileLease> {
        self.open_file(path.as_ref(), true, OpenDisposition::Existing)
    }

    /// Stable private copy for read, including PDF helper-process paths.
    pub fn snapshot_file(&self, path: impl AsRef<Path>) -> io::Result<PinnedSnapshot> {
        let relative = self.relative(path.as_ref())?;
        let lease = self.open_read(path)?;
        let temp_root = create_private_temp_dir("klaus-memory-read")?;
        let name = relative
            .file_name()
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| OsStr::new("memory-file"));
        let staged = temp_root.join(name);
        copy_open_file(&lease.file, &staged)?;
        Ok(PinnedSnapshot {
            temp_root,
            staged,
            logical_base: self.logical_root.clone(),
        })
    }

    /// Stable private tree/file for unchanged ripgrep execution.
    pub fn snapshot_search(&self, path: impl AsRef<Path>) -> io::Result<PinnedSnapshot> {
        let path = path.as_ref();
        let relative = self.relative(path)?;
        let temp_root = create_private_temp_dir("klaus-memory-search")?;
        let staged = temp_root.join(
            relative
                .file_name()
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| OsStr::new("root")),
        );
        snapshot_entry(self, &relative, &staged)?;
        Ok(PinnedSnapshot {
            temp_root,
            staged,
            logical_base: path.to_path_buf(),
        })
    }

    fn relative(&self, path: &Path) -> io::Result<PathBuf> {
        let relative = if path.is_absolute() {
            path.strip_prefix(&self.logical_root)
                .or_else(|_| path.strip_prefix(&self.canonical_root))
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        format!(
                            "memory tool path is outside the pinned root: {}",
                            path.display()
                        ),
                    )
                })?
                .to_path_buf()
        } else {
            path.to_path_buf()
        };
        normalize_relative(&relative)
    }

    fn open_file(
        &self,
        path: &Path,
        write: bool,
        disposition: OpenDisposition,
    ) -> io::Result<PinnedFileLease> {
        let relative = self.relative(path)?;
        if relative.as_os_str().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "memory root is not a regular file",
            ));
        }
        open_regular_relative(self, &relative, write, disposition)
    }

    /// Clone the exact pinned root-directory handle. Consolidation uses this as
    /// its Unix lock domain: replacing `.consolidate-lock` can change an epoch
    /// record, but can never create a second cooperative OS-lock domain inside
    /// the same physical memory directory.
    #[cfg(unix)]
    pub(crate) fn open_root_lock_domain(&self) -> io::Result<File> {
        let root = self
            .chain
            .last()
            .ok_or_else(|| io::Error::other("pinned memory root has no directory handle"))?;
        // `try_clone`/dup shares one open-file description; flocking a second
        // duplicate may be treated as the same owner. `openat(".")` creates an
        // independent description anchored to the same immutable directory
        // inode, so a second acquisition in this process still contends.
        openat_dir(root.as_raw_fd(), OsStr::new("."))
    }
}

/// A stable regular-file handle. Unix intentionally exposes no fd-path
/// projection: all I/O must use the owned handle. On Windows the retained path
/// is used only for identity revalidation while every held handle omits DELETE
/// sharing.
#[derive(Debug)]
pub struct PinnedFileLease {
    file: File,
    #[cfg(windows)]
    projected_path: PathBuf,
    existed_before: bool,
    identity: FileIdentity,
    #[cfg(unix)]
    parent: File,
    #[cfg(unix)]
    leaf: std::ffi::OsString,
    #[cfg(unix)]
    namespace: UnixLeaseNamespace,
    #[cfg(windows)]
    _ancestors: Vec<File>,
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UnixLeaseNamespace {
    /// `leaf` names `file` and `identity` inside the retained parent.
    Installed,
    /// A new-file lease owns an anonymous, still-unpublished inode while
    /// `leaf` is required to remain absent until the no-replace commit.
    PendingCreate,
}

impl PinnedFileLease {
    pub fn existed_before(&self) -> bool {
        self.existed_before
    }

    pub fn metadata(&self) -> io::Result<Metadata> {
        self.file.metadata()
    }

    pub(crate) fn file(&self) -> &File {
        &self.file
    }

    /// Prove that an installed lease still has one link and that the retained
    /// parent names its exact inode. A pending create instead proves that its
    /// staging inode remains anonymous and the destination leaf remains absent.
    pub fn validate_namespace(&self) -> io::Result<()> {
        validate_lease_namespace(self)
    }

    /// Run one blocking file operation under an owner that holds the complete
    /// lease and is synchronously joined if its awaiting async task is cancelled.
    ///
    /// Tokio filesystem helpers reopen pathnames from an uncancellable
    /// `spawn_blocking` closure. Passing `/dev/fd/N` to them is unsafe here: the
    /// outer future may be aborted, close `N`, and let the descriptor number be
    /// reused before that closure opens it. This owner moves the real handle into
    /// a dedicated thread and makes cancellation wait for actual completion.
    pub async fn run_blocking<T, F>(self, operation: F) -> io::Result<(Self, T)>
    where
        T: Send + 'static,
        F: FnOnce(&mut File) -> io::Result<T> + Send + 'static,
    {
        self.run_owned_blocking(move |lease| operation(&mut lease.file))
            .await
    }

    /// Replace the file through a handle-relative, atomically committed staged
    /// inode. Unix never mutates the admitted inode: a hard link created after
    /// admission can therefore observe only the old bytes. Windows retains its
    /// no-delete namespace lease and uses the same handle-owned direct write.
    pub async fn replace_blocking<T, F>(self, operation: F) -> io::Result<(Self, T)>
    where
        T: Send + 'static,
        F: FnOnce(&mut File) -> io::Result<T> + Send + 'static,
    {
        #[cfg(unix)]
        {
            return self
                .run_owned_blocking(move |lease| {
                    replace_file_unix(lease, operation, true, |_| Ok(()))
                })
                .await;
        }
        #[cfg(windows)]
        {
            self.run_owned_blocking(move |lease| operation(&mut lease.file))
                .await
        }
    }

    /// Synchronous atomic replacement for the consolidation epoch record. The
    /// current leaf is already locked; the staged candidate is locked before
    /// exchange so legacy leaf-only owners see no unlocked inode handoff.
    pub(crate) fn replace_sync_locked<T, F>(&mut self, operation: F) -> io::Result<T>
    where
        F: FnOnce(&mut File) -> io::Result<T>,
    {
        #[cfg(unix)]
        {
            replace_file_unix(self, operation, false, |candidate| {
                fs2::FileExt::try_lock_exclusive(candidate)
            })
        }
        #[cfg(windows)]
        {
            operation(&mut self.file)
        }
    }

    async fn run_owned_blocking<T, F>(self, operation: F) -> io::Result<(Self, T)>
    where
        T: Send + 'static,
        F: FnOnce(&mut PinnedFileLease) -> io::Result<T> + Send + 'static,
    {
        let state = Arc::new(BlockingFileState {
            result: Mutex::new(None),
            completed: Condvar::new(),
            notify: tokio::sync::Notify::new(),
        });
        let thread_state = Arc::clone(&state);
        let mut lease = self;
        let thread = std::thread::Builder::new()
            .name("klaus-pinned-file-io".to_string())
            .spawn(move || {
                let result = panic::catch_unwind(AssertUnwindSafe(|| operation(&mut lease)))
                    .unwrap_or_else(|_| {
                        Err(io::Error::other("pinned file blocking operation panicked"))
                    });
                let mut slot = thread_state
                    .result
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner());
                *slot = Some((lease, result));
                drop(slot);
                thread_state.completed.notify_all();
                thread_state.notify.notify_one();
            })?;
        let mut owner = BlockingFileOwner {
            state,
            thread: Some(thread),
            completed: false,
        };
        owner.wait().await
    }
}

struct BlockingFileState<T> {
    result: Mutex<Option<(PinnedFileLease, io::Result<T>)>>,
    completed: Condvar,
    notify: tokio::sync::Notify,
}

struct BlockingFileOwner<T> {
    state: Arc<BlockingFileState<T>>,
    thread: Option<std::thread::JoinHandle<()>>,
    completed: bool,
}

impl<T> BlockingFileOwner<T> {
    async fn wait(&mut self) -> io::Result<(PinnedFileLease, T)> {
        let result = loop {
            // `notify_one` retains a permit, so completion between this check
            // and the await cannot strand the owner.
            let ready = {
                let mut slot = self
                    .state
                    .result
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner());
                slot.take()
            };
            if let Some(result) = ready {
                break result;
            }
            self.state.notify.notified().await;
        };
        let joined = self
            .thread
            .take()
            .expect("a live blocking owner retains its thread")
            .join();
        self.completed = true;
        if joined.is_err() {
            return Err(io::Error::other(
                "pinned file blocking owner panicked after reporting completion",
            ));
        }
        let (lease, result) = result;
        result.map(|value| (lease, value))
    }
}

impl<T> Drop for BlockingFileOwner<T> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        // Cancellation is a synchronous join boundary. This can briefly block
        // the cancelling runtime worker, but the independent OS thread needs no
        // Tokio progress and no file effect can escape the worker's JoinHandle.
        let mut slot = self
            .state
            .result
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        while slot.is_none() {
            slot = self
                .state
                .completed
                .wait(slot)
                .unwrap_or_else(|poison| poison.into_inner());
        }
        drop(slot);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        let _ = self
            .state
            .result
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        self.completed = true;
    }
}

/// Private read/search snapshot; paths in rg output are restored to the
/// model-visible source base.
#[derive(Debug)]
pub struct PinnedSnapshot {
    temp_root: PathBuf,
    staged: PathBuf,
    logical_base: PathBuf,
}

impl PinnedSnapshot {
    pub fn path(&self) -> &Path {
        &self.staged
    }

    pub fn runtime_path(&self) -> RuntimePath {
        runtime_path_from_host(&self.staged)
    }

    pub fn restore_search_output(&self, value: &RuntimeString) -> RuntimeString {
        let staged = runtime_path_from_host(&self.staged).into_runtime_string();
        let logical = runtime_path_from_host(&self.logical_base).into_runtime_string();
        if value.as_utf16().starts_with(staged.as_utf16()) {
            logical.concat(&RuntimeString::from_utf16(
                value.as_utf16()[staged.len_utf16()..].to_vec(),
            ))
        } else {
            value.clone()
        }
    }
}

#[cfg(unix)]
fn runtime_path_from_host(path: &Path) -> RuntimePath {
    RuntimePath::from(NodeBufferUtf8::decode(path.as_os_str().as_bytes()))
}

#[cfg(windows)]
fn runtime_path_from_host(path: &Path) -> RuntimePath {
    use std::os::windows::ffi::OsStrExt;
    RuntimePath::from(RuntimeString::from_utf16(
        path.as_os_str().encode_wide().collect::<Vec<_>>(),
    ))
}

impl Drop for PinnedSnapshot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.temp_root);
    }
}

/// The absolute logical root a pin is asked for. Defined once: a shared pin's
/// registry key and the pin's own `logical_root` have to be the SAME path, or the
/// registry would key a pin by something other than what it names.
fn absolute_logical_root(path: &Path) -> io::Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

/// Pins handed out by [`PinnedMemoryRoot::pin_or_create_shared`], keyed by
/// logical root. `Weak` on purpose: the entry holds no directory handle of its
/// own, so the whole chain is released when the last holder drops.
fn shared_pins() -> &'static Mutex<HashMap<PathBuf, Weak<PinnedMemoryRoot>>> {
    static SHARED: OnceLock<Mutex<HashMap<PathBuf, Weak<PinnedMemoryRoot>>>> = OnceLock::new();
    SHARED.get_or_init(|| Mutex::new(HashMap::new()))
}

fn normalize_relative(path: &Path) -> io::Result<PathBuf> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(value) => normalized.push(value),
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "memory tool path escapes the pinned root",
                ));
            }
        }
    }
    Ok(normalized)
}

fn create_private_temp_dir(prefix: &str) -> io::Result<PathBuf> {
    for _ in 0..32 {
        let path = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
        match std::fs::create_dir(&path) {
            Ok(()) => {
                #[cfg(unix)]
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))?;
                return Ok(path);
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not allocate private memory-tool staging directory",
    ))
}

fn copy_open_file(source: &File, destination: &Path) -> io::Result<()> {
    use std::io::Write;
    let metadata = source.metadata()?;
    validate_regular_single_link(&metadata)?;
    let mut source = source.try_clone()?;
    source.seek(SeekFrom::Start(0))?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options
        .mode(metadata.mode() & 0o777)
        .custom_flags(libc::O_NOFOLLOW);
    let mut destination_file = options.open(destination)?;
    io::copy(&mut source, &mut destination_file)?;
    destination_file.flush()?;
    if let Ok(modified) = metadata.modified() {
        let mut times = std::fs::FileTimes::new().set_modified(modified);
        if let Ok(accessed) = metadata.accessed() {
            times = times.set_accessed(accessed);
        }
        destination_file.set_times(times)?;
    }
    #[cfg(unix)]
    destination_file.set_permissions(std::fs::Permissions::from_mode(metadata.mode() & 0o777))?;
    Ok(())
}

fn validate_regular_single_link(metadata: &Metadata) -> io::Result<()> {
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "memory tool target must be a regular file",
        ));
    }
    #[cfg(unix)]
    if metadata.nlink() != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "memory tool target must have exactly one hard link",
        ));
    }
    Ok(())
}

/// Open the model-visible directory first, then canonicalize/reopen its chain
/// and close the loop with handle identity. Any rename/reparse substitution
/// between those operations fails rather than silently binding a replacement.
fn pin_existing_directory(path: &Path) -> io::Result<(PathBuf, Vec<File>)> {
    let original = open_directory_path_no_follow(path)?;
    let canonical = crate::fs::node_path::canonicalize_host(path)?;
    let mut chain = pin_directory_chain(&canonical)?;
    let reopened = chain.last().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "canonical directory chain is empty",
        )
    })?;
    if !same_directory_identity(&original, reopened)? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "memory root changed while its canonical boundary was being pinned",
        ));
    }
    let _ = chain.pop();
    chain.push(original);
    Ok((canonical, chain))
}

#[cfg(unix)]
fn open_directory_path_no_follow(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
    options.open(path)
}

#[cfg(unix)]
fn file_identity(file: &File) -> io::Result<FileIdentity> {
    let metadata = file.metadata()?;
    Ok(FileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

#[cfg(unix)]
fn same_directory_identity(left: &File, right: &File) -> io::Result<bool> {
    Ok(file_identity(left)? == file_identity(right)?)
}

#[cfg(unix)]
fn c_name(name: &OsStr) -> io::Result<std::ffi::CString> {
    std::ffi::CString::new(name.as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "memory path component contains NUL",
        )
    })
}

#[cfg(unix)]
fn openat_file(parent: i32, name: &OsStr, flags: i32, mode: libc::mode_t) -> io::Result<File> {
    let name = c_name(name)?;
    // C varargs apply the default integer promotions; `mode_t` is `u16` on
    // Apple targets and therefore must be passed as an unsigned int.
    let fd = unsafe { libc::openat(parent, name.as_ptr(), flags, mode as libc::c_uint) };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

#[cfg(unix)]
fn openat_dir(parent: i32, name: &OsStr) -> io::Result<File> {
    openat_file(
        parent,
        name,
        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        0,
    )
}

#[cfg(unix)]
fn mkdirat(parent: i32, name: &OsStr) -> io::Result<()> {
    let name = c_name(name)?;
    let result = unsafe { libc::mkdirat(parent, name.as_ptr(), 0o700) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(unix)]
fn pin_directory_chain(canonical_root: &Path) -> io::Result<Vec<File>> {
    if !canonical_root.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "pinned memory root must be absolute",
        ));
    }
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let mut chain = vec![options.open(Path::new("/"))?];
    for component in canonical_root.components() {
        let Component::Normal(name) = component else {
            continue;
        };
        let next = openat_dir(chain.last().unwrap().as_raw_fd(), name)?;
        chain.push(next);
    }
    Ok(chain)
}

#[cfg(unix)]
fn create_missing_chain(
    chain: &mut Vec<File>,
    _canonical_ancestor: &Path,
    missing: &[std::ffi::OsString],
) -> io::Result<()> {
    for name in missing {
        let parent = chain.last().expect("existing ancestor is pinned");
        let next = match openat_dir(parent.as_raw_fd(), name) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                match mkdirat(parent.as_raw_fd(), name) {
                    Ok(()) => {}
                    Err(race) if race.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error),
                }
                openat_dir(parent.as_raw_fd(), name)?
            }
            Err(error) => return Err(error),
        };
        chain.push(next);
    }
    Ok(())
}

#[cfg(unix)]
fn open_parent_unix(
    root: &PinnedMemoryRoot,
    relative: &Path,
    create: bool,
) -> io::Result<(File, std::ffi::OsString)> {
    let mut components = relative.components().peekable();
    let mut parent = root.chain.last().unwrap().try_clone()?;
    loop {
        let Some(Component::Normal(name)) = components.next() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid memory path",
            ));
        };
        if components.peek().is_none() {
            return Ok((parent, name.to_os_string()));
        }
        parent = match openat_dir(parent.as_raw_fd(), name) {
            Ok(file) => file,
            Err(error) if create && error.kind() == io::ErrorKind::NotFound => {
                match mkdirat(parent.as_raw_fd(), name) {
                    Ok(()) => {}
                    Err(race) if race.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error),
                }
                openat_dir(parent.as_raw_fd(), name)?
            }
            Err(error) => return Err(error),
        };
    }
}

#[cfg(unix)]
fn open_regular_relative(
    root: &PinnedMemoryRoot,
    relative: &Path,
    write: bool,
    disposition: OpenDisposition,
) -> io::Result<PinnedFileLease> {
    let create_parents = disposition != OpenDisposition::Existing;
    let (parent, leaf) = open_parent_unix(root, relative, create_parents)?;
    let access = if write { libc::O_RDWR } else { libc::O_RDONLY };
    let flags = access | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK;
    let (file, existed_before, namespace) = match disposition {
        OpenDisposition::Existing => (
            openat_file(parent.as_raw_fd(), &leaf, flags, 0)?,
            true,
            UnixLeaseNamespace::Installed,
        ),
        OpenDisposition::CreateNew => {
            ensure_atomic_replace_supported()?;
            match openat_file(parent.as_raw_fd(), &leaf, flags, 0) {
                Ok(_) => return Err(io::Error::from(io::ErrorKind::AlreadyExists)),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            (
                create_anonymous_stage_unix(&parent)?,
                false,
                UnixLeaseNamespace::PendingCreate,
            )
        }
        OpenDisposition::OpenOrCreate => match openat_file(parent.as_raw_fd(), &leaf, flags, 0) {
            Ok(file) => (file, true, UnixLeaseNamespace::Installed),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                match openat_file(
                    parent.as_raw_fd(),
                    &leaf,
                    flags | libc::O_CREAT | libc::O_EXCL,
                    0o600,
                ) {
                    Ok(file) => (file, false, UnixLeaseNamespace::Installed),
                    Err(race) if race.kind() == io::ErrorKind::AlreadyExists => (
                        openat_file(parent.as_raw_fd(), &leaf, flags, 0)?,
                        true,
                        UnixLeaseNamespace::Installed,
                    ),
                    Err(error) => return Err(error),
                }
            }
            Err(error) => return Err(error),
        },
    };
    match namespace {
        UnixLeaseNamespace::Installed => validate_regular_single_link(&file.metadata()?)?,
        UnixLeaseNamespace::PendingCreate => validate_unpublished_stage(&file)?,
    }
    let identity = file_identity(&file)?;
    Ok(PinnedFileLease {
        file,
        existed_before,
        identity,
        parent,
        leaf,
        namespace,
    })
}

#[cfg(unix)]
fn validate_lease_namespace(lease: &PinnedFileLease) -> io::Result<()> {
    if lease.namespace == UnixLeaseNamespace::PendingCreate {
        validate_unpublished_stage(&lease.file)?;
        match openat_file(
            lease.parent.as_raw_fd(),
            &lease.leaf,
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
            0,
        ) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "pinned memory create target appeared before its atomic commit",
                ))
            }
        }
    }
    validate_regular_single_link(&lease.file.metadata()?)?;
    let reopened = openat_file(
        lease.parent.as_raw_fd(),
        &lease.leaf,
        libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
        0,
    )?;
    validate_regular_single_link(&reopened.metadata()?)?;
    let current = file_identity(&reopened)?;
    if current != lease.identity {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "pinned memory file was replaced while the operation was in flight",
        ));
    }
    Ok(())
}

#[cfg(unix)]
const STAGED_WRITE_PREFIX: &str = ".klaus-pinned-write-";

#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios"
))]
fn ensure_atomic_replace_supported() -> io::Result<()> {
    Ok(())
}

#[cfg(all(
    unix,
    not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))
))]
fn ensure_atomic_replace_supported() -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic pinned-memory replacement is unsupported on this Unix target",
    ))
}

#[cfg(unix)]
fn validate_unpublished_stage(file: &File) -> io::Result<()> {
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.nlink() != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "pinned memory staging inode must remain an unpublished regular file",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn staged_write_name() -> std::ffi::OsString {
    std::ffi::OsString::from(format!("{STAGED_WRITE_PREFIX}{}.tmp", uuid::Uuid::new_v4()))
}

#[cfg(unix)]
fn unlinkat_file(parent: &File, name: &OsStr) -> io::Result<()> {
    let name = c_name(name)?;
    let result = unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), 0) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(unix)]
fn open_named_regular(parent: &File, name: &OsStr) -> io::Result<File> {
    let file = openat_file(
        parent.as_raw_fd(),
        name,
        libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
        0,
    )?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "pinned memory namespace entry is no longer a regular file",
        ));
    }
    Ok(file)
}

#[cfg(unix)]
fn named_identity(parent: &File, name: &OsStr) -> io::Result<FileIdentity> {
    file_identity(&open_named_regular(parent, name)?)
}

/// Remove only the exact inode this owner published. An identity mismatch is
/// never "cleaned up": it is a third-party namespace update and must survive.
#[cfg(unix)]
fn cleanup_named_identity(parent: &File, name: &OsStr, expected: FileIdentity) -> io::Result<()> {
    let current = match named_identity(parent, name) {
        Ok(identity) => identity,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if current != expected {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "refusing to clean a pinned-memory staging name now owned by another inode",
        ));
    }
    unlinkat_file(parent, name)
}

#[cfg(unix)]
struct NamedStageGuard {
    parent: File,
    name: std::ffi::OsString,
    identity: FileIdentity,
    armed: bool,
}

#[cfg(unix)]
impl NamedStageGuard {
    fn new(parent: &File, name: std::ffi::OsString, identity: FileIdentity) -> io::Result<Self> {
        Ok(Self {
            parent: parent.try_clone()?,
            name,
            identity,
            armed: true,
        })
    }

    fn arm(&mut self, identity: FileIdentity) {
        self.identity = identity;
        self.armed = true;
    }

    fn disarm(&mut self) {
        self.armed = false;
    }

    fn cleanup(&mut self) -> io::Result<()> {
        if self.armed {
            cleanup_named_identity(&self.parent, &self.name, self.identity)?;
            self.armed = false;
        }
        Ok(())
    }
}

#[cfg(unix)]
impl Drop for NamedStageGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = cleanup_named_identity(&self.parent, &self.name, self.identity);
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn create_anonymous_stage_unix(parent: &File) -> io::Result<File> {
    ensure_atomic_replace_supported()?;
    let file = openat_file(
        parent.as_raw_fd(),
        OsStr::new("."),
        libc::O_RDWR | libc::O_TMPFILE | libc::O_CLOEXEC,
        0o600,
    )?;
    validate_unpublished_stage(&file)?;
    Ok(file)
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn create_anonymous_stage_unix(parent: &File) -> io::Result<File> {
    ensure_atomic_replace_supported()?;
    for _ in 0..32 {
        let name = staged_write_name();
        let file = match openat_file(
            parent.as_raw_fd(),
            &name,
            libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0o600,
        ) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        };
        let identity = file_identity(&file)?;
        cleanup_named_identity(parent, &name, identity)?;
        // A racing hard-linker may have linked the briefly named empty inode.
        // Never write into it unless the unlink made it truly anonymous.
        validate_unpublished_stage(&file)?;
        return Ok(file);
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not allocate an anonymous pinned-memory staging inode",
    ))
}

#[cfg(all(
    unix,
    not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))
))]
fn create_anonymous_stage_unix(_parent: &File) -> io::Result<File> {
    ensure_atomic_replace_supported()?;
    unreachable!("unsupported Unix targets return before staging")
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn list_linux_xattrs(file: &File) -> io::Result<Vec<std::ffi::CString>> {
    loop {
        let needed = unsafe { libc::flistxattr(file.as_raw_fd(), std::ptr::null_mut(), 0) };
        if needed < 0 {
            return Err(io::Error::last_os_error());
        }
        if needed == 0 {
            return Ok(Vec::new());
        }
        let mut bytes = vec![0_u8; needed as usize];
        let written =
            unsafe { libc::flistxattr(file.as_raw_fd(), bytes.as_mut_ptr().cast(), bytes.len()) };
        if written < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ERANGE) {
                continue;
            }
            return Err(error);
        }
        bytes.truncate(written as usize);
        return bytes
            .split(|byte| *byte == 0)
            .filter(|name| !name.is_empty())
            .map(|name| {
                std::ffi::CString::new(name).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "xattr name contains NUL")
                })
            })
            .collect();
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn read_linux_xattr(file: &File, name: &std::ffi::CStr) -> io::Result<Vec<u8>> {
    loop {
        let needed =
            unsafe { libc::fgetxattr(file.as_raw_fd(), name.as_ptr(), std::ptr::null_mut(), 0) };
        if needed < 0 {
            return Err(io::Error::last_os_error());
        }
        let mut value = vec![0_u8; needed as usize];
        let pointer = if value.is_empty() {
            std::ptr::null_mut()
        } else {
            value.as_mut_ptr().cast()
        };
        let written =
            unsafe { libc::fgetxattr(file.as_raw_fd(), name.as_ptr(), pointer, value.len()) };
        if written < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ERANGE) {
                continue;
            }
            return Err(error);
        }
        value.truncate(written as usize);
        return Ok(value);
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn copy_linux_xattrs(source: &File, destination: &File) -> io::Result<()> {
    let source_names = list_linux_xattrs(source)?;
    for name in list_linux_xattrs(destination)? {
        if !source_names
            .iter()
            .any(|source_name| source_name.as_bytes() == name.as_bytes())
            && unsafe { libc::fremovexattr(destination.as_raw_fd(), name.as_ptr()) } != 0
        {
            return Err(io::Error::last_os_error());
        }
    }
    for name in source_names {
        let value = read_linux_xattr(source, &name)?;
        let pointer = if value.is_empty() {
            std::ptr::null()
        } else {
            value.as_ptr().cast()
        };
        if unsafe {
            libc::fsetxattr(
                destination.as_raw_fd(),
                name.as_ptr(),
                pointer,
                value.len(),
                0,
            )
        } != 0
        {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn copy_unix_metadata(source: &File, destination: &File) -> io::Result<()> {
    let source_metadata = source.metadata()?;
    let destination_metadata = destination.metadata()?;
    if source_metadata.uid() != destination_metadata.uid()
        || source_metadata.gid() != destination_metadata.gid()
    {
        if unsafe {
            libc::fchown(
                destination.as_raw_fd(),
                source_metadata.uid(),
                source_metadata.gid(),
            )
        } != 0
        {
            return Err(io::Error::last_os_error());
        }
    }
    destination.set_permissions(std::fs::Permissions::from_mode(source_metadata.mode()))?;
    // Apply the exact extended metadata last. chmod/fchown may rewrite ACL masks
    // or clear privilege-related xattrs, so reversing this order would silently
    // lose observable metadata from the admitted inode.
    copy_linux_xattrs(source, destination)?;
    let mut times = std::fs::FileTimes::new();
    if let Ok(accessed) = source_metadata.accessed() {
        times = times.set_accessed(accessed);
    }
    if let Ok(modified) = source_metadata.modified() {
        times = times.set_modified(modified);
    }
    destination.set_times(times)
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn copy_unix_metadata(source: &File, destination: &File) -> io::Result<()> {
    let result = unsafe {
        libc::fcopyfile(
            source.as_raw_fd(),
            destination.as_raw_fd(),
            std::ptr::null_mut(),
            libc::COPYFILE_METADATA,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(all(
    unix,
    not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))
))]
fn copy_unix_metadata(_source: &File, _destination: &File) -> io::Result<()> {
    ensure_atomic_replace_supported()
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn publish_anonymous_stage(parent: &File, source: &File, name: &OsStr) -> io::Result<File> {
    let empty = std::ffi::CString::new("").expect("empty CString");
    let name = c_name(name)?;
    let result = unsafe {
        libc::linkat(
            source.as_raw_fd(),
            empty.as_ptr(),
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::AT_EMPTY_PATH,
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    source.try_clone()
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn publish_anonymous_stage(parent: &File, source: &File, name: &OsStr) -> io::Result<File> {
    const CLONE_ACL: u32 = 0x0004;
    let name = c_name(name)?;
    let result = unsafe {
        libc::fclonefileat(
            source.as_raw_fd(),
            parent.as_raw_fd(),
            name.as_ptr(),
            CLONE_ACL,
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    let cloned = openat_file(
        parent.as_raw_fd(),
        OsStr::from_bytes(name.as_bytes()),
        libc::O_RDWR | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
        0,
    )?;
    // clonefile intentionally clears some privilege bits for unprivileged
    // callers. Reapply the already-admitted staged metadata or fail before the
    // clone can enter the public leaf namespace.
    copy_unix_metadata(source, &cloned)?;
    Ok(cloned)
}

#[cfg(all(
    unix,
    not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))
))]
fn publish_anonymous_stage(_parent: &File, _source: &File, _name: &OsStr) -> io::Result<File> {
    ensure_atomic_replace_supported()?;
    unreachable!("unsupported Unix targets return before publishing")
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn atomic_exchange(parent: &File, left: &OsStr, right: &OsStr) -> io::Result<()> {
    let left = c_name(left)?;
    let right = c_name(right)?;
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            parent.as_raw_fd(),
            left.as_ptr(),
            parent.as_raw_fd(),
            right.as_ptr(),
            libc::RENAME_EXCHANGE as libc::c_uint,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn atomic_exchange(parent: &File, left: &OsStr, right: &OsStr) -> io::Result<()> {
    let left = c_name(left)?;
    let right = c_name(right)?;
    let result = unsafe {
        libc::renameatx_np(
            parent.as_raw_fd(),
            left.as_ptr(),
            parent.as_raw_fd(),
            right.as_ptr(),
            libc::RENAME_SWAP,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(all(
    unix,
    not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))
))]
fn atomic_exchange(_parent: &File, _left: &OsStr, _right: &OsStr) -> io::Result<()> {
    ensure_atomic_replace_supported()
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn atomic_rename_noreplace(parent: &File, from: &OsStr, to: &OsStr) -> io::Result<()> {
    let from = c_name(from)?;
    let to = c_name(to)?;
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            parent.as_raw_fd(),
            from.as_ptr(),
            parent.as_raw_fd(),
            to.as_ptr(),
            libc::RENAME_NOREPLACE as libc::c_uint,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn atomic_rename_noreplace(parent: &File, from: &OsStr, to: &OsStr) -> io::Result<()> {
    let from = c_name(from)?;
    let to = c_name(to)?;
    let result = unsafe {
        libc::renameatx_np(
            parent.as_raw_fd(),
            from.as_ptr(),
            parent.as_raw_fd(),
            to.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(all(
    unix,
    not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))
))]
fn atomic_rename_noreplace(_parent: &File, _from: &OsStr, _to: &OsStr) -> io::Result<()> {
    ensure_atomic_replace_supported()
}

#[cfg(unix)]
fn rollback_exchange(
    parent: &File,
    leaf: &OsStr,
    stage: &OsStr,
    candidate: FileIdentity,
    displaced: FileIdentity,
    guard: &mut NamedStageGuard,
) -> io::Result<()> {
    // Both endpoints are identity-checked immediately before rollback. If a
    // third party changed either name after the failed commit, do not overwrite
    // that later update merely to make our cleanup look successful.
    if named_identity(parent, leaf)? != candidate || named_identity(parent, stage)? != displaced {
        guard.disarm();
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "pinned-memory exchange rollback lost endpoint identity",
        ));
    }
    guard.disarm();
    atomic_exchange(parent, leaf, stage)?;
    if named_identity(parent, leaf)? != displaced || named_identity(parent, stage)? != candidate {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "pinned-memory exchange rollback could not verify the restored namespace",
        ));
    }
    guard.arm(candidate);
    guard.cleanup()
}

#[cfg(unix)]
fn replacement_error(message: &str, rollback: io::Result<()>) -> io::Error {
    match rollback {
        Ok(()) => io::Error::new(io::ErrorKind::NotFound, message.to_string()),
        Err(error) => io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("{message}; identity-safe rollback failed: {error}"),
        ),
    }
}

#[cfg(unix)]
fn replace_file_unix<T, F, P>(
    lease: &mut PinnedFileLease,
    operation: F,
    touch_modified: bool,
    prepare_candidate: P,
) -> io::Result<T>
where
    F: FnOnce(&mut File) -> io::Result<T>,
    P: FnOnce(&File) -> io::Result<()>,
{
    ensure_atomic_replace_supported()?;
    match lease.namespace {
        UnixLeaseNamespace::PendingCreate => {
            lease.validate_namespace()?;
            let value = operation(&mut lease.file)?;
            if touch_modified {
                lease.file.set_times(
                    std::fs::FileTimes::new().set_modified(std::time::SystemTime::now()),
                )?;
            }
            validate_unpublished_stage(&lease.file)?;
            // Recheck absence after the complete byte image exists but before
            // publishing even its private staging name.
            lease.validate_namespace()?;
            let stage_name = staged_write_name();
            let candidate = publish_anonymous_stage(&lease.parent, &lease.file, &stage_name)?;
            validate_regular_single_link(&candidate.metadata()?)?;
            let candidate_identity = file_identity(&candidate)?;
            let mut guard =
                NamedStageGuard::new(&lease.parent, stage_name.clone(), candidate_identity)?;
            prepare_candidate(&candidate)?;
            if let Err(error) = atomic_rename_noreplace(&lease.parent, &stage_name, &lease.leaf) {
                let cleanup = guard.cleanup();
                return Err(match cleanup {
                    Ok(()) => error,
                    Err(cleanup_error) => io::Error::new(
                        error.kind(),
                        format!("{error}; identity-safe staging cleanup failed: {cleanup_error}"),
                    ),
                });
            }
            guard.disarm();
            lease.file = candidate;
            lease.identity = candidate_identity;
            lease.namespace = UnixLeaseNamespace::Installed;
            lease.validate_namespace()?;
            Ok(value)
        }
        UnixLeaseNamespace::Installed => {
            lease.validate_namespace()?;
            let mut staged = create_anonymous_stage_unix(&lease.parent)?;
            copy_unix_metadata(&lease.file, &staged)?;
            let value = operation(&mut staged)?;
            if touch_modified {
                staged.set_times(
                    std::fs::FileTimes::new().set_modified(std::time::SystemTime::now()),
                )?;
            }
            validate_unpublished_stage(&staged)?;
            // The second nlink + parent/leaf/identity check closes the original
            // admission-to-write hardlink window. A link created after this
            // point is still safe because only the new staged inode is committed.
            lease.validate_namespace()?;
            let stage_name = staged_write_name();
            let candidate = publish_anonymous_stage(&lease.parent, &staged, &stage_name)?;
            validate_regular_single_link(&candidate.metadata()?)?;
            let candidate_identity = file_identity(&candidate)?;
            let mut guard =
                NamedStageGuard::new(&lease.parent, stage_name.clone(), candidate_identity)?;
            prepare_candidate(&candidate)?;
            // Publication itself creates no effect at `leaf`; validate once more
            // immediately before the only namespace mutation.
            if let Err(error) = lease.validate_namespace() {
                let cleanup = guard.cleanup();
                return Err(match cleanup {
                    Ok(()) => error,
                    Err(cleanup_error) => io::Error::new(
                        error.kind(),
                        format!("{error}; identity-safe staging cleanup failed: {cleanup_error}"),
                    ),
                });
            }
            atomic_exchange(&lease.parent, &stage_name, &lease.leaf)?;
            let displaced_identity = match named_identity(&lease.parent, &stage_name) {
                Ok(identity) => identity,
                Err(error) => {
                    guard.disarm();
                    return Err(io::Error::new(
                        error.kind(),
                        format!("atomic pinned-memory exchange lost its displaced inode: {error}"),
                    ));
                }
            };
            if displaced_identity != lease.identity {
                let rollback = rollback_exchange(
                    &lease.parent,
                    &lease.leaf,
                    &stage_name,
                    candidate_identity,
                    displaced_identity,
                    &mut guard,
                );
                return Err(replacement_error(
                    "pinned memory file was replaced before its atomic commit",
                    rollback,
                ));
            }
            // The displaced name is the exact admitted inode. It may now have
            // an attacker-created external hardlink, but deleting this one name
            // cannot alter those old bytes and never touches an unknown inode.
            guard.arm(displaced_identity);
            guard.cleanup()?;
            lease.file = candidate;
            lease.identity = candidate_identity;
            lease.validate_namespace()?;
            Ok(value)
        }
    }
}

#[cfg(unix)]
fn snapshot_entry(root: &PinnedMemoryRoot, relative: &Path, destination: &Path) -> io::Result<()> {
    if relative.as_os_str().is_empty() {
        return snapshot_dir_unix(
            openat_dir(root.chain.last().unwrap().as_raw_fd(), OsStr::new("."))?,
            destination,
        );
    }
    let (parent, leaf) = open_parent_unix(root, relative, false)?;
    match openat_dir(parent.as_raw_fd(), &leaf) {
        Ok(dir) => snapshot_dir_unix(dir, destination),
        Err(_) => {
            let file = openat_file(
                parent.as_raw_fd(),
                &leaf,
                libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
                0,
            )?;
            copy_open_file(&file, destination)
        }
    }
}

#[cfg(unix)]
fn pinned_directory_entries(
    root: &PinnedMemoryRoot,
    relative: &Path,
) -> io::Result<Vec<(std::ffi::OsString, bool)>> {
    let directory = if relative.as_os_str().is_empty() {
        openat_dir(root.chain.last().unwrap().as_raw_fd(), OsStr::new("."))?
    } else {
        let (parent, leaf) = open_parent_unix(root, relative, false)?;
        openat_dir(parent.as_raw_fd(), &leaf)?
    };
    Ok(read_dir_names_unix(&directory)?
        .into_iter()
        .map(|name| {
            let is_directory = openat_dir(directory.as_raw_fd(), &name).is_ok();
            (name, is_directory)
        })
        .collect())
}

#[cfg(windows)]
fn pinned_directory_entries(
    root: &PinnedMemoryRoot,
    relative: &Path,
) -> io::Result<Vec<(std::ffi::OsString, bool)>> {
    let mut held = root
        .chain
        .iter()
        .map(File::try_clone)
        .collect::<io::Result<Vec<_>>>()?;
    let mut path = root.canonical_root.clone();
    for component in relative.components() {
        path.push(component.as_os_str());
        let directory = directory_options().open(&path)?;
        validate_windows_directory(&directory)?;
        held.push(directory);
    }
    std::fs::read_dir(path)?
        .map(|entry| {
            let entry = entry?;
            Ok((entry.file_name(), entry.file_type()?.is_dir()))
        })
        .collect()
}

#[cfg(unix)]
fn snapshot_dir_unix(dir: File, destination: &Path) -> io::Result<()> {
    std::fs::create_dir(destination)?;
    std::fs::set_permissions(destination, std::fs::Permissions::from_mode(0o700))?;
    let mut names = read_dir_names_unix(&dir)?;
    names.sort();
    for name in names {
        let child_destination = destination.join(&name);
        match openat_dir(dir.as_raw_fd(), &name) {
            Ok(child) => snapshot_dir_unix(child, &child_destination)?,
            Err(_) => match openat_file(
                dir.as_raw_fd(),
                &name,
                libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
                0,
            ) {
                Ok(file) => copy_open_file(&file, &child_destination)?,
                Err(error)
                    if matches!(error.raw_os_error(), Some(libc::ELOOP))
                        || matches!(error.raw_os_error(), Some(libc::ENXIO))
                        || matches!(error.raw_os_error(), Some(libc::ENODEV)) => {}
                Err(error) => return Err(error),
            },
        }
    }
    Ok(())
}

#[cfg(unix)]
struct OwnedDir(*mut libc::DIR);

#[cfg(unix)]
impl Drop for OwnedDir {
    fn drop(&mut self) {
        unsafe {
            libc::closedir(self.0);
        }
    }
}

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
unsafe fn errno_location() -> *mut libc::c_int {
    unsafe { libc::__error() }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
unsafe fn errno_location() -> *mut libc::c_int {
    unsafe { libc::__errno_location() }
}

/// Enumerate one already-open directory. `fdopendir` owns the duplicate; entry
/// names are treated only as untrusted openat operands, and type/link checks are
/// repeated on the resulting handles.
#[cfg(unix)]
fn read_dir_names_unix(dir: &File) -> io::Result<Vec<std::ffi::OsString>> {
    let duplicate = unsafe { libc::fcntl(dir.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
    if duplicate < 0 {
        return Err(io::Error::last_os_error());
    }
    let raw_dir = unsafe { libc::fdopendir(duplicate) };
    if raw_dir.is_null() {
        let error = io::Error::last_os_error();
        unsafe {
            libc::close(duplicate);
        }
        return Err(error);
    }
    let owned = OwnedDir(raw_dir);
    let mut names = Vec::new();
    loop {
        unsafe {
            *errno_location() = 0;
        }
        let entry = unsafe { libc::readdir(owned.0) };
        if entry.is_null() {
            let errno = unsafe { *errno_location() };
            if errno != 0 {
                return Err(io::Error::from_raw_os_error(errno));
            }
            break;
        }
        let bytes = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if bytes == b"." || bytes == b".." {
            continue;
        }
        names.push(std::ffi::OsString::from_vec(bytes.to_vec()));
    }
    Ok(names)
}

/// Share mode for every handle a pin holds.
///
/// `FILE_SHARE_DELETE` is not optional. Windows refuses to delete or rename anything a
/// process holds open without it, so a pinned memory root made its whole tree undeletable
/// machine-wide for as long as the pin lived: the engine's own session teardown, `git
/// clean` and Explorer all failed with a sharing violation on a directory whose contents
/// had already been removed. Unix pins the same chain with `O_NOFOLLOW` and never blocked
/// unlink, so the guarantee this module documents does not rest on withholding delete
/// access — a held handle keeps addressing the object it opened, and a replacement is
/// still caught by the identity comparison on reopen.
#[cfg(windows)]
const PINNED_SHARE_MODE: u32 = {
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };
    FILE_SHARE_DELETE | FILE_SHARE_READ | FILE_SHARE_WRITE
};

#[cfg(windows)]
fn directory_options() -> OpenOptions {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
    };
    let mut options = OpenOptions::new();
    options
        .read(true)
        .share_mode(PINNED_SHARE_MODE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT);
    options
}

#[cfg(windows)]
fn open_directory_path_no_follow(path: &Path) -> io::Result<File> {
    let file = directory_options().open(path)?;
    validate_windows_directory(&file)?;
    Ok(file)
}

#[cfg(windows)]
fn file_identity(file: &File) -> io::Result<FileIdentity> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
    };
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    if unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, &mut info) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(FileIdentity {
        volume: info.dwVolumeSerialNumber,
        index: (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
    })
}

#[cfg(windows)]
fn same_directory_identity(left: &File, right: &File) -> io::Result<bool> {
    Ok(file_identity(left)? == file_identity(right)?)
}

#[cfg(windows)]
fn validate_windows_directory(file: &File) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION, FILE_ATTRIBUTE_DIRECTORY,
        FILE_ATTRIBUTE_REPARSE_POINT,
    };
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    if unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, &mut info) } == 0 {
        return Err(io::Error::last_os_error());
    }
    if info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY == 0
        || info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "memory root component must be a non-reparse directory",
        ));
    }
    Ok(())
}

#[cfg(windows)]
fn validate_windows_file(file: &File) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION, FILE_ATTRIBUTE_DIRECTORY,
        FILE_ATTRIBUTE_REPARSE_POINT,
    };
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    if unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, &mut info) } == 0 {
        return Err(io::Error::last_os_error());
    }
    if info.dwFileAttributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT) != 0
        || info.nNumberOfLinks != 1
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "memory tool target must be a regular single-link non-reparse file",
        ));
    }
    Ok(())
}

#[cfg(windows)]
fn pin_directory_chain(canonical_root: &Path) -> io::Result<Vec<File>> {
    let mut chain = Vec::new();
    let mut prefix = PathBuf::new();
    for component in canonical_root.components() {
        prefix.push(component.as_os_str());
        if matches!(component, Component::Prefix(_)) {
            continue;
        }
        let file = directory_options().open(&prefix)?;
        validate_windows_directory(&file)?;
        chain.push(file);
    }
    if chain.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "pinned memory root has no directory component",
        ));
    }
    Ok(chain)
}

#[cfg(windows)]
fn create_missing_chain(
    chain: &mut Vec<File>,
    canonical_ancestor: &Path,
    missing: &[std::ffi::OsString],
) -> io::Result<()> {
    let mut current = canonical_ancestor.to_path_buf();
    for name in missing {
        current.push(name);
        let next = match directory_options().open(&current) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                match std::fs::create_dir(&current) {
                    Ok(()) => {}
                    Err(race) if race.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error),
                }
                directory_options().open(&current)?
            }
            Err(error) => return Err(error),
        };
        validate_windows_directory(&next)?;
        chain.push(next);
    }
    Ok(())
}

#[cfg(windows)]
fn open_regular_relative(
    root: &PinnedMemoryRoot,
    relative: &Path,
    write: bool,
    disposition: OpenDisposition,
) -> io::Result<PinnedFileLease> {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
    let mut ancestors = root
        .chain
        .iter()
        .map(File::try_clone)
        .collect::<io::Result<Vec<_>>>()?;
    let mut current = root.canonical_root.clone();
    let mut components = relative.components().peekable();
    while let Some(Component::Normal(name)) = components.next() {
        current.push(name);
        if components.peek().is_none() {
            break;
        }
        let directory = match directory_options().open(&current) {
            Ok(file) => file,
            Err(error)
                if disposition != OpenDisposition::Existing
                    && error.kind() == io::ErrorKind::NotFound =>
            {
                match std::fs::create_dir(&current) {
                    Ok(()) => {}
                    Err(race) if race.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error),
                }
                directory_options().open(&current)?
            }
            Err(error) => return Err(error),
        };
        validate_windows_directory(&directory)?;
        ancestors.push(directory);
    }
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(write)
        .share_mode(PINNED_SHARE_MODE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    let (file, existed_before) = match disposition {
        OpenDisposition::Existing => (options.open(&current)?, true),
        OpenDisposition::CreateNew => {
            options.create_new(true);
            (options.open(&current)?, false)
        }
        OpenDisposition::OpenOrCreate => match options.open(&current) {
            Ok(file) => (file, true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                options.create_new(true);
                match options.open(&current) {
                    Ok(file) => (file, false),
                    Err(race) if race.kind() == io::ErrorKind::AlreadyExists => {
                        options.create_new(false);
                        (options.open(&current)?, true)
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(error) => return Err(error),
        },
    };
    validate_windows_file(&file)?;
    let identity = file_identity(&file)?;
    Ok(PinnedFileLease {
        file,
        projected_path: current,
        existed_before,
        identity,
        _ancestors: ancestors,
    })
}

#[cfg(windows)]
fn validate_lease_namespace(lease: &PinnedFileLease) -> io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
    validate_windows_file(&lease.file)?;
    let mut options = OpenOptions::new();
    options
        .read(true)
        .share_mode(PINNED_SHARE_MODE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    let reopened = options.open(&lease.projected_path)?;
    validate_windows_file(&reopened)?;
    if file_identity(&reopened)? != lease.identity {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "pinned memory file was replaced while the operation was in flight",
        ));
    }
    Ok(())
}

#[cfg(windows)]
fn snapshot_entry(root: &PinnedMemoryRoot, relative: &Path, destination: &Path) -> io::Result<()> {
    let held = root
        .chain
        .iter()
        .map(File::try_clone)
        .collect::<io::Result<Vec<_>>>()?;
    snapshot_entry_windows(&root.canonical_root.join(relative), destination, held)
}

#[cfg(windows)]
fn snapshot_entry_windows(
    source: &Path,
    destination: &Path,
    mut held: Vec<File>,
) -> io::Result<()> {
    if let Ok(directory) = directory_options().open(source) {
        if validate_windows_directory(&directory).is_ok() {
            held.push(directory);
            std::fs::create_dir(destination)?;
            for entry in std::fs::read_dir(source)? {
                let entry = entry?;
                snapshot_entry_windows(
                    &entry.path(),
                    &destination.join(entry.file_name()),
                    held.iter()
                        .map(File::try_clone)
                        .collect::<io::Result<Vec<_>>>()?,
                )?;
            }
            return Ok(());
        }
    }
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
    let mut options = OpenOptions::new();
    options
        .read(true)
        .share_mode(PINNED_SHARE_MODE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    let file = options.open(source)?;
    validate_windows_file(&file)?;
    copy_open_file(&file, destination)
}

#[cfg(not(any(unix, windows)))]
compile_error!("PinnedMemoryRoot requires Unix or Windows handle-relative filesystem support");

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("pinned-memory-root-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn one_root_is_pinned_once_however_many_holders_ask_for_it() {
        let root = temp_dir().join("memory");
        let first = PinnedMemoryRoot::pin_or_create_shared(&root).unwrap();
        let second = PinnedMemoryRoot::pin_or_create_shared(&root).unwrap();
        assert!(
            Arc::ptr_eq(&first, &second),
            "every holder of one root must share its handle chain: pinning per \
             holder is what spent 141 descriptors on a single directory"
        );
        assert_eq!(first.logical_root(), root.as_path());

        // The registry holds Weak, so the entry goes with the last holder and a
        // later ask pins again rather than resurrecting a dead pin.
        let root_path = root.clone();
        drop(first);
        drop(second);
        let third = PinnedMemoryRoot::pin_or_create_shared(&root_path).unwrap();
        assert_eq!(third.logical_root(), root_path.as_path());

        let parent = root.parent().unwrap().to_path_buf();
        drop(third);
        let _ = std::fs::remove_dir_all(parent);
    }

    #[test]
    fn a_replaced_root_is_pinned_again_instead_of_reused() {
        let base = temp_dir();
        let root = base.join("memory");
        let pinned = PinnedMemoryRoot::pin_or_create_shared(&root).unwrap();

        // The directory the pin names is moved aside and a different one takes
        // its pathname: reusing the cached pin here would bind a new holder to
        // the OLD inode, which is the identity the pin exists to guard.
        std::fs::rename(&root, base.join("memory-moved")).unwrap();
        std::fs::create_dir(&root).unwrap();
        let repinned = PinnedMemoryRoot::pin_or_create_shared(&root).unwrap();
        assert!(
            !Arc::ptr_eq(&pinned, &repinned),
            "a root that was replaced must not be served from the registry"
        );

        drop(pinned);
        drop(repinned);
        let _ = std::fs::remove_dir_all(base);
    }

    #[cfg(unix)]
    fn assert_no_staged_write_names(directory: &Path) {
        let names = std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .filter(|name| name.to_string_lossy().starts_with(STAGED_WRITE_PREFIX))
            .collect::<Vec<_>>();
        assert!(names.is_empty(), "staging names leaked: {names:?}");
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn set_test_xattr(path: &Path, value: &[u8]) {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let name = std::ffi::CString::new("user.klaus_pinned_metadata").unwrap();
        assert_eq!(
            unsafe {
                libc::fsetxattr(
                    file.as_raw_fd(),
                    name.as_ptr(),
                    value.as_ptr().cast(),
                    value.len(),
                    0,
                )
            },
            0
        );
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn read_test_xattr(path: &Path) -> Vec<u8> {
        let file = File::open(path).unwrap();
        let name = std::ffi::CString::new("user.klaus_pinned_metadata").unwrap();
        read_linux_xattr(&file, &name).unwrap()
    }

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    fn set_test_xattr(path: &Path, value: &[u8]) {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let name = std::ffi::CString::new("com.openai.klaus.pinned-metadata").unwrap();
        assert_eq!(
            unsafe {
                libc::fsetxattr(
                    file.as_raw_fd(),
                    name.as_ptr(),
                    value.as_ptr().cast(),
                    value.len(),
                    0,
                    0,
                )
            },
            0
        );
    }

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    fn read_test_xattr(path: &Path) -> Vec<u8> {
        let file = File::open(path).unwrap();
        let name = std::ffi::CString::new("com.openai.klaus.pinned-metadata").unwrap();
        let needed = unsafe {
            libc::fgetxattr(
                file.as_raw_fd(),
                name.as_ptr(),
                std::ptr::null_mut(),
                0,
                0,
                0,
            )
        };
        assert!(needed >= 0);
        let mut value = vec![0_u8; needed as usize];
        let written = unsafe {
            libc::fgetxattr(
                file.as_raw_fd(),
                name.as_ptr(),
                value.as_mut_ptr().cast(),
                value.len(),
                0,
                0,
            )
        };
        assert!(written >= 0);
        value.truncate(written as usize);
        value
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))]
    #[tokio::test]
    async fn root_replacement_cannot_redirect_a_pinned_write() {
        let parent = temp_dir();
        let root = parent.join("memory");
        let moved = parent.join("memory-moved");
        let outside = temp_dir();
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("note.md"), "inside").unwrap();
        std::fs::write(outside.join("note.md"), "sentinel").unwrap();
        let boundary = PinnedMemoryRoot::pin(&root).unwrap();
        std::fs::rename(&root, &moved).unwrap();
        std::os::unix::fs::symlink(&outside, &root).unwrap();

        let lease = boundary.open_write(root.join("note.md")).unwrap();
        lease
            .replace_blocking(|file| {
                file.set_len(0)?;
                file.seek(SeekFrom::Start(0))?;
                std::io::Write::write_all(file, b"updated")?;
                std::io::Write::flush(file)
            })
            .await
            .unwrap();

        let snapshot = boundary.snapshot_search(&root).unwrap();
        assert_eq!(
            std::fs::read_to_string(snapshot.path().join("note.md")).unwrap(),
            "updated",
            "search must enumerate the originally pinned directory rather than the replacement"
        );
        let staged_match = runtime_path_from_host(&snapshot.path().join("note.md"))
            .into_runtime_string()
            .concat(&RuntimeString::from(":1:updated"));
        assert_eq!(
            snapshot.restore_search_output(&staged_match),
            runtime_path_from_host(&root.join("note.md"))
                .into_runtime_string()
                .concat(&RuntimeString::from(":1:updated"))
        );

        assert_eq!(
            std::fs::read_to_string(moved.join("note.md")).unwrap(),
            "updated"
        );
        assert_eq!(
            std::fs::read_to_string(outside.join("note.md")).unwrap(),
            "sentinel"
        );
        let _ = std::fs::remove_file(&root);
        let _ = std::fs::remove_dir_all(parent);
        let _ = std::fs::remove_dir_all(outside);
    }

    #[cfg(unix)]
    #[test]
    fn descendant_symlink_is_rejected_without_touching_target() {
        let root = temp_dir();
        let outside = temp_dir().join("sentinel.md");
        std::fs::write(&outside, "sentinel").unwrap();
        std::os::unix::fs::symlink(&outside, root.join("escape.md")).unwrap();
        let boundary = PinnedMemoryRoot::pin(&root).unwrap();
        assert!(boundary.open_write(root.join("escape.md")).is_err());
        assert_eq!(std::fs::read_to_string(&outside).unwrap(), "sentinel");
        let outside_dir = outside.parent().unwrap().to_path_buf();
        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(outside_dir);
    }

    #[cfg(unix)]
    #[test]
    fn hardlinked_regular_file_is_rejected() {
        let root = temp_dir();
        let original = root.join("original.md");
        let alias = root.join("alias.md");
        std::fs::write(&original, "shared").unwrap();
        std::fs::hard_link(&original, &alias).unwrap();
        let boundary = PinnedMemoryRoot::pin(&root).unwrap();

        let error = boundary
            .open_write(&original)
            .expect_err("a multi-link file is not a mutable memory capability");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(std::fs::read_to_string(&alias).unwrap(), "shared");
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(any(
        windows,
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))]
    #[tokio::test]
    async fn missing_write_admission_does_not_create_before_the_mutation_gate() {
        let root = temp_dir();
        let target = root.join("missing-parent").join("new.md");
        let boundary = PinnedMemoryRoot::pin(&root).unwrap();

        let admission = boundary.open_write_existing(&target).unwrap_err();
        assert_eq!(admission.kind(), io::ErrorKind::NotFound);
        assert!(
            !target.exists() && !target.parent().unwrap().exists(),
            "stale/LSP admission must not expose an empty leaf or parent"
        );

        let created = boundary.create_write_new(&target).unwrap();
        #[cfg(unix)]
        assert!(
            !target.exists(),
            "Unix CreateNew owns an anonymous inode until no-replace commit"
        );
        created
            .replace_blocking(|file| {
                std::io::Write::write_all(file, b"committed")?;
                std::io::Write::flush(file)
            })
            .await
            .unwrap();
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "committed");
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(any(
        windows,
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))]
    #[test]
    fn concurrent_create_after_admission_is_not_overwritten() {
        let root = temp_dir();
        let target = root.join("new.md");
        let boundary = PinnedMemoryRoot::pin(&root).unwrap();
        assert_eq!(
            boundary.open_write_existing(&target).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );

        std::fs::write(&target, "concurrent owner").unwrap();
        let error = boundary
            .create_write_new(&target)
            .expect_err("CreateNew must fail closed when the leaf appears after admission");
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            "concurrent owner"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))]
    #[tokio::test]
    async fn concurrent_create_after_anonymous_staging_is_not_overwritten() {
        use std::io::Write;

        let root = temp_dir();
        let target = root.join("new.md");
        let boundary = PinnedMemoryRoot::pin(&root).unwrap();
        let lease = boundary.create_write_new(&target).unwrap();
        assert!(!target.exists());
        std::fs::write(&target, "concurrent owner").unwrap();

        let error = lease
            .replace_blocking(|file| {
                file.write_all(b"staged owner")?;
                file.flush()
            })
            .await
            .expect_err("the no-replace commit must preserve a concurrent creator");
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            "concurrent owner"
        );
        assert_no_staged_write_names(&root);
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_blocking_owner_keeps_the_real_fd_until_io_and_join_complete() {
        use std::io::Write;
        use std::os::fd::AsRawFd;

        let root = temp_dir();
        let target = root.join("cancelled-write.md");
        std::fs::write(&target, "before").unwrap();
        let boundary = PinnedMemoryRoot::pin(&root).unwrap();
        let lease = boundary.open_write_existing(&target).unwrap();
        let raw_fd = lease.file().as_raw_fd();
        let (entered_send, entered_receive) = std::sync::mpsc::channel();
        let (release_send, release_receive) = std::sync::mpsc::channel();
        let worker = tokio::spawn(async move {
            lease
                .replace_blocking(move |file| {
                    entered_send.send(()).unwrap();
                    release_receive.recv().unwrap();
                    file.set_len(0)?;
                    file.seek(SeekFrom::Start(0))?;
                    file.write_all(b"completed-after-cancel")?;
                    file.flush()
                })
                .await
                .unwrap()
        });
        entered_receive
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();

        worker.abort();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(
            !worker.is_finished(),
            "outer cancellation must wait for the handle-owned blocking operation"
        );
        assert!(
            unsafe { libc::fcntl(raw_fd, libc::F_GETFD) } >= 0,
            "the owned descriptor must remain live while cancellation waits for the I/O owner"
        );
        let probes = (0..64)
            .map(|_| File::open("/dev/null").unwrap())
            .collect::<Vec<_>>();
        assert!(
            probes.iter().all(|probe| probe.as_raw_fd() != raw_fd),
            "the pinned descriptor number must not be reusable by an unrelated file"
        );

        release_send.send(()).unwrap();
        assert!(worker.await.unwrap_err().is_cancelled());
        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            "completed-after-cancel"
        );
        assert_no_staged_write_names(&root);
        drop(probes);
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_external_hardlink_never_observes_the_staged_replacement_bytes() {
        use std::io::Write;

        let root = temp_dir();
        let outside = temp_dir();
        let target = root.join("note.md");
        let alias = outside.join("external-alias.md");
        std::fs::write(&target, "original bytes").unwrap();
        let boundary = PinnedMemoryRoot::pin(&root).unwrap();
        let lease = boundary.open_write_existing(&target).unwrap();
        let (entered_send, entered_receive) = std::sync::mpsc::channel();
        let (release_send, release_receive) = std::sync::mpsc::channel();
        let worker = tokio::spawn(async move {
            lease
                .replace_blocking(move |file| {
                    file.set_len(0)?;
                    file.seek(SeekFrom::Start(0))?;
                    file.write_all(b"replacement bytes")?;
                    file.flush()?;
                    entered_send.send(()).unwrap();
                    release_receive.recv().unwrap();
                    Ok(())
                })
                .await
        });
        entered_receive
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();

        std::fs::hard_link(&target, &alias).unwrap();
        assert_eq!(
            std::fs::read_to_string(&alias).unwrap(),
            "original bytes",
            "the operation must write only its anonymous staged inode"
        );
        release_send.send(()).unwrap();
        let error = worker
            .await
            .unwrap()
            .expect_err("the second nlink check must fail closed");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "original bytes");
        assert_eq!(std::fs::read_to_string(&alias).unwrap(), "original bytes");
        assert_no_staged_write_names(&root);
        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(outside);
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))]
    #[tokio::test]
    async fn atomic_replacement_preserves_mode_and_ownership() {
        use std::io::Write;

        let root = temp_dir();
        let target = root.join("metadata.md");
        std::fs::write(&target, "before").unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o640)).unwrap();
        set_test_xattr(&target, b"retained xattr");
        let before = std::fs::metadata(&target).unwrap();
        let boundary = PinnedMemoryRoot::pin(&root).unwrap();
        let lease = boundary.open_write_existing(&target).unwrap();
        lease
            .replace_blocking(|file| {
                file.set_len(0)?;
                file.seek(SeekFrom::Start(0))?;
                file.write_all(b"after")?;
                file.flush()
            })
            .await
            .unwrap();

        let after = std::fs::metadata(&target).unwrap();
        assert_eq!(after.mode() & 0o7777, before.mode() & 0o7777);
        assert_eq!(after.uid(), before.uid());
        assert_eq!(after.gid(), before.gid());
        assert_eq!(read_test_xattr(&target), b"retained xattr");
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "after");
        assert_no_staged_write_names(&root);
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))]
    #[test]
    fn identity_checked_cleanup_preserves_a_replaced_staging_name() {
        let root = temp_dir();
        let stage_name = std::ffi::OsString::from(format!("{STAGED_WRITE_PREFIX}cleanup-race.tmp"));
        let stage = root.join(&stage_name);
        let replacement = root.join("replacement.tmp");
        std::fs::write(&stage, "owned stage").unwrap();
        std::fs::write(&replacement, "third-party replacement").unwrap();
        let boundary = PinnedMemoryRoot::pin(&root).unwrap();
        let parent = boundary.chain.last().unwrap();
        let owned_identity = named_identity(parent, &stage_name).unwrap();
        std::fs::rename(&replacement, &stage).unwrap();

        cleanup_named_identity(parent, &stage_name, owned_identity)
            .expect_err("cleanup must not unlink a later third-party inode");
        assert_eq!(
            std::fs::read_to_string(&stage).unwrap(),
            "third-party replacement"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))]
    #[test]
    fn exchange_rollback_restores_only_identity_checked_endpoints() {
        let root = temp_dir();
        let leaf_name = std::ffi::OsString::from("note.md");
        let stage_name = std::ffi::OsString::from(format!("{STAGED_WRITE_PREFIX}rollback.tmp"));
        std::fs::write(root.join(&leaf_name), "third-party leaf").unwrap();
        std::fs::write(root.join(&stage_name), "candidate bytes").unwrap();
        let boundary = PinnedMemoryRoot::pin(&root).unwrap();
        let parent = boundary.chain.last().unwrap();
        let displaced = named_identity(parent, &leaf_name).unwrap();
        let candidate = named_identity(parent, &stage_name).unwrap();
        let mut guard = NamedStageGuard::new(parent, stage_name.clone(), candidate).unwrap();

        atomic_exchange(parent, &stage_name, &leaf_name).unwrap();
        rollback_exchange(
            parent,
            &leaf_name,
            &stage_name,
            candidate,
            displaced,
            &mut guard,
        )
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(root.join(&leaf_name)).unwrap(),
            "third-party leaf"
        );
        assert!(!root.join(&stage_name).exists());
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))]
    #[test]
    fn exchange_rollback_refuses_to_overwrite_a_later_leaf_update() {
        let root = temp_dir();
        let leaf_name = std::ffi::OsString::from("note.md");
        let stage_name =
            std::ffi::OsString::from(format!("{STAGED_WRITE_PREFIX}rollback-race.tmp"));
        let later = root.join("later.tmp");
        std::fs::write(root.join(&leaf_name), "candidate bytes").unwrap();
        std::fs::write(root.join(&stage_name), "displaced bytes").unwrap();
        std::fs::write(&later, "later third-party leaf").unwrap();
        let boundary = PinnedMemoryRoot::pin(&root).unwrap();
        let parent = boundary.chain.last().unwrap();
        let candidate = named_identity(parent, &leaf_name).unwrap();
        let displaced = named_identity(parent, &stage_name).unwrap();
        let mut guard = NamedStageGuard::new(parent, stage_name.clone(), candidate).unwrap();
        std::fs::rename(&later, root.join(&leaf_name)).unwrap();

        rollback_exchange(
            parent,
            &leaf_name,
            &stage_name,
            candidate,
            displaced,
            &mut guard,
        )
        .expect_err("rollback must stop when either endpoint identity changed");
        assert_eq!(
            std::fs::read_to_string(root.join(&leaf_name)).unwrap(),
            "later third-party leaf"
        );
        assert_eq!(
            std::fs::read_to_string(root.join(&stage_name)).unwrap(),
            "displaced bytes"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))]
    #[tokio::test]
    async fn atomic_leaf_replacement_is_rejected_before_success_can_be_published() {
        use std::io::Write;

        let root = temp_dir();
        let target = root.join("note.md");
        let replacement = root.join("replacement.md");
        std::fs::write(&target, "old inode").unwrap();
        std::fs::write(&replacement, "replacement sentinel").unwrap();
        let boundary = PinnedMemoryRoot::pin(&root).unwrap();
        let lease = boundary.open_write_existing(&target).unwrap();
        let replaced_target = target.clone();

        let error = lease
            .replace_blocking(move |file| {
                std::fs::rename(&replacement, &replaced_target)?;
                file.set_len(0)?;
                file.seek(SeekFrom::Start(0))?;
                file.write_all(b"staged write")?;
                file.flush()
            })
            .await
            .expect_err("the directory entry no longer names the pinned inode");
        assert!(matches!(
            error.kind(),
            io::ErrorKind::NotFound | io::ErrorKind::InvalidData
        ));
        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            "replacement sentinel"
        );
        assert_no_staged_write_names(&root);
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))]
    #[tokio::test]
    async fn unlinked_existing_leaf_is_rejected_without_mutating_the_unlinked_inode() {
        use std::io::Write;

        let root = temp_dir();
        let target = root.join("existing.md");
        std::fs::write(&target, "old bytes").unwrap();
        let boundary = PinnedMemoryRoot::pin(&root).unwrap();
        let lease = boundary.open_write_existing(&target).unwrap();

        let removed_target = target.clone();
        let error = lease
            .replace_blocking(move |file| {
                std::fs::remove_file(&removed_target)?;
                file.write_all(b"staged bytes")?;
                file.flush()
            })
            .await
            .expect_err("an unlinked admitted leaf must fail before namespace commit");
        assert!(matches!(
            error.kind(),
            io::ErrorKind::NotFound | io::ErrorKind::InvalidData
        ));
        assert!(!target.exists());
        assert_no_staged_write_names(&root);
        let _ = std::fs::remove_dir_all(root);
    }
}
