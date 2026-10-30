//! A cross-process advisory file lock
//! (a `<target>.lock` DIRECTORY, since `mkdir` is atomic across processes) with
//! retry/backoff and stale-lock reclaim. The retries are sized for ~10 concurrent
//! sub-agents/teammates writing one task list; the defaults here reflect that
//! (retries 30, 5ms→100ms backoff). Used by the task store and (later) the swarm
//! mailbox — anywhere multiple agents in possibly-separate processes mutate a
//! shared file.

use std::future::Future;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;

use tokio::time::{sleep, Duration};

/// Tuning knobs for [`with_lock`]; `None` fields take the defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LockOptions {
    /// Max acquisition attempts after the first (default 30).
    pub retries: Option<u32>,
    /// Initial backoff between attempts, ms (default 5).
    pub min_timeout_ms: Option<u64>,
    /// Cap on backoff, ms (default 100).
    pub max_timeout_ms: Option<u64>,
    /// A lock older than this is treated as stale and reclaimed (default 10000).
    pub stale_ms: Option<u64>,
}

const DEFAULT_RETRIES: u32 = 30;
const DEFAULT_MIN_TIMEOUT_MS: u64 = 5;
const DEFAULT_MAX_TIMEOUT_MS: u64 = 100;
const DEFAULT_STALE_MS: u64 = 10_000;
const MIN_STALE_MS: u64 = 2_000;
const MIN_UPDATE_MS: u64 = 1_000;

/// Acquire `<target>.lock`, run `f`, then release — even if `f` fails.
pub async fn with_lock<T, F, Fut>(target: &str, f: F, opts: LockOptions) -> io::Result<T>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = io::Result<T>>,
{
    let lock_path = format!("{target}.lock");
    let guard = acquire(&lock_path, &opts).await?;
    let result = f().await;
    let release = guard.release().await;
    match (result, release) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(release)) => Err(io::Error::new(
            error.kind(),
            format!("{error}; lock release failed: {release}"),
        )),
    }
}

struct LockDirectoryGuard {
    lock_path: String,
    lease: Arc<Mutex<LockLease>>,
    heartbeat_cancel: CancellationToken,
    heartbeat: Option<tokio::task::JoinHandle<io::Result<()>>>,
    armed: bool,
}

impl LockDirectoryGuard {
    async fn release(mut self) -> io::Result<()> {
        self.armed = false;
        self.heartbeat_cancel.cancel();
        // The spawned owner is deliberately detached if this awaiting future is
        // cancelled. It still waits for the heartbeat and completes release,
        // so cancellation cannot strand a heartbeat-less lock directory.
        tokio::spawn(finish_release(
            self.lock_path.clone(),
            self.lease.clone(),
            self.heartbeat.take(),
        ))
        .await
        .map_err(|error| io::Error::other(format!("lock release task failed: {error}")))?
    }
}

impl Drop for LockDirectoryGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        self.heartbeat_cancel.cancel();
        let lock_path = self.lock_path.clone();
        let lease = self.lease.clone();
        let heartbeat = self.heartbeat.take();

        // Dropping `with_lock`'s future cancels the critical section. Keep a
        // release owner alive until the heartbeat has actually stopped; aborting
        // it and immediately calling rmdir races an in-flight mtime update.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = finish_release(lock_path, lease, heartbeat).await;
            });
        } else if let Some(heartbeat) = heartbeat {
            // There is no executor on which we can wait. Do not risk deleting a
            // directory while its final update might still be in flight. A
            // process shutdown leaves the ordinary stale-reclaim protocol to
            // the next holder.
            heartbeat.abort();
        }
    }
}

struct ProvisionalLockDirectory {
    path: String,
    identity: LockDirectoryIdentity,
    expected_mtime_ms: Option<i64>,
    armed: bool,
}

impl Drop for ProvisionalLockDirectory {
    fn drop(&mut self) {
        if self.armed
            && lock_directory_snapshot(Path::new(&self.path)).is_ok_and(|snapshot| {
                snapshot.identity == self.identity
                    && self
                        .expected_mtime_ms
                        .is_none_or(|expected| snapshot.mtime_ms == expected)
            })
        {
            let _ = std::fs::remove_dir(&self.path);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MtimePrecision {
    Seconds,
    Milliseconds,
}

#[derive(Debug)]
struct LockLease {
    identity: LockDirectoryIdentity,
    expected_mtime_ms: i64,
    precision: MtimePrecision,
    last_update: Instant,
    stale: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LockDirectorySnapshot {
    identity: LockDirectoryIdentity,
    mtime_ms: i64,
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LockDirectoryIdentity {
    device: u64,
    inode: u64,
}

#[cfg(unix)]
fn lock_directory_snapshot(path: &Path) -> io::Result<LockDirectorySnapshot> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::metadata(path)?;
    Ok(LockDirectorySnapshot {
        identity: LockDirectoryIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        },
        mtime_ms: system_time_to_unix_ms(metadata.modified()?)?,
    })
}

#[cfg(windows)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LockDirectoryIdentity {
    volume: u32,
    index: u64,
}

#[cfg(windows)]
fn lock_directory_snapshot(path: &Path) -> io::Result<LockDirectorySnapshot> {
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION, FILE_FLAG_BACKUP_SEMANTICS,
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };
    let file = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?;
    let mut information = BY_HANDLE_FILE_INFORMATION::default();
    if unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, &mut information) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let filetime_ticks = ((information.ftLastWriteTime.dwHighDateTime as u64) << 32)
        | information.ftLastWriteTime.dwLowDateTime as u64;
    const WINDOWS_TO_UNIX_EPOCH_MS: i128 = 11_644_473_600_000;
    let mtime_ms = i64::try_from(i128::from(filetime_ticks / 10_000) - WINDOWS_TO_UNIX_EPOCH_MS)
        .map_err(|_| io::Error::other("lock directory mtime is outside the supported range"))?;
    Ok(LockDirectorySnapshot {
        identity: LockDirectoryIdentity {
            volume: information.dwVolumeSerialNumber,
            index: ((information.nFileIndexHigh as u64) << 32) | information.nFileIndexLow as u64,
        },
        mtime_ms,
    })
}

#[cfg(not(any(unix, windows)))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LockDirectoryIdentity {
    created: Option<SystemTime>,
}

#[cfg(not(any(unix, windows)))]
fn lock_directory_snapshot(path: &Path) -> io::Result<LockDirectorySnapshot> {
    let metadata = std::fs::metadata(path)?;
    Ok(LockDirectorySnapshot {
        identity: LockDirectoryIdentity {
            created: metadata.created().ok(),
        },
        mtime_ms: system_time_to_unix_ms(metadata.modified()?)?,
    })
}

fn system_time_to_unix_ms(time: SystemTime) -> io::Result<i64> {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis())
            .map_err(|_| io::Error::other("mtime is outside the supported range")),
        Err(error) => {
            let millis = i64::try_from(error.duration().as_millis())
                .map_err(|_| io::Error::other("mtime is outside the supported range"))?;
            Ok(-millis)
        }
    }
}

fn unix_ms_now() -> io::Result<i64> {
    system_time_to_unix_ms(SystemTime::now())
}

fn set_lock_mtime(lock_path: &str, mtime_ms: i64) -> io::Result<()> {
    let seconds = mtime_ms.div_euclid(1_000);
    let nanoseconds = mtime_ms.rem_euclid(1_000) as u32 * 1_000_000;
    let mtime = filetime::FileTime::from_unix_time(seconds, nanoseconds);
    filetime::set_file_times(lock_path, mtime, mtime)
}

fn probe_mtime_precision(lock_path: &str) -> io::Result<LockDirectorySnapshot> {
    // proper-lockfile@4.1.2 probes by setting a deliberately non-second mtime.
    // Filesystems that round it back to a whole second use second precision.
    let now_ms = unix_ms_now()?;
    let probe_ms = now_ms.saturating_add(999).div_euclid(1_000) * 1_000 + 5;
    set_lock_mtime(lock_path, probe_ms)?;
    lock_directory_snapshot(Path::new(lock_path))
}

fn heartbeat_interval(stale_ms: u64) -> Duration {
    Duration::from_millis((stale_ms / 2).max(MIN_UPDATE_MS))
}

fn next_heartbeat_mtime(precision: MtimePrecision) -> io::Result<i64> {
    let now_ms = unix_ms_now()?;
    Ok(match precision {
        MtimePrecision::Seconds => now_ms.saturating_add(999).div_euclid(1_000) * 1_000,
        MtimePrecision::Milliseconds => now_ms,
    })
}

fn lock_lease<'a>(lease: &'a Mutex<LockLease>) -> io::Result<std::sync::MutexGuard<'a, LockLease>> {
    lease
        .lock()
        .map_err(|_| io::Error::other("lock lease state was poisoned"))
}

fn ownership_error(lock_path: &str, detail: &str) -> io::Error {
    io::Error::other(format!(
        "lock ownership was compromised for {lock_path}: {detail}"
    ))
}

fn validate_owned_snapshot(
    lock_path: &str,
    lease: &LockLease,
    snapshot: LockDirectorySnapshot,
) -> io::Result<()> {
    if snapshot.identity != lease.identity {
        return Err(ownership_error(lock_path, "directory identity changed"));
    }
    if snapshot.mtime_ms != lease.expected_mtime_ms {
        return Err(ownership_error(
            lock_path,
            &format!(
                "mtime changed from {} to {}",
                lease.expected_mtime_ms, snapshot.mtime_ms
            ),
        ));
    }
    Ok(())
}

fn heartbeat_update(lock_path: &str, lease: &Mutex<LockLease>) -> io::Result<()> {
    // Keep the validation, update and post-validation under one lease mutex so
    // release can never race a partially-completed heartbeat.
    let mut lease = lock_lease(lease)?;
    let before = lock_directory_snapshot(Path::new(lock_path))?;
    validate_owned_snapshot(lock_path, &lease, before)?;

    let requested_mtime_ms = next_heartbeat_mtime(lease.precision)?;
    set_lock_mtime(lock_path, requested_mtime_ms)?;

    let after = lock_directory_snapshot(Path::new(lock_path))?;
    if after.identity != lease.identity {
        return Err(ownership_error(
            lock_path,
            "directory identity changed during heartbeat",
        ));
    }
    if after.mtime_ms != requested_mtime_ms {
        return Err(ownership_error(
            lock_path,
            &format!(
                "heartbeat mtime update was not retained (requested {requested_mtime_ms}, observed {})",
                after.mtime_ms
            ),
        ));
    }
    lease.expected_mtime_ms = after.mtime_ms;
    lease.last_update = Instant::now();
    Ok(())
}

async fn heartbeat_lock_directory(
    lock_path: String,
    lease: Arc<Mutex<LockLease>>,
    cancel: CancellationToken,
    interval: Duration,
) -> io::Result<()> {
    let mut delay = interval;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = sleep(delay) => {
                match heartbeat_update(&lock_path, &lease) {
                    Ok(()) => delay = interval,
                    Err(error) => {
                        let (is_missing, over_stale) = {
                            let lease = lock_lease(&lease)?;
                            (
                                error.kind() == io::ErrorKind::NotFound,
                                lease.last_update.elapsed() > lease.stale,
                            )
                        };
                        if is_missing || over_stale || error.kind() == io::ErrorKind::Other {
                            return Err(ownership_error(&lock_path, &error.to_string()));
                        }
                        // proper-lockfile retries transient stat/utimes errors
                        // every second until the stale threshold is exceeded.
                        delay = Duration::from_millis(MIN_UPDATE_MS);
                    }
                }
            }
        }
    }
}

async fn finish_release(
    lock_path: String,
    lease: Arc<Mutex<LockLease>>,
    heartbeat: Option<tokio::task::JoinHandle<io::Result<()>>>,
) -> io::Result<()> {
    if let Some(heartbeat) = heartbeat {
        heartbeat
            .await
            .map_err(|error| io::Error::other(format!("lock heartbeat task failed: {error}")))??;
    }

    #[cfg(test)]
    pause_release_before_validation(&lock_path).await;

    let lease = lock_lease(&lease)?;
    let snapshot = lock_directory_snapshot(Path::new(&lock_path))
        .map_err(|error| ownership_error(&lock_path, &format!("release stat failed: {error}")))?;
    validate_owned_snapshot(&lock_path, &lease, snapshot)?;
    // The proper-lockfile directory remains empty. `remove_dir` is deliberate:
    // a non-empty directory is compromised and must never be recursively removed.
    std::fs::remove_dir(&lock_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("failed to release lock {lock_path}: {error}"),
        )
    })
}

#[cfg(test)]
#[derive(Debug)]
struct AcquisitionPause {
    lock_path: String,
    reached: Arc<tokio::sync::Notify>,
    resume: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
static ACQUISITION_PAUSE: std::sync::OnceLock<Mutex<Option<Arc<AcquisitionPause>>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
#[derive(Debug)]
struct ReleasePause {
    lock_path: String,
    reached: Arc<tokio::sync::Notify>,
    resume: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
static RELEASE_PAUSE: std::sync::OnceLock<Mutex<Option<Arc<ReleasePause>>>> =
    std::sync::OnceLock::new();

/// Deterministic cross-module seam for proving that callers do not publish a
/// captured business result before the final lock-ownership verdict.
#[cfg(test)]
pub(crate) fn install_release_validation_pause(
    lock_path: String,
) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
    let reached = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    *RELEASE_PAUSE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("release test hook mutex") = Some(Arc::new(ReleasePause {
        lock_path,
        reached: reached.clone(),
        resume: resume.clone(),
    }));
    (reached, resume)
}

#[cfg(test)]
pub(crate) fn clear_release_validation_pause() {
    *RELEASE_PAUSE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("release test hook mutex") = None;
}

#[cfg(test)]
async fn pause_acquisition_after_probe(lock_path: &str) {
    let pause = ACQUISITION_PAUSE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("acquisition test hook mutex")
        .as_ref()
        .filter(|pause| pause.lock_path == lock_path)
        .cloned();
    if let Some(pause) = pause {
        pause.reached.notify_one();
        pause.resume.notified().await;
    }
}

#[cfg(test)]
async fn pause_release_before_validation(lock_path: &str) {
    let pause = RELEASE_PAUSE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("release test hook mutex")
        .as_ref()
        .filter(|pause| pause.lock_path == lock_path)
        .cloned();
    if let Some(pause) = pause {
        pause.reached.notify_one();
        pause.resume.notified().await;
    }
}

async fn acquire(lock_path: &str, opts: &LockOptions) -> io::Result<LockDirectoryGuard> {
    let retries = opts.retries.unwrap_or(DEFAULT_RETRIES);
    let min_timeout_ms = opts.min_timeout_ms.unwrap_or(DEFAULT_MIN_TIMEOUT_MS);
    let max_timeout_ms = opts.max_timeout_ms.unwrap_or(DEFAULT_MAX_TIMEOUT_MS);
    let stale_ms = opts.stale_ms.unwrap_or(DEFAULT_STALE_MS).max(MIN_STALE_MS);
    let mut attempt: u32 = 0;
    while attempt <= retries {
        // Keep successful acquisition synchronous from mkdir through heartbeat
        // ownership transfer. Cancellation can only happen at an `.await`, so
        // it cannot strand a newly-created, heartbeat-less directory here.
        match std::fs::create_dir(lock_path) {
            Ok(()) => {
                let created = match lock_directory_snapshot(Path::new(lock_path)) {
                    Ok(created) => created,
                    Err(error) => {
                        // No await has occurred since our successful mkdir, so
                        // this is still the directory we just created.
                        let _ = std::fs::remove_dir(lock_path);
                        return Err(error);
                    }
                };
                let mut provisional = ProvisionalLockDirectory {
                    path: lock_path.to_string(),
                    identity: created.identity,
                    expected_mtime_ms: None,
                    armed: true,
                };
                let snapshot = probe_mtime_precision(lock_path)?;
                if snapshot.identity != created.identity {
                    return Err(ownership_error(
                        lock_path,
                        "directory identity changed during mtime precision probe",
                    ));
                }
                provisional.expected_mtime_ms = Some(snapshot.mtime_ms);
                let precision = if snapshot.mtime_ms.rem_euclid(1_000) == 0 {
                    MtimePrecision::Seconds
                } else {
                    MtimePrecision::Milliseconds
                };
                let lease = Arc::new(Mutex::new(LockLease {
                    identity: snapshot.identity,
                    expected_mtime_ms: snapshot.mtime_ms,
                    precision,
                    last_update: Instant::now(),
                    stale: Duration::from_millis(stale_ms),
                }));
                #[cfg(test)]
                pause_acquisition_after_probe(lock_path).await;
                let heartbeat_cancel = CancellationToken::new();
                let heartbeat = tokio::spawn(heartbeat_lock_directory(
                    lock_path.to_string(),
                    lease.clone(),
                    heartbeat_cancel.clone(),
                    heartbeat_interval(stale_ms),
                ));
                provisional.armed = false;
                return Ok(LockDirectoryGuard {
                    lock_path: lock_path.to_string(),
                    lease,
                    heartbeat_cancel,
                    heartbeat: Some(heartbeat),
                    armed: true,
                });
            }
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                // Reclaim a stale lock (a holder that crashed without releasing).
                if reclaim_stale_lock(lock_path, stale_ms)? {
                    // A stale reclaim does not consume the final opportunity:
                    // the next loop must actually create and own the directory.
                    continue;
                }
                if attempt == retries {
                    return Err(io::Error::other(format!(
                        "could not acquire lock {lock_path} after {retries} retries"
                    )));
                }
                let backoff = (max_timeout_ms as f64)
                    .min(min_timeout_ms as f64 * 2f64.powf(f64::from(attempt)));
                sleep(Duration::from_millis(backoff as u64)).await;
                attempt += 1;
            }
            Err(err) => return Err(err),
        }
    }
    Err(io::Error::other(format!(
        "could not acquire lock {lock_path} after {retries} retries"
    )))
}

fn reclaim_stale_lock(lock_path: &str, stale_ms: u64) -> io::Result<bool> {
    let stale_ms = stale_ms.max(MIN_STALE_MS);
    let observed = match lock_directory_snapshot(Path::new(lock_path)) {
        Ok(snapshot) => snapshot,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(true),
        Err(error) => return Err(error),
    };
    if !snapshot_is_stale(observed, stale_ms) {
        return Ok(false);
    }
    // Recheck the exact identity and mtime immediately before rmdir. A Rust
    // heartbeat or proper-lockfile update that raced the first stat makes the
    // directory live again. Removal is only reclaim; the caller must mkdir.
    match lock_directory_snapshot(Path::new(lock_path)) {
        Ok(current) if current == observed && snapshot_is_stale(current, stale_ms) => {}
        Ok(_) => return Ok(false),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(true),
        Err(error) => return Err(error),
    }
    match std::fs::remove_dir(lock_path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(error),
    }
}

fn snapshot_is_stale(snapshot: LockDirectorySnapshot, stale_ms: u64) -> bool {
    let stale_ms = i64::try_from(stale_ms).unwrap_or(i64::MAX);
    unix_ms_now().is_ok_and(|now_ms| snapshot.mtime_ms < now_ms.saturating_sub(stale_ms))
}

#[cfg(test)]
fn is_stale(lock_path: &str, stale_ms: u64) -> bool {
    lock_directory_snapshot(Path::new(lock_path))
        .is_ok_and(|snapshot| snapshot_is_stale(snapshot, stale_ms.max(MIN_STALE_MS)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::process::{Command, Output, Stdio};
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::sync::Arc;
    use tokio::time::timeout;

    fn temp_dir(tag: &str) -> PathBuf {
        let base = std::env::temp_dir();
        let uniq = format!(
            "ae-lockfile-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = base.join(uniq);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn old_mtime(path: &str) {
        set_lock_mtime(path, unix_ms_now().unwrap() - 5_000).unwrap();
    }

    async fn wait_for_path(path: &Path, exists: bool) {
        timeout(Duration::from_secs(5), async {
            while path.exists() != exists {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "timed out waiting for {} to {}",
                path.display(),
                if exists { "exist" } else { "disappear" }
            )
        });
    }

    /// The Node interpreter the oracle runs in: the `KLAUS_NODE` override, or the
    /// first `node` on PATH.
    fn node_executable() -> std::ffi::OsString {
        std::env::var_os("KLAUS_NODE").unwrap_or_else(|| "node".into())
    }

    fn proper_lockfile_module() -> Option<PathBuf> {
        if let Some(path) = std::env::var_os("KLAUS_PROPER_LOCKFILE") {
            let path = PathBuf::from(path);
            assert!(path.join("package.json").is_file(), "{}", path.display());
            let manifest = std::fs::read_to_string(path.join("package.json")).unwrap();
            assert!(
                manifest.contains("\"version\": \"4.1.2\""),
                "Node interop oracle must be proper-lockfile@4.1.2: {}",
                path.display()
            );
            return Some(path);
        }

        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let workspace = manifest
            .ancestors()
            .nth(3)
            .expect("core crate must be nested under the workspace directory");
        let candidates = [
            // Submodule layout: the engine is mounted at <app>/engine/<engine>, so
            // `workspace` here IS the mount directory and the app's node_modules
            // sits one level above it. The older two-levels-up guess resolved
            // outside the app repo entirely, and the candidate before it pointed
            // into a client tree that no longer exists — between them this oracle
            // could not find the module in a normal checkout.
            workspace.join("../node_modules/proper-lockfile"),
            workspace.join("../../node_modules/proper-lockfile"),
            manifest.join("node_modules/proper-lockfile"),
        ];
        let module = candidates
            .into_iter()
            .find(|path| path.join("package.json").is_file())?;
        let manifest = std::fs::read_to_string(module.join("package.json")).unwrap();
        assert!(
            manifest.contains("\"version\": \"4.1.2\""),
            "Node interop oracle must be proper-lockfile@4.1.2: {}",
            module.display()
        );
        Some(module)
    }

    /// The Node oracle, or `None` on a host that cannot run it.
    ///
    /// These tests compare this lock against `proper-lockfile@4.1.2` running in a real
    /// Node process, so they need BOTH the module and a Node interpreter. A machine
    /// missing either used to fail six tests with a spawn or missing-module panic, which
    /// says nothing about the lock; the reason is printed instead of swallowed so an
    /// oracle that disappears from a machine that should have one is still visible.
    fn node_oracle() -> Option<PathBuf> {
        if !crate::testing::node_interpreter_present() {
            println!(
                "skipping the Node lock-interop check: no `node` on PATH (set KLAUS_NODE, \
                 or install Node.js)"
            );
            return None;
        }
        match proper_lockfile_module() {
            Some(module) => Some(module),
            None => {
                println!(
                    "skipping the Node lock-interop check: proper-lockfile@4.1.2 is not \
                     installed here (set KLAUS_PROPER_LOCKFILE, or run the app's npm install)"
                );
                None
            }
        }
    }

    fn node_command(script: &str, target: &str, module: &Path) -> Command {
        let mut command = Command::new(node_executable());
        command
            .arg("-e")
            .arg(script)
            .arg(module)
            .arg(target)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
    }

    fn assert_node_success(output: Output, context: &str) -> String {
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        assert!(
            output.status.success(),
            "{context}: status={} stdout={stdout:?} stderr={stderr:?}",
            output.status
        );
        stdout
    }

    const NODE_LOCK_AND_RELEASE: &str = r#"
const lockfile = require(process.argv[1]);
const target = process.argv[2];
(async () => {
  const release = await lockfile.lock(target, {
    stale: 2000,
    update: 1000,
    retries: { retries: 200, factor: 1, minTimeout: 20, maxTimeout: 20 },
  });
  process.stdout.write('LOCKED\n');
  await release();
})().catch((error) => {
  console.error(error && (error.stack || error));
  process.exitCode = 1;
});
"#;

    #[tokio::test]
    async fn runs_fn_and_releases_lock() {
        let dir = temp_dir("run");
        let target = dir.join("state.json").to_string_lossy().into_owned();
        let lock = format!("{target}.lock");
        let held = with_lock(
            &target,
            || async {
                // CC proper-lockfile requires the lease directory to remain
                // empty; Rust uses the same identity + mtime lease model.
                Ok(Path::new(&lock).exists() && std::fs::read_dir(&lock)?.next().is_none())
            },
            LockOptions::default(),
        )
        .await
        .unwrap();
        assert!(held);
        assert!(!Path::new(&lock).exists()); // released afterwards
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn releases_lock_when_fn_fails() {
        let dir = temp_dir("fail");
        let target = dir.join("t").to_string_lossy().into_owned();
        let lock = format!("{target}.lock");
        let err = with_lock::<(), _, _>(
            &target,
            || async { Err(io::Error::other("boom")) },
            LockOptions::default(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.to_string(), "boom"); // the failure propagates...
        assert!(!Path::new(&lock).exists()); // ...but the lock is still released
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn fails_with_exact_message_when_lock_is_held() {
        let dir = temp_dir("held");
        let target = dir.join("held").to_string_lossy().into_owned();
        let lock = format!("{target}.lock");
        std::fs::create_dir(&lock).unwrap(); // a fresh (non-stale) foreign lock
        let opts = LockOptions {
            retries: Some(2),
            min_timeout_ms: Some(1),
            max_timeout_ms: Some(2),
            stale_ms: Some(60_000),
        };
        let err = with_lock::<(), _, _>(&target, || async { Ok(()) }, opts)
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            format!("could not acquire lock {lock} after 2 retries")
        );
        assert!(Path::new(&lock).exists()); // the foreign lock is left in place
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn reclaims_stale_lock() {
        let dir = temp_dir("stale");
        let target = dir.join("stale").to_string_lossy().into_owned();
        let lock = format!("{target}.lock");
        std::fs::create_dir(&lock).unwrap(); // a crashed holder's leftover
        old_mtime(&lock);
        let ran = Arc::new(AtomicBool::new(false));
        let flag = ran.clone();
        let opts = LockOptions {
            // proper-lockfile clamps stale to a minimum of 2 seconds.
            stale_ms: Some(1),
            ..Default::default()
        };
        with_lock(
            &target,
            || async move {
                flag.store(true, Ordering::SeqCst);
                Ok(())
            },
            opts,
        )
        .await
        .unwrap();
        assert!(ran.load(Ordering::SeqCst));
        assert!(!Path::new(&lock).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn final_attempt_stale_reclaim_reacquires_before_running() {
        let dir = temp_dir("quirk");
        let target = dir.join("quirk").to_string_lossy().into_owned();
        let lock = format!("{target}.lock");
        std::fs::create_dir(&lock).unwrap();
        old_mtime(&lock);
        let opts = LockOptions {
            retries: Some(0),
            stale_ms: Some(1),
            ..Default::default()
        };
        let held = with_lock(&target, || async { Ok(Path::new(&lock).exists()) }, opts)
            .await
            .unwrap();
        assert!(held);
        assert!(!Path::new(&lock).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn cancelling_acquisition_after_mkdir_leaves_no_orphan() {
        let dir = temp_dir("cancel-acquire");
        let target = dir.join("cancel-acquire").to_string_lossy().into_owned();
        let lock = format!("{target}.lock");
        let pause = Arc::new(AcquisitionPause {
            lock_path: lock.clone(),
            reached: Arc::new(tokio::sync::Notify::new()),
            resume: Arc::new(tokio::sync::Notify::new()),
        });
        *ACQUISITION_PAUSE
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap() = Some(pause.clone());

        let task = tokio::spawn(async move {
            with_lock::<(), _, _>(&target, || async { Ok(()) }, LockOptions::default()).await
        });
        timeout(Duration::from_secs(5), pause.reached.notified())
            .await
            .expect("acquisition reached the post-mkdir cancellation point");
        assert!(Path::new(&lock).is_dir());

        task.abort();
        let _ = task.await;
        *ACQUISITION_PAUSE
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap() = None;
        wait_for_path(Path::new(&lock), false).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn cancelled_critical_section_releases_its_owned_directory() {
        let dir = temp_dir("cancel");
        let target = dir.join("cancel").to_string_lossy().into_owned();
        let lock = format!("{target}.lock");
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let entered_for_task = entered.clone();
        let release_for_task = release.clone();
        let task = tokio::spawn(async move {
            with_lock::<(), _, _>(
                &target,
                || async move {
                    entered_for_task.notify_one();
                    release_for_task.notified().await;
                    Ok(())
                },
                LockOptions::default(),
            )
            .await
        });
        entered.notified().await;
        assert!(Path::new(&lock).exists());
        task.abort();
        let _ = task.await;
        wait_for_path(Path::new(&lock), false).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn cancelling_release_keeps_a_release_owner_until_rmdir() {
        let dir = temp_dir("cancel-release");
        let target = dir.join("cancel-release").to_string_lossy().into_owned();
        let lock = format!("{target}.lock");
        let pause = Arc::new(ReleasePause {
            lock_path: lock.clone(),
            reached: Arc::new(tokio::sync::Notify::new()),
            resume: Arc::new(tokio::sync::Notify::new()),
        });
        *RELEASE_PAUSE
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap() = Some(pause.clone());

        let task = tokio::spawn(async move {
            with_lock::<(), _, _>(&target, || async { Ok(()) }, LockOptions::default()).await
        });
        timeout(Duration::from_secs(5), pause.reached.notified())
            .await
            .expect("release reached the retained-owner cancellation point");
        assert!(Path::new(&lock).is_dir());

        task.abort();
        let _ = task.await;
        *RELEASE_PAUSE
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap() = None;
        pause.resume.notify_one();
        wait_for_path(Path::new(&lock), false).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn delayed_release_never_removes_a_successor_owner() {
        let dir = temp_dir("owner-token");
        let target = dir.join("owner-token").to_string_lossy().into_owned();
        let lock = format!("{target}.lock");
        let guard = acquire(&lock, &LockOptions::default()).await.unwrap();
        std::fs::remove_dir(&lock).unwrap();
        std::fs::create_dir(&lock).unwrap();

        let error = guard
            .release()
            .await
            .expect_err("replaced lock identity is a compromised release");

        assert!(Path::new(&lock).exists());
        assert!(std::fs::read_dir(&lock).unwrap().next().is_none());
        assert!(error.to_string().contains("ownership was compromised"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn heartbeat_keeps_a_long_running_rust_holder_live_past_stale_age() {
        let dir = temp_dir("heartbeat");
        let target = dir.join("heartbeat").to_string_lossy().into_owned();
        let lock = format!("{target}.lock");
        let opts = LockOptions {
            stale_ms: Some(2_000),
            ..Default::default()
        };
        let guard = acquire(&lock, &opts).await.unwrap();
        tokio::time::sleep(Duration::from_millis(2_600)).await;

        assert!(!is_stale(&lock, 2_000));
        assert!(!reclaim_stale_lock(&lock, 2_000).unwrap());

        guard.release().await.unwrap();
        assert!(!Path::new(&lock).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn same_identity_with_wrong_mtime_is_compromised_on_release() {
        let dir = temp_dir("mtime-owner-token");
        let target = dir.join("mtime-owner-token").to_string_lossy().into_owned();
        let lock = format!("{target}.lock");
        let guard = acquire(&lock, &LockOptions::default()).await.unwrap();
        let before = lock_directory_snapshot(Path::new(&lock)).unwrap();
        set_lock_mtime(&lock, before.mtime_ms + 137).unwrap();
        let after = lock_directory_snapshot(Path::new(&lock)).unwrap();
        assert_eq!(
            before.identity, after.identity,
            "the inode was not replaced"
        );

        let error = guard.release().await.unwrap_err();
        assert!(error.to_string().contains("mtime changed"));
        assert!(Path::new(&lock).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn mtime_compromise_turns_business_ok_into_error() {
        let dir = temp_dir("mtime-business-ok");
        let target = dir.join("mtime-business-ok").to_string_lossy().into_owned();
        let lock = format!("{target}.lock");
        let lock_for_fn = lock.clone();
        let result = with_lock(
            &target,
            || async move {
                let snapshot = lock_directory_snapshot(Path::new(&lock_for_fn))?;
                set_lock_mtime(&lock_for_fn, snapshot.mtime_ms + 211)?;
                Ok("business-ok")
            },
            LockOptions::default(),
        )
        .await;

        let error = result.expect_err("a compromised release must replace business Ok");
        assert!(error.to_string().contains("mtime changed"));
        assert!(Path::new(&lock).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn heartbeat_compromise_turns_business_ok_into_error() {
        let dir = temp_dir("heartbeat-compromise");
        let target = dir
            .join("heartbeat-compromise")
            .to_string_lossy()
            .into_owned();
        let lock = format!("{target}.lock");
        let lock_for_fn = lock.clone();
        let result = with_lock(
            &target,
            || async move {
                let snapshot = lock_directory_snapshot(Path::new(&lock_for_fn))?;
                set_lock_mtime(&lock_for_fn, snapshot.mtime_ms + 313)?;
                sleep(Duration::from_millis(1_150)).await;
                Ok("business-ok")
            },
            LockOptions {
                stale_ms: Some(2_000),
                ..Default::default()
            },
        )
        .await;

        let error = result.expect_err("heartbeat compromise must replace business Ok");
        assert!(error.to_string().contains("ownership was compromised"));
        assert!(Path::new(&lock).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn rmdir_failure_turns_business_ok_into_error() {
        let dir = temp_dir("release-failure");
        let target = dir.join("release-failure").to_string_lossy().into_owned();
        let lock = format!("{target}.lock");
        let lock_for_fn = lock.clone();
        let result = with_lock(
            &target,
            || async move {
                let owned = lock_directory_snapshot(Path::new(&lock_for_fn))?;
                std::fs::write(Path::new(&lock_for_fn).join("intruder"), b"not empty")?;
                // Preserve the owned directory mtime so release reaches the
                // rmdir itself; otherwise creating the entry is correctly
                // detected earlier as an mtime compromise.
                set_lock_mtime(&lock_for_fn, owned.mtime_ms)?;
                Ok("business-ok")
            },
            LockOptions::default(),
        )
        .await;

        let error = result.expect_err("rmdir failure must replace business Ok");
        assert!(error.to_string().contains("failed to release lock"));
        assert!(Path::new(&lock).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rust_held_lock_makes_node_wait() {
        let Some(module) = node_oracle() else { return };
        let dir = temp_dir("rust-held-node-waits");
        let target = dir.join("target").to_string_lossy().into_owned();
        std::fs::write(&target, b"target").unwrap();
        let lock = format!("{target}.lock");
        let guard = acquire(
            &lock,
            &LockOptions {
                stale_ms: Some(2_000),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let mut child = node_command(NODE_LOCK_AND_RELEASE, &target, &module)
            .spawn()
            .expect("launch Node proper-lockfile contender");
        sleep(Duration::from_millis(300)).await;
        assert!(
            child.try_wait().unwrap().is_none(),
            "Node must still be waiting while Rust owns the lock"
        );

        guard.release().await.unwrap();
        let output = tokio::task::spawn_blocking(move || child.wait_with_output())
            .await
            .unwrap()
            .unwrap();
        let stdout = assert_node_success(output, "Node acquire after Rust release");
        assert!(stdout.contains("LOCKED"));
        assert!(!Path::new(&lock).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn node_held_lock_makes_rust_wait() {
        const SCRIPT: &str = r#"
const fs = require('fs');
const lockfile = require(process.argv[1]);
const target = process.argv[2];
const ready = process.argv[3];
(async () => {
  const release = await lockfile.lock(target, { stale: 2000, update: 1000, retries: 0 });
  fs.writeFileSync(ready, 'LOCKED');
  await new Promise((resolve) => setTimeout(resolve, 1200));
  await release();
})().catch((error) => {
  console.error(error && (error.stack || error));
  process.exitCode = 1;
});
"#;
        let Some(module) = node_oracle() else { return };
        let dir = temp_dir("node-held-rust-waits");
        let target = dir.join("target").to_string_lossy().into_owned();
        let ready = dir.join("node-ready");
        std::fs::write(&target, b"target").unwrap();
        let mut command = node_command(SCRIPT, &target, &module);
        let child = command
            .arg(&ready)
            .spawn()
            .expect("launch Node proper-lockfile holder");
        wait_for_path(&ready, true).await;

        let started = Instant::now();
        let lock = format!("{target}.lock");
        let guard = acquire(
            &lock,
            &LockOptions {
                retries: Some(100),
                min_timeout_ms: Some(20),
                max_timeout_ms: Some(20),
                stale_ms: Some(2_000),
            },
        )
        .await
        .unwrap();
        assert!(
            started.elapsed() >= Duration::from_millis(700),
            "Rust acquired before the Node holder released"
        );

        let output = tokio::task::spawn_blocking(move || child.wait_with_output())
            .await
            .unwrap()
            .unwrap();
        assert_node_success(output, "Node holder release");
        guard.release().await.unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rust_crash_holder_helper() {
        let Some(target) = std::env::var_os("KLAUS_LOCKFILE_CRASH_TARGET") else {
            return;
        };
        let target = target.to_string_lossy().into_owned();
        let lock = format!("{target}.lock");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _guard = runtime
            .block_on(acquire(
                &lock,
                &LockOptions {
                    stale_ms: Some(2_000),
                    ..Default::default()
                },
            ))
            .unwrap();
        println!("LOCKED");
        std::io::stdout().flush().unwrap();
        std::process::exit(86);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rust_crash_lock_is_reclaimed_by_node() {
        let Some(module) = node_oracle() else { return };
        let dir = temp_dir("rust-crash-node-reclaim");
        let target = dir.join("target").to_string_lossy().into_owned();
        std::fs::write(&target, b"target").unwrap();
        let output = Command::new(std::env::current_exe().unwrap())
            .arg("fs::lockfile::tests::rust_crash_holder_helper")
            .arg("--exact")
            .arg("--nocapture")
            .env("KLAUS_LOCKFILE_CRASH_TARGET", &target)
            .output()
            .expect("launch crashing Rust lock holder");
        assert_eq!(output.status.code(), Some(86));
        assert!(String::from_utf8_lossy(&output.stdout).contains("LOCKED"));
        let lock = format!("{target}.lock");
        assert!(Path::new(&lock).is_dir());
        old_mtime(&lock);

        let output = node_command(NODE_LOCK_AND_RELEASE, &target, &module)
            .output()
            .expect("launch Node stale reclaimer");
        let stdout = assert_node_success(output, "Node reclaim after Rust crash");
        assert!(stdout.contains("LOCKED"));
        assert!(!Path::new(&lock).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn node_crash_lock_is_reclaimed_by_rust() {
        const SCRIPT: &str = r#"
const fs = require('fs');
const lockfile = require(process.argv[1]);
const target = process.argv[2];
const ready = process.argv[3];
(async () => {
  await lockfile.lock(target, { stale: 2000, update: 1000, retries: 0 });
  fs.writeFileSync(ready, 'LOCKED');
  setInterval(() => {}, 1000);
})().catch((error) => {
  console.error(error && (error.stack || error));
  process.exitCode = 1;
});
"#;
        let Some(module) = node_oracle() else { return };
        let dir = temp_dir("node-crash-rust-reclaim");
        let target = dir.join("target").to_string_lossy().into_owned();
        let ready = dir.join("node-ready");
        std::fs::write(&target, b"target").unwrap();
        let mut command = node_command(SCRIPT, &target, &module);
        let mut child = command
            .arg(&ready)
            .spawn()
            .expect("launch crashing Node lock holder");
        wait_for_path(&ready, true).await;
        child.kill().unwrap();
        let status = child.wait().unwrap();
        assert!(!status.success());

        let lock = format!("{target}.lock");
        assert!(Path::new(&lock).is_dir());
        old_mtime(&lock);
        let guard = acquire(
            &lock,
            &LockOptions {
                retries: Some(0),
                stale_ms: Some(2_000),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        guard.release().await.unwrap();
        assert!(!Path::new(&lock).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn node_cannot_steal_rust_lock_held_beyond_stale_threshold() {
        const SCRIPT: &str = r#"
const lockfile = require(process.argv[1]);
const target = process.argv[2];
(async () => {
  try {
    const release = await lockfile.lock(target, { stale: 2000, update: 1000, retries: 0 });
    await release();
    console.error('Node unexpectedly acquired the live Rust lock');
    process.exitCode = 2;
  } catch (error) {
    if (error && error.code === 'ELOCKED') {
      process.stdout.write('ELOCKED\n');
      return;
    }
    throw error;
  }
})().catch((error) => {
  console.error(error && (error.stack || error));
  process.exitCode = 1;
});
"#;
        let Some(module) = node_oracle() else { return };
        let dir = temp_dir("rust-long-hold-node-no-steal");
        let target = dir.join("target").to_string_lossy().into_owned();
        std::fs::write(&target, b"target").unwrap();
        let lock = format!("{target}.lock");
        let guard = acquire(
            &lock,
            &LockOptions {
                stale_ms: Some(2_000),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        sleep(Duration::from_millis(2_600)).await;

        let output = node_command(SCRIPT, &target, &module)
            .output()
            .expect("launch Node live-lock probe");
        let stdout = assert_node_success(output, "Node must reject live Rust lock");
        assert!(stdout.contains("ELOCKED"));
        guard.release().await.unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn serializes_concurrent_critical_sections() {
        let dir = temp_dir("conc");
        let target = dir.join("counter").to_string_lossy().into_owned();
        let file = dir.join("counter");
        std::fs::write(&file, "0").unwrap();
        let inside = Arc::new(AtomicBool::new(false));
        let overlaps = Arc::new(AtomicU32::new(0));
        let mut handles = Vec::new();
        for _ in 0..5 {
            let target = target.clone();
            let file = file.clone();
            let inside = inside.clone();
            let overlaps = overlaps.clone();
            handles.push(tokio::spawn(async move {
                with_lock(
                    &target,
                    || async {
                        if inside.swap(true, Ordering::SeqCst) {
                            overlaps.fetch_add(1, Ordering::SeqCst);
                        }
                        let n: u32 = tokio::fs::read_to_string(&file)
                            .await?
                            .trim()
                            .parse()
                            .unwrap();
                        sleep(Duration::from_millis(5)).await;
                        tokio::fs::write(&file, format!("{}", n + 1)).await?;
                        inside.store(false, Ordering::SeqCst);
                        Ok(())
                    },
                    LockOptions::default(),
                )
                .await
            }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }
        assert_eq!(overlaps.load(Ordering::SeqCst), 0); // never two holders at once
        let n: u32 = std::fs::read_to_string(&file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert_eq!(n, 5); // no lost updates
        let _ = std::fs::remove_dir_all(&dir);
    }
}
