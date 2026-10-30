//! Cross-process lock for the auto-memory consolidation pass.
//!
//! A single `.consolidate-lock` file inside the memory directory does double
//! duty:
//!  - its **mtime** is the last-consolidated timestamp (the 24h time-gate reads
//!    it), and
//!  - its **body** is the holder PID (so a crashed holder is reclaimable by
//!    diagnostics for the current holder).
//!
//! Acquisition preserves the prior successful-consolidation mtime. On Unix an
//! independent lock on the pinned physical directory is the stable ownership
//! domain, while the leaf lock remains for wire-compatible interoperation; on
//! Windows retained no-delete handles keep the leaf namespace stable. On success
//! the mtime advances to the consolidation time; on failure or forced
//! cancellation it is restored. Preserving it while held also means a process
//! crash cannot create a false cooldown for work that never completed.

use fs2::FileExt;
use std::io;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use crate::memory::pinned_root::{PinnedFileLease, PinnedMemoryRoot};

const LOCK_FILE: &str = ".consolidate-lock";

/// A holder older than this (by mtime) is treated as stale and reclaimable — 1h.
pub const HOLDER_STALE_MS: f64 = 60.0 * 60.0 * 1000.0;

/// A held consolidation lock.
#[derive(Debug)]
pub struct ConsolidationLock {
    /// The lock's mtime (ms) BEFORE we acquired — the previous last-consolidated
    /// time, for rollback.
    pub prior_mtime: f64,
    boundary: Arc<PinnedMemoryRoot>,
    file: Option<PinnedFileLease>,
    /// On Unix the physical memory-directory inode is the cooperative lock
    /// domain. A replaceable `.consolidate-lock` directory entry alone cannot be
    /// authoritative because unlinking it would create a second lockable inode.
    #[cfg(unix)]
    domain: Option<std::fs::File>,
}

impl Drop for ConsolidationLock {
    fn drop(&mut self) {
        // An aborted future cannot run the async rollback path. Restore the
        // pre-acquisition timestamp synchronously before releasing the OS lock
        // so a crash/forced shutdown never creates a false Dream cooldown.
        if self.file.is_some() {
            let prior_mtime = self.prior_mtime;
            let _ = update_locked_file(self, prior_mtime, true);
        }
        #[cfg(unix)]
        if let Some(domain) = self.domain.take() {
            let _ = FileExt::unlock(&domain);
        }
    }
}

/// Pin the canonical absolute consolidation directory component by component.
pub fn pin_consolidation_directory(dir: &str) -> io::Result<Arc<PinnedMemoryRoot>> {
    PinnedMemoryRoot::pin(Path::new(dir))
}

/// The last consolidation time (the lock file's mtime in ms), or 0 if it has
/// never run.
pub async fn read_last_consolidated_at(dir: &str) -> f64 {
    read_last_consolidated_at_checked(dir).await.unwrap_or(0.0)
}

/// Checked form used by the authoritative lifecycle path so real filesystem
/// failures are observable instead of being mistaken for "never consolidated".
pub async fn read_last_consolidated_at_checked(dir: &str) -> io::Result<f64> {
    let boundary = pin_consolidation_directory(dir)?;
    read_last_consolidated_at_pinned(&boundary).await
}

/// Read the time gate through the already-admitted directory handle.
pub async fn read_last_consolidated_at_pinned(boundary: &Arc<PinnedMemoryRoot>) -> io::Result<f64> {
    match boundary.open_read(boundary.logical_root().join(LOCK_FILE)) {
        Ok(file) => Ok(mtime_ms(&file.metadata()?)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(0.0),
        Err(error) => Err(error),
    }
}

/// Try to acquire the consolidation lock. Returns the prior mtime (for rollback)
/// on success, or `None` when another session/process owns the OS lock.
pub async fn try_acquire_consolidation_lock(
    dir: &str,
    pid: i64,
    _now: f64,
) -> io::Result<Option<ConsolidationLock>> {
    let boundary = pin_consolidation_directory(dir)?;
    try_acquire_consolidation_lock_pinned(boundary, pid, _now).await
}

/// Acquire the leaf through the same pinned root used by the pre-lock read.
pub async fn try_acquire_consolidation_lock_pinned(
    boundary: Arc<PinnedMemoryRoot>,
    pid: i64,
    _now: f64,
) -> io::Result<Option<ConsolidationLock>> {
    #[cfg(unix)]
    let domain = {
        let domain = boundary.open_root_lock_domain()?;
        match crate::fs::exclusive_lock::try_lock_exclusive(&domain)? {
            crate::fs::exclusive_lock::LockAttempt::Acquired => domain,
            crate::fs::exclusive_lock::LockAttempt::Contended => return Ok(None),
        }
    };
    let lease = boundary.open_write(boundary.logical_root().join(LOCK_FILE))?;
    match crate::fs::exclusive_lock::try_lock_exclusive(lease.file())? {
        crate::fs::exclusive_lock::LockAttempt::Acquired => {}
        crate::fs::exclusive_lock::LockAttempt::Contended => return Ok(None),
    }
    let prior_mtime = if !lease.existed_before() {
        0.0
    } else {
        mtime_ms(&lease.metadata()?)
    };
    // Establish RAII ownership before the first mutation. Any intermediate
    // truncate/write/flush/timestamp failure then restores the previous epoch
    // and releases the OS lock through Drop.
    let mut lock = ConsolidationLock {
        prior_mtime,
        boundary,
        file: Some(lease),
        #[cfg(unix)]
        domain: Some(domain),
    };
    let mutation = (|| {
        let lease = lock.file.as_mut().expect("newly owned lock file");
        lease.validate_namespace()?;
        lease.replace_sync_locked(|file| {
            file.set_len(0)?;
            file.seek(SeekFrom::Start(0))?;
            file.write_all(format!("{pid}\n").as_bytes())?;
            file.flush()?;
            set_file_times_ms_on(file, prior_mtime)
        })?;
        lease.validate_namespace()
    })();
    mutation?;
    Ok(Some(lock))
}

impl ConsolidationLock {
    /// The exact boundary retained by the lock and passed into the Dream fork.
    pub fn pinned_root(&self) -> Arc<PinnedMemoryRoot> {
        self.boundary.clone()
    }
}

/// Success: stamp the lock's mtime to `now` (the new last-consolidated time) and
/// clear the holder.
pub async fn record_consolidation(
    _dir: &str,
    now: f64,
    mut lock: ConsolidationLock,
) -> io::Result<()> {
    let prior_mtime = lock.prior_mtime;
    let result = update_locked_file(&mut lock, 0.0_f64.max(now), true);
    if result.is_err() {
        let _ = update_locked_file(&mut lock, prior_mtime, true);
    }
    result
}

/// Failure: restore the lock to its prior state so the time-gate re-opens for a
/// later retry. A first-ever acquisition leaves an empty epoch-stamped state
/// file instead of unlinking after unlock, avoiding a remove/recreate lock race.
pub async fn rollback_consolidation_lock(
    _dir: &str,
    mut lock: ConsolidationLock,
) -> io::Result<()> {
    let prior_mtime = lock.prior_mtime;
    update_locked_file(&mut lock, prior_mtime, true)
}

fn update_locked_file(
    lock: &mut ConsolidationLock,
    timestamp_ms: f64,
    clear_holder: bool,
) -> io::Result<()> {
    let Some(lease) = lock.file.as_mut() else {
        return Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "consolidation lock is no longer owned",
        ));
    };
    lease.validate_namespace()?;
    lease.replace_sync_locked(|file| {
        if clear_holder {
            file.set_len(0)?;
            file.seek(SeekFrom::Start(0))?;
            file.flush()?;
        }
        set_file_times_ms_on(file, timestamp_ms)
    })?;
    lease.validate_namespace()?;
    // Release the stable domain first while the epoch leaf is still locked.
    // A new implementation may acquire the directory domain but must then wait
    // on this leaf; a legacy implementation that knows only the leaf also
    // remains excluded until all mutations above are complete.
    #[cfg(unix)]
    if let Some(domain) = lock.domain.take() {
        FileExt::unlock(&domain)?;
    }
    FileExt::unlock(lease.file())?;
    lock.file = None;
    Ok(())
}

/// A file's mtime in milliseconds (whole + fractional), or 0 on anomaly.
fn mtime_ms(meta: &std::fs::Metadata) -> f64 {
    meta.modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as f64 * 1000.0 + f64::from(d.subsec_nanos()) / 1e6)
        .unwrap_or(0.0)
}

fn set_file_times_ms_on(file: &std::fs::File, ms: f64) -> io::Result<()> {
    let secs = (ms / 1000.0).floor();
    let sub_ms = ms - secs * 1000.0;
    let dur = Duration::new(secs.max(0.0) as u64, (sub_ms * 1_000_000.0).round() as u32);
    let t = UNIX_EPOCH + dur;
    file.set_times(std::fs::FileTimes::new().set_accessed(t).set_modified(t))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    const LOCK: &str = ".consolidate-lock";

    fn temp_dir() -> PathBuf {
        static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "klaus-agent-dream-lock-{}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        dir.push(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn self_pid() -> i64 {
        std::process::id() as i64
    }

    /// A fixed "now".
    const T: f64 = 1_000_000_000_000.0;

    #[tokio::test]
    async fn reports_no_prior_consolidation_on_an_empty_dir() {
        let dir = temp_dir();
        let d = dir.to_string_lossy().into_owned();
        assert_eq!(read_last_consolidated_at(&d).await, 0.0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The holder PID, read through the owner's own handle.
    ///
    /// Two constraints leave no other vantage point. The lock is a whole-file exclusive
    /// lock, advisory on unix but mandatory on Windows, where a read through any other
    /// handle fails with ERROR_LOCK_VIOLATION while it is held; and releasing restores the
    /// pre-acquisition content, so the PID no longer exists afterwards.
    fn holder_pid_through_owner(lock: &ConsolidationLock) -> String {
        use std::io::{Read, Seek, SeekFrom};
        let lease = lock.file.as_ref().expect("a live owner holds its lease");
        let mut handle = lease.file();
        handle.seek(SeekFrom::Start(0)).unwrap();
        let mut content = String::new();
        handle.read_to_string(&mut content).unwrap();
        content.trim().to_string()
    }

    #[tokio::test]
    async fn acquires_a_fresh_lock_preserving_the_epoch_and_writing_the_holder_pid() {
        let dir = temp_dir();
        let d = dir.to_string_lossy().into_owned();
        let self_pid = self_pid();
        let lock = try_acquire_consolidation_lock(&d, self_pid, T)
            .await
            .unwrap()
            .expect("first owner");
        assert_eq!(lock.prior_mtime, 0.0);
        assert_eq!(read_last_consolidated_at(&d).await, 0.0);
        assert_eq!(holder_pid_through_owner(&lock), self_pid.to_string());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn refuses_a_second_owner_in_the_same_live_process() {
        let dir = temp_dir();
        let d = dir.to_string_lossy().into_owned();
        // holder = this (live) process
        let first = try_acquire_consolidation_lock(&d, self_pid(), T)
            .await
            .unwrap()
            .expect("first owner");
        let second = try_acquire_consolidation_lock(&d, self_pid(), T + 60_000.0)
            .await
            .unwrap();
        assert!(second.is_none());
        drop(first);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn never_steals_a_still_live_os_lock_after_the_legacy_staleness_window() {
        let dir = temp_dir();
        let d = dir.to_string_lossy().into_owned();
        let first = try_acquire_consolidation_lock(&d, self_pid(), T)
            .await
            .unwrap()
            .expect("first owner");
        let reclaimed = try_acquire_consolidation_lock(&d, self_pid(), T + HOLDER_STALE_MS + 1.0)
            .await
            .unwrap();
        assert!(reclaimed.is_none());
        drop(first);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn reacquires_after_the_prior_owner_drops_without_cleanup() {
        let dir = temp_dir();
        let d = dir.to_string_lossy().into_owned();
        let prior_success = T - HOLDER_STALE_MS - 1.0;
        let seed = try_acquire_consolidation_lock(&d, self_pid(), prior_success)
            .await
            .unwrap()
            .expect("seed owner");
        record_consolidation(&d, prior_success, seed).await.unwrap();
        let mut first = try_acquire_consolidation_lock(&d, self_pid(), T)
            .await
            .unwrap()
            .expect("first owner");
        assert!((read_last_consolidated_at(&d).await - prior_success).abs() < 1000.0);
        // Simulate process exit: the kernel releases the OS lock, but Rust Drop
        // does not run and therefore cannot restore the pre-acquisition epoch.
        let lease = first.file.take().expect("simulated crashed owner file");
        FileExt::unlock(lease.file()).unwrap();
        drop(lease);
        drop(first);
        let reclaimed = try_acquire_consolidation_lock(&d, self_pid(), T + 1000.0)
            .await
            .unwrap()
            .expect("reclaimed owner");
        assert!((reclaimed.prior_mtime - prior_success).abs() < 1000.0);
        assert_eq!(holder_pid_through_owner(&reclaimed), self_pid().to_string());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn records_a_successful_consolidation_by_stamping_mtime_now_and_clearing_the_holder() {
        let dir = temp_dir();
        let d = dir.to_string_lossy().into_owned();
        let lock = try_acquire_consolidation_lock(&d, self_pid(), T)
            .await
            .unwrap()
            .expect("owner");
        let done = T + 5000.0;
        record_consolidation(&d, done, lock).await.unwrap();
        assert!((read_last_consolidated_at(&d).await - done).abs() < 1000.0);
        let content = std::fs::read_to_string(dir.join(LOCK)).unwrap();
        assert_eq!(content.trim(), ""); // holder cleared
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn rolls_back_a_first_ever_acquisition_to_an_unlocked_epoch_state() {
        let dir = temp_dir();
        let d = dir.to_string_lossy().into_owned();
        let lock = try_acquire_consolidation_lock(&d, self_pid(), T)
            .await
            .unwrap()
            .expect("owner");
        assert_eq!(lock.prior_mtime, 0.0);
        rollback_consolidation_lock(&d, lock).await.unwrap();
        assert_eq!(read_last_consolidated_at(&d).await, 0.0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn rolls_back_a_re_acquisition_to_the_prior_consolidation_time() {
        let dir = temp_dir();
        let d = dir.to_string_lossy().into_owned();
        // A prior consolidation happened at T.
        let first = try_acquire_consolidation_lock(&d, self_pid(), T - HOLDER_STALE_MS - 1.0)
            .await
            .unwrap()
            .expect("first owner");
        record_consolidation(&d, T - HOLDER_STALE_MS - 1.0, first)
            .await
            .unwrap();
        let prior = read_last_consolidated_at(&d).await;
        // A later attempt acquires (prior is stale) but then fails → rolls back to `prior`.
        let lock = try_acquire_consolidation_lock(&d, self_pid(), T)
            .await
            .unwrap()
            .expect("second owner");
        rollback_consolidation_lock(&d, lock).await.unwrap();
        assert!((read_last_consolidated_at(&d).await - prior).abs() < 1000.0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn refuses_a_symlink_lock_file_without_touching_its_target() {
        let dir = temp_dir();
        let outside = temp_dir().join("outside.txt");
        std::fs::write(&outside, "do not truncate").unwrap();
        std::os::unix::fs::symlink(&outside, dir.join(LOCK)).unwrap();
        let d = dir.to_string_lossy().into_owned();

        assert!(read_last_consolidated_at_checked(&d).await.is_err());
        assert!(try_acquire_consolidation_lock(&d, self_pid(), T)
            .await
            .is_err());
        assert_eq!(
            std::fs::read_to_string(&outside).unwrap(),
            "do not truncate"
        );
        let outside_dir = outside.parent().unwrap().to_path_buf();
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(outside_dir);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn root_replacement_cannot_redirect_lock_record_or_read() {
        let parent = temp_dir();
        let dir = parent.join("memory");
        let moved = parent.join("memory-moved");
        let outside = temp_dir();
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(outside.join(LOCK), "outside sentinel").unwrap();
        let boundary = PinnedMemoryRoot::pin(&dir).unwrap();
        let lock = try_acquire_consolidation_lock_pinned(boundary.clone(), self_pid(), T)
            .await
            .unwrap()
            .expect("pinned owner");

        std::fs::rename(&dir, &moved).unwrap();
        std::os::unix::fs::symlink(&outside, &dir).unwrap();
        let done = T + 5_000.0;
        record_consolidation(&dir.to_string_lossy(), done, lock)
            .await
            .unwrap();

        assert!(
            (read_last_consolidated_at_pinned(&boundary).await.unwrap() - done).abs() < 1_000.0
        );
        assert_eq!(
            std::fs::read_to_string(moved.join(LOCK)).unwrap(),
            "",
            "recording must clear the holder on the originally pinned lock file"
        );
        assert_eq!(
            std::fs::read_to_string(outside.join(LOCK)).unwrap(),
            "outside sentinel"
        );
        assert!(
            read_last_consolidated_at_checked(&dir.to_string_lossy())
                .await
                .is_err(),
            "a fresh pathname pin must reject the replacement symlink"
        );

        let _ = std::fs::remove_file(&dir);
        let _ = std::fs::remove_dir_all(parent);
        let _ = std::fs::remove_dir_all(outside);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn replacing_the_epoch_leaf_cannot_create_a_second_lock_domain() {
        let dir = temp_dir();
        let d = dir.to_string_lossy().into_owned();
        let boundary = PinnedMemoryRoot::pin(&dir).unwrap();
        let first = try_acquire_consolidation_lock_pinned(boundary.clone(), self_pid(), T)
            .await
            .unwrap()
            .expect("first physical-directory owner");
        let displaced = dir.join(".consolidate-lock.displaced");
        std::fs::rename(dir.join(LOCK), &displaced).unwrap();
        std::fs::write(dir.join(LOCK), "replacement epoch").unwrap();

        let same_session_second =
            try_acquire_consolidation_lock_pinned(boundary.clone(), self_pid(), T + 1_000.0)
                .await
                .unwrap();
        assert!(
            same_session_second.is_none(),
            "independent opens from one retained boundary must still contend"
        );
        let independent_boundary = PinnedMemoryRoot::pin(&dir).unwrap();
        let independent_second = try_acquire_consolidation_lock_pinned(
            independent_boundary.clone(),
            self_pid(),
            T + 1_000.0,
        )
        .await
        .unwrap();
        assert!(
            independent_second.is_none(),
            "the pinned directory inode, not the replaceable epoch leaf, is the cross-process lock domain"
        );
        let error = record_consolidation(&d, T + 1_000.0, first)
            .await
            .expect_err("record must reject an epoch leaf that lost its pinned identity");
        assert!(matches!(
            error.kind(),
            io::ErrorKind::NotFound | io::ErrorKind::InvalidData
        ));
        assert_eq!(
            std::fs::read_to_string(dir.join(LOCK)).unwrap(),
            "replacement epoch"
        );

        let replacement_owner =
            try_acquire_consolidation_lock_pinned(independent_boundary, self_pid(), T + 2_000.0)
                .await
                .unwrap()
                .expect("the directory domain is released only after the first owner settles");
        rollback_consolidation_lock(&d, replacement_owner)
            .await
            .unwrap();
        let _ = std::fs::remove_dir_all(dir);
    }
}
