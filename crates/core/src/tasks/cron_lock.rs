//! Scheduler lease lock for `.klaus/scheduled_tasks.json`.
//!
//! When multiple engine sessions run in the same project directory, only one
//! should drive the cron scheduler. The first session to acquire this lock
//! becomes the scheduler; others stay passive and periodically probe the lock.
//! If the owner dies (PID no longer running), a passive session takes over.
//!
//! Pattern: O_EXCL atomic create, PID liveness probe, stale-lock recovery.
//! (This lease is distinct from the short-lived read-modify-write lock the
//! store takes around individual JSON mutations.)

use crate::runtime_values::{
    runtime_codec, runtime_json, NodeBufferUtf8, RuntimeCodecError, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};
use std::io;

const LOCK_FILE_NAME: &str = "scheduled_tasks.lock";

/// The lease body: a stable owner key plus the PID used as the liveness probe.
#[derive(Debug, Clone, PartialEq)]
pub struct CronSchedulerLock {
    /// A stable owner key for the lifetime of one session (the session id).
    pub session_id: RuntimeString,
    pub pid: i64,
    pub acquired_at: i64,
}

impl RuntimeEncode for CronSchedulerLock {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("sessionId", &self.session_id)?;
        object.field("pid", &self.pid)?;
        object.field("acquiredAt", &self.acquired_at)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for CronSchedulerLock {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                session_id: object.required("sessionId", &[])?,
                pid: object.required("pid", &[])?,
                acquired_at: object.required("acquiredAt", &[])?,
            })
        })
    }
}

fn lock_path(dir: &str) -> String {
    format!("{dir}/.klaus/{LOCK_FILE_NAME}")
}

async fn read_lock(dir: &str) -> Option<CronSchedulerLock> {
    let bytes = tokio::fs::read(lock_path(dir)).await.ok()?;
    let parsed = runtime_json::parse(&NodeBufferUtf8::decode(&bytes)).ok()?;
    CronSchedulerLock::decode_runtime(&parsed).ok()
}

/// Attempt the O_EXCL create. `false` = the file already exists.
async fn try_create_exclusive(lock: &CronSchedulerLock, dir: &str) -> io::Result<bool> {
    let path = lock_path(dir);
    let body = runtime_json::stringify(
        &runtime_codec::encode_value(lock).expect("CronSchedulerLock encodes to exact JSON"),
    )
    .to_node_utf8();
    // `.klaus/` may not exist yet — create it up front; in steady state the
    // directory already exists (scheduled_tasks.json lives there).
    if let Some(parent) = std::path::Path::new(&path).parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let path_for_create = path.clone();
    let created = tokio::task::spawn_blocking(move || {
        use std::io::Write;
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path_for_create)
        {
            Ok(mut file) => file.write_all(&body).map(|_| true),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(false),
            Err(error) => Err(error),
        }
    })
    .await
    .map_err(|error| io::Error::other(format!("lock create task failed: {error}")))??;
    Ok(created)
}

/// True if a local process with this PID currently exists.
#[cfg(unix)]
fn is_process_alive(pid: i64) -> bool {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return false;
    };
    if pid <= 0 {
        return false;
    }
    let rc = unsafe { libc::kill(pid, 0) };
    if rc == 0 {
        return true;
    }
    io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// True if a local process with this PID currently exists on Windows.
#[cfg(windows)]
fn is_process_alive(pid: i64) -> bool {
    use winapi::shared::winerror::{ERROR_ACCESS_DENIED, ERROR_INVALID_PARAMETER};
    use winapi::um::handleapi::CloseHandle;
    use winapi::um::minwinbase::STILL_ACTIVE;
    use winapi::um::processthreadsapi::{GetExitCodeProcess, OpenProcess};
    use winapi::um::winnt::PROCESS_QUERY_LIMITED_INFORMATION;

    let Ok(pid) = u32::try_from(pid) else {
        return false;
    };
    if pid == 0 {
        return false;
    }

    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if handle.is_null() {
        let error = io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or_default() as u32;
        return match error {
            ERROR_INVALID_PARAMETER => false,
            ERROR_ACCESS_DENIED => true,
            _ => true,
        };
    }

    let mut exit_code = 0;
    let queried = unsafe { GetExitCodeProcess(handle, &mut exit_code) };
    unsafe { CloseHandle(handle) };
    queried == 0 || exit_code == STILL_ACTIVE
}

/// Try to acquire the scheduler lock for `session_id`.
/// Returns `Ok(true)` on success, `Ok(false)` if another live session holds it.
///
/// Uses O_EXCL for atomic test-and-set. If the file exists:
///   - Already ours → `true` (idempotent re-acquire; a resumed session with a
///     new PID rewrites the lock so other sessions see a live PID)
///   - Another live PID → `false`
///   - Stale (PID dead / corrupt) → unlink and retry exclusive create once
///
/// If two sessions race to recover a stale lock, only one create succeeds.
pub async fn try_acquire_cron_scheduler_lock(
    dir: &str,
    session_id: &RuntimeString,
    now_ms: i64,
) -> io::Result<bool> {
    let lock = CronSchedulerLock {
        session_id: session_id.clone(),
        pid: i64::from(std::process::id()),
        acquired_at: now_ms,
    };

    if try_create_exclusive(&lock, dir).await? {
        return Ok(true);
    }

    let existing = read_lock(dir).await;

    // Already ours (idempotent). After a resume the session id is restored but
    // the process has a new PID — update the lock file so other sessions see a
    // live PID and don't steal it.
    if let Some(existing) = &existing {
        if &existing.session_id == session_id {
            if existing.pid != lock.pid {
                let body = runtime_json::stringify(
                    &runtime_codec::encode_value(&lock)
                        .expect("CronSchedulerLock encodes to exact JSON"),
                )
                .to_node_utf8();
                tokio::fs::write(lock_path(dir), body).await?;
            }
            return Ok(true);
        }
    }

    // Corrupt or unparseable → treat as stale. Another live session → blocked.
    if let Some(existing) = &existing {
        if is_process_alive(existing.pid) {
            return Ok(false);
        }
    }

    // Stale — unlink and retry the exclusive create once.
    let _ = tokio::fs::remove_file(lock_path(dir)).await;
    if try_create_exclusive(&lock, dir).await? {
        return Ok(true);
    }
    // Another session won the recovery race.
    Ok(false)
}

/// Release the scheduler lock if `session_id` owns it.
pub async fn release_cron_scheduler_lock(dir: &str, session_id: &RuntimeString) {
    let Some(existing) = read_lock(dir).await else {
        return;
    };
    if &existing.session_id != session_id {
        return;
    }
    let _ = tokio::fs::remove_file(lock_path(dir)).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> String {
        let base = std::env::temp_dir();
        let uniq = format!(
            "klaus-cron-lock-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = base.join(uniq);
        std::fs::create_dir_all(&dir).unwrap();
        dir.to_string_lossy().into_owned()
    }

    fn rs(value: &str) -> RuntimeString {
        RuntimeString::from(value)
    }

    #[tokio::test]
    async fn acquires_and_reacquires_idempotently() {
        let dir = temp_dir("acquire");
        let owner = rs("session-a");
        assert!(try_acquire_cron_scheduler_lock(&dir, &owner, 1)
            .await
            .unwrap());
        assert!(std::path::Path::new(&lock_path(&dir)).is_file());
        // Idempotent re-acquire by the same owner.
        assert!(try_acquire_cron_scheduler_lock(&dir, &owner, 2)
            .await
            .unwrap());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn blocks_a_second_live_session() {
        let dir = temp_dir("blocked");
        assert!(try_acquire_cron_scheduler_lock(&dir, &rs("session-a"), 1)
            .await
            .unwrap());
        // Same PID (this test process) is alive → the second session is blocked.
        assert!(!try_acquire_cron_scheduler_lock(&dir, &rs("session-b"), 2)
            .await
            .unwrap());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn reclaims_a_dead_pid_and_a_corrupt_lock() {
        let dir = temp_dir("stale");
        std::fs::create_dir_all(format!("{dir}/.klaus")).unwrap();
        // A dead-PID holder: PID 1 is init/launchd — never EPERM-visible as our
        // own live child, but always alive. Use an impossible PID instead.
        std::fs::write(
            lock_path(&dir),
            br#"{"sessionId":"gone","pid":999999999,"acquiredAt":1}"#,
        )
        .unwrap();
        assert!(try_acquire_cron_scheduler_lock(&dir, &rs("session-b"), 2)
            .await
            .unwrap());

        // Corrupt lock body → treated as stale.
        std::fs::write(lock_path(&dir), b"not json").unwrap();
        assert!(try_acquire_cron_scheduler_lock(&dir, &rs("session-c"), 3)
            .await
            .unwrap());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn release_only_removes_our_own_lock() {
        let dir = temp_dir("release");
        assert!(try_acquire_cron_scheduler_lock(&dir, &rs("session-a"), 1)
            .await
            .unwrap());
        // A different owner's release is a no-op.
        release_cron_scheduler_lock(&dir, &rs("session-b")).await;
        assert!(std::path::Path::new(&lock_path(&dir)).is_file());
        release_cron_scheduler_lock(&dir, &rs("session-a")).await;
        assert!(!std::path::Path::new(&lock_path(&dir)).exists());
        // Releasing when already gone is fine.
        release_cron_scheduler_lock(&dir, &rs("session-a")).await;
        let _ = std::fs::remove_dir_all(&dir);
    }
}
