use std::ffi::OsStr;
use std::sync::{Mutex, PoisonError};

/// The one process-environment mutex shared by exact parent capture and every
/// repository-owned mutation.
///
/// Node 20 protects `RealEnvStore::{Get,Set,Query,Delete,Enumerate}` with one
/// `per_process::env_var_mutex`. Rust's standard environment mutation API does
/// not expose that lock, so all repository mutation enters through the two
/// functions below while exact raw `environ`/`getenv` capture holds this same
/// mutex for its complete enumerate-plus-getter transaction.
static PROCESS_ENVIRONMENT_LOCK: Mutex<()> = Mutex::new(());

pub(crate) fn with_process_environment_lock<T>(operation: impl FnOnce() -> T) -> T {
    let _guard = PROCESS_ENVIRONMENT_LOCK
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    operation()
}

/// Mutate the process environment under the exact parent-capture mutex.
///
/// This API is public only because the workspace Eval crate owns environment
/// test/evaluation setup in the same process. It is hidden from generated
/// documentation, and a repository-wide source guard forbids any raw
/// `std::env::set_var` call outside this module.
#[doc(hidden)]
pub fn set_process_environment_var<K, V>(key: K, value: V)
where
    K: AsRef<OsStr>,
    V: AsRef<OsStr>,
{
    with_process_environment_lock(|| {
        set_process_environment_var_unlocked(key.as_ref(), value.as_ref())
    });
}

// Keep the one standard-library mutation reference in this non-generic,
// non-inlined function. The compiled-symbol guard can then prove that macro or
// generated code did not add a second direct mutation call outside the lock.
#[inline(never)]
fn set_process_environment_var_unlocked(key: &OsStr, value: &OsStr) {
    std::env::set_var(key, value);
}

/// Remove a process environment entry under the exact parent-capture mutex.
///
/// See [`set_process_environment_var`] for the intentionally narrow cross-crate
/// visibility and the source guard that closes raw mutation bypasses.
#[doc(hidden)]
pub fn remove_process_environment_var<K>(key: K)
where
    K: AsRef<OsStr>,
{
    with_process_environment_lock(|| remove_process_environment_var_unlocked(key.as_ref()));
}

#[inline(never)]
fn remove_process_environment_var_unlocked(key: &OsStr) {
    std::env::remove_var(key);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_capture_and_mutation_lock_is_exclusive() {
        let guard = PROCESS_ENVIRONMENT_LOCK
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (completed_tx, completed_rx) = std::sync::mpsc::channel();
        let contender = std::thread::spawn(move || {
            entered_tx.send(()).unwrap();
            with_process_environment_lock(|| completed_tx.send(()).unwrap());
        });

        entered_rx.recv().unwrap();
        assert!(
            completed_rx
                .recv_timeout(std::time::Duration::from_millis(50))
                .is_err(),
            "a second environment operation entered while capture held the lock",
        );
        drop(guard);
        completed_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        contender.join().unwrap();
    }
}
