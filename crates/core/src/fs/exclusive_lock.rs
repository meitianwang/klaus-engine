//! Whole-file advisory locks whose contention reads the same on every platform.
//!
//! `fs2` documents `try_lock_exclusive` as failing with [`io::ErrorKind::WouldBlock`] when
//! another holder has the file, and on Unix it does (`EWOULDBLOCK`). On Windows
//! `LockFileEx` fails with `ERROR_LOCK_VIOLATION`, which Rust maps to no kind at all, so a
//! `kind() == WouldBlock` test classifies a busy lock as a hard I/O failure. Every caller
//! that guessed that way reported "this store is broken" on Windows where it should have
//! reported "someone else holds it", turning ordinary contention into a fatal error.

use std::fs::File;
use std::io;

/// What a non-blocking exclusive lock attempt found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LockAttempt {
    /// This process now holds the lock; it is released when the file is closed.
    Acquired,
    /// Another holder has it. Not an error — the caller decides whether to wait.
    Contended,
}

/// Take the whole-file exclusive lock without blocking.
pub(crate) fn try_lock_exclusive(file: &File) -> io::Result<LockAttempt> {
    match fs2::FileExt::try_lock_exclusive(file) {
        Ok(()) => Ok(LockAttempt::Acquired),
        Err(error) if is_contended(&error) => Ok(LockAttempt::Contended),
        Err(error) => Err(error),
    }
}

/// Whether `error` means another holder has the lock rather than a real failure.
///
/// Both spellings are accepted: the kind for the platforms that set one, and the raw code
/// `fs2` itself uses for contention on the ones that do not.
pub(crate) fn is_contended(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::WouldBlock
        || (error.raw_os_error().is_some()
            && error.raw_os_error() == fs2::lock_contended_error().raw_os_error())
}

/// The error a caller surfaces for a lock it could not take, so contention is one shape
/// across the engine regardless of which platform reported it.
pub(crate) fn contended_error(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_busy_lock_is_contention_and_not_a_failure() {
        let dir = std::env::temp_dir().join(format!("ae-exclusive-lock-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("target");
        let holder = File::create(&path).unwrap();
        assert_eq!(try_lock_exclusive(&holder).unwrap(), LockAttempt::Acquired);

        let contender = File::open(&path).unwrap();
        assert_eq!(
            try_lock_exclusive(&contender).unwrap(),
            LockAttempt::Contended,
            "a second exclusive attempt must report contention, not an I/O error"
        );

        drop(contender);
        drop(holder);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn contention_is_recognised_by_kind_and_by_the_platform_code() {
        assert!(is_contended(&fs2::lock_contended_error()));
        assert!(is_contended(&io::Error::new(
            io::ErrorKind::WouldBlock,
            "busy"
        )));
        assert!(!is_contended(&io::Error::from(io::ErrorKind::NotFound)));
        // A kind-less error with no OS code must not be mistaken for contention.
        assert!(!is_contended(&io::Error::other("some other failure")));
    }
}
