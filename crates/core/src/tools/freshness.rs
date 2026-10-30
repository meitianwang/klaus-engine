//! The freshness checks the write-class tools run during input validation,
//! before a write is applied.
//!
//! There are two rules because the tools carry different risk. Write and
//! NotebookEdit put down content the model never had to match against the file,
//! so an unread file could be silently clobbered: they demand a prior complete
//! read ([`check_read_before_write`]). Edit and MultiEdit only ever replace text
//! they matched byte-for-byte in the content they just read off disk, so an
//! unread file is not at risk and is not blocked; what they guard is a baseline
//! the model reasoned about having moved underneath it ([`check_stale_write`]).

use crate::runtime_values::{RuntimePath, RuntimeString};
use crate::types::file_state::FileStateCache;

/// The write-tool read-before-write / stale-write messages (verbatim).
pub const NOT_READ_MESSAGE: &str =
    "File has not been read yet. Read it first before writing to it.";
pub const STALE_MESSAGE: &str = "File has been modified since read, either by the user or by a linter. Read it again before attempting to write it.";

/// The read-before-write + stale-write guard for the tools that put down content
/// wholesale (Write, NotebookEdit). Returns a blocking error message, or `None`
/// to proceed.
///
/// - Not fully read (no captured content) → [`NOT_READ_MESSAGE`].
/// - On-disk mtime later than the recorded baseline → [`STALE_MESSAGE`], UNLESS
///   `disk_content` is supplied and still matches the recorded full content
///   (the content fallback that prevents mtime-only false positives).
pub fn check_read_before_write(
    abs: &RuntimePath,
    st_mtime_ms: f64,
    file_state: &dyn FileStateCache,
    disk_content: Option<&RuntimeString>,
) -> Option<&'static str> {
    let entry = match file_state.get(abs) {
        Some(e) if e.content.is_some() => e,
        _ => return Some(NOT_READ_MESSAGE),
    };
    // Floor both sides to whole ms to avoid sub-ms jitter.
    if st_mtime_ms.floor() > entry.mtime_ms.floor() {
        if let Some(disk) = disk_content {
            if entry.content.as_ref() == Some(disk) {
                return None; // content fallback: mtime moved but content identical
            }
        }
        return Some(STALE_MESSAGE);
    }
    None
}

/// The stale-write guard for the string-replacing tools. Returns a blocking
/// error message, or `None` to proceed.
///
/// Never reports "not read": these tools resolve `oldString` against
/// `disk_content`, the text they just read, so a file with no baseline cannot be
/// clobbered by them. It reports [`STALE_MESSAGE`] when the agent DID capture a
/// full baseline, the on-disk mtime is later, and the content really differs —
/// the model is reasoning about a version that no longer exists, so it is sent
/// back to read again. A baseline without content (a ranged or truncated read)
/// holds nothing to compare against and proceeds.
pub fn check_stale_write(
    abs: &RuntimePath,
    st_mtime_ms: f64,
    file_state: &dyn FileStateCache,
    disk_content: &RuntimeString,
) -> Option<&'static str> {
    let entry = file_state.get(abs)?;
    let baseline = entry.content?;
    // Floor both sides to whole ms to avoid sub-ms jitter.
    if st_mtime_ms.floor() > entry.mtime_ms.floor() && &baseline != disk_content {
        return Some(STALE_MESSAGE);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::file_state::{FileStateEntry, RecordOptions};
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct TestCache {
        map: Mutex<HashMap<RuntimePath, FileStateEntry>>,
    }

    impl FileStateCache for TestCache {
        fn get(&self, path: &RuntimePath) -> Option<FileStateEntry> {
            self.map.lock().unwrap().get(path).cloned()
        }
        fn record(&self, path: &RuntimePath, mtime_ms: f64, opts: RecordOptions) {
            self.map.lock().unwrap().insert(
                path.clone(),
                FileStateEntry {
                    mtime_ms,
                    size: opts.size,
                    content: opts.content,
                    encoding: opts.encoding,
                },
            );
        }
        fn has(&self, path: &RuntimePath) -> bool {
            self.map.lock().unwrap().contains_key(path)
        }
        fn entries(&self) -> Vec<(RuntimePath, FileStateEntry)> {
            self.map
                .lock()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        }
        fn delete(&self, path: &RuntimePath) {
            self.map.lock().unwrap().remove(path);
        }
        fn clear(&self) {
            self.map.lock().unwrap().clear();
        }
        fn clone_box(&self) -> Box<dyn FileStateCache> {
            Box::new(TestCache {
                map: Mutex::new(self.map.lock().unwrap().clone()),
            })
        }
    }

    fn full(content: &str) -> RecordOptions {
        RecordOptions {
            size: None,
            content: Some(RuntimeString::from(content)),
            encoding: None,
        }
    }

    #[test]
    fn not_read_when_absent_or_partial() {
        let c = TestCache::default();
        let path = RuntimePath::from("/a");
        assert_eq!(
            check_read_before_write(&path, 10.0, &c, None),
            Some(NOT_READ_MESSAGE)
        );
        // Recorded without content (partial/truncated/binary view).
        c.record(&path, 10.0, RecordOptions::default());
        assert_eq!(
            check_read_before_write(&path, 10.0, &c, None),
            Some(NOT_READ_MESSAGE)
        );
    }

    #[test]
    fn ok_when_mtime_unchanged() {
        let c = TestCache::default();
        let path = RuntimePath::from("/a");
        c.record(&path, 100.0, full("hi"));
        assert_eq!(check_read_before_write(&path, 100.0, &c, None), None);
    }

    #[test]
    fn stale_when_mtime_newer_and_no_disk_content() {
        let c = TestCache::default();
        let path = RuntimePath::from("/a");
        c.record(&path, 100.0, full("hi"));
        assert_eq!(
            check_read_before_write(&path, 200.0, &c, None),
            Some(STALE_MESSAGE)
        );
    }

    #[test]
    fn content_fallback_prevents_false_positive() {
        let c = TestCache::default();
        let path = RuntimePath::from("/a");
        c.record(&path, 100.0, full("hi"));
        // mtime moved but the disk content is identical → not stale.
        assert_eq!(
            check_read_before_write(&path, 200.0, &c, Some(&RuntimeString::from("hi"))),
            None
        );
        // disk content differs → still stale.
        assert_eq!(
            check_read_before_write(&path, 200.0, &c, Some(&RuntimeString::from("changed")),),
            Some(STALE_MESSAGE)
        );
    }

    #[test]
    fn stale_write_never_demands_a_prior_read() {
        let c = TestCache::default();
        let path = RuntimePath::from("/a");
        let disk = RuntimeString::from("hi");
        // No baseline at all: an edit resolves oldString against `disk`, so it runs.
        assert_eq!(check_stale_write(&path, 100.0, &c, &disk), None);
        // Recorded without content (a ranged read): nothing to compare, so it runs
        // even though the file has since been touched.
        c.record(&path, 100.0, RecordOptions::default());
        assert_eq!(check_stale_write(&path, 200.0, &c, &disk), None);
    }

    #[test]
    fn stale_write_blocks_only_a_real_change_under_a_full_baseline() {
        let c = TestCache::default();
        let path = RuntimePath::from("/a");
        c.record(&path, 100.0, full("hi"));
        // Untouched.
        assert_eq!(
            check_stale_write(&path, 100.0, &c, &RuntimeString::from("hi")),
            None
        );
        // Touched, content identical → the mtime-only false positive.
        assert_eq!(
            check_stale_write(&path, 200.0, &c, &RuntimeString::from("hi")),
            None
        );
        // Touched and genuinely different → the model saw a version that is gone.
        assert_eq!(
            check_stale_write(&path, 200.0, &c, &RuntimeString::from("changed")),
            Some(STALE_MESSAGE)
        );
    }
}
