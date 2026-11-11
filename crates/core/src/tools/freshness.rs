//! The freshness checks the write-class tools run during input validation,
//! before a write is applied, plus the post-command sweep that tells the model
//! when a shell command rewrote files it has already read.
//!
//! There are two rules because the tools carry different risk. Write and
//! NotebookEdit put down content the model never had to match against the file,
//! so an unread file could be silently clobbered: they demand a prior complete
//! read ([`check_read_before_write`]). Edit and MultiEdit only ever replace text
//! they matched byte-for-byte in the content they just read off disk, so an
//! unread file is not at risk and is not blocked; what they guard is a baseline
//! the model reasoned about having moved underneath it ([`check_stale_write`]).
//!
//! A baseline flagged `partial_view` — an auto-injected file whose raw disk bytes
//! are tracked while the model only saw a processed view of them — counts as a
//! faithful diff baseline but NOT as a complete read. [`check_read_before_write`]
//! therefore still refuses it, keeping that guard fail-closed. [`check_stale_write`]
//! compares its raw baseline against the raw text the caller just read off disk,
//! which is the same comparison it already makes, so an injected file whose disk
//! copy later moved is reported as stale like any other tracked baseline.

use crate::runtime_values::{RuntimePath, RuntimeString};
use crate::types::file_state::FileStateCache;
use regex::Regex;
use std::sync::LazyLock;

/// The write-tool read-before-write / stale-write messages (verbatim).
pub const NOT_READ_MESSAGE: &str =
    "File has not been read yet. Read it first before writing to it.";
pub const STALE_MESSAGE: &str = "File has been modified since read, either by the user or by a linter. Read it again before attempting to write it.";

/// The read-before-write + stale-write guard for the tools that put down content
/// wholesale (Write, NotebookEdit). Returns a blocking error message, or `None`
/// to proceed.
///
/// - Not fully read (no captured content, or a content baseline the model only
///   saw a processed view of) → [`NOT_READ_MESSAGE`].
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
        // A partial-view baseline holds raw disk bytes the model never saw in
        // full, so it cannot stand in for the read this guard demands.
        Some(e) if e.content.is_some() && !e.partial_view => e,
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
/// holds nothing to compare against and proceeds. A partial-view baseline holds
/// the raw disk bytes, so it compares against `disk_content` on the same terms
/// as any other full baseline.
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

/// Command-text prefilter for the post-command stale-read sweep: only a
/// command that plausibly rewrites files in place pays for stat-ing the
/// tracked baselines. The in-place `sed`/`perl` forms are included because
/// the shell runs them directly here — nothing else refreshes the baseline
/// the model read before them.
static MODIFYING_COMMAND_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"--write|--fix|--in-place|--auto-correct|\brun\s+format\b|\brun\s+fix\b|\b(yarn|pnpm)\s+format\b|\blint:file\b|\blint:fix\b|\bblack\b|\bisort\b|\bruff\s+format\b|\bcargo\s+(fmt|fix)\b|\brustfmt\b|\bgo\s+fmt\b|\bterraform\s+fmt\b|\bdprint\s+fmt\b|\bswiftformat\b|\bphpcbf\b|\bsed\b[^\r\n]*\s-\w*i|\bperl\b[^\r\n]*\s-\w*i",
    )
    .expect("static stale-read sweep pattern")
});

pub fn command_may_modify_files(command: &str) -> bool {
    MODIFYING_COMMAND_PATTERN.is_match(command)
}

/// Milliseconds since the UNIX epoch in a metadata snapshot (the unit read
/// baselines record).
pub(crate) fn metadata_mtime_ms(meta: &std::fs::Metadata) -> f64 {
    meta.modified()
        .ok()
        .and_then(|modified| modified.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as f64 * 1000.0 + f64::from(d.subsec_nanos()) / 1e6)
        .unwrap_or(0.0)
}

/// Current wall-clock milliseconds since the UNIX epoch.
pub(crate) fn now_ms() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as f64 * 1e3 + d.subsec_nanos() as f64 / 1e6)
        .unwrap_or(0.0)
}

/// Tracked read baselines a shell command invalidated: a file is stale when
/// its on-disk mtime moved past BOTH the command start and the recorded
/// baseline. A file already newer than its baseline before the command ran
/// is the write guards' business, not this sweep's.
pub async fn tracked_files_modified_since(
    command: &RuntimeString,
    started_at_ms: f64,
    file_state: &dyn FileStateCache,
) -> Vec<RuntimePath> {
    if !command_may_modify_files(&command.try_to_utf8().unwrap_or_default()) {
        return Vec::new();
    }
    let mut modified = Vec::new();
    for (path, entry) in file_state.entries() {
        let Ok(os_path) = crate::fs::node_path::project_runtime_path(&path, "path") else {
            continue;
        };
        let Ok(meta) = tokio::fs::metadata(&os_path).await else {
            continue;
        };
        let disk_mtime = metadata_mtime_ms(&meta);
        if disk_mtime > started_at_ms && disk_mtime > entry.mtime_ms {
            modified.push(path);
        }
    }
    modified.sort_by_cached_key(|path| path.as_runtime_string().try_to_utf8().unwrap_or_default());
    modified
}

/// The model-visible notice that a shell command rewrote files whose read
/// baselines are now stale, listing up to five cwd-relative paths.
pub fn stale_read_hint(modified: &[RuntimePath], cwd: &RuntimePath) -> Option<RuntimeString> {
    if modified.is_empty() {
        return None;
    }
    let cwd_text = cwd.as_runtime_string().try_to_utf8().unwrap_or_default();
    let listed: Vec<String> = modified
        .iter()
        .take(5)
        .map(|path| {
            let text = path.as_runtime_string().try_to_utf8().unwrap_or_default();
            match strip_cwd_prefix(&text, &cwd_text) {
                Some(rest) if !rest.is_empty() => rest.to_owned(),
                _ => text,
            }
        })
        .collect();
    let extra = modified.len().saturating_sub(5);
    let more = if extra > 0 {
        format!(" and {extra} more")
    } else {
        String::new()
    };
    let noun = if modified.len() == 1 { "file" } else { "files" };
    Some(RuntimeString::from_utf8(&format!(
        "[This command modified {} {} you've previously read: {}{}. Call Read before editing.]",
        modified.len(),
        noun,
        listed.join(", "),
        more
    )))
}

fn strip_cwd_prefix<'a>(path: &'a str, cwd: &'a str) -> Option<&'a str> {
    let trimmed = cwd
        .strip_suffix('/')
        .or_else(|| cwd.strip_suffix('\\'))
        .unwrap_or(cwd);
    if trimmed.is_empty() {
        return None;
    }
    let rest = path.strip_prefix(trimmed)?;
    rest.strip_prefix('/').or_else(|| rest.strip_prefix('\\'))
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
                    partial_view: opts.partial_view,
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
            partial_view: false,
        }
    }

    /// A baseline holding the raw disk bytes while the model only saw a
    /// processed view of them (an auto-injected memory file).
    fn partial_view(disk_content: &str) -> RecordOptions {
        RecordOptions {
            partial_view: true,
            ..full(disk_content)
        }
    }

    #[test]
    fn partial_view_content_does_not_satisfy_read_before_write() {
        // The regression this pins: the entry HAS content, so a guard that only
        // tested `content.is_some()` would wave a wholesale write through over
        // bytes the model never saw.
        let c = TestCache::default();
        let path = RuntimePath::from("/memory/AGENTS.md");
        c.record(&path, 100.0, partial_view("---\nk: v\n---\nbody\n"));
        assert_eq!(
            check_read_before_write(&path, 100.0, &c, None),
            Some(NOT_READ_MESSAGE)
        );
        // A current on-disk copy does not unlock it either.
        let disk = RuntimeString::from("---\nk: v\n---\nbody\n");
        assert_eq!(
            check_read_before_write(&path, 100.0, &c, Some(&disk)),
            Some(NOT_READ_MESSAGE)
        );
    }

    #[test]
    fn an_explicit_complete_read_replaces_the_partial_view_marker() {
        let c = TestCache::default();
        let path = RuntimePath::from("/memory/AGENTS.md");
        c.record(&path, 100.0, partial_view("raw"));
        assert_eq!(
            check_read_before_write(&path, 100.0, &c, None),
            Some(NOT_READ_MESSAGE)
        );
        // The read tool re-records the same path with what the model now saw.
        c.record(&path, 100.0, full("raw"));
        assert_eq!(check_read_before_write(&path, 100.0, &c, None), None);
        // The re-baselined entry guards staleness normally again.
        assert_eq!(
            check_read_before_write(&path, 200.0, &c, None),
            Some(STALE_MESSAGE)
        );
    }

    #[test]
    fn partial_view_baseline_is_a_raw_bytes_baseline_for_stale_write() {
        let c = TestCache::default();
        let path = RuntimePath::from("/memory/AGENTS.md");
        c.record(&path, 100.0, partial_view("---\nk: v\n---\nbody\n"));
        // Untouched: the injection recorded the file's own mtime, so nothing is stale.
        assert_eq!(
            check_stale_write(
                &path,
                100.0,
                &c,
                &RuntimeString::from("---\nk: v\n---\nbody\n")
            ),
            None
        );
        // Touched but identical on disk → the mtime-only false positive stays suppressed.
        assert_eq!(
            check_stale_write(
                &path,
                200.0,
                &c,
                &RuntimeString::from("---\nk: v\n---\nbody\n")
            ),
            None
        );
        // Genuinely rewritten out-of-band → the model's injected view is gone.
        assert_eq!(
            check_stale_write(&path, 200.0, &c, &RuntimeString::from("body only\n")),
            Some(STALE_MESSAGE)
        );
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

    #[test]
    fn modifying_command_prefilter_matches_in_place_rewrites() {
        assert!(command_may_modify_files("cargo fmt"));
        assert!(command_may_modify_files("cargo fix --allow-dirty"));
        assert!(command_may_modify_files("prettier --write src/app.ts"));
        assert!(command_may_modify_files("ruff format ."));
        assert!(command_may_modify_files("black ."));
        assert!(command_may_modify_files("npm run format"));
        assert!(command_may_modify_files("sed -i '' 's/a/b/' f.txt"));
        assert!(command_may_modify_files("sed -i 's/a/b/' f.txt"));
        assert!(command_may_modify_files("perl -pi -e 's/a/b/' f.txt"));
    }

    #[test]
    fn modifying_command_prefilter_ignores_read_only_commands() {
        assert!(!command_may_modify_files("ls -la"));
        assert!(!command_may_modify_files("cat f.txt"));
        assert!(!command_may_modify_files("grep -rn pattern ."));
        assert!(!command_may_modify_files("git status"));
        assert!(!command_may_modify_files("echo hi"));
    }

    #[tokio::test]
    async fn sweep_detects_files_modified_after_the_command_started() {
        let dir =
            std::env::temp_dir().join(format!("klaus-freshness-sweep-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("tracked.txt");
        std::fs::write(&file, "original").unwrap();
        let path = RuntimePath::from(file.to_string_lossy().into_owned());
        let c = TestCache::default();
        // A baseline recorded long before the command ran.
        c.record(&path, 1.0, full("original"));
        let modified =
            tracked_files_modified_since(&RuntimeString::from("cargo fmt"), 2.0, &c).await;
        assert_eq!(modified.len(), 1);
        assert_eq!(
            modified[0].as_runtime_string().try_to_utf8().unwrap(),
            path.as_runtime_string().try_to_utf8().unwrap()
        );
        // A command that cannot modify files skips the sweep entirely.
        let skipped = tracked_files_modified_since(&RuntimeString::from("ls -la"), 2.0, &c).await;
        assert!(skipped.is_empty());
        // A baseline already at (or past) the on-disk mtime is not stale.
        let disk = metadata_mtime_ms(&std::fs::metadata(&file).unwrap());
        c.record(&path, disk + 1000.0, full("original"));
        let fresh = tracked_files_modified_since(&RuntimeString::from("cargo fmt"), 2.0, &c).await;
        assert!(fresh.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn stale_read_hint_lists_relative_paths_and_caps_at_five() {
        let cwd = RuntimePath::from("/work/project");
        let one = vec![RuntimePath::from("/work/project/src/main.rs")];
        let hint = stale_read_hint(&one, &cwd).unwrap().try_to_utf8().unwrap();
        assert_eq!(
            hint,
            "[This command modified 1 file you've previously read: src/main.rs. Call Read before editing.]"
        );
        let many: Vec<RuntimePath> = (0..7)
            .map(|i| RuntimePath::from(format!("/work/project/f{i}.txt")))
            .collect();
        let hint = stale_read_hint(&many, &cwd).unwrap().try_to_utf8().unwrap();
        assert!(
            hint.contains(
                "7 files you've previously read: f0.txt, f1.txt, f2.txt, f3.txt, f4.txt and 2 more."
            ),
            "{hint}"
        );
        assert!(stale_read_hint(&[], &cwd).is_none());
        // A path outside cwd stays absolute.
        let outside = vec![RuntimePath::from("/elsewhere/x.txt")];
        let hint = stale_read_hint(&outside, &cwd)
            .unwrap()
            .try_to_utf8()
            .unwrap();
        assert!(hint.contains("/elsewhere/x.txt"), "{hint}");
    }
}
