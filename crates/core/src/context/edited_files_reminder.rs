//! Edited-files reminder: surface files
//! that changed on disk out-of-band (user / linter / formatter) since the agent
//! last read them, as a diff of the changed regions.

use crate::context::edited_files_diff::{diff_snippet, DiffSnippetOptions};
use crate::fs::node_path::project_runtime_path;
use crate::fs::text_file::decode_text_buffer;
use crate::runtime_process::{
    path_is_absolute, path_relative, NodePathDialect, NodePathHostPlatform, RuntimePathCwdSnapshot,
};
use crate::runtime_values::{RuntimePath, RuntimeString};
use crate::types::file_state::FileStateCache;
use crate::types::reminder::{ReminderContext, ReminderProvider};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::fs;

/// NUL-CHARACTER scan over a leading window of the DECODED text — a file that became binary
/// out-of-band. Operating on the decoded string (not raw bytes) lets a utf16le text file through
/// while catching a binary that carries an aligned 0x0000 code unit, matching the read tool's check
/// (a blob whose bytes are valid UTF-16LE text is read as text).
fn looks_binary_text(text: &RuntimeString) -> bool {
    // The window is measured in UTF-16 code units, the same unit the read tool samples in.
    text.as_utf16().iter().take(8000).any(|&unit| unit == 0)
}

/// Report observed disk changes without guessing who made them or why.
const PREAMBLE: &str = "These files changed on disk since you last read them. The shown content is current. Preserve changes unrelated to your task and do not revert edits you did not make unless the user asks. Inspect unexpected changes before deciding how to proceed. Mention a change when it affects the requested work or requires a decision. Modified regions, by current line number:";

#[cfg(windows)]
const fn host_path_dialect() -> NodePathDialect {
    NodePathDialect::Win32
}

#[cfg(not(windows))]
const fn host_path_dialect() -> NodePathDialect {
    NodePathDialect::Posix
}

#[cfg(windows)]
const fn host_path_platform() -> NodePathHostPlatform {
    NodePathHostPlatform::Windows
}

#[cfg(not(windows))]
const fn host_path_platform() -> NodePathHostPlatform {
    NodePathHostPlatform::Posix
}

/// Exact Node `path.relative(from, to)` over the logical runtime paths. The
/// active turn cwd is also the lazy resolution snapshot for relative operands.
fn relative(from: &RuntimePath, to: &RuntimePath) -> RuntimePath {
    let mut snapshot = RuntimePathCwdSnapshot::new(host_path_platform(), Ok(from.clone()));
    path_relative(host_path_dialect(), from, to, &mut snapshot).unwrap_or_else(|_| to.clone())
}

/// Show a tracked path relative to the turn's cwd when it sits inside it;
/// otherwise fall back to the path as tracked.
fn display(path: &RuntimePath, cwd: &RuntimePath) -> RuntimeString {
    let rel = relative(cwd, path);
    if rel.as_runtime_string().is_empty()
        || rel.as_runtime_string().starts_with("..")
        || path_is_absolute(host_path_dialect(), &rel)
    {
        path.as_runtime_string().clone()
    } else {
        rel.into_runtime_string()
    }
}

fn join_runtime_strings(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let Some(mut output) = values.first().cloned() else {
        return RuntimeString::default();
    };
    let separator = RuntimeString::from(separator);
    for value in &values[1..] {
        output = output.concat(&separator).concat(value);
    }
    output
}

/// mtime in milliseconds, whole + fractional (the shape `record` stores, i.e.
/// Node's `stat.mtimeMs`). `None` on a platform/filesystem anomaly.
fn mtime_millis(meta: &std::fs::Metadata) -> Option<f64> {
    let modified = meta.modified().ok()?;
    let d = modified.duration_since(std::time::UNIX_EPOCH).ok()?;
    Some(d.as_secs() as f64 * 1000.0 + f64::from(d.subsec_nanos()) / 1e6)
}

/// Surfaces files that changed on disk out-of-band (user / linter / formatter)
/// since the agent last read them. Diffs each
/// tracked file's recorded content against the current disk content and injects the
/// changed regions. The agent's OWN edits are excluded because read/edit/write
/// re-baseline the recorded content (so their mtime matches). This does NOT
/// re-baseline after reporting: an out-of-band change is re-surfaced each turn until
/// a tool re-reads the file (which updates the baseline). Never fails: every I/O
/// error is handled by skipping the file.
pub struct EditedFilesReminderProvider {
    get_file_state: Box<dyn Fn() -> Arc<dyn FileStateCache> + Send + Sync>,
}

impl EditedFilesReminderProvider {
    /// The core pass (the async trait method delegates to this).
    pub async fn tick(&self, ctx: &ReminderContext) -> Option<RuntimeString> {
        let file_state = (self.get_file_state)();
        let mut blocks: Vec<RuntimeString> = Vec::new();
        for (path, entry) in file_state.entries() {
            // Only files with a text baseline can be diffed (partial/truncated/binary
            // reads stored no content). A baseline flagged `partial_view` still holds
            // the raw disk bytes, so it diffs normally: the marker says the model saw
            // a processed view, not that the baseline is approximate.
            let Some(baseline) = entry.content.as_ref() else {
                continue;
            };
            let Ok(native_path) = project_runtime_path(&path, "edited-files path") else {
                continue;
            };
            let st = match fs::metadata(&native_path).await {
                Ok(st) => st,
                Err(e) => {
                    // Deleted → drop it. Any OTHER (transient) error → keep it, skip this turn
                    // (don't evict on an atomic-save rename gap / EACCES / network blip).
                    if e.kind() == std::io::ErrorKind::NotFound {
                        file_state.delete(&path);
                    }
                    continue;
                }
            };
            // mtime gate (floored to whole ms to ignore sub-ms watcher jitter). Equal or
            // older ⇒ unchanged, or the agent's own re-baselined edit ⇒ skip.
            let Some(st_mtime_ms) = mtime_millis(&st) else {
                continue;
            };
            if st_mtime_ms.floor() <= entry.mtime_ms.floor() {
                continue;
            }
            let buf = match fs::read(&native_path).await {
                Ok(b) => b,
                Err(_) => continue,
            };
            // Decode + CRLF→LF normalize exactly as the read/edit baseline did, so a CRLF or utf16le
            // file isn't reported as wholly-changed just from the newline/encoding form. Refuse a file
            // that became binary out-of-band — checked on the DECODED text (NUL *characters*) so a
            // utf16le binary is caught too, while a genuine utf16le text file (legit NUL bytes) passes.
            let current = decode_text_buffer(&buf);
            if looks_binary_text(&current.content) {
                continue;
            }
            let snippet = diff_snippet(baseline, &current.content, DiffSnippetOptions::default());
            if snippet.is_empty() {
                continue; // touched but content identical
            }
            blocks.push(
                display(&path, &ctx.cwd)
                    .concat(&RuntimeString::from(":\n"))
                    .concat(&snippet),
            );
        }
        if blocks.is_empty() {
            return None;
        }
        Some(
            RuntimeString::from(PREAMBLE)
                .concat(&RuntimeString::from("\n\n"))
                .concat(&join_runtime_strings(&blocks, "\n\n")),
        )
    }
}

#[async_trait]
impl ReminderProvider for EditedFilesReminderProvider {
    async fn reminder(&self, ctx: &ReminderContext) -> Option<RuntimeString> {
        self.tick(ctx).await
    }
}

/// Build an edited-files reminder provider over a live view of the file-state
/// cache (the getter always yields the current cache, surviving swaps).
pub fn create_edited_files_reminder_provider(
    get_file_state: impl Fn() -> Arc<dyn FileStateCache> + Send + Sync + 'static,
) -> EditedFilesReminderProvider {
    EditedFilesReminderProvider {
        get_file_state: Box::new(get_file_state),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::node_path::runtime_path_from_native;
    use crate::types::file_state::{FileStateEntry, RecordOptions};
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

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

    fn temp_dir() -> PathBuf {
        static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-edited-{}-{}-{}",
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

    fn ctx(cwd: &Path) -> ReminderContext {
        ReminderContext {
            messages: vec![],
            cwd: runtime_path_from_native(cwd),
            task_list_id: None,
            task_plan: None,
            signal: None,

            plan_file_path: None,
            plan_file_exists: None,
            has_task_tool: false,
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

    #[test]
    fn display_path_keeps_non_scalar_runtime_units_distinct_from_replacement() {
        let mut units = "/w/file-".encode_utf16().collect::<Vec<_>>();
        units.push(0xd800);
        let path = RuntimePath::from_runtime_string(RuntimeString::from_utf16(units));
        let shown = display(&path, &RuntimePath::from("/w"));
        assert!(shown.as_utf16().contains(&0xd800));
        assert!(!shown.as_utf16().contains(&0xfffd));
    }

    /// A shared cache handle plus a provider whose getter yields that cache.
    fn setup() -> (PathBuf, Arc<TestCache>, EditedFilesReminderProvider) {
        let dir = temp_dir();
        let cache = Arc::new(TestCache::default());
        let shared = cache.clone();
        let provider = create_edited_files_reminder_provider(move || {
            shared.clone() as Arc<dyn FileStateCache>
        });
        (dir, cache, provider)
    }

    #[tokio::test]
    async fn reports_an_out_of_band_edit_as_a_diff_with_the_behavioral_instructions() {
        let (dir, cache, provider) = setup();
        let native = dir.join("f.txt");
        let p = runtime_path_from_native(&native);
        std::fs::write(&native, "one\ntwo\nthree").unwrap();
        // Baseline: old content + an OLD mtime so the current file looks newer.
        cache.record(&p, 1000.0, full("one\nOLD_TWO\nthree"));
        let out = provider.tick(&ctx(&dir)).await.unwrap_or_default();
        assert!(out.contains("f.txt"));
        assert!(out.contains("two")); // current content surfaced
        assert!(out.contains("Preserve changes unrelated to your task"));
        assert!(out.contains("Mention a change when it affects the requested work"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn does_not_report_when_mtime_matches_the_baseline_agent_own_edit() {
        let (dir, cache, provider) = setup();
        let native = dir.join("f.txt");
        let p = runtime_path_from_native(&native);
        std::fs::write(&native, "hello\nworld").unwrap();
        let st = std::fs::metadata(&native).unwrap();
        // content intentionally differs, but the mtime gate (file == recorded) skips it.
        cache.record(&p, mtime_millis(&st).unwrap(), full("DIFFERENT"));
        assert_eq!(provider.tick(&ctx(&dir)).await, None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn returns_none_when_the_file_was_touched_but_content_is_identical() {
        let (dir, cache, provider) = setup();
        let native = dir.join("f.txt");
        let p = runtime_path_from_native(&native);
        std::fs::write(&native, "same\ncontent").unwrap();
        cache.record(&p, 1000.0, full("same\ncontent")); // old mtime, identical content
        assert_eq!(provider.tick(&ctx(&dir)).await, None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn evicts_a_tracked_file_that_was_deleted_on_disk() {
        let (dir, cache, provider) = setup();
        let p = runtime_path_from_native(&dir.join("gone.txt"));
        cache.record(&p, 1000.0, full("x")); // never created
        assert_eq!(provider.tick(&ctx(&dir)).await, None);
        assert!(!cache.has(&p));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn skips_files_with_no_recorded_content_partial_truncated_reads() {
        let (dir, cache, provider) = setup();
        let native = dir.join("f.txt");
        let p = runtime_path_from_native(&native);
        std::fs::write(&native, "changed").unwrap();
        cache.record(&p, 1000.0, RecordOptions::default()); // mtime only, no content
        assert_eq!(provider.tick(&ctx(&dir)).await, None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn skips_a_file_that_became_binary_out_of_band() {
        let (dir, cache, provider) = setup();
        let native = dir.join("f.txt");
        let p = runtime_path_from_native(&native);
        cache.record(&p, 1000.0, full("text"));
        std::fs::write(&native, [0x01u8, 0x00, 0x02, 0x00]).unwrap(); // binary, newer mtime
        assert_eq!(provider.tick(&ctx(&dir)).await, None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn surfaces_utf16le_lone_surrogate_without_aliasing_replacement() {
        let (dir, cache, provider) = setup();
        let native = dir.join("exact.txt");
        let p = runtime_path_from_native(&native);
        let units = [
            0xfeff,
            b'n' as u16,
            b'e' as u16,
            b'w' as u16,
            b'-' as u16,
            0xd800,
        ];
        let bytes = units
            .into_iter()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        std::fs::write(&native, bytes).unwrap();
        cache.record(&p, 1000.0, full("old"));

        let out = provider.tick(&ctx(&dir)).await.unwrap();
        assert!(out.as_utf16().contains(&0xd800));
        assert!(!out.as_utf16().contains(&0xfffd));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn re_reports_the_same_change_on_repeated_calls() {
        // The provider does NOT re-baseline (consume) the change; the query loop calls
        // it before every model request, so the same out-of-band edit must keep
        // surfacing until a tool re-reads the file.
        let (dir, cache, provider) = setup();
        let native = dir.join("f.txt");
        let p = runtime_path_from_native(&native);
        std::fs::write(&native, "one\ntwo\nthree").unwrap();
        cache.record(&p, 1000.0, full("one\nOLD\nthree"));
        let first = provider.tick(&ctx(&dir)).await;
        let second = provider.tick(&ctx(&dir)).await;
        assert!(first.unwrap().contains("two"));
        assert!(second.unwrap().contains("two")); // still reported — not consumed
        std::fs::remove_dir_all(&dir).ok();
    }
}
