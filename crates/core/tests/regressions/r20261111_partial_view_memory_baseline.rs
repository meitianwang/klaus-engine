//! Defect: an auto-injected nested memory file could never take part in
//! mid-session content diffing. A memory file the engine injected on the model's
//! behalf was tracked with mtime only, so an out-of-band rewrite of it could be
//! announced as "something changed" but never shown, and the ledger carried no
//! basis to diff against.
//!
//! Root cause: the injected body is a PROCESSED view of the file — frontmatter is
//! stripped before the text reaches the context — while the ledger entry had no
//! way to say "this content is the raw disk bytes, but the model only saw part of
//! them". Recording the processed body would have made the edited-files reminder
//! diff it against disk and report the injection itself as an out-of-band change;
//! recording nothing lost the diff. mtime-only was the compromise, and its cost
//! was the missing content diff.
//!
//! Fixed by: a `partial_view` flag on the ledger entry. The injection now records
//! the raw disk bytes with the flag set, so out-of-band diffing works against
//! those bytes, while the write guards keep treating the path as unread — an
//! entry whose content the model never saw in full cannot stand in for the read
//! that Write and NotebookEdit demand.

use klaus_agent_core::context::edited_files_reminder::create_edited_files_reminder_provider;
use klaus_agent_core::context::nested_memory_reminder::{
    create_nested_memory_reminder_provider, NestedMemoryReminderDeps,
};
use klaus_agent_core::runtime_values::{RuntimePath, RuntimeString};
use klaus_agent_core::tools::file_state::InMemoryFileStateCache;
use klaus_agent_core::tools::freshness::{check_read_before_write, NOT_READ_MESSAGE};
use klaus_agent_core::types::file_state::{Encoding, FileStateCache, RecordOptions};
use klaus_agent_core::types::reminder::{ReminderContext, ReminderProvider};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A memory file with frontmatter, so the injected body differs from disk.
const DISK_MEMORY: &str = "---\ndescription: nested rule\ntier: project\n---\n\nSUB-MEM-BODY\n";

fn temp_dir(slug: &str) -> PathBuf {
    let unique = format!(
        "klaus-partial-view-{}-{}-{}",
        slug,
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let dir = std::env::temp_dir().join(unique);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Join a `/`-separated relative path onto `base` one component at a time, so a
/// Windows test build gets the same path the code under test normalizes to.
fn at(base: &Path, relative: &str) -> PathBuf {
    let mut path = base.to_path_buf();
    for segment in relative.split('/') {
        path.push(segment);
    }
    path
}

fn write(dir: &Path, relative: &str, content: &str) {
    let path = at(dir, relative);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, content).unwrap();
}

fn runtime(path: &Path) -> RuntimePath {
    RuntimePath::from(path.to_string_lossy().into_owned())
}

fn context(dir: &Path) -> ReminderContext {
    ReminderContext {
        messages: vec![],
        cwd: runtime(&at(dir, "proj")),
        task_list_id: None,
        task_plan: None,
        signal: None,
        plan_file_path: None,
        plan_file_exists: None,
        has_task_tool: false,
    }
}

/// Inject the nested memory file for `proj/sub/file.ts` and hand back the ledger
/// it recorded into, plus the memory file's path.
async fn inject(dir: &Path) -> (Arc<InMemoryFileStateCache>, RuntimePath) {
    write(dir, "proj/sub/AGENTS.md", DISK_MEMORY);
    let memory_path = runtime(&at(dir, "proj/sub/AGENTS.md"));
    let touched = runtime(&at(dir, "proj/sub/file.ts"));
    let file_state: Arc<InMemoryFileStateCache> = Arc::new(InMemoryFileStateCache::new());
    let provider = create_nested_memory_reminder_provider(NestedMemoryReminderDeps {
        drain_triggers: Box::new(move || vec![touched.clone()]),
        loaded_paths: Arc::new(Mutex::new(HashSet::new())),
        file_state: file_state.clone(),
        gates: None,
        // A private home so the loader never reaches the real user profile.
        user_config_dir: Some(runtime(&at(dir, "userhome"))),
    });
    let injected = provider
        .reminder(&context(dir))
        .await
        .expect("the nested memory file is injected");
    assert!(injected.contains("SUB-MEM-BODY"));
    // The model never sees the frontmatter.
    assert!(!injected.contains("description: nested rule"));
    (file_state, memory_path)
}

/// Advance the on-disk mtime past the recorded baseline. The reminder floors both
/// sides to whole milliseconds, so the sleep has to outlast that granularity.
fn bump_mtime() {
    std::thread::sleep(Duration::from_millis(20));
}

/// The entry records the RAW disk bytes — frontmatter included — and flags them as
/// a view the model only partly saw. This is the basis every other test relies on.
#[tokio::test]
async fn injection_records_raw_disk_bytes_flagged_as_a_partial_view() {
    let dir = temp_dir("raw-bytes");
    let (file_state, memory_path) = inject(&dir).await;

    let entry = file_state
        .get(&memory_path)
        .expect("the injected path is tracked");
    assert!(entry.partial_view, "the model saw a processed view");
    assert_eq!(
        entry
            .content
            .expect("raw disk baseline")
            .try_to_utf8()
            .unwrap(),
        DISK_MEMORY,
        "the baseline is byte-identical to disk, not the stripped body"
    );
    assert!(entry.mtime_ms > 0.0);
    std::fs::remove_dir_all(&dir).ok();
}

/// The anti-regression core: the entry HAS content now, so a write guard that only
/// tested for captured content would let a wholesale write land over bytes the
/// model never saw. It must still demand an explicit read.
#[tokio::test]
async fn an_injected_memory_file_still_demands_an_explicit_read_before_a_wholesale_write() {
    let dir = temp_dir("read-guard");
    let (file_state, memory_path) = inject(&dir).await;

    let mtime_ms = {
        let meta = std::fs::metadata(at(&dir, "proj/sub/AGENTS.md")).unwrap();
        meta.modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64()
            * 1000.0
    };
    assert_eq!(
        check_read_before_write(&memory_path, mtime_ms, file_state.as_ref(), None),
        Some(NOT_READ_MESSAGE)
    );
    // Supplying the current disk bytes does not unlock it either: the guard is
    // about what the model saw, not about what is on disk now.
    let disk = RuntimeString::from(DISK_MEMORY);
    assert_eq!(
        check_read_before_write(&memory_path, mtime_ms, file_state.as_ref(), Some(&disk)),
        Some(NOT_READ_MESSAGE)
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// The injection is not an out-of-band change. The baseline is written at the
/// file's own mtime, so the reminder's gate skips it — the spurious diff that
/// recording the stripped body would have caused.
#[tokio::test]
async fn injecting_a_memory_file_is_not_reported_as_an_out_of_band_change() {
    let dir = temp_dir("no-spurious");
    let (file_state, _memory_path) = inject(&dir).await;

    let cache: Arc<dyn FileStateCache> = file_state.clone();
    let provider = create_edited_files_reminder_provider(move || cache.clone());
    assert_eq!(
        provider.reminder(&context(&dir)).await,
        None,
        "nothing changed on disk, so nothing is reported"
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// The capability the mtime-only compromise gave up: an out-of-band rewrite of an
/// injected memory file is diffed and surfaced, against the raw bytes.
#[tokio::test]
async fn an_out_of_band_rewrite_of_an_injected_memory_file_is_diffed() {
    let dir = temp_dir("diffed");
    let (file_state, _memory_path) = inject(&dir).await;

    bump_mtime();
    write(
        &dir,
        "proj/sub/AGENTS.md",
        "---\ndescription: nested rule\ntier: project\n---\n\nREWRITTEN-OUT-OF-BAND\n",
    );

    let cache: Arc<dyn FileStateCache> = file_state.clone();
    let provider = create_edited_files_reminder_provider(move || cache.clone());
    let report = provider
        .reminder(&context(&dir))
        .await
        .expect("the rewrite is reported");
    let text = report.try_to_utf8().unwrap();
    assert!(
        text.contains("REWRITTEN-OUT-OF-BAND"),
        "the changed line is shown: {text}"
    );
    assert!(
        text.contains("AGENTS.md"),
        "the report names the file: {text}"
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// A touch that leaves the content identical must stay silent — the content
/// fallback that keeps mtime-only churn from reading as an edit.
#[tokio::test]
async fn a_touch_that_leaves_the_content_identical_is_not_reported() {
    let dir = temp_dir("touch-only");
    let (file_state, _memory_path) = inject(&dir).await;

    bump_mtime();
    write(&dir, "proj/sub/AGENTS.md", DISK_MEMORY);

    let cache: Arc<dyn FileStateCache> = file_state.clone();
    let provider = create_edited_files_reminder_provider(move || cache.clone());
    assert_eq!(
        provider.reminder(&context(&dir)).await,
        None,
        "mtime moved but the bytes did not"
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// An explicit complete read re-records the path and clears the marker, so the
/// file behaves like any other read file afterwards. This runs against the real
/// cache to pin that re-recording replaces the flag rather than preserving it.
#[tokio::test]
async fn an_explicit_complete_read_clears_the_partial_view_marker() {
    let dir = temp_dir("cleared");
    let (file_state, memory_path) = inject(&dir).await;
    assert!(file_state.get(&memory_path).unwrap().partial_view);

    let meta = std::fs::metadata(at(&dir, "proj/sub/AGENTS.md")).unwrap();
    let mtime_ms = meta
        .modified()
        .unwrap()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
        * 1000.0;
    // What the read tool records after a complete read: the same raw bytes, but
    // now as a view the model actually saw.
    file_state.record(
        &memory_path,
        mtime_ms,
        RecordOptions {
            size: Some(meta.len()),
            content: Some(RuntimeString::from(DISK_MEMORY)),
            encoding: Some(Encoding::Utf8),
            partial_view: false,
        },
    );

    let entry = file_state.get(&memory_path).unwrap();
    assert!(
        !entry.partial_view,
        "the marker does not survive a real read"
    );
    assert_eq!(
        check_read_before_write(&memory_path, mtime_ms, file_state.as_ref(), None),
        None,
        "a wholesale write is allowed now"
    );
    std::fs::remove_dir_all(&dir).ok();
}
