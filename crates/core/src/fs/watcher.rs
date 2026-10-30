//! A recursive directory watcher — the file-watcher half of the incremental
//! index. It leans on the platform's native recursive file-notification backend
//! (inotify on Linux, FSEvents on macOS, ReadDirectoryChanges on Windows) and
//! layers a short coalescing window plus an ignore filter on top. Raw OS events
//! are debounced (or throttled) and delivered as a deduped, sorted batch of
//! absolute paths — the consumer (e.g. the file index) reconciles each path by
//! stat-ing it, which is robust to the inconsistent event shapes different
//! platforms report.

use notify::{recommended_watcher, Event, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Predicate that returns true to SKIP a path (POSIX-relative path, is-dir flag).
pub type WatchIgnore = Arc<dyn Fn(&str, bool) -> bool + Send + Sync>;

/// Coalescing strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchMode {
    /// Flush a fixed window after the FIRST event of a burst.
    Debounce,
    /// Flush at most once per window even under a continuous stream.
    Throttle,
}

/// Options for [`DirectoryWatcher::start`].
pub struct DirectoryWatcherOptions {
    /// Root directory to watch (absolute).
    pub root: String,
    /// Return true to SKIP a path. `rel_path` is the POSIX-style path relative
    /// to `root`; `is_dir` is true for directories. Skipped directories produce
    /// no batch entries.
    pub ignore: Option<WatchIgnore>,
    /// Coalesce window in milliseconds (default 50).
    pub debounce_ms: Option<u64>,
    /// Cap on the number of directory watchers (default 20000). Retained for
    /// compatibility; the native recursive backend manages its own handles.
    pub max_watchers: Option<usize>,
    /// Coalescing strategy (default [`WatchMode::Debounce`]).
    pub mode: Option<WatchMode>,
    /// Delivered a deduped, sorted batch of absolute changed paths.
    pub on_batch: Box<dyn Fn(Vec<String>) + Send>,
}

/// A recursive directory watcher delivering coalesced batches of changed paths.
pub struct DirectoryWatcher {
    watcher: Mutex<Option<RecommendedWatcher>>,
    cancel: CancellationToken,
    handle: Mutex<Option<JoinHandle<()>>>,
    active: AtomicBool,
    healthy: Arc<AtomicBool>,
}

impl DirectoryWatcher {
    /// Begin watching the tree under `root`.
    pub async fn start(opts: DirectoryWatcherOptions) -> notify::Result<DirectoryWatcher> {
        let root = opts.root.clone();
        let ignore = opts.ignore.clone();
        let debounce = Duration::from_millis(opts.debounce_ms.unwrap_or(50));
        let _max_watchers = opts.max_watchers.unwrap_or(20_000);
        let mode = opts.mode.unwrap_or(WatchMode::Debounce);
        let on_batch = opts.on_batch;

        // The native backend may report canonical (symlink-resolved) paths that
        // differ from `root`; keep both forms so a changed path can be mapped
        // back to a path lexically under the caller-facing root.
        let canonical_root = tokio::fs::canonicalize(&root)
            .await
            .map(crate::fs::node_path::simplify_host)
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|_| root.clone());

        let (tx, rx) = mpsc::unbounded_channel::<String>();

        let cb_root = root.clone();
        let cb_canon = canonical_root.clone();
        let cb_ignore = ignore.clone();
        let healthy = Arc::new(AtomicBool::new(true));
        let callback_healthy = healthy.clone();
        let mut watcher = recommended_watcher(move |res: notify::Result<Event>| {
            // Keep backend failure observable so consumers can repair a lost
            // registration without rebuilding healthy watches periodically.
            let event = match res {
                Ok(e) => e,
                Err(_) => {
                    callback_healthy.store(false, Ordering::SeqCst);
                    return;
                }
            };
            for path in &event.paths {
                let path_str = path.to_string_lossy();
                let rel = match compute_rel(path_str.as_ref(), &cb_canon, &cb_root) {
                    Some(r) => r,
                    None => continue,
                };
                // We don't yet know if it's a dir; only filter when the predicate
                // rejects it as a FILE *and* as a DIR (a rejected file path can't
                // become a dir path under predicates that key on extension/name).
                let skip = match &cb_ignore {
                    Some(f) => (f.as_ref())(&rel, false) && (f.as_ref())(&rel, true),
                    None => false,
                };
                if skip {
                    continue;
                }
                let _ = tx.send(join(&cb_root, &rel));
            }
        })?;
        watcher.watch(Path::new(&root), RecursiveMode::Recursive)?;

        let cancel = CancellationToken::new();
        let handle = tokio::spawn(flusher_loop(rx, cancel.clone(), on_batch, mode, debounce));

        Ok(DirectoryWatcher {
            watcher: Mutex::new(Some(watcher)),
            cancel,
            handle: Mutex::new(Some(handle)),
            active: AtomicBool::new(true),
            healthy,
        })
    }

    /// Number of active watch registrations (for tests / diagnostics).
    pub fn watcher_count(&self) -> usize {
        if self.active.load(Ordering::SeqCst) {
            1
        } else {
            0
        }
    }

    /// Whether the registration remains active without a reported backend error.
    pub fn is_healthy(&self) -> bool {
        self.active.load(Ordering::SeqCst) && self.healthy.load(Ordering::SeqCst)
    }

    /// Stop watching and release all OS handles. Idempotent.
    pub fn close(&self) {
        if !self.active.swap(false, Ordering::SeqCst) {
            return;
        }
        self.cancel.cancel();
        *self.watcher.lock().unwrap() = None;
        if let Some(h) = self.handle.lock().unwrap().take() {
            h.abort();
        }
    }
}

impl Drop for DirectoryWatcher {
    fn drop(&mut self) {
        self.close();
    }
}

/// The coalescing loop: gather changed paths into a burst, wait out the window
/// (debounce or throttle), then deliver a deduped, sorted batch. Returns as soon
/// as the watcher is closed (no batch is delivered after close).
async fn flusher_loop(
    mut rx: mpsc::UnboundedReceiver<String>,
    cancel: CancellationToken,
    on_batch: Box<dyn Fn(Vec<String>) + Send>,
    mode: WatchMode,
    debounce: Duration,
) {
    let mut last_flush: Option<Instant> = None;
    loop {
        // Wait for the first event of a burst (or for close / channel shutdown).
        let first = tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            msg = rx.recv() => match msg {
                Some(p) => p,
                None => return,
            },
        };
        let mut pending: HashSet<String> = HashSet::new();
        pending.insert(first);

        // debounce: fixed window after the first event of a burst.
        // throttle: at most one flush per window, so a continuous stream drains
        // regularly (the remaining time until this window's flush is allowed).
        let wait = match mode {
            WatchMode::Throttle => match last_flush {
                Some(t) => debounce.saturating_sub(t.elapsed()),
                None => Duration::ZERO,
            },
            WatchMode::Debounce => debounce,
        };

        let sleep = tokio::time::sleep(wait);
        tokio::pin!(sleep);
        let mut channel_closed = false;
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                _ = &mut sleep => break,
                msg = rx.recv() => match msg {
                    Some(p) => { pending.insert(p); }
                    None => { channel_closed = true; break; }
                },
            }
        }

        last_flush = Some(Instant::now());
        let mut batch: Vec<String> = pending.into_iter().collect();
        batch.sort();
        if !batch.is_empty() {
            on_batch(batch);
        }
        if channel_closed {
            return;
        }
    }
}

/// Map an absolute changed `path` to a POSIX path relative to the watch root,
/// trying the canonical root first (the native backend may resolve symlinks)
/// then the caller-facing root. `None` for the root itself or an out-of-tree
/// path.
fn compute_rel(path: &str, canonical_root: &str, original_root: &str) -> Option<String> {
    let r1 = path_relative(canonical_root, path);
    if !r1.is_empty() && !r1.starts_with("..") {
        return Some(r1);
    }
    let r2 = path_relative(original_root, path);
    if !r2.is_empty() && !r2.starts_with("..") {
        return Some(r2);
    }
    None
}

/// `path.relative(from, to)` in the host dialect, rendered with `/` separators.
///
/// The result is only ever used as the ignore predicate's `rel_path`, which this module
/// documents as POSIX-style, so the separator is normalized after the comparison. Doing
/// the comparison itself by splitting on `/` alone made every Windows event look
/// out-of-tree: `C:\root\file` shares no `/`-delimited prefix with `C:\root`, so
/// [`compute_rel`] answered `None` for everything and the watcher delivered no batches at
/// all — the file index simply never saw a change on that platform.
fn path_relative(from: &str, to: &str) -> String {
    let relative = crate::permission::bash::node_path::relative(from, to);
    if cfg!(windows) {
        relative.replace('\\', "/")
    } else {
        relative
    }
}

/// Join a directory with a single child segment, POSIX-style.
fn join(a: &str, b: &str) -> String {
    if a.is_empty() {
        b.to_string()
    } else if a.ends_with('/') {
        format!("{a}{b}")
    } else {
        format!("{a}/{b}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> std::path::PathBuf {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-wthr-{}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        dir.push(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    async fn sleep(ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }

    #[tokio::test]
    async fn delivers_coalesced_batches_in_throttle_mode() {
        let dir = temp_dir();
        let batches: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = batches.clone();
        let watcher = DirectoryWatcher::start(DirectoryWatcherOptions {
            root: dir.to_string_lossy().into_owned(),
            ignore: None,
            debounce_ms: Some(30),
            max_watchers: None,
            mode: Some(WatchMode::Throttle),
            on_batch: Box::new(move |paths| sink.lock().unwrap().push(paths)),
        })
        .await
        .unwrap();
        {
            std::fs::write(dir.join("a.ts"), "x").unwrap();
            std::fs::write(dir.join("b.ts"), "y").unwrap();
            let mut seen = false;
            for _ in 0..60 {
                if seen {
                    break;
                }
                let hit = batches
                    .lock()
                    .unwrap()
                    .iter()
                    .flatten()
                    .any(|p| p.ends_with("a.ts"));
                if hit {
                    seen = true;
                } else {
                    sleep(50).await;
                }
            }
            assert!(seen);
        }
        watcher.close();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn close_is_idempotent() {
        let dir = temp_dir();
        let watcher = DirectoryWatcher::start(DirectoryWatcherOptions {
            root: dir.to_string_lossy().into_owned(),
            ignore: None,
            debounce_ms: Some(20),
            max_watchers: None,
            mode: None,
            on_batch: Box::new(|_| {}),
        })
        .await
        .unwrap();
        assert_eq!(watcher.watcher_count(), 1);
        watcher.close();
        assert_eq!(watcher.watcher_count(), 0);
        watcher.close(); // second close is a no-op
        std::fs::remove_dir_all(&dir).ok();
    }
}
