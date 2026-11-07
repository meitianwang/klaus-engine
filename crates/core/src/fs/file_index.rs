//! An incremental, gitignore-aware file index with fuzzy search — the large-repo
//! indexing half, paired with a [`DirectoryWatcher`]:
//!
//!   - Initial population prefers `git ls-files` (tracked + untracked-not-ignored,
//!     gitignore-aware, and fast on huge repos because git already has the list),
//!     falling back to a pure filesystem walk when the root is not a git repo.
//!     Either way `.ignore`/`.rgignore` are honored, and `.gitignore` is honored
//!     inside a git repo.
//!   - Both FILES and DIRECTORIES are indexed and searchable, so a query can
//!     resolve a directory by name.
//!   - Updates are INCREMENTAL: a [`DirectoryWatcher`] feeds changed paths to
//!     [`FileIndex::reconcile`], which stats each and adds/removes just those
//!     entries — no full re-walk per change.
//!   - [`FileIndex::search`] ranks the entries with [`fuzzy_score`].
//!
//! Honest boundaries:
//!   - Ignore files (`.gitignore` in-repo, `.ignore`/`.rgignore` everywhere) are
//!     evaluated at the initial load, against `root`'s ignore files. Incremental
//!     adds are filtered by the [`FileIndexOptions::ignore`] predicate +
//!     [`FileIndexOptions::exclude`] globs + a built-in skip set, not a fresh
//!     re-evaluation of the ignore files — so a freshly created file an ignore file
//!     would exclude but the predicate doesn't can enter until the next full load.
//!     Nested ignore files below `root` are not yet read (only `root`'s own).
//!   - In the `git ls-files` path, directory entries are DERIVED from the file
//!     list (every ancestor of a tracked file). That matches git's model (no empty
//!     directories, no symlink-following). The filesystem-walk path (non-git roots)
//!     and incremental `reconcile` do a real traversal that includes empty
//!     directories and follows symlinked directories (with cycle protection).

use crate::fs::fuzzy::fuzzy_score;
use crate::fs::node_path::{
    project_runtime_path, relative_posix_runtime, resolve_runtime, runtime_path_from_native,
    runtime_string_from_native,
};
use crate::fs::watcher::{DirectoryWatcher, DirectoryWatcherOptions, WatchIgnore};
use crate::runtime_process::{
    RuntimeCompletedProcessRequest, RuntimeCompletedProcessSurface, RuntimeProcessContext,
};
use crate::runtime_values::{NodeBufferUtf8, RuntimePath, RuntimeString};
use futures::stream::Stream;
use ignore::gitignore::GitignoreBuilder;
use std::collections::HashSet;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

/// Extra ignore predicate: `(rel_path, is_dir) -> should_skip`.
pub type IgnorePredicate = Arc<dyn Fn(&RuntimeString, bool) -> bool + Send + Sync>;

/// Override the initial file-list source (test seam). Returns POSIX-relative
/// file paths for the given root.
pub type LoadFilesFn = Arc<
    dyn Fn(RuntimePath) -> Pin<Box<dyn Future<Output = Vec<RuntimeString>> + Send>> + Send + Sync,
>;

/// Options controlling how a [`FileIndex`] is built.
#[derive(Clone, Default)]
pub struct FileIndexOptions {
    /// Root directory to index (absolute).
    pub root: RuntimePath,
    /// Extra ignore predicate (POSIX-relative path, is_dir). Composed with the
    /// built-in skip set.
    pub ignore: Option<IgnorePredicate>,
    /// Glob patterns to exclude. `*` matches within a path segment, `**` matches
    /// across segments, `?` one char. Matched against POSIX-relative paths and
    /// composed with `ignore`.
    pub exclude: Option<Vec<RuntimeString>>,
    /// Start a [`DirectoryWatcher`] to keep the index fresh (default false).
    pub watch: bool,
    /// Watcher debounce window (ms).
    pub debounce_ms: Option<u64>,
    /// Override the initial file-list source (test seam).
    pub load_files: Option<LoadFilesFn>,
}

/// What a search hit refers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    File,
    Dir,
}

/// One fuzzy search hit.
#[derive(Debug, Clone, PartialEq)]
pub struct FileSearchResult {
    /// POSIX-relative path.
    pub path: RuntimeString,
    /// Whether the hit is a file or a directory.
    pub kind: EntryKind,
    pub score: f64,
    /// Matched char indices into `path`, ascending.
    pub indices: Vec<usize>,
}

/// How a [`FileChange`] mutated the index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileChangeKind {
    Add,
    Unlink,
}

/// A single incremental change applied during [`FileIndex::reconcile`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileChange {
    pub path: RuntimeString,
    pub kind: FileChangeKind,
}

/// Options for [`FileIndex::search_stream`].
#[derive(Default)]
pub struct SearchStreamOptions {
    pub limit: Option<usize>,
    pub signal: Option<crate::types::abort::AbortSignal>,
}

const DEFAULT_SKIP_DIRS: [&str; 5] = ["node_modules", ".git", "dist", ".hg", ".svn"];

/// Mutable index state, guarded by a mutex so the watcher's reconcile can run
/// concurrently with reads.
struct State {
    files: HashSet<RuntimeString>,
    dirs: HashSet<RuntimeString>,
    sorted_cache: Option<Vec<RuntimeString>>,
    scanned_file_count: u64,
}

/// The shared core of a [`FileIndex`]: shared between the handle and the
/// watcher's reconcile callback.
struct FileIndexInner {
    root: RuntimePath,
    ignore: IgnorePredicate,
    state: Mutex<State>,
}

/// An incremental, gitignore-aware file index with fuzzy search.
pub struct FileIndex {
    shared: Arc<FileIndexInner>,
    watcher: Mutex<Option<DirectoryWatcher>>,
}

impl FileIndex {
    /// Build the index: load the initial entries, then (optionally) start watching
    /// for changes. Watching is best-effort — failure to start the watcher never
    /// fails index construction.
    pub async fn create(
        process_context: RuntimeProcessContext,
        opts: FileIndexOptions,
    ) -> FileIndex {
        let exclude_match = match &opts.exclude {
            Some(e) if !e.is_empty() => Some(compile_exclude(e)),
            _ => None,
        };
        let user_ignore = opts.ignore.clone();
        let ignore: IgnorePredicate = Arc::new(move |rel: &RuntimeString, is_dir: bool| {
            if is_default_ignored(rel) {
                return true;
            }
            if let Some(ex) = &exclude_match {
                if (ex.as_ref())(rel) {
                    return true;
                }
            }
            if let Some(ui) = &user_ignore {
                if (ui.as_ref())(rel, is_dir) {
                    return true;
                }
            }
            false
        });

        // The load_files seam yields files only; directories are derived from
        // them. Without a seam, the default loader returns both (and a real walk
        // on a non-git root includes empty + symlinked directories).
        let (files, dirs): (Vec<RuntimeString>, Vec<RuntimeString>) =
            if let Some(load) = &opts.load_files {
                // Filter FIRST, then derive directories — so an excluded file does not
                // drag its (otherwise empty) ancestor directories into the index.
                let loaded = load(opts.root.clone()).await;
                let files: Vec<RuntimeString> = loaded
                    .into_iter()
                    .filter(|rel| !rel.is_empty() && !ignore_matches(&ignore, rel, false))
                    .collect();
                let dirs = derive_ancestor_dirs(&files);
                (files, dirs)
            } else {
                // default_load_entries already applies `ignore` during the walk.
                default_load_entries(&process_context, &opts.root, &ignore).await
            };

        let mut state = State {
            files: HashSet::new(),
            dirs: HashSet::new(),
            sorted_cache: None,
            scanned_file_count: 0,
        };
        for rel in files {
            state.files.insert(rel);
        }
        for rel in dirs {
            if !ignore_matches(&ignore, &rel, true) {
                state.dirs.insert(rel);
            }
        }

        let shared = Arc::new(FileIndexInner {
            root: opts.root.clone(),
            ignore: ignore.clone(),
            state: Mutex::new(state),
        });

        let watcher = if opts.watch {
            let shared_cb = shared.clone();
            let on_batch: Box<dyn Fn(Vec<String>) + Send> = Box::new(move |paths| {
                let s = shared_cb.clone();
                tokio::spawn(async move {
                    s.reconcile(paths.into_iter().map(RuntimePath::from).collect())
                        .await;
                });
            });
            // Watching is an optimization; the index still works as a snapshot.
            let exact_ignore = ignore.clone();
            let watch_ignore: WatchIgnore = Arc::new(move |rel: &str, is_dir: bool| {
                exact_ignore(&RuntimeString::from(rel), is_dir)
            });
            match watcher_root_string(&opts.root) {
                Some(root) => DirectoryWatcher::start(DirectoryWatcherOptions {
                    root,
                    ignore: Some(watch_ignore),
                    debounce_ms: opts.debounce_ms,
                    max_watchers: None,
                    mode: None,
                    on_batch,
                })
                .await
                .ok(),
                None => None,
            }
        } else {
            None
        };

        FileIndex {
            shared,
            watcher: Mutex::new(watcher),
        }
    }

    /// All indexed FILE paths, sorted (cached until the next mutation).
    pub fn all(&self) -> Vec<RuntimeString> {
        let mut state = self.shared.state.lock().unwrap();
        if state.sorted_cache.is_none() {
            let mut v: Vec<RuntimeString> = state.files.iter().cloned().collect();
            v.sort();
            state.sorted_cache = Some(v);
        }
        state.sorted_cache.clone().unwrap()
    }

    /// All indexed DIRECTORY paths, sorted.
    pub fn all_dirs(&self) -> Vec<RuntimeString> {
        let state = self.shared.state.lock().unwrap();
        let mut v: Vec<RuntimeString> = state.dirs.iter().cloned().collect();
        v.sort();
        v
    }

    /// Whether `rel_path` is an indexed file.
    pub fn has(&self, rel_path: &RuntimeString) -> bool {
        self.shared.state.lock().unwrap().files.contains(rel_path)
    }

    /// Whether `rel_path` is an indexed directory.
    pub fn has_dir(&self, rel_path: &RuntimeString) -> bool {
        self.shared.state.lock().unwrap().dirs.contains(rel_path)
    }

    /// Number of indexed files (directories are tracked separately).
    pub fn size(&self) -> usize {
        self.shared.state.lock().unwrap().files.len()
    }

    /// Monotonic count of entries scored across all search / search_stream calls.
    pub fn scanned_file_count(&self) -> u64 {
        self.shared.state.lock().unwrap().scanned_file_count
    }

    /// Rank the indexed entries (files AND directories) against `query`. An empty
    /// query returns the first `limit` entries in path order. Results are sorted by
    /// descending score, ties broken purely by ascending path.
    pub fn search(&self, query: &RuntimeString, limit: usize) -> Vec<FileSearchResult> {
        let mut state = self.shared.state.lock().unwrap();
        state.scanned_file_count += (state.files.len() + state.dirs.len()) as u64;

        if query.is_empty() {
            let mut all: Vec<FileSearchResult> = Vec::new();
            for path in &state.files {
                all.push(FileSearchResult {
                    path: path.clone(),
                    kind: EntryKind::File,
                    score: 0.0,
                    indices: Vec::new(),
                });
            }
            for path in &state.dirs {
                all.push(FileSearchResult {
                    path: path.clone(),
                    kind: EntryKind::Dir,
                    score: 0.0,
                    indices: Vec::new(),
                });
            }
            all.sort_by(|a, b| a.path.cmp(&b.path));
            all.truncate(limit);
            return all;
        }

        let mut hits: Vec<FileSearchResult> = Vec::new();
        for path in &state.files {
            if let Some(m) = fuzzy_score(query, path) {
                hits.push(FileSearchResult {
                    path: path.clone(),
                    kind: EntryKind::File,
                    score: m.score,
                    indices: m.indices,
                });
            }
        }
        for path in &state.dirs {
            if let Some(m) = fuzzy_score(query, path) {
                hits.push(FileSearchResult {
                    path: path.clone(),
                    kind: EntryKind::Dir,
                    score: m.score,
                    indices: m.indices,
                });
            }
        }
        hits.sort_by(cmp_score_desc_then_path_asc);
        hits.truncate(limit);
        hits
    }

    /// Streaming, cancelable variant of [`FileIndex::search`]. Scores entries
    /// (checking `signal` so a long scan aborts promptly and yielding to the
    /// runtime periodically), then yields the ranked top-`limit` one at a time.
    /// `scanned_file_count` accumulates across calls.
    pub async fn search_stream(
        &self,
        query: &RuntimeString,
        opts: SearchStreamOptions,
    ) -> Pin<Box<dyn Stream<Item = FileSearchResult> + Send>> {
        self.shared.search_stream(query, opts).await
    }

    /// Apply an incremental update for a batch of absolute paths (typically from
    /// the watcher). Returns the concrete FILE adds/removes that changed the index.
    pub async fn reconcile(&self, abs_paths: Vec<RuntimePath>) -> Vec<FileChange> {
        self.shared.reconcile(abs_paths).await
    }

    /// Stop the watcher (if any) and release its handles. Idempotent.
    pub fn close(&self) {
        if let Some(w) = self.watcher.lock().unwrap().take() {
            w.close();
        }
    }
}

impl FileIndexInner {
    async fn search_stream(
        &self,
        query: &RuntimeString,
        opts: SearchStreamOptions,
    ) -> Pin<Box<dyn Stream<Item = FileSearchResult> + Send>> {
        let limit = opts.limit.unwrap_or(50);
        let signal = opts.signal;

        // Snapshot the entries so scoring can yield to the runtime without
        // holding the lock across await points.
        let (files, dirs): (Vec<RuntimeString>, Vec<RuntimeString>) = {
            let state = self.state.lock().unwrap();
            (
                state.files.iter().cloned().collect(),
                state.dirs.iter().cloned().collect(),
            )
        };

        let empty_query = query.is_empty();
        let mut hits: Vec<FileSearchResult> = Vec::new();
        let mut scanned: u64 = 0;
        let mut aborted_early = false;

        let mut i = 0usize;
        for path in &files {
            if let Some(sig) = &signal {
                if sig.is_cancelled() {
                    aborted_early = true;
                    break;
                }
            }
            scanned += 1;
            if empty_query {
                hits.push(FileSearchResult {
                    path: path.clone(),
                    kind: EntryKind::File,
                    score: 0.0,
                    indices: Vec::new(),
                });
            } else if let Some(m) = fuzzy_score(query, path) {
                hits.push(FileSearchResult {
                    path: path.clone(),
                    kind: EntryKind::File,
                    score: m.score,
                    indices: m.indices,
                });
            }
            i += 1;
            if i.is_multiple_of(1024) {
                tokio::task::yield_now().await; // give the runtime a turn
            }
        }
        if !aborted_early {
            for path in &dirs {
                if let Some(sig) = &signal {
                    if sig.is_cancelled() {
                        aborted_early = true;
                        break;
                    }
                }
                scanned += 1;
                if empty_query {
                    hits.push(FileSearchResult {
                        path: path.clone(),
                        kind: EntryKind::Dir,
                        score: 0.0,
                        indices: Vec::new(),
                    });
                } else if let Some(m) = fuzzy_score(query, path) {
                    hits.push(FileSearchResult {
                        path: path.clone(),
                        kind: EntryKind::Dir,
                        score: m.score,
                        indices: m.indices,
                    });
                }
            }
        }

        self.state.lock().unwrap().scanned_file_count += scanned;

        if aborted_early {
            return Box::pin(futures::stream::empty::<FileSearchResult>());
        }

        if empty_query {
            hits.sort_by(|a, b| a.path.cmp(&b.path));
        } else {
            hits.sort_by(cmp_score_desc_then_path_asc);
        }
        hits.truncate(limit);

        let sig = signal;
        let stream = futures::stream::unfold(hits.into_iter(), move |mut it| {
            let sig = sig.clone();
            async move {
                if let Some(s) = &sig {
                    if s.is_cancelled() {
                        return None;
                    }
                }
                it.next().map(|h| (h, it))
            }
        });
        Box::pin(stream)
    }

    async fn reconcile(&self, abs_paths: Vec<RuntimePath>) -> Vec<FileChange> {
        let mut changes: Vec<FileChange> = Vec::new();
        for abs in &abs_paths {
            let rel = match to_rel(&self.root, abs) {
                Some(r) => r,
                None => continue, // outside root
            };

            // A missing path removes itself (and, if it was a directory prefix,
            // everything under it); an existing file is added; an existing
            // directory is walked so its files + subdirectories enter the index.
            let projected_abs = match project_runtime_path(abs, "file index path") {
                Ok(path) => path,
                Err(_) => continue,
            };
            let kind = match tokio::fs::metadata(&projected_abs).await {
                Ok(st) => {
                    if st.is_dir() {
                        Kind::Dir
                    } else {
                        Kind::File
                    }
                }
                Err(_) => Kind::Missing,
            };

            match kind {
                Kind::Missing => {
                    let mut guard = self.state.lock().unwrap();
                    remove_under(&mut guard, &rel, &mut changes);
                }
                Kind::File => {
                    if !ignore_matches(&self.ignore, &rel, false) {
                        let mut guard = self.state.lock().unwrap();
                        add_path(&mut guard, &self.ignore, &rel, &mut changes);
                    }
                }
                Kind::Dir => {
                    if ignore_matches(&self.ignore, &rel, true) {
                        continue;
                    }
                    self.add_tree(abs, &mut changes).await;
                }
            }
        }
        if !changes.is_empty() {
            self.state.lock().unwrap().sorted_cache = None;
        }
        changes
    }

    /// Walk a newly-seen directory (following symlinked dirs), adding its entries.
    async fn add_tree(&self, abs_dir: &RuntimePath, changes: &mut Vec<FileChange>) {
        let dir_rel = to_rel(&self.root, abs_dir);
        let (files, dirs) = collect_tree(abs_dir, &self.root, &self.ignore).await;
        let mut guard = self.state.lock().unwrap();
        let state = &mut *guard;
        if let Some(dr) = &dir_rel {
            if !ignore_matches(&self.ignore, dr, true) {
                state.dirs.insert(dr.clone());
            }
        }
        for d in dirs {
            state.dirs.insert(d);
        }
        for f in files {
            add_path(state, &self.ignore, &f, changes);
        }
    }
}

enum Kind {
    File,
    Dir,
    Missing,
}

/// Ascending-path comparator (POSIX-string order), the tie-break for equal scores.
fn cmp_score_desc_then_path_asc(a: &FileSearchResult, b: &FileSearchResult) -> std::cmp::Ordering {
    b.score
        .partial_cmp(&a.score)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| a.path.cmp(&b.path))
}

/// Call the composed ignore predicate.
fn ignore_matches(ig: &IgnorePredicate, rel: &RuntimeString, is_dir: bool) -> bool {
    (ig.as_ref())(rel, is_dir)
}

fn add_path(
    state: &mut State,
    ignore: &IgnorePredicate,
    rel: &RuntimeString,
    changes: &mut Vec<FileChange>,
) {
    if state.files.contains(rel) {
        return;
    }
    state.files.insert(rel.clone());
    changes.push(FileChange {
        path: rel.clone(),
        kind: FileChangeKind::Add,
    });
    // Register the file's ancestor directories so they are searchable too.
    for dir in ancestor_dirs_of(rel) {
        if !ignore_matches(ignore, &dir, true) {
            state.dirs.insert(dir);
        }
    }
}

fn remove_under(state: &mut State, rel: &RuntimeString, changes: &mut Vec<FileChange>) {
    if state.files.remove(rel) {
        changes.push(FileChange {
            path: rel.clone(),
            kind: FileChangeKind::Unlink,
        });
    }
    state.dirs.remove(rel);
    // If `rel` was a directory, drop every file and subdirectory beneath it.
    let prefix = rel.concat(&RuntimeString::from("/"));
    let dead_files: Vec<RuntimeString> = state
        .files
        .iter()
        .filter(|f| runtime_starts_with(f, &prefix))
        .cloned()
        .collect();
    for f in dead_files {
        state.files.remove(&f);
        changes.push(FileChange {
            path: f,
            kind: FileChangeKind::Unlink,
        });
    }
    let dead_dirs: Vec<RuntimeString> = state
        .dirs
        .iter()
        .filter(|d| runtime_starts_with(d, &prefix))
        .cloned()
        .collect();
    for d in dead_dirs {
        state.dirs.remove(&d);
    }
}

fn is_default_ignored(rel: &RuntimeString) -> bool {
    split_on_unit(rel, u16::from(b'/')).iter().any(|segment| {
        DEFAULT_SKIP_DIRS
            .iter()
            .any(|ignored| segment.as_utf16() == ignored.encode_utf16().collect::<Vec<_>>())
    })
}

/// Every ancestor directory of a relative path, e.g. `a/b/c.ts` → `a`, `a/b`.
fn ancestor_dirs_of(rel: &RuntimeString) -> Vec<RuntimeString> {
    let mut out = Vec::new();
    for (index, unit) in rel.as_utf16().iter().enumerate() {
        if *unit == u16::from(b'/') {
            out.push(rel.slice_utf16(..index));
        }
    }
    out
}

/// Derive the set of ancestor directories spanned by a list of file paths.
fn derive_ancestor_dirs(files: &[RuntimeString]) -> Vec<RuntimeString> {
    let mut set = HashSet::new();
    for f in files {
        for d in ancestor_dirs_of(f) {
            set.insert(d);
        }
    }
    set.into_iter().collect()
}

/// Compile exclude globs into a single predicate over POSIX-relative paths.
fn compile_exclude(
    patterns: &[RuntimeString],
) -> Arc<dyn Fn(&RuntimeString) -> bool + Send + Sync> {
    let patterns = patterns.to_vec();
    Arc::new(move |rel: &RuntimeString| {
        patterns.iter().any(|pattern| {
            if pattern.contains("/") {
                glob_matches(pattern.as_utf16(), rel.as_utf16())
            } else {
                split_on_unit(rel, u16::from(b'/'))
                    .iter()
                    .any(|segment| glob_matches(pattern.as_utf16(), segment.as_utf16()))
            }
        })
    })
}

fn glob_matches(pattern: &[u16], value: &[u16]) -> bool {
    let mut memo = std::collections::HashMap::new();
    glob_matches_from(pattern, value, 0, 0, &mut memo)
}

fn glob_matches_from(
    pattern: &[u16],
    value: &[u16],
    pi: usize,
    vi: usize,
    memo: &mut std::collections::HashMap<(usize, usize), bool>,
) -> bool {
    if let Some(result) = memo.get(&(pi, vi)) {
        return *result;
    }
    let result = if pi == pattern.len() {
        vi == value.len()
    } else if pattern[pi] == u16::from(b'*') {
        let double = pattern.get(pi + 1) == Some(&u16::from(b'*'));
        let next = pi + if double { 2 } else { 1 };
        let mut matched = glob_matches_from(pattern, value, next, vi, memo);
        if double && pattern.get(next) == Some(&u16::from(b'/')) {
            matched |= glob_matches_from(pattern, value, next + 1, vi, memo);
        }
        if !matched && vi < value.len() && (double || value[vi] != u16::from(b'/')) {
            matched = glob_matches_from(pattern, value, pi, vi + 1, memo);
        }
        matched
    } else if vi < value.len()
        && (pattern[pi] == value[vi]
            || (pattern[pi] == u16::from(b'?') && value[vi] != u16::from(b'/')))
    {
        glob_matches_from(pattern, value, pi + 1, vi + 1, memo)
    } else {
        false
    };
    memo.insert((pi, vi), result);
    result
}

/// A relative path under `root`, or `None` when `abs` is the root itself or
/// outside it.
fn to_rel(root: &RuntimePath, abs: &RuntimePath) -> Option<RuntimeString> {
    // POSIX-rendered: the index key is what the search tools hand the model.
    let rel = relative_posix_runtime(root, root, abs)
        .ok()?
        .into_runtime_string();
    if rel.is_empty() || rel.starts_with("..") {
        return None;
    }
    Some(rel)
}

fn join(a: &RuntimePath, b: &RuntimeString) -> RuntimePath {
    resolve_runtime(a, &[a.clone(), RuntimePath::from(b.clone())])
        .expect("a native file-index child resolves exactly")
}

/// Recursively collect files + directories under `start_dir`, returning paths
/// relative to `root`. Follows symlinked directories with realpath-based cycle
/// protection, and includes empty directories.
async fn collect_tree(
    start_dir: &RuntimePath,
    root: &RuntimePath,
    ignore: &IgnorePredicate,
) -> (Vec<RuntimeString>, Vec<RuntimeString>) {
    let mut files = Vec::new();
    let mut dirs = Vec::new();
    // Cycle protection tracks only the realpaths on the CURRENT descent (a stack),
    // not every dir ever seen — so two sibling symlinks to the same directory are
    // both indexed, while a symlink back to an ancestor is skipped.
    let mut ancestors: HashSet<RuntimePath> = HashSet::new();

    let projected_start = match project_runtime_path(start_dir, "file index root") {
        Ok(path) => path,
        Err(_) => return (files, dirs),
    };
    match tokio::fs::canonicalize(projected_start)
        .await
        .map(crate::fs::node_path::simplify_host)
    {
        Ok(p) => {
            ancestors.insert(runtime_path_from_native(&p));
        }
        Err(_) => return (files, dirs), // unreadable root — nothing to collect
    }

    walk(
        start_dir.clone(),
        root,
        ignore,
        &mut ancestors,
        &mut files,
        &mut dirs,
    )
    .await;
    (files, dirs)
}

fn walk<'a>(
    abs_dir: RuntimePath,
    root: &'a RuntimePath,
    ignore: &'a IgnorePredicate,
    ancestors: &'a mut HashSet<RuntimePath>,
    files: &'a mut Vec<RuntimeString>,
    dirs: &'a mut Vec<RuntimeString>,
) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
    Box::pin(async move {
        let projected_dir = match project_runtime_path(&abs_dir, "file index directory") {
            Ok(path) => path,
            Err(_) => return,
        };
        let mut rd = match tokio::fs::read_dir(projected_dir).await {
            Ok(r) => r,
            Err(_) => return,
        };
        loop {
            let entry = match rd.next_entry().await {
                Ok(Some(e)) => e,
                _ => break,
            };
            let child_name = runtime_string_from_native(&entry.file_name());
            let child_abs = join(&abs_dir, &child_name);
            let rel = match to_rel(root, &child_abs) {
                Some(r) => r,
                None => continue,
            };

            // Resolve directory-ness, following symlinks.
            let ft = match entry.file_type().await {
                Ok(t) => t,
                Err(_) => continue,
            };
            let mut is_dir = ft.is_dir();
            if ft.is_symlink() {
                let projected_child = match project_runtime_path(&child_abs, "file index path") {
                    Ok(path) => path,
                    Err(_) => continue,
                };
                match tokio::fs::metadata(projected_child).await {
                    Ok(m) => is_dir = m.is_dir(),
                    Err(_) => continue, // dangling symlink
                }
            }

            if ignore_matches(ignore, &rel, is_dir) {
                continue;
            }
            if is_dir {
                let projected_child = match project_runtime_path(&child_abs, "file index path") {
                    Ok(path) => path,
                    Err(_) => continue,
                };
                let real = match tokio::fs::canonicalize(projected_child)
                    .await
                    .map(crate::fs::node_path::simplify_host)
                {
                    Ok(p) => runtime_path_from_native(&p),
                    Err(_) => continue,
                };
                if ancestors.contains(&real) {
                    continue; // would descend into an ancestor — cycle
                }
                ancestors.insert(real.clone());
                dirs.push(rel);
                walk(
                    child_abs,
                    root,
                    ignore,
                    &mut *ancestors,
                    &mut *files,
                    &mut *dirs,
                )
                .await;
                ancestors.remove(&real); // pop: siblings may share a realpath
            } else {
                files.push(rel);
            }
        }
    })
}

/// Default initial loader: `git ls-files` (gitignore-aware, fast) when `root` is a
/// git repo — directories derived from the file list — else a filesystem walk that
/// returns real files + directories (empty + symlinked dirs included).
///
/// Git presence is decided by [`is_repo`] (a filesystem `.git` check), NOT by
/// `git ls-files` succeeding — so a transient failure inside a repo never silently
/// downgrades to an ignore-blind full-tree walk.
///
/// Ignore files: `.ignore`/`.rgignore` are honored EVERYWHERE, composed onto both
/// the git list and the walk fallback. `.gitignore` is honored ONLY inside a git
/// repo (a stray `.gitignore` in a non-git dir must not suppress an unrelated tree).
async fn default_load_entries(
    process_context: &RuntimeProcessContext,
    root: &RuntimePath,
    ignore: &IgnorePredicate,
) -> (Vec<RuntimeString>, Vec<RuntimeString>) {
    let in_repo = is_repo_runtime(root).await;
    let ignore_files = load_repo_ignore(root, in_repo).await;

    if in_repo {
        if let Some(files) = git_list_files(process_context, root).await {
            // `git ls-files --exclude-standard` already drops .gitignore'd files but
            // NOT .ignore/.rgignore; apply the ignore-file matcher so those are
            // honored uniformly with the walk path.
            let kept: Vec<RuntimeString> = match &ignore_files {
                Some(ig) => files
                    .into_iter()
                    .filter(|p| !ignore_matches(ig, p, false))
                    .collect(),
                None => files,
            };
            let dirs = derive_ancestor_dirs(&kept);
            return (kept, dirs);
        }
        // ls-files failed inside a repo (rare): fall through to an ignore-AWARE walk.
    }

    let composed: IgnorePredicate = match &ignore_files {
        Some(ig) => {
            let base = ignore.clone();
            let igc = ig.clone();
            Arc::new(move |rel: &RuntimeString, is_dir: bool| {
                ignore_matches(&base, rel, is_dir) || ignore_matches(&igc, rel, is_dir)
            })
        }
        None => ignore.clone(),
    };
    collect_tree(root, root, &composed).await
}

/// Run `git ls-files` (tracked + untracked-not-ignored) for `root`, returning the
/// POSIX-relative file list, or `None` if git errors / is unavailable.
async fn git_list_files(
    process_context: &RuntimeProcessContext,
    root: &RuntimePath,
) -> Option<Vec<RuntimeString>> {
    let output = process_context
        .exec_completed(
            RuntimeCompletedProcessRequest::new(
                RuntimeCompletedProcessSurface::FileIndexGitStream,
                RuntimeString::from("git"),
            )
            .with_args(vec![
                RuntimeString::from("-C"),
                root.as_runtime_string().clone(),
                RuntimeString::from("ls-files"),
                RuntimeString::from("-z"),
                RuntimeString::from("--cached"),
                RuntimeString::from("--others"),
                RuntimeString::from("--exclude-standard"),
            ]),
        )
        .await
        .ok()?;
    if !output.status.success() {
        return None; // not a repo / git error
    }
    // `git ls-files` already uses POSIX separators.
    let decoded = NodeBufferUtf8::decode(&output.stdout);
    Some(
        split_on_unit(&decoded, 0)
            .into_iter()
            .filter(|path| !path.is_empty())
            .collect(),
    )
}

/// Ripgrep-style ignore files, honored regardless of git presence.
const RG_IGNORE_FILES: [&str; 2] = [".ignore", ".rgignore"];

/// Build a matcher from `root`'s ignore files: always `.ignore`/`.rgignore`
/// (honored everywhere), plus `.gitignore` only when `in_repo`. Returns `None` if
/// none are present. Full gitignore syntax (negation, anchoring, `**`) is honored.
async fn load_repo_ignore(root: &RuntimePath, in_repo: bool) -> Option<IgnorePredicate> {
    let names: Vec<&str> = if in_repo {
        let mut v = vec![".gitignore"];
        v.extend_from_slice(&RG_IGNORE_FILES);
        v
    } else {
        RG_IGNORE_FILES.to_vec()
    };
    let mut sources: Vec<RuntimeString> = Vec::new();
    for name in names {
        let path = join(root, &RuntimeString::from(name));
        let projected = match project_runtime_path(&path, "ignore file") {
            Ok(path) => path,
            Err(_) => continue,
        };
        if let Ok(bytes) = tokio::fs::read(projected).await {
            sources.push(NodeBufferUtf8::decode(&bytes));
        }
        // Missing/unreadable ignore file — skip it.
    }
    if sources.is_empty() {
        return None;
    }
    let mut builder = GitignoreBuilder::new("");
    for source in sources {
        for line in split_on_unit(&source, u16::from(b'\n')) {
            let line = if line.ends_with("\r") {
                line.slice_utf16(..line.len_utf16().saturating_sub(1))
            } else {
                line
            };
            let encoded = matcher_string(&line, true);
            let _ = builder.add_line(None, &encoded);
        }
    }
    let matcher = match builder.build() {
        Ok(m) => Arc::new(m),
        Err(_) => return None,
    };
    // A directory-only pattern (`build/`) matches only when the entry is queried
    // as a directory; the crate handles this via the `is_dir` flag, so a directory
    // entry is not leaked as a phantom.
    Some(Arc::new(move |rel: &RuntimeString, is_dir: bool| {
        !rel.is_empty()
            && matcher
                .matched_path_or_any_parents(Path::new(&matcher_string(rel, false)), is_dir)
                .is_ignore()
    }))
}

/// A [`FileSearchResult`] annotated with which root it came from.
#[derive(Debug, Clone, PartialEq)]
pub struct MultiRootSearchResult {
    /// POSIX-relative path.
    pub path: RuntimeString,
    pub kind: EntryKind,
    pub score: f64,
    pub indices: Vec<usize>,
    /// Absolute root directory this entry belongs to.
    pub root: RuntimePath,
}

/// Search across MULTIPLE roots. Each root gets its own [`FileIndex`]; results
/// are merged and re-ranked, annotated with their originating root. Each path
/// stays relative to its own root, so the `root` field disambiguates same-named
/// paths across roots.
pub struct MultiRootFileIndex {
    indexes: Vec<(RuntimePath, FileIndex)>,
}

impl MultiRootFileIndex {
    /// Build one [`FileIndex`] per root (the `root` field of `opts` is ignored).
    pub async fn create(
        process_context: RuntimeProcessContext,
        roots: Vec<RuntimePath>,
        opts: FileIndexOptions,
    ) -> MultiRootFileIndex {
        let mut indexes = Vec::new();
        for root in roots {
            let mut o = opts.clone();
            o.root = root.clone();
            let index = FileIndex::create(process_context.clone(), o).await;
            indexes.push((root, index));
        }
        MultiRootFileIndex { indexes }
    }

    /// Merge per-root rankings and take the global top-`limit`.
    pub fn search(&self, query: &RuntimeString, limit: usize) -> Vec<MultiRootSearchResult> {
        let mut all: Vec<MultiRootSearchResult> = Vec::new();
        for (root, index) in &self.indexes {
            for r in index.search(query, limit) {
                all.push(MultiRootSearchResult {
                    path: r.path,
                    kind: r.kind,
                    score: r.score,
                    indices: r.indices,
                    root: root.clone(),
                });
            }
        }
        if query.is_empty() {
            all.sort_by(|a, b| a.path.cmp(&b.path));
        } else {
            all.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.path.cmp(&b.path))
            });
        }
        all.truncate(limit);
        all
    }

    /// Total entries scored across all roots.
    pub fn scanned_file_count(&self) -> u64 {
        self.indexes
            .iter()
            .map(|(_, index)| index.scanned_file_count())
            .sum()
    }

    /// Stop every root's watcher. Idempotent.
    pub fn close(&self) {
        for (_, index) in &self.indexes {
            index.close();
        }
    }
}

fn runtime_starts_with(value: &RuntimeString, prefix: &RuntimeString) -> bool {
    value.as_utf16().starts_with(prefix.as_utf16())
}

fn split_on_unit(value: &RuntimeString, separator: u16) -> Vec<RuntimeString> {
    let units = value.as_utf16();
    let mut output = Vec::new();
    let mut start = 0;
    for (index, unit) in units.iter().enumerate() {
        if *unit == separator {
            output.push(RuntimeString::from_utf16(units[start..index].to_vec()));
            start = index + 1;
        }
    }
    output.push(RuntimeString::from_utf16(units[start..].to_vec()));
    output
}

fn matcher_string(value: &RuntimeString, pattern: bool) -> String {
    let mut output = String::with_capacity(value.len_utf16());
    for point in value.code_points() {
        let syntax = pattern
            && matches!(
                point,
                0x0021 | 0x0023 | 0x002a | 0x003f | 0x005b | 0x005d | 0x005c | 0x002d | 0x005e
            );
        let separator = matches!(point, 0x002f | 0x005c);
        if syntax || separator {
            output.push(char::from_u32(point).expect("ASCII matcher syntax"));
        } else if let Some(scalar) = char::from_u32(point) {
            let mut encoded = [0; 2];
            for unit in scalar.encode_utf16(&mut encoded).iter().copied() {
                output.push(
                    char::from_u32(0x0f0000 + u32::from(unit))
                        .expect("one private scalar exists per UTF-16 code unit"),
                );
            }
        } else {
            output.push(
                char::from_u32(0x0f0000 + point)
                    .expect("one private scalar exists per UTF-16 code unit"),
            );
        }
    }
    output
}

async fn is_repo_runtime(root: &RuntimePath) -> bool {
    let git = join(root, &RuntimeString::from(".git"));
    let projected = match project_runtime_path(&git, "git metadata") {
        Ok(path) => path,
        Err(_) => return false,
    };
    tokio::fs::metadata(projected).await.is_ok()
}

fn watcher_root_string(root: &RuntimePath) -> Option<String> {
    #[cfg(unix)]
    {
        let projected = project_runtime_path(root, "watch root").ok()?;
        use std::os::unix::ffi::OsStrExt;
        String::from_utf8(projected.as_os_str().as_bytes().to_vec()).ok()
    }
    #[cfg(windows)]
    {
        String::from_utf8(root.as_runtime_string().to_node_utf8()).ok()
    }
    #[cfg(not(any(unix, windows)))]
    {
        let projected = project_runtime_path(root, "watch root").ok()?;
        projected.into_os_string().into_string().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-{}-{}-{}-{}",
            tag,
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
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }

    fn r(value: &str) -> RuntimeString {
        RuntimeString::from(value)
    }

    fn p(value: &str) -> RuntimePath {
        RuntimePath::from(value)
    }

    fn score(query: &str, candidate: &str) -> Option<crate::fs::fuzzy::FuzzyMatch> {
        fuzzy_score(&r(query), &r(candidate))
    }

    /// A `load_files` seam returning a fixed list, ignoring the root.
    fn load_files(items: &[&str]) -> LoadFilesFn {
        let items: Vec<RuntimeString> = items.iter().map(|s| r(s)).collect();
        Arc::new(move |_root: RuntimePath| {
            let v = items.clone();
            Box::pin(async move { v }) as Pin<Box<dyn Future<Output = Vec<RuntimeString>> + Send>>
        })
    }

    async fn collect(
        mut s: Pin<Box<dyn Stream<Item = FileSearchResult> + Send>>,
    ) -> Vec<FileSearchResult> {
        let mut out = Vec::new();
        while let Some(x) = s.next().await {
            out.push(x);
        }
        out
    }

    // ── fuzzy_score ────────────────────────────────────────────────────

    #[test]
    fn matches_subsequences_and_rejects_non_subsequences() {
        assert!(score("abc", "xaybzc").is_some());
        assert!(score("abc", "acb").is_none());
        let m = score("", "anything").unwrap();
        assert_eq!(m.score, 0.0);
        assert!(m.indices.is_empty());
    }

    #[test]
    fn ranks_basename_match_above_scattered_ancestor_match() {
        let base = score("readme", "README.md");
        let scattered = score("readme", "docs/r/e/a/d/m/e/x.md");
        assert!(base.is_some());
        assert!(scattered.is_some());
        assert!(base.unwrap().score > scattered.unwrap().score);
    }

    #[test]
    fn rewards_consecutive_runs_over_gappy_matches() {
        let consec = score("index", "index.ts").unwrap().score;
        let gappy = score("index", "i_n_d_e_x.ts").unwrap().score;
        assert!(consec > gappy);
    }

    #[test]
    fn finds_the_optimal_alignment_not_the_greedy_first_match() {
        // Greedy first-match would align a@0, b@3; the optimal alignment is the
        // consecutive a@2, b@3 — proving the DP picks the best positions.
        let m = score("ab", "a_ab").unwrap();
        assert_eq!(m.indices, vec![2, 3]);
    }

    // ── FileIndex — load & search ──────────────────────────────────────

    #[tokio::test]
    async fn loads_an_injected_file_list_applying_the_default_ignore_set() {
        let idx = FileIndex::create(
            crate::runtime_process::test_runtime_process_context(),
            FileIndexOptions {
                root: p("/repo"),
                load_files: Some(load_files(&[
                    "src/a.ts",
                    "src/b/c.ts",
                    "README.md",
                    "node_modules/dep/x.js", // dropped by default ignore
                    ".git/config",           // dropped by default ignore
                ])),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(idx.size(), 3);
        assert!(idx.has(&r("src/a.ts")));
        assert!(!idx.has(&r("node_modules/dep/x.js")));
        assert_eq!(idx.all(), vec!["README.md", "src/a.ts", "src/b/c.ts"]);
    }

    #[tokio::test]
    async fn honors_a_custom_ignore_predicate_composed_with_the_default() {
        let idx = FileIndex::create(
            crate::runtime_process::test_runtime_process_context(),
            FileIndexOptions {
                root: p("/repo"),
                ignore: Some(Arc::new(|rel: &RuntimeString, _is_dir: bool| {
                    rel.ends_with(".snap")
                })),
                load_files: Some(load_files(&["a.ts", "b.snap", "c.ts"])),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(idx.all(), vec!["a.ts", "c.ts"]);
    }

    #[tokio::test]
    async fn ranks_search_results_exact_name_first_files_and_dirs() {
        let idx = FileIndex::create(
            crate::runtime_process::test_runtime_process_context(),
            FileIndexOptions {
                root: p("/repo"),
                load_files: Some(load_files(&[
                    "src/config.ts",
                    "config/legacy/old.ts",
                    "src/index.ts",
                ])),
                ..Default::default()
            },
        )
        .await;
        let hits = idx.search(&r("config"), 50);
        // The `config` directory is an exact name match → ranks first, tagged dir.
        assert_eq!(hits[0].path, "config");
        assert_eq!(hits[0].kind, EntryKind::Dir);
        // The file is found too, tagged as a file.
        assert_eq!(
            hits.iter()
                .find(|h| h.path == "src/config.ts")
                .unwrap()
                .kind,
            EntryKind::File
        );
        // `src/index.ts` is not a subsequence of the query.
        assert!(!hits.iter().any(|h| h.path == "src/index.ts"));
    }

    #[tokio::test]
    async fn empty_query_returns_sorted_paths_up_to_the_limit() {
        let idx = FileIndex::create(
            crate::runtime_process::test_runtime_process_context(),
            FileIndexOptions {
                root: p("/repo"),
                load_files: Some(load_files(&["c.ts", "a.ts", "b.ts"])),
                ..Default::default()
            },
        )
        .await;
        let paths: Vec<RuntimeString> = idx.search(&r(""), 2).into_iter().map(|h| h.path).collect();
        assert_eq!(paths, vec!["a.ts", "b.ts"]);
    }

    // ── FileIndex — incremental reconcile (real fs) ─────────────────────────

    #[tokio::test]
    async fn adds_a_new_file_walks_a_new_directory_and_removes_deleted_paths() {
        let dir = temp_dir("fidx");
        let idx = FileIndex::create(
            crate::runtime_process::test_runtime_process_context(),
            FileIndexOptions {
                root: runtime_path_from_native(&dir),
                load_files: Some(load_files(&[])),
                ..Default::default()
            },
        )
        .await;

        // New file.
        std::fs::write(dir.join("a.txt"), "x").unwrap();
        let changes = idx
            .reconcile(vec![runtime_path_from_native(&dir.join("a.txt"))])
            .await;
        assert_eq!(
            changes,
            vec![FileChange {
                path: r("a.txt"),
                kind: FileChangeKind::Add
            }]
        );
        assert!(idx.has(&r("a.txt")));

        // New directory with contents → reconciling the dir walks it.
        std::fs::create_dir(dir.join("sub")).unwrap();
        std::fs::write(dir.join("sub").join("b.txt"), "y").unwrap();
        std::fs::write(dir.join("sub").join("c.txt"), "z").unwrap();
        let changes = idx
            .reconcile(vec![runtime_path_from_native(&dir.join("sub"))])
            .await;
        let mut paths: Vec<RuntimeString> = changes.iter().map(|c| c.path.clone()).collect();
        paths.sort();
        assert_eq!(paths, vec!["sub/b.txt", "sub/c.txt"]);

        // Delete the directory → every entry beneath it is removed.
        std::fs::remove_dir_all(dir.join("sub")).unwrap();
        let changes = idx
            .reconcile(vec![runtime_path_from_native(&dir.join("sub"))])
            .await;
        let mut paths: Vec<RuntimeString> = changes.iter().map(|c| c.path.clone()).collect();
        paths.sort();
        assert_eq!(paths, vec!["sub/b.txt", "sub/c.txt"]);
        assert!(changes.iter().all(|c| c.kind == FileChangeKind::Unlink));
        assert!(!idx.has(&r("sub/b.txt")));

        // A path outside the root is ignored.
        assert_eq!(idx.reconcile(vec![p("/etc/hosts")]).await, Vec::new());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn is_idempotent_re_adding_an_existing_file_yields_no_change() {
        let dir = temp_dir("fidx");
        let idx = FileIndex::create(
            crate::runtime_process::test_runtime_process_context(),
            FileIndexOptions {
                root: runtime_path_from_native(&dir),
                load_files: Some(load_files(&[])),
                ..Default::default()
            },
        )
        .await;
        std::fs::write(dir.join("a.txt"), "x").unwrap();
        let path = runtime_path_from_native(&dir.join("a.txt"));
        assert_eq!(idx.reconcile(vec![path.clone()]).await.len(), 1);
        assert_eq!(idx.reconcile(vec![path]).await.len(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    // ── FileIndex — live watcher integration (real fs) ──────────────────────

    #[tokio::test]
    async fn picks_up_a_newly_created_file_via_the_watcher() {
        let dir = temp_dir("fidxw");
        let idx = FileIndex::create(
            crate::runtime_process::test_runtime_process_context(),
            FileIndexOptions {
                root: runtime_path_from_native(&dir),
                watch: true,
                debounce_ms: Some(20),
                load_files: Some(load_files(&[])),
                ..Default::default()
            },
        )
        .await;
        std::fs::write(dir.join("fresh.ts"), "export {}").unwrap();
        // Poll for the watcher → reconcile to land (native watch latency varies).
        let mut seen = false;
        for _ in 0..50 {
            if seen {
                break;
            }
            if idx.has(&r("fresh.ts")) {
                seen = true;
            } else {
                sleep(100).await;
            }
        }
        assert!(seen);
        idx.close();
        std::fs::remove_dir_all(&dir).ok();
    }

    // ── FileIndex — directories, exclude, tie-break ────────────────────────

    #[tokio::test]
    async fn derives_searchable_directories_from_the_file_list() {
        let idx = FileIndex::create(
            crate::runtime_process::test_runtime_process_context(),
            FileIndexOptions {
                root: p("/repo"),
                load_files: Some(load_files(&["pkgs/core/a.ts", "pkgs/cli/b.ts"])),
                ..Default::default()
            },
        )
        .await;
        assert!(idx.has_dir(&r("pkgs")));
        assert!(idx.has_dir(&r("pkgs/core")));
        assert!(idx.has_dir(&r("pkgs/cli")));
        assert_eq!(idx.all_dirs(), vec!["pkgs", "pkgs/cli", "pkgs/core"]);
        let hit = idx
            .search(&r("core"), 50)
            .into_iter()
            .find(|h| h.path == "pkgs/core");
        assert_eq!(hit.unwrap().kind, EntryKind::Dir);
    }

    #[tokio::test]
    async fn excludes_paths_by_glob_composed_with_the_default_ignore_set() {
        let idx = FileIndex::create(
            crate::runtime_process::test_runtime_process_context(),
            FileIndexOptions {
                root: p("/repo"),
                exclude: Some(vec![r("gen/**"), r("*.snap")]),
                load_files: Some(load_files(&[
                    "src/a.ts",
                    "gen/b.ts",
                    "gen/deep/c.ts",
                    "src/x.snap",
                ])),
                ..Default::default()
            },
        )
        .await;
        assert!(idx.has(&r("src/a.ts")));
        assert!(!idx.has(&r("gen/b.ts")));
        assert!(!idx.has(&r("gen/deep/c.ts")));
        assert!(!idx.has(&r("src/x.snap")));
        assert!(!idx.has_dir(&r("gen")));
    }

    #[tokio::test]
    async fn breaks_score_ties_purely_by_ascending_path_not_by_length() {
        let idx = FileIndex::create(
            crate::runtime_process::test_runtime_process_context(),
            FileIndexOptions {
                root: p("/repo"),
                // Both basenames are an exact `x.ts` match (equal score); `aaaa/x.ts`
                // is LONGER but lexicographically smaller than `zzz/x.ts`.
                load_files: Some(load_files(&["zzz/x.ts", "aaaa/x.ts"])),
                ..Default::default()
            },
        )
        .await;
        let hits: Vec<FileSearchResult> = idx
            .search(&r("x.ts"), 50)
            .into_iter()
            .filter(|h| h.kind == EntryKind::File)
            .collect();
        assert_eq!(hits[0].path, "aaaa/x.ts"); // lexicographic, not shortest-first
    }

    // ── FileIndex — streaming search, scanned_file_count ────────────────────

    #[tokio::test]
    async fn search_stream_yields_the_same_ranked_results_as_search() {
        let idx = FileIndex::create(
            crate::runtime_process::test_runtime_process_context(),
            FileIndexOptions {
                root: p("/repo"),
                load_files: Some(load_files(&[
                    "src/config.ts",
                    "config/legacy/old.ts",
                    "src/index.ts",
                ])),
                ..Default::default()
            },
        )
        .await;
        let streamed: Vec<RuntimeString> = collect(
            idx.search_stream(&r("config"), SearchStreamOptions::default())
                .await,
        )
        .await
        .into_iter()
        .map(|h| h.path)
        .collect();
        let sync: Vec<RuntimeString> = idx
            .search(&r("config"), 50)
            .into_iter()
            .map(|h| h.path)
            .collect();
        assert_eq!(streamed, sync);
    }

    #[tokio::test]
    async fn search_stream_stops_promptly_when_its_signal_is_aborted() {
        let items: Vec<String> = (0..100).map(|i| format!("f{i}.ts")).collect();
        let refs: Vec<&str> = items.iter().map(|s| s.as_str()).collect();
        let idx = FileIndex::create(
            crate::runtime_process::test_runtime_process_context(),
            FileIndexOptions {
                root: p("/repo"),
                load_files: Some(load_files(&refs)),
                ..Default::default()
            },
        )
        .await;
        let signal = crate::types::abort::AbortSignal::new();
        signal.cancel(); // already aborted before iterating
        let results = collect(
            idx.search_stream(
                &r("f"),
                SearchStreamOptions {
                    limit: None,
                    signal: Some(signal),
                },
            )
            .await,
        )
        .await;
        assert_eq!(results, Vec::new());
    }

    #[tokio::test]
    async fn scanned_file_count_accumulates_across_calls() {
        let idx = FileIndex::create(
            crate::runtime_process::test_runtime_process_context(),
            FileIndexOptions {
                root: p("/repo"),
                load_files: Some(load_files(&["a.ts", "b.ts"])), // 2 files; dirs derived = none
                ..Default::default()
            },
        )
        .await;
        assert_eq!(idx.scanned_file_count(), 0);
        idx.search(&r("a"), 50);
        assert_eq!(idx.scanned_file_count(), 2);
        let _ = collect(
            idx.search_stream(&r("b"), SearchStreamOptions::default())
                .await,
        )
        .await;
        assert_eq!(idx.scanned_file_count(), 4);
    }

    // ── MultiRootFileIndex — multi search-root ────────────────────────────

    #[tokio::test]
    async fn merges_and_reranks_results_across_roots_annotating_each_with_its_root() {
        let load: LoadFilesFn = Arc::new(|root: RuntimePath| {
            let v: Vec<RuntimeString> = if root == p("/a") {
                vec![r("pkg/config.ts")]
            } else {
                vec![r("pkg/config.ts"), r("other.ts")]
            };
            Box::pin(async move { v })
        });
        let multi = MultiRootFileIndex::create(
            crate::runtime_process::test_runtime_process_context(),
            vec![p("/a"), p("/b")],
            FileIndexOptions {
                load_files: Some(load),
                ..Default::default()
            },
        )
        .await;
        let hits = multi.search(&r("config"), 50);
        let mut roots: Vec<RuntimePath> = hits
            .iter()
            .filter(|h| h.path == "pkg/config.ts")
            .map(|h| h.root.clone())
            .collect();
        roots.sort();
        assert_eq!(roots, vec![p("/a"), p("/b")]);
        assert!(multi.scanned_file_count() > 0);
        multi.close();
    }

    // ── FileIndex — real-fs walk: directories & symlinks ────────────────────

    #[cfg(unix)]
    #[tokio::test]
    async fn indexes_real_directories_and_follows_symlinked_dirs_without_looping() {
        use std::os::unix::fs::symlink;
        let dir = temp_dir("fidxsym");
        std::fs::create_dir_all(dir.join("real")).unwrap();
        std::fs::write(dir.join("real").join("f.ts"), "x").unwrap();
        std::fs::create_dir_all(dir.join("empty")).unwrap();
        // A symlink to a sibling directory (should be followed) …
        symlink(dir.join("real"), dir.join("link")).unwrap();
        // … and a self-referential symlink (must not loop forever).
        symlink(&dir, dir.join("selfloop")).unwrap();

        // No load_files → real walk (temp dir is not a git repo → collect_tree).
        let idx = FileIndex::create(
            crate::runtime_process::test_runtime_process_context(),
            FileIndexOptions {
                root: runtime_path_from_native(&dir),
                ..Default::default()
            },
        )
        .await;

        assert!(idx.has(&r("real/f.ts")));
        assert!(idx.has_dir(&r("real")));
        assert!(idx.has_dir(&r("empty"))); // empty dirs are indexed in the walk path
        assert!(idx.has(&r("link/f.ts"))); // followed the symlinked directory
                                           // Completing at all proves the self-loop was broken by realpath tracking.
        std::fs::remove_dir_all(&dir).ok();
    }
}
