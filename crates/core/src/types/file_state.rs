//! File-state tracking: last-seen baselines for stale-overwrite + external-edit
//! detection.

use crate::runtime_values::{RuntimePath, RuntimeString};

/// Text encoding a file was last read/written as (the subset of text
/// encodings the engine uses).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Encoding {
    /// utf8 — the default.
    #[default]
    Utf8,
    /// utf16le — for a BOM'd file, preserved on overwrite.
    Utf16Le,
}

/// A tracked file's last-seen state.
#[derive(Debug, Clone, PartialEq)]
pub struct FileStateEntry {
    /// mtime (ms) the agent last saw. Backs stale-overwrite detection.
    pub mtime_ms: f64,
    /// Byte size last seen (a cheap pre-filter for external-edit detection).
    pub size: Option<u64>,
    /// The raw on-disk text the agent last saw — present ONLY for a complete
    /// text read / edit / write (absent for partial, truncated, or binary
    /// reads). It backs external-edit diffing.
    pub content: Option<RuntimeString>,
    /// The encoding the file was last read/written as (utf8 default, utf16le for
    /// a BOM'd file). A full Write reuses this to preserve a utf16le file's
    /// encoding on overwrite.
    pub encoding: Option<Encoding>,
}

/// Optional metadata passed to [`FileStateCache::record`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RecordOptions {
    pub size: Option<u64>,
    pub content: Option<RuntimeString>,
    pub encoding: Option<Encoding>,
}

/// Tracks files the agent has read/written, so (a) writes/edits can detect a
/// stale overwrite (file changed on disk since the agent last saw it), and (b)
/// the edited-files reminder can surface out-of-band changes (user/linter
/// edits).
///
/// The mutating methods take `&self`: the cache is shared by reference and
/// mutated in place, so implementations use interior mutability (a `Mutex`),
/// letting the cache live behind a shared `Arc` in the tool context.
pub trait FileStateCache: Send + Sync {
    fn get(&self, path: &RuntimePath) -> Option<FileStateEntry>;
    fn record(&self, path: &RuntimePath, mtime_ms: f64, opts: RecordOptions);
    fn has(&self, path: &RuntimePath) -> bool;
    /// A snapshot of all tracked entries (safe to iterate while the cache
    /// mutates).
    fn entries(&self) -> Vec<(RuntimePath, FileStateEntry)>;
    /// Drop a tracked path (e.g. it was deleted on disk).
    fn delete(&self, path: &RuntimePath);
    /// Drop ALL tracked baselines — used after a wholesale change to the working
    /// tree (a checkpoint restore) that may have altered every file.
    fn clear(&self);
    /// The content THIS TURN first found at `path`, installing `content` as that
    /// baseline when the turn has not touched the file yet.
    ///
    /// Edit volume is measured against this, not against the content the previous
    /// edit left behind: summing per-edit counts charges every line a later edit
    /// touches again, which reads as more lines changed than the file even has.
    ///
    /// `None` means the baseline is unknowable — the caller had none, or holding it
    /// would exceed the cache's budget — and the volume is then unknown rather
    /// than zero.
    ///
    /// The default keeps no turn memory and hands back the caller's own content,
    /// which is the honest answer for a cache that does not track turns.
    fn turn_baseline(
        &self,
        _path: &RuntimePath,
        content: Option<&RuntimeString>,
    ) -> Option<RuntimeString> {
        content.cloned()
    }
    /// Whether this turn first found a file at `path`. Shared with child turn
    /// metadata, separately from content so an empty file differs from absence.
    fn turn_existed(&self, _path: &RuntimePath, observed_before: bool) -> bool {
        observed_before
    }
    /// Forget the per-turn baselines: the next edit to each file starts a new one.
    fn begin_turn(&self) {}
    /// A detached copy — a fork sub-agent inherits the parent's baselines; a
    /// typed sub-agent starts from a fresh one.
    fn clone_box(&self) -> Box<dyn FileStateCache>;
}
