//! In-memory file tracker backing stale-overwrite detection and external-edit
//! diffing.

use crate::runtime_values::{RuntimePath, RuntimeString};
use crate::types::file_state::{FileStateCache, FileStateEntry, RecordOptions};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Don't retain content for very large files — diffing them every turn would be
/// costly, and the read tool already caps reads. Above this, only mtime/size are
/// kept (no external-edit diff for that file).
const MAX_TRACKED_CONTENT_BYTES: usize = 512 * 1024;
/// Bound the cache so a long session can't grow it without limit, using an
/// LRU-style cap. Update-order eviction: the least-recently-recorded entry
/// is dropped first.
const MAX_ENTRIES: usize = 256;
const MAX_TOTAL_CONTENT_BYTES: usize = 24 * 1024 * 1024;
/// Turn baselines are held whole, because a partial baseline would measure a
/// smaller edit than really happened. A file or a turn past these budgets reports
/// its volume as unknown instead.
const MAX_TURN_BASELINE_BYTES: usize = 32 * 1024 * 1024;
const MAX_TURN_BASELINE_TOTAL_BYTES: usize = 64 * 1024 * 1024;

/// What each file looked like when the CURRENT turn first touched it.
///
/// A recorded `None` is a file whose baseline could not be held — the turn knows
/// it was edited and knows it cannot say by how much.
#[derive(Default)]
struct TurnBaselines {
    first_seen: HashMap<RuntimePath, Option<RuntimeString>>,
    existed: HashMap<RuntimePath, bool>,
    bytes: usize,
}

/// The tracked entries in record order (oldest first) plus the running content
/// total. Kept behind a `Mutex` so the cache mutates through `&self`.
#[derive(Default)]
struct Inner {
    /// Re-recording a path moves it to the back, so the front is always the
    /// least-recently-recorded entry.
    entries: Vec<(RuntimePath, FileStateEntry)>,
    total_content: usize,
}

impl Inner {
    fn position(&self, path: &RuntimePath) -> Option<usize> {
        self.entries.iter().position(|(p, _)| p == path)
    }

    /// Subtract the tracked content of `path` (if any) from the running total,
    /// ahead of replacing or removing its entry.
    fn drop_content_bytes(&mut self, path: &RuntimePath) {
        if let Some(i) = self.position(path) {
            if let Some(content) = &self.entries[i].1.content {
                self.total_content -= content.to_node_utf8().len();
            }
        }
    }

    fn delete(&mut self, path: &RuntimePath) {
        self.drop_content_bytes(path);
        if let Some(i) = self.position(path) {
            self.entries.remove(i);
        }
    }

    fn evict(&mut self) {
        while self.entries.len() > MAX_ENTRIES || self.total_content > MAX_TOTAL_CONTENT_BYTES {
            let oldest = match self.entries.first() {
                Some((path, _)) => path.clone(),
                None => break,
            };
            self.delete(&oldest);
        }
    }
}

/// In-memory file tracker: stale-overwrite detection + external-edit diffing.
#[derive(Default)]
pub struct InMemoryFileStateCache {
    inner: Mutex<Inner>,
    /// Shared, NOT cloned, by [`FileStateCache::clone_box`]. The read-guard state
    /// in `inner` has to be per-agent (a sub-agent's read must not satisfy the
    /// parent's read-before-write guard), but a turn baseline is the opposite: when
    /// a parent and its sub-agent both edit one file in one turn, that is one
    /// turn's effect on that file. Give each its own baseline and whichever
    /// reported last measures only its own share and overwrites the other's.
    turn_baselines: Arc<Mutex<TurnBaselines>>,
}

impl InMemoryFileStateCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// A typed child starts with no inherited read guards while contributing to
    /// the same parent's turn snapshots. Only the parent opens a new turn.
    pub fn child_with_fresh_read_state(&self) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            turn_baselines: Arc::clone(&self.turn_baselines),
        }
    }
}

impl FileStateCache for InMemoryFileStateCache {
    fn get(&self, path: &RuntimePath) -> Option<FileStateEntry> {
        let inner = self.inner.lock().unwrap();
        inner.position(path).map(|i| inner.entries[i].1.clone())
    }

    fn record(&self, path: &RuntimePath, mtime_ms: f64, opts: RecordOptions) {
        let mut inner = self.inner.lock().unwrap();
        inner.drop_content_bytes(path);
        // Remove any existing slot and re-insert at the back so it counts as
        // most-recent.
        if let Some(i) = inner.position(path) {
            inner.entries.remove(i);
        }
        let mut entry = FileStateEntry {
            mtime_ms,
            size: None,
            content: None,
            encoding: None,
        };
        if let Some(size) = opts.size {
            entry.size = Some(size);
        }
        if let Some(encoding) = opts.encoding {
            entry.encoding = Some(encoding);
        }
        if let Some(content) = opts.content {
            let content_bytes = content.to_node_utf8().len();
            if content_bytes <= MAX_TRACKED_CONTENT_BYTES {
                inner.total_content += content_bytes;
                entry.content = Some(content);
            }
        }
        inner.entries.push((path.clone(), entry));
        inner.evict();
    }

    fn has(&self, path: &RuntimePath) -> bool {
        self.inner.lock().unwrap().position(path).is_some()
    }

    fn entries(&self) -> Vec<(RuntimePath, FileStateEntry)> {
        self.inner.lock().unwrap().entries.clone()
    }

    fn delete(&self, path: &RuntimePath) {
        self.inner.lock().unwrap().delete(path);
    }

    fn clear(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.entries.clear();
        inner.total_content = 0;
    }

    fn turn_baseline(
        &self,
        path: &RuntimePath,
        content: Option<&RuntimeString>,
    ) -> Option<RuntimeString> {
        let mut baselines = self.turn_baselines.lock().unwrap();
        if let Some(existing) = baselines.first_seen.get(path) {
            return existing.clone();
        }
        let holdable = content.and_then(|content| {
            let bytes = content.to_node_utf8().len();
            let fits = bytes <= MAX_TURN_BASELINE_BYTES
                && baselines.bytes + bytes <= MAX_TURN_BASELINE_TOTAL_BYTES;
            fits.then(|| {
                baselines.bytes += bytes;
                content.clone()
            })
        });
        baselines.first_seen.insert(path.clone(), holdable.clone());
        holdable
    }

    fn turn_existed(&self, path: &RuntimePath, observed_before: bool) -> bool {
        *self
            .turn_baselines
            .lock()
            .unwrap()
            .existed
            .entry(path.clone())
            .or_insert(observed_before)
    }

    fn begin_turn(&self) {
        let mut baselines = self.turn_baselines.lock().unwrap();
        baselines.first_seen.clear();
        baselines.existed.clear();
        baselines.bytes = 0;
    }

    fn clone_box(&self) -> Box<dyn FileStateCache> {
        let inner = self.inner.lock().unwrap();
        Box::new(InMemoryFileStateCache {
            inner: Mutex::new(Inner {
                entries: inner.entries.clone(),
                total_content: inner.total_content,
            }),
            turn_baselines: Arc::clone(&self.turn_baselines),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::RuntimeString;

    fn path(value: &str) -> RuntimePath {
        RuntimePath::from(value)
    }

    fn opts(size: Option<u64>, content: Option<&str>) -> RecordOptions {
        RecordOptions {
            size,
            content: content.map(RuntimeString::from),
            encoding: None,
        }
    }

    fn entry(mtime_ms: f64, size: Option<u64>, content: Option<&str>) -> FileStateEntry {
        FileStateEntry {
            mtime_ms,
            size,
            content: content.map(RuntimeString::from),
            encoding: None,
        }
    }

    #[test]
    fn records_and_reads_back_mtime_size_and_content() {
        let c = InMemoryFileStateCache::new();
        c.record(&path("/a"), 100.0, opts(Some(3), Some("abc")));
        assert_eq!(c.get(&path("/a")), Some(entry(100.0, Some(3), Some("abc"))));
        assert!(c.has(&path("/a")));
    }

    #[test]
    fn a_turn_keeps_the_baseline_it_first_saw() {
        let c = InMemoryFileStateCache::new();
        let first = c.turn_baseline(&path("/a"), Some(&RuntimeString::from("original")));
        assert_eq!(first, Some(RuntimeString::from("original")));
        // A later edit in the same turn measures against the ORIGINAL, not against
        // what the previous edit left behind.
        let second = c.turn_baseline(&path("/a"), Some(&RuntimeString::from("after-edit-1")));
        assert_eq!(second, Some(RuntimeString::from("original")));
    }

    #[test]
    fn begin_turn_starts_the_measurement_over() {
        let c = InMemoryFileStateCache::new();
        c.turn_baseline(&path("/a"), Some(&RuntimeString::from("turn-1-original")));
        c.begin_turn();
        assert_eq!(
            c.turn_baseline(&path("/a"), Some(&RuntimeString::from("turn-2-original"))),
            Some(RuntimeString::from("turn-2-original")),
            "the next turn measures from what IT finds, not the previous turn's baseline"
        );
    }

    /// A sub-agent shares the turn baseline even though its read-guard state is its
    /// own: a parent and a child editing one file in one turn are one turn's effect
    /// on that file, and the host keeps the newest report — so if the child
    /// measured against its own baseline it would silently drop the parent's share.
    #[test]
    fn a_clone_shares_the_turn_baseline_while_keeping_its_own_read_state() {
        let parent = InMemoryFileStateCache::new();
        let original = RuntimeString::from("original");
        assert_eq!(
            parent.turn_baseline(&path("/a"), Some(&original)),
            Some(original.clone())
        );
        let child = parent.clone_box();
        assert_eq!(
            child.turn_baseline(&path("/a"), Some(&RuntimeString::from("after-parent-edit"))),
            Some(original),
            "the child measures from where the TURN started"
        );
        child.begin_turn();
        assert_eq!(
            parent.turn_baseline(&path("/a"), Some(&RuntimeString::from("fresh"))),
            Some(RuntimeString::from("fresh")),
            "one turn boundary, shared"
        );
    }

    #[test]
    fn typed_child_shares_content_and_existence_without_inheriting_reads() {
        let parent = InMemoryFileStateCache::new();
        parent.record(&path("/a"), 1.0, opts(None, Some("original")));
        parent.turn_baseline(&path("/a"), Some(&RuntimeString::from("original")));
        assert!(parent.turn_existed(&path("/a"), true));
        let child = parent.child_with_fresh_read_state();
        assert!(!child.has(&path("/a")));
        assert_eq!(
            child.turn_baseline(&path("/a"), Some(&RuntimeString::from("parent edit"))),
            Some(RuntimeString::from("original"))
        );
        assert!(child.turn_existed(&path("/a"), false));
        parent.begin_turn();
        assert!(!child.turn_existed(&path("/a"), false));
        assert_eq!(
            child.turn_baseline(&path("/a"), Some(&RuntimeString::default())),
            Some(RuntimeString::default())
        );
        assert!(parent.has(&path("/a")));
    }

    #[test]
    fn clone_is_independent_of_the_parent() {
        let parent = InMemoryFileStateCache::new();
        parent.record(&path("/a"), 1.0, opts(None, Some("A")));
        let child = parent.clone_box();
        assert_eq!(child.get(&path("/a")), Some(entry(1.0, None, Some("A"))));
        child.record(&path("/a"), 2.0, opts(None, Some("A2")));
        child.record(&path("/b"), 3.0, opts(None, Some("B")));
        // parent unchanged
        assert_eq!(parent.get(&path("/a")), Some(entry(1.0, None, Some("A"))));
        assert!(!parent.has(&path("/b")));
        // child has its own state
        assert_eq!(child.get(&path("/a")), Some(entry(2.0, None, Some("A2"))));
    }

    #[test]
    fn drops_content_over_the_per_entry_cap() {
        let c = InMemoryFileStateCache::new();
        let huge = "x".repeat(512 * 1024 + 1);
        c.record(
            &path("/big"),
            1.0,
            opts(Some(huge.len() as u64), Some(&huge)),
        );
        let e = c.get(&path("/big")).unwrap();
        assert_eq!(e.mtime_ms, 1.0);
        assert!(e.content.is_none()); // too large to retain
    }

    #[test]
    fn evicts_the_least_recently_recorded_entry_past_the_entry_cap() {
        let c = InMemoryFileStateCache::new();
        // Record more than MAX_ENTRIES (256) distinct paths.
        for i in 0..300 {
            c.record(
                &path(&format!("/f{i}")),
                i as f64,
                opts(None, Some(&format!("c{i}"))),
            );
        }
        assert!(c.entries().len() <= 256);
        // The oldest (f0) is gone; the newest (f299) is retained.
        assert!(!c.has(&path("/f0")));
        assert!(c.has(&path("/f299")));
    }

    #[test]
    fn re_recording_a_path_refreshes_its_recency() {
        let c = InMemoryFileStateCache::new();
        for i in 0..256 {
            c.record(
                &path(&format!("/f{i}")),
                i as f64,
                opts(None, Some(&format!("c{i}"))),
            );
        }
        c.record(&path("/f0"), 1000.0, opts(None, Some("refreshed"))); // touch f0 → most recent
        c.record(&path("/f256"), 256.0, opts(None, Some("new"))); // evicts the now-oldest (f1), not f0
        assert!(c.has(&path("/f0")));
        assert!(!c.has(&path("/f1")));
    }

    #[test]
    fn delete_removes_an_entry() {
        let c = InMemoryFileStateCache::new();
        c.record(&path("/a"), 1.0, opts(None, Some("A")));
        c.delete(&path("/a"));
        assert!(!c.has(&path("/a")));
    }
}
