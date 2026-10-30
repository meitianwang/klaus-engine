//! Incrementally validated session metadata catalog. Rollouts remain canonical;
//! the durable cache contains only first-line metadata and filesystem identity.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsStr;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use crate::fs::node_path::runtime_string_from_native;
use crate::runtime_values::{
    runtime_codec, runtime_json, NodeBufferUtf8, RuntimeJson, RuntimeString,
};
use crate::schemas::rollout::{RolloutItem, SessionMeta};
use crate::store::rollout::parse_rollout_line;
use crate::store::session_store::ThreadSummary;

/// One indexed header entry: the parsed session meta plus its on-disk location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderEntry {
    pub meta: SessionMeta,
    pub path: String,
    /// Last time the rollout content changed, in epoch milliseconds.
    pub updated_at: i64,
    /// Whether the file lives under the archived subdir.
    pub archived: bool,
}

/// Options for [`HeaderIndex::scan`].
#[derive(Debug, Clone, Copy, Default)]
pub struct ScanOptions {
    /// Whether the scanned root is the archived subdir.
    pub archived: bool,
    /// Re-scan a root that was already scanned (e.g. after external writes).
    pub force: bool,
}

const CACHE_FILE: &str = ".session-header-index-v1.json";
const CACHE_VERSION: u32 = 1;
const HEADER_LIMIT: usize = 64 * 1024;

/// Non-sensitive I/O counters for catalog diagnostics and public regressions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CatalogIoStats {
    pub directory_scans: u64,
    pub metadata_reads: u64,
    pub header_reads: u64,
    pub header_bytes: u64,
    pub cache_reads: u64,
    pub cache_writes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FileStamp {
    length: u64,
    modified_nanos: Option<u128>,
    created_nanos: Option<u128>,
    identity: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CachedFile {
    stamp: FileStamp,
    /// Original encoded header preserves exact runtime strings and host metadata.
    line: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct DiskCache {
    version: u32,
    files: BTreeMap<String, CachedFile>,
}

/// A derived cache, never a second source of truth. Complete fresh scans stat
/// each rollout but only open changed headers. Continuation pages are owned by
/// the store's immutable catalog snapshots and never enter this scanner.
#[derive(Debug, Clone, Default)]
pub struct HeaderIndex {
    /// thread id → entry.
    by_id: HashMap<RuntimeString, HeaderEntry>,
    /// Thread ids in first-insertion order. A re-`set` of a known id keeps its
    /// original slot, so iteration behaves like an insertion-ordered map and
    /// `entries()` breaks `updated_at` ties deterministically.
    order: Vec<RuntimeString>,
    /// Roots already scanned at least once, to avoid redundant directory walks.
    scanned_roots: HashSet<PathBuf>,
    /// Last complete id/path snapshot contributed by each scanned root. Forced
    /// scans use this provenance to remove files that were moved or deleted by
    /// another store/process without deleting a newer entry contributed by the
    /// other namespace.
    scanned_entries_by_root: HashMap<PathBuf, HashMap<RuntimeString, String>>,
    files_by_root: HashMap<PathBuf, BTreeMap<String, CachedFile>>,
    loaded_roots: HashSet<PathBuf>,
    dirty_roots: HashSet<PathBuf>,
    stats: CatalogIoStats,
}

impl HeaderIndex {
    pub fn new() -> Self {
        HeaderIndex::default()
    }

    pub fn io_stats(&self) -> CatalogIoStats {
        self.stats
    }

    /// Complete canonical header metadata, independent of filesystem recency
    /// and transcript size/content. IDs and nested object keys sort by UTF-16.
    pub fn metadata_fingerprint(&self, summaries: &[ThreadSummary]) -> RuntimeString {
        let mut metadata: Vec<_> = summaries
            .iter()
            .map(|row| {
                self.by_id
                    .get(&row.id)
                    .expect("summary belongs to current catalog")
                    .meta
                    .clone()
            })
            .collect();
        metadata.sort_by(|left, right| left.id.cmp(&right.id));
        let exact =
            runtime_codec::encode_value(&metadata).expect("session metadata encodes exactly");
        let mut digest = Sha256::new();
        hash_canonical(&exact, &mut digest);
        RuntimeString::from(format!("{:x}", digest.finalize()))
    }

    /// Record/refresh a single header entry (e.g. on create or archive).
    pub fn set(&mut self, entry: HeaderEntry) {
        let id = entry.meta.id.clone();
        if self.by_id.insert(id.clone(), entry).is_none() {
            self.order.push(id);
        }
    }

    /// Look up a known entry by thread id without touching disk.
    pub fn get(&self, id: &str) -> Option<&HeaderEntry> {
        self.by_id.get(&RuntimeString::from(id))
    }

    /// Drop a thread from the index (e.g. after its file moved and was re-keyed).
    pub fn delete(&mut self, id: &str) {
        if self.by_id.remove(&RuntimeString::from(id)).is_some() {
            self.order.retain(|known| known != id);
        }
    }

    /// All currently-known entries, most recently updated first.
    pub fn entries(&self) -> Vec<&HeaderEntry> {
        let mut all: Vec<&HeaderEntry> = self
            .order
            .iter()
            .filter_map(|id| self.by_id.get(id))
            .collect();
        // The cursor carries `(updated_at, id)` and resumes with the same
        // newest-first comparison. Keep the list order identical so equal-time
        // pages cannot skip or repeat entries.
        all.sort_by(|a, b| {
            b.updated_at
                .cmp(&a.updated_at)
                .then_with(|| b.meta.id.cmp(&a.meta.id))
        });
        all
    }

    /// Scan a directory's rollout files and fold their headers into the index.
    /// Safe to call repeatedly; pass `force` to re-scan a root that was already
    /// scanned (e.g. after external writes).
    pub async fn scan(&mut self, dir: &Path, opts: ScanOptions) -> io::Result<()> {
        self.scan_roots(&[(dir, opts)]).await
    }

    /// Publish all namespaces together. A failed archive scan must not publish
    /// an already-refreshed active namespace or persist its partial snapshot.
    pub async fn scan_roots(&mut self, roots: &[(&Path, ScanOptions)]) -> io::Result<()> {
        self.refresh_roots(roots, true).await
    }

    /// Display/summary lookup may share validated metadata without creating
    /// any sidecar. Only an explicit catalog listing persists derived caches.
    pub async fn scan_roots_read_only(&mut self, roots: &[(&Path, ScanOptions)]) -> io::Result<()> {
        self.refresh_roots(roots, false).await
    }

    async fn refresh_roots(
        &mut self,
        roots: &[(&Path, ScanOptions)],
        persist: bool,
    ) -> io::Result<()> {
        let mut staged = self.clone();
        let mut changed = Vec::new();
        for (dir, opts) in roots {
            match staged.scan_root(dir, *opts).await {
                Ok(true) => changed.push(*dir),
                Ok(false) => {}
                Err(error) => {
                    self.stats = staged.stats;
                    return Err(error);
                }
            }
        }
        for dir in changed {
            if !persist {
                staged.dirty_roots.insert(dir.to_path_buf());
                continue;
            }
            // The catalog is derived data: read-only media or a cache-write
            // failure must not hide otherwise readable canonical sessions.
            if let Err(error) = staged.persist_root(dir).await {
                staged.dirty_roots.insert(dir.to_path_buf());
                tracing::warn!(error = %error, "session catalog cache persistence failed");
            }
        }
        *self = staged;
        Ok(())
    }

    async fn load_root(&mut self, dir: &Path) {
        if !self.loaded_roots.insert(dir.to_path_buf()) {
            return;
        }
        self.stats.cache_reads += 1;
        let files = match tokio::fs::read(dir.join(CACHE_FILE)).await {
            Ok(bytes) => serde_json::from_slice::<DiskCache>(&bytes)
                .ok()
                .filter(|cache| cache.version == CACHE_VERSION)
                .map(|cache| {
                    cache
                        .files
                        .into_iter()
                        .filter(|(name, _)| {
                            crate::store::compression::is_rollout_name(name)
                                && Path::new(name).components().count() == 1
                                && matches!(
                                    Path::new(name).components().next(),
                                    Some(std::path::Component::Normal(_))
                                )
                        })
                        .collect()
                })
                .unwrap_or_default(),
            Err(_) => BTreeMap::new(),
        };
        self.files_by_root.insert(dir.to_path_buf(), files);
    }

    async fn scan_root(&mut self, dir: &Path, opts: ScanOptions) -> io::Result<bool> {
        if self.scanned_roots.contains(dir) && !opts.force {
            return Ok(false);
        }
        self.load_root(dir).await;
        self.stats.directory_scans += 1;
        let mut names: Vec<String> = Vec::new();
        match tokio::fs::read_dir(dir).await {
            Ok(mut entries) => {
                while let Some(entry) = entries.next_entry().await? {
                    names.push(native_text_to_node_host(&entry.file_name()));
                }
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                self.replace_scanned_root(dir.to_path_buf(), Vec::new());
                self.files_by_root
                    .insert(dir.to_path_buf(), BTreeMap::new());
                self.scanned_roots.insert(dir.to_path_buf());
                return Ok(false);
            }
            Err(err) => return Err(err),
        }

        names.sort();
        let previous = self.files_by_root.get(dir).cloned().unwrap_or_default();
        let mut files = BTreeMap::new();
        let mut discovered = Vec::new();
        let plain_names = names.iter().cloned().collect::<HashSet<_>>();
        for name in names {
            if name.ends_with(".jsonl.zst")
                && plain_names.contains(name.strip_suffix(".zst").unwrap())
            {
                continue;
            }
            if !crate::store::compression::is_rollout_name(&name) {
                continue;
            }
            let path = dir.join(&name);
            let Some(file) = self.refresh_file(&path, previous.get(&name)).await? else {
                continue;
            };
            if let Some(entry) = cached_entry(&path, &file, opts.archived) {
                discovered.push(entry);
            }
            files.insert(name, file);
        }

        // Commit only after the complete directory scan succeeds. A failed
        // refresh leaves the last coherent cache intact.
        self.replace_scanned_root(dir.to_path_buf(), discovered);
        self.scanned_roots.insert(dir.to_path_buf());
        let changed = files != previous || self.dirty_roots.contains(dir);
        self.files_by_root.insert(dir.to_path_buf(), files);
        Ok(changed)
    }

    async fn stamp(&mut self, path: &Path) -> io::Result<Option<FileStamp>> {
        self.stats.metadata_reads += 1;
        match file_stamp(path).await {
            Ok(stamp) => Ok(Some(stamp)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn refresh_file(
        &mut self,
        path: &Path,
        cached: Option<&CachedFile>,
    ) -> io::Result<Option<CachedFile>> {
        let Some(mut stamp) = self.stamp(path).await? else {
            return Ok(None);
        };
        if let Some(cached) = cached.filter(|cached| cached.stamp == stamp) {
            return Ok(Some(cached.clone()));
        }
        for _ in 0..3 {
            self.stats.header_reads += 1;
            let (line, bytes) = read_first_line(path).await?;
            self.stats.header_bytes += bytes as u64;
            let Some(after) = self.stamp(path).await? else {
                return Ok(None);
            };
            if stamp == after {
                return Ok(Some(CachedFile { stamp, line }));
            }
            stamp = after;
        }
        Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "session metadata changed repeatedly while cataloging; retry refresh",
        ))
    }

    /// Validate only a known target. A miss or move is reconciled by the caller
    /// across both namespaces before it reports a missing session.
    pub async fn read_known(
        &mut self,
        id: &str,
        roots: &[(&Path, bool)],
    ) -> io::Result<Option<HeaderEntry>> {
        let mut candidate = self.get(id).cloned();
        if candidate.is_none() {
            for (root, archived) in roots {
                self.load_root(root).await;
                if let Some(files) = self.files_by_root.get(*root) {
                    candidate = files.iter().find_map(|(name, file)| {
                        cached_entry(&root.join(name), file, *archived)
                            .filter(|entry| entry.meta.id == id)
                    });
                }
                if candidate.is_some() {
                    break;
                }
            }
        }
        let Some(candidate) = candidate else {
            return Ok(None);
        };
        let resolved = match crate::store::compression::resolve(Path::new(&candidate.path)) {
            Ok(path) => path,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let path = resolved.as_path();
        let root = path.parent().expect("indexed rollout has a parent");
        let name = native_text_to_node_host(path.file_name().expect("indexed rollout has a name"));
        let cached = self
            .files_by_root
            .get(root)
            .and_then(|files| files.get(&name))
            .cloned();
        let Some(file) = self.refresh_file(path, cached.as_ref()).await? else {
            return Ok(None);
        };
        let Some(entry) =
            cached_entry(path, &file, candidate.archived).filter(|entry| entry.meta.id == id)
        else {
            return Ok(None);
        };
        // Only a complete root refresh replaces its durable snapshot. Target
        // refreshes update the in-memory entry without persisting a partial scan.
        self.set(entry.clone());
        if cached.as_ref() != Some(&file) {
            self.dirty_roots.insert(root.to_path_buf());
        }
        self.files_by_root
            .entry(root.to_path_buf())
            .or_default()
            .insert(name, file);
        Ok(Some(entry))
    }

    async fn persist_root(&mut self, root: &Path) -> io::Result<()> {
        let cache = DiskCache {
            version: CACHE_VERSION,
            files: self.files_by_root.get(root).cloned().unwrap_or_default(),
        };
        let bytes = serde_json::to_vec(&cache).map_err(io::Error::other)?;
        let temporary = root.join(format!(
            ".session-header-index-{}.tmp",
            uuid::Uuid::new_v4()
        ));
        let result = async {
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
                .await?;
            file.write_all(&bytes).await?;
            file.sync_all().await?;
            drop(file);
            tokio::fs::rename(&temporary, root.join(CACHE_FILE)).await?;
            #[cfg(unix)]
            tokio::fs::File::open(root).await?.sync_all().await?;
            Ok::<_, io::Error>(())
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&temporary).await;
        } else {
            self.stats.cache_writes += 1;
            self.dirty_roots.remove(root);
        }
        result
    }

    fn replace_scanned_root(&mut self, root: PathBuf, entries: Vec<HeaderEntry>) {
        let current = entries
            .iter()
            .map(|entry| (entry.meta.id.clone(), entry.path.clone()))
            .collect::<HashMap<_, _>>();
        // Also cover entries inserted by create/update before this root's first
        // scan. A subsequent external delete must not leave such rows orphaned.
        let orphaned: HashSet<_> = self
            .by_id
            .iter()
            .filter(|(id, entry)| {
                Path::new(&entry.path).parent() == Some(root.as_path())
                    && current.get(*id) != Some(&entry.path)
            })
            .map(|(id, _)| id.clone())
            .collect();
        self.order.retain(|known| !orphaned.contains(known));
        for id in orphaned {
            self.by_id.remove(&id);
        }
        let previous = self
            .scanned_entries_by_root
            .insert(root, current.clone())
            .unwrap_or_default();

        for (id, previous_path) in previous {
            if current.get(&id) == Some(&previous_path) {
                continue;
            }
            let still_points_to_previous_path = self
                .by_id
                .get(&id)
                .is_some_and(|entry| entry.path == previous_path);
            if still_points_to_previous_path {
                self.by_id.remove(&id);
                self.order.retain(|known| known != &id);
            }
        }

        for entry in entries {
            self.set(entry);
        }
    }
}

fn hash_canonical(value: &RuntimeJson, digest: &mut Sha256) {
    match value {
        RuntimeJson::Array(values) => {
            digest.update(b"[");
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    digest.update(b",");
                }
                hash_canonical(value, digest);
            }
            digest.update(b"]");
        }
        RuntimeJson::Object(object) => {
            let mut entries = object.entries().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            digest.update(b"{");
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index != 0 {
                    digest.update(b",");
                }
                digest.update(runtime_json::stringify(&RuntimeJson::String(key)).to_node_utf8());
                digest.update(b":");
                hash_canonical(value, digest);
            }
            digest.update(b"}");
        }
        _ => digest.update(runtime_json::stringify(value).to_node_utf8()),
    }
}

/// Convert a header entry to the public list summary shape.
pub fn entry_to_summary(entry: &HeaderEntry) -> ThreadSummary {
    ThreadSummary {
        id: entry.meta.id.clone(),
        created_at: entry.meta.created_at,
        updated_at: entry.updated_at,
        cwd: entry.meta.cwd.clone(),
        model: entry.meta.model.clone(),
        title: entry.meta.title.clone(),
        role: entry
            .meta
            .role
            .unwrap_or(crate::schemas::rollout::SessionRole::Main),
        parent_thread_id: entry.meta.parent_thread_id.clone(),
        forked_from_id: entry.meta.forked_from_id.clone(),
        archived: Some(entry.archived),
        host_metadata: entry.meta.host_metadata.clone(),
    }
}

pub(crate) async fn file_modified_at(path: &Path) -> io::Result<i64> {
    let modified = tokio::fs::metadata(path).await?.modified()?;
    let millis = modified
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    Ok(i64::try_from(millis).unwrap_or(i64::MAX))
}

fn cached_entry(path: &Path, cached: &CachedFile, archived: bool) -> Option<HeaderEntry> {
    let RolloutItem::SessionMeta { meta, .. } = parse_rollout_line(cached.line.as_deref()?)? else {
        return None;
    };
    let updated_at = cached
        .stamp
        .modified_nanos
        .map(|nanos| i64::try_from(nanos / 1_000_000).unwrap_or(i64::MAX))
        .unwrap_or(meta.created_at);
    Some(HeaderEntry {
        meta,
        path: native_text_to_node_host(path.as_os_str()),
        updated_at,
        archived,
    })
}

async fn file_stamp(path: &Path) -> io::Result<FileStamp> {
    let metadata = tokio::fs::metadata(path).await?;
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "rollout path is not a regular file",
        ));
    }
    let timestamp = |time: io::Result<std::time::SystemTime>| {
        time.ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|time| time.as_nanos())
    };
    #[cfg(unix)]
    let identity = {
        use std::os::unix::fs::MetadataExt;
        vec![
            metadata.dev(),
            metadata.ino(),
            metadata.ctime() as u64,
            metadata.ctime_nsec() as u64,
        ]
    };
    #[cfg(windows)]
    let identity = {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            FileBasicInfo, GetFileInformationByHandle, GetFileInformationByHandleEx,
            BY_HANDLE_FILE_INFORMATION, FILE_BASIC_INFO,
        };
        let handle = tokio::fs::File::open(path).await?.into_std().await;
        let mut info = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
        // SAFETY: the handle remains live and the API initializes the output on success.
        let ok =
            unsafe { GetFileInformationByHandle(handle.as_raw_handle().cast(), info.as_mut_ptr()) };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: success above initialized every field.
        let info = unsafe { info.assume_init() };
        let mut basic = std::mem::MaybeUninit::<FILE_BASIC_INFO>::uninit();
        // SAFETY: the live handle and output buffer match FileBasicInfo's ABI.
        let ok = unsafe {
            GetFileInformationByHandleEx(
                handle.as_raw_handle().cast(),
                FileBasicInfo,
                basic.as_mut_ptr().cast(),
                std::mem::size_of::<FILE_BASIC_INFO>() as u32,
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: success above initialized every field.
        let basic = unsafe { basic.assume_init() };
        vec![
            u64::from(info.dwVolumeSerialNumber),
            u64::from(info.nFileIndexHigh),
            u64::from(info.nFileIndexLow),
            basic.ChangeTime as u64,
        ]
    };
    #[cfg(not(any(unix, windows)))]
    let identity = Vec::new();
    Ok(FileStamp {
        length: metadata.len(),
        modified_nanos: timestamp(metadata.modified()),
        created_nanos: timestamp(metadata.created()),
        identity,
    })
}

/// Stop requesting bytes as soon as the first newline has arrived. A small
/// chunk bounds read-ahead; the remainder of a large transcript is never read.
async fn read_first_line(file: &Path) -> io::Result<(Option<String>, usize)> {
    let data = match crate::store::compression::read_prefix(file, HEADER_LIMIT).await {
        Ok(data) => data,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok((None, 0)),
        Err(error) => return Err(error),
    };
    let bytes_read = data.len();
    let buf = data
        .split(|byte| *byte == b'\n')
        .next()
        .unwrap_or_default()
        .to_vec();
    if buf.is_empty() {
        return Ok((None, bytes_read));
    }
    let first = NodeBufferUtf8::decode(&buf)
        .try_to_utf8()
        .expect("Node UTF-8 decode yields Unicode scalar text");
    Ok((Some(first), bytes_read))
}

fn native_text_to_node_host(value: &OsStr) -> String {
    String::from_utf8(runtime_string_from_native(value).to_node_utf8())
        .expect("Node UTF-8 projection yields valid UTF-8")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::rollout::ROLLOUT_SCHEMA_VERSION;
    use crate::store::rollout::serialize_rollout_item;

    fn temp_dir() -> PathBuf {
        // A per-process counter keeps parallel tests from colliding even when
        // they start within the same clock tick.
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-headerindex-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        );
        dir.push(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn meta(id: &str, created_at: i64) -> SessionMeta {
        SessionMeta {
            id: id.into(),
            created_at,
            cwd: "/repo".into(),
            model: "m".into(),
            title: None,
            originator: None,
            cli_version: None,
            source: None,
            role: None,
            parent_thread_id: None,
            forked_from_id: None,
            git: None,
            host_metadata: None,
        }
    }

    fn entry(id: &str, created_at: i64, path: &str) -> HeaderEntry {
        HeaderEntry {
            meta: meta(id, created_at),
            path: path.into(),
            updated_at: created_at,
            archived: false,
        }
    }

    fn header_line(m: &SessionMeta) -> String {
        serialize_rollout_item(&RolloutItem::SessionMeta {
            schema_version: ROLLOUT_SCHEMA_VERSION,
            meta: m.clone(),
        })
    }

    fn ids(entries: &[&HeaderEntry]) -> Vec<RuntimeString> {
        entries.iter().map(|e| e.meta.id.clone()).collect()
    }

    // set / get / delete

    #[test]
    fn set_get_delete_round_trip() {
        let mut index = HeaderIndex::new();
        index.set(entry("a", 1, "/s/1-a.jsonl"));
        assert_eq!(index.get("a").unwrap().path, "/s/1-a.jsonl");
        assert!(index.get("missing").is_none());
        index.delete("a");
        assert!(index.get("a").is_none());
        assert!(index.entries().is_empty());
    }

    #[test]
    fn set_refreshes_an_existing_id_in_place() {
        let mut index = HeaderIndex::new();
        index.set(entry("a", 1, "/s/1-a.jsonl"));
        // Re-key after a move (e.g. archive): the same id points at the new path.
        index.set(HeaderEntry {
            meta: meta("a", 1),
            path: "/s/archived/1-a.jsonl".into(),
            updated_at: 1,
            archived: true,
        });
        assert_eq!(index.entries().len(), 1);
        let refreshed = index.get("a").unwrap();
        assert_eq!(refreshed.path, "/s/archived/1-a.jsonl");
        assert!(refreshed.archived);
    }

    #[test]
    fn entries_are_sorted_by_update_time_with_descending_id_ties() {
        let mut index = HeaderIndex::new();
        let mut old = entry("old", 1, "p1");
        old.updated_at = 30;
        let mut new = entry("new", 3, "p3");
        new.updated_at = 10;
        let mut mid = entry("mid", 2, "p2");
        mid.updated_at = 20;
        index.set(old);
        index.set(new);
        index.set(mid);
        assert_eq!(ids(&index.entries()), vec!["old", "mid", "new"]);

        // Equal updatedAt uses the same descending id tie-break as cursors.
        let mut tie = entry("tie-b", 3, "p4");
        tie.updated_at = 30;
        index.set(tie);
        assert_eq!(ids(&index.entries()), vec!["tie-b", "old", "mid", "new"]);
    }

    // entry_to_summary

    #[test]
    fn entry_to_summary_maps_the_header_fields() {
        let mut with_title = entry("a", 9, "/s/9-a.jsonl");
        with_title.meta.title = Some("t".into());
        with_title.meta.forked_from_id = Some("source-a".into());
        with_title.archived = true;
        assert_eq!(with_title.path, "/s/9-a.jsonl");
        let summary = entry_to_summary(&with_title);
        assert_eq!(summary.id, "a");
        assert_eq!(summary.created_at, 9);
        assert_eq!(summary.updated_at, 9);
        assert_eq!(summary.cwd, "/repo");
        assert_eq!(summary.model, "m");
        assert_eq!(summary.title, Some(RuntimeString::from("t")));
        assert_eq!(
            summary.forked_from_id,
            Some(RuntimeString::from("source-a"))
        );
        assert_eq!(summary.archived, Some(true));

        // An untitled session has no title in its summary.
        let untitled = entry_to_summary(&entry("b", 1, "p"));
        assert!(untitled.title.is_none());
        assert_eq!(untitled.archived, Some(false));
    }

    // scan

    #[tokio::test]
    async fn scan_indexes_jsonl_headers_and_skips_invalid_files() {
        let dir = temp_dir();
        // A valid header followed by trailing garbage: only line one matters.
        std::fs::write(
            dir.join("10-a.jsonl"),
            format!("{}\nnot json at all\n", header_line(&meta("a", 10))),
        )
        .unwrap();
        // Wrong extension → ignored even though the content is a valid header.
        std::fs::write(
            dir.join("notes.txt"),
            format!("{}\n", header_line(&meta("x", 1))),
        )
        .unwrap();
        // Corrupt first line → skipped.
        std::fs::write(dir.join("20-b.jsonl"), "{corrupt\n").unwrap();
        // First item is not a session_meta header → skipped.
        std::fs::write(
            dir.join("30-c.jsonl"),
            "{\"kind\":\"event\",\"event\":\"turn_started\"}\n",
        )
        .unwrap();
        // Empty file (mid-write) → skipped.
        std::fs::write(dir.join("40-d.jsonl"), "").unwrap();

        let mut index = HeaderIndex::new();
        index.scan(&dir, ScanOptions::default()).await.unwrap();

        assert_eq!(ids(&index.entries()), vec!["a"]);
        let found = index.get("a").unwrap();
        assert_eq!(
            found.path,
            native_text_to_node_host(dir.join("10-a.jsonl").as_os_str())
        );
        assert!(!found.archived);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn scan_records_the_archived_flag() {
        let dir = temp_dir();
        std::fs::write(
            dir.join("5-z.jsonl"),
            format!("{}\n", header_line(&meta("z", 5))),
        )
        .unwrap();

        let mut index = HeaderIndex::new();
        index
            .scan(
                &dir,
                ScanOptions {
                    archived: true,
                    force: false,
                },
            )
            .await
            .unwrap();
        assert!(index.get("z").unwrap().archived);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn scan_tolerates_a_missing_directory_and_marks_it_scanned() {
        let dir = temp_dir();
        let missing = dir.join("does-not-exist");

        let mut index = HeaderIndex::new();
        index.scan(&missing, ScanOptions::default()).await.unwrap();
        assert!(index.entries().is_empty());

        // The root is remembered: creating it later is invisible without `force`.
        std::fs::create_dir_all(&missing).unwrap();
        std::fs::write(
            missing.join("1-a.jsonl"),
            format!("{}\n", header_line(&meta("a", 1))),
        )
        .unwrap();
        index.scan(&missing, ScanOptions::default()).await.unwrap();
        assert!(index.entries().is_empty());

        index
            .scan(
                &missing,
                ScanOptions {
                    archived: false,
                    force: true,
                },
            )
            .await
            .unwrap();
        assert_eq!(ids(&index.entries()), vec!["a"]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn rescan_without_force_is_a_no_op_and_force_rescans() {
        let dir = temp_dir();
        std::fs::write(
            dir.join("1-a.jsonl"),
            format!("{}\n", header_line(&meta("a", 1))),
        )
        .unwrap();

        let mut index = HeaderIndex::new();
        index.scan(&dir, ScanOptions::default()).await.unwrap();
        assert_eq!(ids(&index.entries()), vec!["a"]);

        // A file written after the first scan is not seen until a forced re-scan.
        std::fs::write(
            dir.join("2-b.jsonl"),
            format!("{}\n", header_line(&meta("b", 2))),
        )
        .unwrap();
        index.scan(&dir, ScanOptions::default()).await.unwrap();
        assert_eq!(ids(&index.entries()), vec!["a"]);

        index
            .scan(
                &dir,
                ScanOptions {
                    archived: false,
                    force: true,
                },
            )
            .await
            .unwrap();
        assert_eq!(ids(&index.entries()), vec!["b", "a"]);

        // A forced scan is authoritative for files previously contributed by
        // this root, so external moves/deletes cannot remain cached.
        std::fs::remove_file(dir.join("1-a.jsonl")).unwrap();
        index
            .scan(
                &dir,
                ScanOptions {
                    archived: false,
                    force: true,
                },
            )
            .await
            .unwrap();
        assert_eq!(ids(&index.entries()), vec!["b"]);
        std::fs::remove_dir_all(&dir).ok();
    }
}
