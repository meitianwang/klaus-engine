//! Lossless cold-rollout storage and representation-transparent readers.

use crate::runtime_values::{
    RuntimeCodecError, RuntimeEncode, RuntimeEncoder, RuntimeObjectEncoder, RuntimePath,
    RuntimeString,
};
use fs2::FileExt;
use sha2::{Digest, Sha256};
use std::cell::Cell;
use std::fs::{File, Metadata, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};
thread_local! { static DEADLINE: Cell<Option<Instant>> = const { Cell::new(None) }; }
struct Budget(Option<Instant>);
impl Drop for Budget {
    fn drop(&mut self) {
        DEADLINE.with(|value| value.set(self.0));
    }
}

const COLD_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompressionReport {
    pub scanned: u64,
    pub compressed: u64,
    pub skipped_hot: u64,
    pub skipped_active: u64,
    pub skipped_changed: u64,
    pub failed: u64,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub busy: bool,
    pub failures: Vec<CompressionFailure>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompressionFailure {
    pub path: RuntimePath,
    pub error: RuntimeString,
}
impl RuntimeEncode for CompressionFailure {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("path", &self.path)?;
        object.field("error", &self.error)?;
        out.emit(crate::runtime_values::RuntimeJson::Object(object.finish()))
    }
}
impl RuntimeEncode for CompressionReport {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("scanned", &self.scanned)?;
        object.field("compressed", &self.compressed)?;
        object.field("skippedHot", &self.skipped_hot)?;
        object.field("skippedActive", &self.skipped_active)?;
        object.field("skippedChanged", &self.skipped_changed)?;
        object.field("failed", &self.failed)?;
        object.field("inputBytes", &self.input_bytes)?;
        object.field("outputBytes", &self.output_bytes)?;
        object.field("busy", &self.busy)?;
        object.field("failures", &self.failures)?;
        out.emit(crate::runtime_values::RuntimeJson::Object(object.finish()))
    }
}

pub fn is_rollout_name(name: &str) -> bool {
    name.ends_with(".jsonl") || name.ends_with(".jsonl.zst")
}
pub fn plain_path(path: &Path) -> PathBuf {
    if path.extension().is_some_and(|extension| extension == "zst")
        && path.file_stem().is_some_and(|stem| {
            Path::new(stem)
                .extension()
                .is_some_and(|extension| extension == "jsonl")
        })
    {
        path.with_extension("")
    } else {
        path.to_path_buf()
    }
}
pub fn compressed_path(path: &Path) -> PathBuf {
    let mut name = plain_path(path).into_os_string();
    name.push(".zst");
    PathBuf::from(name)
}
pub fn writer_lock_path(path: &str) -> String {
    format!("{}.lock", plain_path(Path::new(path)).display())
}
/// Prefer a surviving plain source after an interrupted representation transition.
pub fn resolve(path: &Path) -> io::Result<PathBuf> {
    let plain = plain_path(path);
    match std::fs::metadata(&plain) {
        Ok(_) => Ok(plain),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let compressed = compressed_path(path);
            std::fs::metadata(&compressed)?;
            Ok(compressed)
        }
        Err(error) => Err(error),
    }
}
pub(crate) struct MaintenanceCancellation {
    flag: std::sync::Arc<AtomicBool>,
    armed: bool,
}
impl MaintenanceCancellation {
    pub(crate) fn new(flag: std::sync::Arc<AtomicBool>) -> Self {
        Self { flag, armed: true }
    }
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}
impl Drop for MaintenanceCancellation {
    fn drop(&mut self) {
        if self.armed {
            self.flag.store(true, Ordering::Release);
        }
    }
}

pub(crate) fn check(cancelled: &AtomicBool) -> io::Result<()> {
    if DEADLINE.with(|value| value.get().is_some_and(|end| Instant::now() >= end)) {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "rollout maintenance exceeded its five hour budget",
        ));
    }
    if cancelled.load(Ordering::Acquire) {
        Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "rollout compression cancelled",
        ))
    } else {
        Ok(())
    }
}
fn copy(
    reader: &mut impl Read,
    writer: &mut impl Write,
    cancelled: &AtomicBool,
) -> io::Result<(u64, Vec<u8>)> {
    let mut buffer = [0_u8; 128 * 1024];
    let mut length = 0_u64;
    let mut hash = Sha256::new();
    loop {
        check(cancelled)?;
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        writer.write_all(&buffer[..count])?;
        hash.update(&buffer[..count]);
        length += count as u64;
    }
    Ok((length, hash.finalize().to_vec()))
}

/// Seek over the decoded representation without changing either source path.
/// Anonymous temporary storage is deleted when the returned reader closes.
pub fn open_seekable(path: &Path) -> io::Result<File> {
    open_seekable_cancelled(path, &AtomicBool::new(false))
}
pub(crate) fn open_seekable_cancelled(path: &Path, cancelled: &AtomicBool) -> io::Result<File> {
    for _ in 0..3 {
        let path = resolve(path)?;
        let source = match File::open(&path) {
            Ok(source) => source,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        if !path.as_os_str().to_string_lossy().ends_with(".zst") {
            return Ok(source);
        }
        let metadata = source.metadata()?;
        let mut decoder = zstd::stream::read::Decoder::new(source)?;
        let mut decoded = tempfile::tempfile()?;
        copy(&mut decoder, &mut decoded, cancelled)?;
        decoded.set_times(std::fs::FileTimes::new().set_modified(metadata.modified()?))?;
        decoded.seek(SeekFrom::Start(0))?;
        return Ok(decoded);
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "rollout representation changed while opening",
    ))
}
pub(crate) async fn open_async(path: impl AsRef<Path>) -> io::Result<tokio::fs::File> {
    let path = path.as_ref().to_path_buf();
    let cancellation = crate::store::runtime_resume::ReadCancellation::new();
    let signal = cancellation.0.clone();
    let file = tokio::task::spawn_blocking(move || open_seekable_cancelled(&path, &signal))
        .await
        .map_err(io::Error::other)??;
    Ok(tokio::fs::File::from_std(file))
}
pub(crate) async fn read(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let mut file = open_async(path).await?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).await?;
    Ok(bytes)
}

struct Scratch(PathBuf);
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
pub(crate) struct Lease {
    file: Option<File>,
    path: PathBuf,
}
impl Drop for Lease {
    fn drop(&mut self) {
        drop(self.file.take());
        let _ = std::fs::remove_file(&self.path);
    }
}
pub(crate) fn lease(path: PathBuf) -> io::Result<Option<Lease>> {
    let file = match OpenOptions::new().write(true).create_new(true).open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut guard = Lease {
        file: Some(file),
        path,
    };
    write!(guard.file.as_mut().unwrap(), "{}", std::process::id())?;
    guard.file.as_ref().unwrap().sync_all()?;
    Ok(Some(guard))
}
fn fingerprint(metadata: &Metadata) -> (u64, Option<SystemTime>, Vec<u64>) {
    #[cfg(unix)]
    let identity = {
        use std::os::unix::fs::MetadataExt;
        vec![
            metadata.dev(),
            metadata.ino(),
            metadata.ctime() as u64,
            metadata.ctime_nsec() as u64,
            metadata.mode() as u64,
        ]
    };
    #[cfg(not(unix))]
    let identity = vec![u64::from(metadata.permissions().readonly())];
    (metadata.len(), metadata.modified().ok(), identity)
}
fn sync_parent(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    File::open(
        path.parent()
            .ok_or_else(|| io::Error::other("rollout has no parent"))?,
    )?
    .sync_all()?;
    Ok(())
}

/// Called only while the caller owns the same lifecycle and writer lease used
/// by appenders. The compressed source survives every decode/publication error.
pub(crate) fn materialize(path: &Path, cancelled: &AtomicBool) -> io::Result<PathBuf> {
    check(cancelled)?;
    let plain = plain_path(path);
    let compressed = compressed_path(path);
    if plain.exists() {
        // A plain source is authoritative. A shadow must not resurrect after a
        // later archive/delete or a writer append to the plain representation.
        if compressed.exists() {
            std::fs::remove_file(&compressed)?;
            sync_parent(&plain)?;
        }
        return Ok(plain);
    }
    if !compressed.exists() {
        return Ok(plain);
    }
    let before = std::fs::metadata(&compressed)?;
    let temporary = tempfile::Builder::new()
        .prefix(".rollout-decode-")
        .tempfile_in(
            plain
                .parent()
                .ok_or_else(|| io::Error::other("rollout has no parent"))?,
        )?;
    let mut source = zstd::stream::read::Decoder::new(File::open(&compressed)?)?;
    let mut file = temporary;
    copy(&mut source, file.as_file_mut(), cancelled)?;
    drop(source);
    file.as_file().set_permissions(before.permissions())?;
    file.as_file()
        .set_times(std::fs::FileTimes::new().set_modified(before.modified()?))?;
    file.as_file().sync_all()?;
    check(cancelled)?;
    if fingerprint(&before) != fingerprint(&std::fs::metadata(&compressed)?) {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "compressed rollout changed while reopening",
        ));
    }
    file.persist_noclobber(&plain)
        .map_err(|error| error.error)?;
    sync_parent(&plain)?;
    // Once the plain source is durable, cancellation must leave a usable source.
    std::fs::remove_file(&compressed)?;
    sync_parent(&plain)?;
    Ok(plain)
}

pub(crate) fn parent_identity(root: &Path, path: &Path) -> (String, PathBuf) {
    let parent = path.parent().unwrap_or(root);
    let main = if parent.file_name().is_some_and(|name| name == "subagents") {
        {
            let mut name = parent.parent().unwrap_or(root).as_os_str().to_os_string();
            name.push(".jsonl");
            PathBuf::from(name)
        }
    } else {
        plain_path(path)
    };
    let stem = main.file_stem().unwrap_or_default().to_string_lossy();
    (
        stem.split_once('-')
            .map_or(stem.as_ref(), |(_, id)| id)
            .to_string(),
        main,
    )
}
fn validate_json(file: &mut File, cancelled: &AtomicBool) -> io::Result<()> {
    struct CancelRead<'a>(&'a mut File, &'a AtomicBool);
    impl Read for CancelRead<'_> {
        fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            check(self.1)?;
            self.0.read(bytes)
        }
    }
    file.seek(SeekFrom::Start(0))?;
    let mut count = 0;
    for value in serde_json::Deserializer::from_reader(BufReader::new(CancelRead(file, cancelled)))
        .into_iter::<serde::de::IgnoredAny>()
    {
        check(cancelled)?;
        value.map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        count += 1;
    }
    if count == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty rollout cannot be compressed",
        ));
    }
    Ok(())
}
pub(crate) fn list_rollouts(root: &Path, archive: &Path) -> io::Result<Vec<PathBuf>> {
    fn namespace(root: &Path, out: &mut Vec<PathBuf>) -> io::Result<()> {
        let entries = match std::fs::read_dir(root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        for entry in entries {
            let entry = entry?;
            let ty = entry.file_type()?;
            if ty.is_file() && is_rollout_name(&entry.file_name().to_string_lossy()) {
                out.push(entry.path());
            }
            if ty.is_dir() && !entry.file_name().to_string_lossy().starts_with('.') {
                let children = entry.path().join("subagents");
                if children.is_dir() {
                    for child in std::fs::read_dir(children)? {
                        let child = child?;
                        if child.file_type()?.is_file()
                            && is_rollout_name(&child.file_name().to_string_lossy())
                        {
                            out.push(child.path());
                        }
                    }
                }
            }
        }
        Ok(())
    }
    let mut paths = Vec::new();
    namespace(root, &mut paths)?;
    if archive != root {
        namespace(archive, &mut paths)?;
    }
    paths.sort();
    paths.dedup();
    let plain = paths
        .iter()
        .filter(|path| path.to_string_lossy().ends_with(".jsonl"))
        .cloned()
        .collect::<std::collections::HashSet<_>>();
    paths.retain(|path| {
        !path.to_string_lossy().ends_with(".jsonl.zst") || !plain.contains(&plain_path(path))
    });
    Ok(paths)
}

pub(crate) fn run(
    root: &Path,
    archive_root: &Path,
    cancelled: &AtomicBool,
) -> io::Result<CompressionReport> {
    check(cancelled)?;
    std::fs::create_dir_all(root)?;
    let Some(_singleflight) = maintenance_lock(root)? else {
        return Ok(CompressionReport {
            busy: true,
            ..Default::default()
        });
    };
    let marker_path = root.join(".compression-run.json");
    if let Ok(meta) = std::fs::metadata(&marker_path) {
        if SystemTime::now()
            .duration_since(meta.modified()?)
            .unwrap_or_default()
            < Duration::from_secs(6 * 60 * 60)
        {
            return Ok(CompressionReport {
                busy: true,
                ..Default::default()
            });
        }
        std::fs::remove_file(&marker_path)?;
    }
    let mut marker = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&marker_path)?;
    let _marker = Scratch(marker_path);
    write!(
        marker,
        "{{\"pid\":{},\"startedAtMs\":{}}}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    )?;
    marker.sync_all()?;
    drop(marker);
    let _budget = Budget(
        DEADLINE
            .with(|value| value.replace(Some(Instant::now() + Duration::from_secs(5 * 60 * 60)))),
    );
    let paths = list_rollouts(root, archive_root)?;
    let now = SystemTime::now();
    let mut report = CompressionReport::default();
    for path in paths {
        check(cancelled)?;
        if !path.to_string_lossy().ends_with(".jsonl") {
            continue;
        }
        report.scanned += 1;
        let result = (|| -> io::Result<()> {
            if PathBuf::from(format!("{}.migration.pending", path.display())).exists() {
                report.skipped_active += 1;
                return Ok(());
            }
            let before = std::fs::metadata(&path)?;
            if now.duration_since(before.modified()?).unwrap_or_default() < COLD_AGE {
                report.skipped_hot += 1;
                return Ok(());
            }
            let (parent_id, parent_main) = parent_identity(root, &path);
            let locks = root.join(".lifecycle-locks");
            std::fs::create_dir_all(&locks)?;
            let lifecycle = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(locks.join(format!("{:x}.lock", Sha256::digest(parent_id.as_bytes()))))?;
            if let Err(error) = lifecycle.try_lock_exclusive() {
                if error.kind() == io::ErrorKind::WouldBlock {
                    report.skipped_active += 1;
                    return Ok(());
                }
                return Err(error);
            }
            if parent_main != path
                && Path::new(&writer_lock_path(&parent_main.to_string_lossy())).exists()
            {
                report.skipped_active += 1;
                return Ok(());
            }
            let Some(_writer) = lease(PathBuf::from(writer_lock_path(&path.to_string_lossy())))?
            else {
                report.skipped_active += 1;
                return Ok(());
            };
            if fingerprint(&before) != fingerprint(&std::fs::metadata(&path)?) {
                report.skipped_changed += 1;
                return Ok(());
            }
            let destination = compressed_path(&path);
            if destination.exists() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "compressed destination already exists; preserving plain source",
                ));
            }
            let mut source = File::open(&path)?;
            validate_json(&mut source, cancelled)?;
            source.seek(SeekFrom::Start(0))?;
            let mut temporary = tempfile::Builder::new()
                .prefix(".rollout-compress-")
                .tempfile_in(path.parent().unwrap())?;
            let mut encoder = zstd::stream::Encoder::new(temporary.as_file_mut(), 3)?;
            encoder.include_checksum(true)?;
            let (input_bytes, digest) = copy(&mut source, &mut encoder, cancelled)?;
            encoder.finish()?;
            temporary.as_file().set_permissions(before.permissions())?;
            temporary
                .as_file()
                .set_times(std::fs::FileTimes::new().set_modified(before.modified()?))?;
            temporary.as_file().sync_all()?;
            let mut decoder = zstd::stream::read::Decoder::new(File::open(temporary.path())?)?;
            let decoded = copy(&mut decoder, &mut io::sink(), cancelled)?;
            source.seek(SeekFrom::Start(0))?;
            let current = copy(&mut source, &mut io::sink(), cancelled)?;
            drop(source);
            drop(decoder);
            if decoded != (input_bytes, digest.clone())
                || current != (input_bytes, digest)
                || fingerprint(&before) != fingerprint(&std::fs::metadata(&path)?)
            {
                report.skipped_changed += 1;
                return Ok(());
            }
            let output_bytes = temporary.as_file().metadata()?.len();
            check(cancelled)?;
            temporary
                .persist_noclobber(&destination)
                .map_err(|error| error.error)?;
            let published = Scratch(destination.clone());
            sync_parent(&destination)?;
            if fingerprint(&before) != fingerprint(&std::fs::metadata(&path)?) {
                report.skipped_changed += 1;
                return Ok(());
            }
            std::fs::remove_file(&path)?;
            std::mem::forget(published);
            sync_parent(&destination)?;
            report.compressed += 1;
            report.input_bytes += input_bytes;
            report.output_bytes += output_bytes;
            Ok(())
        })();
        if let Err(error) = result {
            if error.kind() == io::ErrorKind::Interrupted {
                return Err(error);
            }
            report.failed += 1;
            report.failures.push(CompressionFailure {
                path: RuntimePath::from(path.to_string_lossy().into_owned()),
                error: error.to_string().into(),
            });
        }
    }
    std::fs::remove_file(&_marker.0)?;
    Ok(report)
}

/// Shared exclusion for every operation that rewrites stored rollout bytes.
/// The retained file owns an advisory lock; dropping it releases that lock.
pub(crate) struct MaintenanceLease {
    _file: File,
}
pub(crate) fn maintenance_lock(root: &Path) -> io::Result<Option<MaintenanceLease>> {
    std::fs::create_dir_all(root)?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.join(".rollout-maintenance.lock"))?;
    match file.try_lock_exclusive() {
        Ok(()) => Ok(Some(MaintenanceLease { _file: file })),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(None),
        Err(error) => Err(error),
    }
}

pub(crate) async fn read_prefix(path: impl AsRef<Path>, limit: usize) -> io::Result<Vec<u8>> {
    let path = path.as_ref().to_path_buf();
    tokio::task::spawn_blocking(move || {
        let path = resolve(&path)?;
        let source = File::open(&path)?;
        let mut reader: Box<dyn Read> = if path.as_os_str().to_string_lossy().ends_with(".zst") {
            Box::new(zstd::stream::read::Decoder::new(source)?)
        } else {
            Box::new(source)
        };
        let mut bytes = Vec::new();
        let mut chunk = [0_u8; 4096];
        while bytes.len() < limit {
            let wanted = (limit - bytes.len()).min(chunk.len());
            let count = reader.read(&mut chunk[..wanted])?;
            if count == 0 {
                break;
            }
            bytes.extend_from_slice(&chunk[..count]);
            if chunk[..count].contains(&b'\n') {
                break;
            }
        }
        Ok(bytes)
    })
    .await
    .map_err(io::Error::other)?
}
