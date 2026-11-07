//! Safe plugin bundle extraction.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use tar::{Archive, EntryType};

pub const MAX_PLUGIN_BUNDLE_EXTRACT_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum PluginArchiveError {
    #[error("plugin bundle extracted size would be {bytes} bytes, exceeding maximum of {max_bytes} bytes")]
    ExtractedBundleTooLarge { bytes: u64, max_bytes: u64 },

    #[error("plugin bundle entry `{path}` escapes extraction root")]
    PathEscapesRoot { path: String },

    #[error("plugin bundle entry has an empty path")]
    EmptyPath,

    #[error("plugin bundle entry `{path}` is a link")]
    LinkEntry { path: String },

    #[error("plugin bundle entry `{path}` has unsupported type {entry_type:?}")]
    UnsupportedEntryType { path: String, entry_type: EntryType },

    #[error("{context}: {source}")]
    Io {
        context: &'static str,
        #[source]
        source: io::Error,
    },
}

impl PluginArchiveError {
    fn io(context: &'static str, source: io::Error) -> Self {
        Self::Io { context, source }
    }
}

impl From<PluginArchiveError> for io::Error {
    fn from(value: PluginArchiveError) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, value)
    }
}

pub async fn unpack_tar_gz_file(
    path: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    max_total_bytes: u64,
) -> io::Result<()> {
    let path = path.as_ref().to_path_buf();
    let destination = destination.as_ref().to_path_buf();
    tokio::task::spawn_blocking(move || {
        unpack_tar_gz_file_blocking(&path, &destination, max_total_bytes)
    })
    .await
    .map_err(|err| io::Error::other(format!("plugin bundle extraction task failed: {err}")))?
    .map_err(io::Error::from)
}

fn unpack_tar_gz_file_blocking(
    path: &Path,
    destination: &Path,
    max_total_bytes: u64,
) -> Result<(), PluginArchiveError> {
    fs::create_dir_all(destination).map_err(|source| {
        PluginArchiveError::io(
            "failed to create plugin bundle extraction directory",
            source,
        )
    })?;
    let file = fs::File::open(path)
        .map_err(|source| PluginArchiveError::io("failed to open plugin bundle", source))?;
    let decoder = GzDecoder::new(file);
    let mut archive = Archive::new(decoder);
    unpack_tar(&mut archive, destination, max_total_bytes)
}

fn unpack_tar<R: io::Read>(
    archive: &mut Archive<R>,
    destination: &Path,
    max_total_bytes: u64,
) -> Result<(), PluginArchiveError> {
    let mut extracted_bytes = 0u64;
    let entries = archive
        .entries()
        .map_err(|source| PluginArchiveError::io("failed to read plugin bundle", source))?;
    for entry in entries {
        let mut entry = entry.map_err(|source| {
            PluginArchiveError::io("failed to read plugin bundle entry", source)
        })?;
        let entry_type = entry.header().entry_type();
        let entry_size = entry.size();
        let entry_path = entry
            .path()
            .map_err(|source| {
                PluginArchiveError::io("failed to read plugin bundle entry path", source)
            })?
            .into_owned();
        let output_path = checked_output_path(destination, &entry_path)?;

        if entry_type.is_dir() {
            fs::create_dir_all(&output_path).map_err(|source| {
                PluginArchiveError::io("failed to create plugin bundle directory", source)
            })?;
            continue;
        }

        if entry_type.is_file() {
            add_extracted_size(entry_size, &mut extracted_bytes, max_total_bytes)?;
            let parent = output_path.parent().ok_or(PluginArchiveError::EmptyPath)?;
            fs::create_dir_all(parent).map_err(|source| {
                PluginArchiveError::io("failed to create plugin bundle directory", source)
            })?;
            entry.unpack(&output_path).map_err(|source| {
                PluginArchiveError::io("failed to unpack plugin bundle entry", source)
            })?;
            continue;
        }

        let path = entry_path.to_string_lossy().into_owned();
        if entry_type.is_symlink() || entry_type.is_hard_link() {
            return Err(PluginArchiveError::LinkEntry { path });
        }
        return Err(PluginArchiveError::UnsupportedEntryType { path, entry_type });
    }
    Ok(())
}

fn checked_output_path(
    destination: &Path,
    entry_path: &Path,
) -> Result<PathBuf, PluginArchiveError> {
    let mut output = destination.to_path_buf();
    let mut has_component = false;
    for component in entry_path.components() {
        match component {
            std::path::Component::Normal(part) => {
                has_component = true;
                output.push(part);
            }
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir
            | std::path::Component::RootDir
            | std::path::Component::Prefix(_) => {
                return Err(PluginArchiveError::PathEscapesRoot {
                    path: entry_path.to_string_lossy().into_owned(),
                });
            }
        }
    }
    if has_component {
        Ok(output)
    } else {
        Err(PluginArchiveError::EmptyPath)
    }
}

fn add_extracted_size(
    entry_size: u64,
    extracted_bytes: &mut u64,
    max_total_bytes: u64,
) -> Result<(), PluginArchiveError> {
    let next = extracted_bytes.checked_add(entry_size).ok_or(
        PluginArchiveError::ExtractedBundleTooLarge {
            bytes: u64::MAX,
            max_bytes: max_total_bytes,
        },
    )?;
    if next > max_total_bytes {
        return Err(PluginArchiveError::ExtractedBundleTooLarge {
            bytes: next,
            max_bytes: max_total_bytes,
        });
    }
    *extracted_bytes = next;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tar::{Builder, Header};

    fn temp_dir() -> PathBuf {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "ae-plugin-archive-{}-{}-{:016x}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_archive(path: &Path, entries: Vec<(&str, EntryType, Vec<u8>)>) {
        let file = std::fs::File::create(path).unwrap();
        let encoder = GzEncoder::new(file, Compression::default());
        let mut archive = Builder::new(encoder);
        for (name, entry_type, body) in entries {
            let mut header = Header::new_gnu();
            header.set_entry_type(entry_type);
            header.set_size(if entry_type.is_file() {
                body.len() as u64
            } else {
                0
            });
            header.set_mode(0o644);
            header.set_cksum();
            archive
                .append_data(&mut header, name, body.as_slice())
                .unwrap();
        }
        let encoder = archive.into_inner().unwrap();
        encoder.finish().unwrap();
    }

    fn write_raw_archive(path: &Path, name: &str, entry_type: u8, body: &[u8]) {
        let file = std::fs::File::create(path).unwrap();
        let mut encoder = GzEncoder::new(file, Compression::default());
        let mut header = [0u8; 512];
        header[0..name.len()].copy_from_slice(name.as_bytes());
        write_octal(&mut header[100..108], 0o644);
        write_octal(&mut header[108..116], 0);
        write_octal(&mut header[116..124], 0);
        write_octal(&mut header[124..136], body.len() as u64);
        write_octal(&mut header[136..148], 0);
        for byte in &mut header[148..156] {
            *byte = b' ';
        }
        header[156] = entry_type;
        header[257..263].copy_from_slice(b"ustar\0");
        header[263..265].copy_from_slice(b"00");
        let checksum: u32 = header.iter().map(|byte| u32::from(*byte)).sum();
        write_checksum(&mut header[148..156], checksum);
        encoder.write_all(&header).unwrap();
        encoder.write_all(body).unwrap();
        let padding = (512 - (body.len() % 512)) % 512;
        if padding > 0 {
            encoder.write_all(&vec![0u8; padding]).unwrap();
        }
        encoder.write_all(&[0u8; 1024]).unwrap();
        encoder.finish().unwrap();
    }

    fn write_octal(dst: &mut [u8], value: u64) {
        let encoded = format!("{value:0width$o}\0", width = dst.len() - 1);
        dst.copy_from_slice(encoded.as_bytes());
    }

    fn write_checksum(dst: &mut [u8], value: u32) {
        let encoded = format!("{value:06o}\0 ",);
        dst.copy_from_slice(encoded.as_bytes());
    }

    #[tokio::test]
    async fn extracts_regular_plugin_bundle_entries() {
        let base = temp_dir();
        let archive = base.join("plugin.tgz");
        write_archive(
            &archive,
            vec![
                (
                    "pkg/plugin.json",
                    EntryType::Regular,
                    br#"{"name":"pkg"}"#.to_vec(),
                ),
                ("pkg/commands/hi.md", EntryType::Regular, b"hello".to_vec()),
            ],
        );
        let out = base.join("out");

        unpack_tar_gz_file(&archive, &out, MAX_PLUGIN_BUNDLE_EXTRACT_BYTES)
            .await
            .unwrap();

        assert!(out.join("pkg/plugin.json").exists());
        assert_eq!(
            std::fs::read_to_string(out.join("pkg/commands/hi.md")).unwrap(),
            "hello"
        );
    }

    #[tokio::test]
    async fn rejects_paths_that_escape_the_destination() {
        let base = temp_dir();
        let archive = base.join("bad.tgz");
        write_raw_archive(&archive, "../evil", b'0', b"nope");
        let out = base.join("out");

        assert!(
            unpack_tar_gz_file(&archive, &out, MAX_PLUGIN_BUNDLE_EXTRACT_BYTES)
                .await
                .is_err()
        );
        assert!(!base.join("evil").exists());
    }

    #[tokio::test]
    async fn rejects_link_entries() {
        let base = temp_dir();
        let archive = base.join("link.tgz");
        write_archive(&archive, vec![("link", EntryType::Symlink, Vec::new())]);
        let out = base.join("out");

        assert!(
            unpack_tar_gz_file(&archive, &out, MAX_PLUGIN_BUNDLE_EXTRACT_BYTES)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn rejects_archives_over_the_extracted_size_limit() {
        let base = temp_dir();
        let archive = base.join("large.tgz");
        write_archive(
            &archive,
            vec![("big.bin", EntryType::Regular, vec![b'x'; 16])],
        );
        let out = base.join("out");

        assert!(unpack_tar_gz_file(&archive, &out, 8).await.is_err());
    }
}
