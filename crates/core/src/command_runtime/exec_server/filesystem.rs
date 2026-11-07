use super::backend::{ExecServerError, ExecServerResult};
use super::protocol::{
    runtime_path_uri_from_host_native_path, runtime_path_uri_to_host_path,
    FileSystemSandboxContext, FsCanonicalizeParams, FsCanonicalizeResponse, FsCloseParams,
    FsCloseResponse, FsCopyParams, FsCopyResponse, FsCreateDirectoryParams,
    FsCreateDirectoryResponse, FsGetMetadataParams, FsGetMetadataResponse, FsOpenParams,
    FsOpenResponse, FsReadBlockParams, FsReadBlockResponse, FsReadDirectoryEntry,
    FsReadDirectoryParams, FsReadDirectoryResponse, FsReadFileParams, FsReadFileResponse,
    FsRemoveParams, FsRemoveResponse, FsWalkEntry, FsWalkParams, FsWalkResponse, FsWriteFileParams,
    FsWriteFileResponse,
};
use crate::fs::node_path::runtime_string_from_native;
use crate::runtime_values::{RuntimePath, RuntimeString};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

#[derive(Default)]
pub struct FileSystemHandler {
    handles: Mutex<HashMap<RuntimeString, PathBuf>>,
}

impl FileSystemHandler {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn read_file(
        &self,
        params: FsReadFileParams,
    ) -> ExecServerResult<FsReadFileResponse> {
        let path = checked_path(&params.path, params.sandbox.as_ref(), AccessKind::Read)?;
        let data = tokio::fs::read(path)
            .await
            .map_err(|error| ExecServerError::internal(error.to_string()))?;
        Ok(FsReadFileResponse {
            data_base64: RuntimeString::from(BASE64_STANDARD.encode(data)),
        })
    }

    pub async fn open(&self, params: FsOpenParams) -> ExecServerResult<FsOpenResponse> {
        if runtime_trim_is_empty(&params.handle_id) {
            return Err(ExecServerError::invalid_params(
                "handleId must not be empty",
            ));
        }
        let path = checked_path(&params.path, params.sandbox.as_ref(), AccessKind::Read)?;
        tokio::fs::metadata(&path)
            .await
            .map_err(|error| ExecServerError::internal(error.to_string()))?;
        self.handles()
            .insert(params.handle_id.clone(), path.to_path_buf());
        Ok(FsOpenResponse {
            handle_id: params.handle_id,
        })
    }

    pub async fn read_block(
        &self,
        params: FsReadBlockParams,
    ) -> ExecServerResult<FsReadBlockResponse> {
        let path = self
            .handles()
            .get(&params.handle_id)
            .cloned()
            .ok_or_else(|| ExecServerError::invalid_params("unknown file handle"))?;
        let mut file = tokio::fs::File::open(&path)
            .await
            .map_err(|error| ExecServerError::internal(error.to_string()))?;
        let file_len = file
            .metadata()
            .await
            .map_err(|error| ExecServerError::internal(error.to_string()))?
            .len();
        file.seek(std::io::SeekFrom::Start(params.offset))
            .await
            .map_err(|error| ExecServerError::internal(error.to_string()))?;
        let mut buf = vec![0; params.len];
        let read = file
            .read(&mut buf)
            .await
            .map_err(|error| ExecServerError::internal(error.to_string()))?;
        buf.truncate(read);
        let eof = read < params.len || params.offset.saturating_add(read as u64) >= file_len;
        Ok(FsReadBlockResponse {
            chunk: buf.into(),
            eof,
        })
    }

    pub async fn close(&self, params: FsCloseParams) -> ExecServerResult<FsCloseResponse> {
        self.handles().remove(&params.handle_id);
        Ok(FsCloseResponse {})
    }

    pub async fn write_file(
        &self,
        params: FsWriteFileParams,
    ) -> ExecServerResult<FsWriteFileResponse> {
        let path = checked_path(&params.path, params.sandbox.as_ref(), AccessKind::Write)?;
        let data = BASE64_STANDARD
            .decode(
                params
                    .data_base64
                    .try_to_utf8()
                    .map_err(|error| ExecServerError::invalid_params(error.to_string()))?
                    .as_bytes(),
            )
            .map_err(|error| ExecServerError::invalid_params(error.to_string()))?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|error| ExecServerError::internal(error.to_string()))?;
        }
        tokio::fs::write(path, data)
            .await
            .map_err(|error| ExecServerError::internal(error.to_string()))?;
        Ok(FsWriteFileResponse {})
    }

    pub async fn create_directory(
        &self,
        params: FsCreateDirectoryParams,
    ) -> ExecServerResult<FsCreateDirectoryResponse> {
        let path = checked_path(&params.path, params.sandbox.as_ref(), AccessKind::Write)?;
        if params.recursive.unwrap_or(false) {
            tokio::fs::create_dir_all(path).await
        } else {
            tokio::fs::create_dir(path).await
        }
        .map_err(|error| ExecServerError::internal(error.to_string()))?;
        Ok(FsCreateDirectoryResponse {})
    }

    pub async fn get_metadata(
        &self,
        params: FsGetMetadataParams,
    ) -> ExecServerResult<FsGetMetadataResponse> {
        let path = checked_path(&params.path, params.sandbox.as_ref(), AccessKind::Read)?;
        let metadata = tokio::fs::symlink_metadata(path)
            .await
            .map_err(|error| ExecServerError::internal(error.to_string()))?;
        Ok(FsGetMetadataResponse {
            is_directory: metadata.is_dir(),
            is_file: metadata.is_file(),
            is_symlink: metadata.file_type().is_symlink(),
            size: metadata.len(),
            created_at_ms: time_ms(metadata.created().ok()),
            modified_at_ms: time_ms(metadata.modified().ok()),
        })
    }

    pub async fn canonicalize(
        &self,
        params: FsCanonicalizeParams,
    ) -> ExecServerResult<FsCanonicalizeResponse> {
        let path = checked_path(&params.path, params.sandbox.as_ref(), AccessKind::Read)?;
        let canonical = tokio::fs::canonicalize(path)
            .await
            .map(crate::fs::node_path::simplify_host)
            .map_err(|error| ExecServerError::internal(error.to_string()))?;
        Ok(FsCanonicalizeResponse {
            path: runtime_path_uri_from_host_native_path(canonical)
                .map_err(ExecServerError::internal)?,
        })
    }

    pub async fn read_directory(
        &self,
        params: FsReadDirectoryParams,
    ) -> ExecServerResult<FsReadDirectoryResponse> {
        let path = checked_path(&params.path, params.sandbox.as_ref(), AccessKind::Read)?;
        let mut entries = Vec::new();
        let mut rd = tokio::fs::read_dir(path)
            .await
            .map_err(|error| ExecServerError::internal(error.to_string()))?;
        while let Some(entry) = rd
            .next_entry()
            .await
            .map_err(|error| ExecServerError::internal(error.to_string()))?
        {
            let file_type = entry
                .file_type()
                .await
                .map_err(|error| ExecServerError::internal(error.to_string()))?;
            entries.push(FsReadDirectoryEntry {
                file_name: runtime_string_from_native(&entry.file_name()),
                is_directory: file_type.is_dir(),
                is_file: file_type.is_file(),
            });
        }
        entries.sort_by(|a, b| a.file_name.cmp(&b.file_name));
        Ok(FsReadDirectoryResponse { entries })
    }

    pub async fn walk(&self, params: FsWalkParams) -> ExecServerResult<FsWalkResponse> {
        let root = checked_path(&params.path, params.sandbox.as_ref(), AccessKind::Read)?;
        let max_depth = params.options.max_depth.unwrap_or(usize::MAX);
        let max_entries = params.options.max_entries.unwrap_or(usize::MAX);
        let mut entries = Vec::new();
        let mut stack = vec![(root, 0usize)];
        let mut truncated = false;

        while let Some((dir, depth)) = stack.pop() {
            if depth >= max_depth {
                continue;
            }
            let mut children = read_child_paths(&dir).await?;
            children.sort();
            for child in children {
                if entries.len() >= max_entries {
                    truncated = true;
                    break;
                }
                let file_type = tokio::fs::symlink_metadata(&child)
                    .await
                    .map_err(|error| ExecServerError::internal(error.to_string()))?
                    .file_type();
                let is_directory = file_type.is_dir();
                entries.push(FsWalkEntry {
                    path: runtime_path_uri_from_host_native_path(&child)
                        .map_err(ExecServerError::internal)?,
                    is_directory,
                    is_file: file_type.is_file(),
                });
                if is_directory {
                    stack.push((child, depth + 1));
                }
            }
            if truncated {
                break;
            }
        }

        Ok(FsWalkResponse { entries, truncated })
    }

    pub async fn remove(&self, params: FsRemoveParams) -> ExecServerResult<FsRemoveResponse> {
        let path = checked_path(&params.path, params.sandbox.as_ref(), AccessKind::Write)?;
        let metadata = match tokio::fs::symlink_metadata(&path).await {
            Ok(metadata) => metadata,
            Err(error)
                if params.force.unwrap_or(false)
                    && error.kind() == std::io::ErrorKind::NotFound =>
            {
                return Ok(FsRemoveResponse {});
            }
            Err(error) => return Err(ExecServerError::internal(error.to_string())),
        };
        if metadata.is_dir() && params.recursive.unwrap_or(false) {
            tokio::fs::remove_dir_all(path).await
        } else if metadata.is_dir() {
            tokio::fs::remove_dir(path).await
        } else {
            tokio::fs::remove_file(path).await
        }
        .map_err(|error| ExecServerError::internal(error.to_string()))?;
        Ok(FsRemoveResponse {})
    }

    pub async fn copy(&self, params: FsCopyParams) -> ExecServerResult<FsCopyResponse> {
        let source = checked_path(
            &params.source_path,
            params.sandbox.as_ref(),
            AccessKind::Read,
        )?;
        let destination = checked_path(
            &params.destination_path,
            params.sandbox.as_ref(),
            AccessKind::Write,
        )?;
        let metadata = tokio::fs::symlink_metadata(&source)
            .await
            .map_err(|error| ExecServerError::internal(error.to_string()))?;
        if metadata.is_dir() {
            if !params.recursive {
                return Err(ExecServerError::invalid_params(
                    "recursive must be true when copying a directory",
                ));
            }
            copy_dir_recursive(&source, &destination).await?;
        } else {
            if let Some(parent) = destination.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|error| ExecServerError::internal(error.to_string()))?;
            }
            tokio::fs::copy(source, destination)
                .await
                .map_err(|error| ExecServerError::internal(error.to_string()))?;
        }
        Ok(FsCopyResponse {})
    }

    fn handles(&self) -> std::sync::MutexGuard<'_, HashMap<RuntimeString, PathBuf>> {
        self.handles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[derive(Debug, Clone, Copy)]
enum AccessKind {
    Read,
    Write,
}

fn checked_path(
    uri: &RuntimePath,
    sandbox: Option<&FileSystemSandboxContext>,
    access: AccessKind,
) -> ExecServerResult<PathBuf> {
    let path = runtime_path_uri_to_host_path(uri).map_err(ExecServerError::invalid_params)?;
    if let Some(sandbox) = sandbox {
        ensure_in_sandbox(&path, sandbox, access)?;
    }
    Ok(path)
}

fn ensure_in_sandbox(
    path: &Path,
    sandbox: &FileSystemSandboxContext,
    access: AccessKind,
) -> ExecServerResult<()> {
    let roots = sandbox_roots(sandbox, access)?;
    let normalized_path = normalize_for_policy(path);
    if roots
        .iter()
        .map(|root| normalize_for_policy(root))
        .any(|root| normalized_path.starts_with(root))
    {
        Ok(())
    } else {
        Err(ExecServerError::invalid_params(format!(
            "path {} is outside sandbox",
            path.display()
        )))
    }
}

fn sandbox_roots(
    sandbox: &FileSystemSandboxContext,
    access: AccessKind,
) -> ExecServerResult<Vec<PathBuf>> {
    let cwd =
        runtime_path_uri_to_host_path(&sandbox.cwd).map_err(ExecServerError::invalid_params)?;
    let mut roots = match access {
        AccessKind::Read => vec![cwd.clone()],
        AccessKind::Write if sandbox.writable_roots.is_empty() => vec![cwd.clone()],
        AccessKind::Write => Vec::new(),
    };
    for root in &sandbox.writable_roots {
        roots.push(runtime_path_uri_to_host_path(root).map_err(ExecServerError::invalid_params)?);
    }
    Ok(roots)
}

fn runtime_trim_is_empty(value: &RuntimeString) -> bool {
    value.as_utf16().iter().all(|unit| {
        matches!(
            *unit,
            0x0009..=0x000d
                | 0x0020
                | 0x00a0
                | 0x1680
                | 0x2000..=0x200a
                | 0x2028
                | 0x2029
                | 0x202f
                | 0x205f
                | 0x3000
                | 0xfeff
        )
    })
}

fn normalize_for_policy(path: &Path) -> PathBuf {
    if let Ok(canonical) = crate::fs::node_path::canonicalize_host(path) {
        return canonical;
    }
    let mut suffix = Vec::new();
    let mut cursor = path;
    while let Some(parent) = cursor.parent() {
        if let Some(name) = cursor.file_name() {
            suffix.push(name.to_os_string());
        }
        if let Ok(mut canonical) = crate::fs::node_path::canonicalize_host(parent) {
            for part in suffix.iter().rev() {
                canonical.push(part);
            }
            return canonical;
        }
        cursor = parent;
    }
    path.to_path_buf()
}

fn time_ms(time: Option<SystemTime>) -> i64 {
    time.and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

async fn read_child_paths(dir: &Path) -> ExecServerResult<Vec<PathBuf>> {
    let mut children = Vec::new();
    let mut rd = tokio::fs::read_dir(dir)
        .await
        .map_err(|error| ExecServerError::internal(error.to_string()))?;
    while let Some(entry) = rd
        .next_entry()
        .await
        .map_err(|error| ExecServerError::internal(error.to_string()))?
    {
        children.push(entry.path());
    }
    Ok(children)
}

async fn copy_dir_recursive(source: &Path, destination: &Path) -> ExecServerResult<()> {
    tokio::fs::create_dir_all(destination)
        .await
        .map_err(|error| ExecServerError::internal(error.to_string()))?;
    let mut stack = vec![(source.to_path_buf(), destination.to_path_buf())];
    while let Some((src_dir, dst_dir)) = stack.pop() {
        tokio::fs::create_dir_all(&dst_dir)
            .await
            .map_err(|error| ExecServerError::internal(error.to_string()))?;
        let children = read_child_paths(&src_dir).await?;
        for child in children {
            let name = child
                .file_name()
                .ok_or_else(|| ExecServerError::internal("directory entry has no file name"))?;
            let target = dst_dir.join(name);
            let metadata = tokio::fs::symlink_metadata(&child)
                .await
                .map_err(|error| ExecServerError::internal(error.to_string()))?;
            if metadata.is_dir() {
                stack.push((child, target));
            } else {
                if let Some(parent) = target.parent() {
                    tokio::fs::create_dir_all(parent)
                        .await
                        .map_err(|error| ExecServerError::internal(error.to_string()))?;
                }
                tokio::fs::copy(child, target)
                    .await
                    .map_err(|error| ExecServerError::internal(error.to_string()))?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command_runtime::exec_server::protocol::{
        FileSystemSandboxContext, FsCanonicalizeParams, FsCloseParams, FsCopyParams,
        FsCreateDirectoryParams, FsGetMetadataParams, FsOpenParams, FsReadBlockParams,
        FsReadDirectoryParams, FsReadFileParams, FsRemoveParams, FsWalkParams, FsWriteFileParams,
        WalkOptions,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(label: &str) -> std::path::PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("klaus-exec-fs-{label}-{stamp}"))
    }

    fn runtime_path(path: impl AsRef<std::path::Path>) -> RuntimePath {
        runtime_path_uri_from_host_native_path(path).unwrap()
    }

    #[tokio::test]
    async fn reads_writes_blocks_and_metadata() {
        let root = temp_root("rw");
        tokio::fs::create_dir_all(&root).await.unwrap();
        let handler = FileSystemHandler::new();
        let file = root.join("nested").join("data.bin");

        handler
            .create_directory(FsCreateDirectoryParams {
                path: runtime_path(file.parent().unwrap()),
                recursive: Some(true),
                sandbox: None,
            })
            .await
            .unwrap();
        handler
            .write_file(FsWriteFileParams {
                path: runtime_path(&file),
                data_base64: "AAECA2hlbGxv".into(),
                sandbox: None,
            })
            .await
            .unwrap();

        let read = handler
            .read_file(FsReadFileParams {
                path: runtime_path(&file),
                sandbox: None,
            })
            .await
            .unwrap();
        assert_eq!(read.data_base64, "AAECA2hlbGxv");

        handler
            .open(FsOpenParams {
                handle_id: "h1".into(),
                path: runtime_path(&file),
                sandbox: None,
            })
            .await
            .unwrap();
        let block = handler
            .read_block(FsReadBlockParams {
                handle_id: "h1".into(),
                offset: 4,
                len: 5,
            })
            .await
            .unwrap();
        assert_eq!(block.chunk.into_inner(), b"hello");
        assert!(block.eof);
        handler
            .close(FsCloseParams {
                handle_id: "h1".into(),
            })
            .await
            .unwrap();

        let metadata = handler
            .get_metadata(FsGetMetadataParams {
                path: runtime_path(&file),
                sandbox: None,
            })
            .await
            .unwrap();
        assert!(metadata.is_file);
        assert_eq!(metadata.size, 9);

        let canonical = handler
            .canonicalize(FsCanonicalizeParams {
                path: runtime_path(&file),
                sandbox: None,
            })
            .await
            .unwrap();
        assert!(canonical.path.as_runtime_string().starts_with("file://"));

        let _ = tokio::fs::remove_dir_all(&root).await;
    }

    #[tokio::test]
    async fn reads_directories_walks_copies_and_removes() {
        let root = temp_root("walk");
        tokio::fs::create_dir_all(root.join("src/sub"))
            .await
            .unwrap();
        tokio::fs::write(root.join("src/a.txt"), b"a")
            .await
            .unwrap();
        tokio::fs::write(root.join("src/sub/b.txt"), b"b")
            .await
            .unwrap();
        let handler = FileSystemHandler::new();

        let listing = handler
            .read_directory(FsReadDirectoryParams {
                path: runtime_path(root.join("src")),
                sandbox: None,
            })
            .await
            .unwrap();
        let names = listing
            .entries
            .iter()
            .map(|entry| entry.file_name.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![RuntimeString::from("a.txt"), RuntimeString::from("sub")]
        );

        let walked = handler
            .walk(FsWalkParams {
                path: runtime_path(root.join("src")),
                options: WalkOptions {
                    max_depth: Some(3),
                    max_entries: Some(8),
                },
                sandbox: None,
            })
            .await
            .unwrap();
        assert!(walked
            .entries
            .iter()
            .any(|entry| entry.path.as_runtime_string().ends_with("/sub/b.txt")));

        handler
            .copy(FsCopyParams {
                source_path: runtime_path(root.join("src")),
                destination_path: runtime_path(root.join("dst")),
                recursive: true,
                sandbox: None,
            })
            .await
            .unwrap();
        assert_eq!(
            tokio::fs::read(root.join("dst/sub/b.txt")).await.unwrap(),
            b"b"
        );

        handler
            .remove(FsRemoveParams {
                path: runtime_path(root.join("dst")),
                recursive: Some(true),
                force: Some(false),
                sandbox: None,
            })
            .await
            .unwrap();
        assert!(!root.join("dst").exists());

        let _ = tokio::fs::remove_dir_all(&root).await;
    }

    #[tokio::test]
    async fn sandbox_context_denies_paths_outside_allowed_root() {
        let allowed = temp_root("allowed");
        let denied = temp_root("denied");
        tokio::fs::create_dir_all(&allowed).await.unwrap();
        tokio::fs::create_dir_all(&denied).await.unwrap();
        tokio::fs::write(denied.join("secret.txt"), b"nope")
            .await
            .unwrap();
        let handler = FileSystemHandler::new();
        let sandbox = FileSystemSandboxContext {
            cwd: runtime_path(&allowed),
            writable_roots: vec![runtime_path(&allowed)],
            network_allowed: false,
        };

        let err = handler
            .read_file(FsReadFileParams {
                path: runtime_path(denied.join("secret.txt")),
                sandbox: Some(sandbox.clone()),
            })
            .await
            .expect_err("sandbox should deny read outside allowed root");
        assert!(err.message.contains("outside sandbox"));

        let err = handler
            .write_file(FsWriteFileParams {
                path: runtime_path(denied.join("secret.txt")),
                data_base64: "b3ZlcndyaXRl".into(),
                sandbox: Some(sandbox),
            })
            .await
            .expect_err("sandbox should deny write outside writable root");
        assert!(err.message.contains("outside sandbox"));

        let _ = tokio::fs::remove_dir_all(&allowed).await;
        let _ = tokio::fs::remove_dir_all(&denied).await;
    }
}
