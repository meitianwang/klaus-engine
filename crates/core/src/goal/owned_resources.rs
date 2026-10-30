//! Exact file capabilities for a human-submitted goal preparation.
//!
//! Objective text never creates a capability. The host supplies a typed manifest,
//! which is validated against engine-owned storage before it becomes durable.

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::fs::node_path::project_runtime_path;
use crate::memory::pinned_root::PinnedMemoryRoot;
use crate::runtime_values::RuntimePath;

mod runtime_codec;

pub const MAX_GOAL_RESOURCE_BYTES: u64 = 10 * 1024 * 1024;
pub const MAX_GOAL_RESOURCE_FILES: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GoalOwnedResourceFile {
    pub name: String,
    pub size_bytes: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GoalOwnedResourcesInput {
    pub preparation_id: String,
    pub files: Vec<GoalOwnedResourceFile>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GoalOwnedResourcesManifest {
    pub source_thread_id: String,
    pub preparation_id: String,
    pub files: Vec<GoalOwnedResourceFile>,
}

/// A restored manifest retains a handle to its exact preparation directory.
/// This runtime capability cannot be deserialized without validating its files.
#[derive(Clone, Debug)]
pub struct GoalOwnedResources {
    manifest: GoalOwnedResourcesManifest,
    config_root: PathBuf,
    logical_config_root: PathBuf,
    logical_root: PathBuf,
    canonical_root: PathBuf,
    pinned: Arc<PinnedMemoryRoot>,
}

impl PartialEq for GoalOwnedResources {
    fn eq(&self, other: &Self) -> bool {
        self.manifest == other.manifest && self.canonical_root == other.canonical_root
    }
}

impl Eq for GoalOwnedResources {}

/// A private, verified copy for ReadTool, including binary/image delivery.
#[derive(Debug)]
pub struct GoalOwnedReadSnapshot {
    directory: PathBuf,
    path: PathBuf,
}

impl GoalOwnedReadSnapshot {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for GoalOwnedReadSnapshot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

/// Bind a trusted host preparation to the actual target thread, never to a path
/// or thread identifier supplied inside objective text.
pub async fn validate(
    thread_id: &str,
    input: &GoalOwnedResourcesInput,
    config_root: &Path,
) -> io::Result<GoalOwnedResourcesManifest> {
    let manifest = GoalOwnedResourcesManifest {
        source_thread_id: thread_id.to_owned(),
        preparation_id: input.preparation_id.clone(),
        files: input.files.clone(),
    };
    Ok(GoalOwnedResources::restore(&manifest, config_root)
        .await?
        .manifest)
}

impl GoalOwnedResources {
    /// Restart/fork recovery revalidates the captured source preparation. A fork
    /// preserves source_thread_id rather than looking under the target's hash.
    pub async fn restore(
        manifest: &GoalOwnedResourcesManifest,
        config_root: &Path,
    ) -> io::Result<Self> {
        let manifest = manifest.clone();
        let config_root = config_root.to_owned();
        blocking(move || Self::restore_sync(manifest, &config_root)).await
    }

    pub fn manifest(&self) -> &GoalOwnedResourcesManifest {
        &self.manifest
    }

    /// Permission evaluation must still apply explicit deny/ask rules first.
    /// Actual execution must use snapshot_read, not reopen the original path.
    pub async fn allows_read(&self, path: &RuntimePath) -> bool {
        let resources = self.clone();
        let path = path.clone();
        blocking(move || {
            let Some(file) = resources.matching_file(&path)? else {
                return Ok(false);
            };
            resources.verified_bytes(file)?;
            Ok(true)
        })
        .await
        .unwrap_or(false)
    }

    /// Revalidate the final effective input after hooks/approval. A matching but
    /// changed or missing file is an error; only an unlisted path returns None.
    pub async fn snapshot_read(
        &self,
        path: &RuntimePath,
    ) -> io::Result<Option<GoalOwnedReadSnapshot>> {
        let resources = self.clone();
        let path = path.clone();
        blocking(move || {
            let Some(file) = resources.matching_file(&path)? else {
                return Ok(None);
            };
            let bytes = resources.verified_bytes(file)?;
            let snapshot = create_snapshot(&file.name, &bytes)?;
            Ok(Some(snapshot))
        })
        .await
    }

    fn restore_sync(
        mut manifest: GoalOwnedResourcesManifest,
        config_root: &Path,
    ) -> io::Result<Self> {
        validate_manifest(&manifest)?;
        manifest
            .files
            .sort_by(|left, right| left.name.cmp(&right.name));
        if !config_root.is_absolute() {
            return Err(invalid("Goal resource configuration root must be absolute"));
        }
        let canonical_config = crate::fs::node_path::canonicalize_host(config_root)?;
        if !canonical_config.is_dir() {
            return Err(invalid(
                "Goal resource configuration root must be a directory",
            ));
        }
        let suffix = PathBuf::from("goal-objectives")
            .join(format!(
                "{:x}",
                Sha256::digest(manifest.source_thread_id.as_bytes())
            ))
            .join(&manifest.preparation_id);
        let logical_root = config_root.join(&suffix);
        let canonical_root = canonical_config.join(suffix);
        validate_directories(&canonical_config, &canonical_root)?;
        let pinned = PinnedMemoryRoot::pin(&canonical_root)?;
        if pinned.canonical_root() != canonical_root {
            return Err(invalid(
                "Goal preparation directory contains a symbolic link",
            ));
        }
        let resources = Self {
            manifest,
            config_root: canonical_config,
            logical_config_root: config_root.to_owned(),
            logical_root,
            canonical_root,
            pinned,
        };
        for file in &resources.manifest.files {
            resources.verified_bytes(file)?;
        }
        Ok(resources)
    }

    fn matching_file(&self, path: &RuntimePath) -> io::Result<Option<&GoalOwnedResourceFile>> {
        let path = project_runtime_path(path, "owned goal read")
            .map_err(|error| invalid(format!("Invalid owned goal path: {error:?}")))?;
        if !path.is_absolute() {
            return Ok(None);
        }
        Ok(self.manifest.files.iter().find(|file| {
            path == self.canonical_root.join(&file.name)
                || path == self.logical_root.join(&file.name)
        }))
    }

    fn verified_bytes(&self, expected: &GoalOwnedResourceFile) -> io::Result<Vec<u8>> {
        if crate::fs::node_path::canonicalize_host(&self.logical_config_root)? != self.config_root {
            return Err(invalid("Goal resource configuration root changed"));
        }
        validate_directories(&self.config_root, &self.canonical_root)?;
        self.pinned.validate_root_namespace()?;
        let lease = self
            .pinned
            .open_read(self.canonical_root.join(&expected.name))?;
        lease.validate_namespace()?;
        if lease.metadata()?.len() != expected.size_bytes {
            return Err(invalid("Goal resource size changed"));
        }
        let mut file = lease.file();
        let bytes = read_bounded(&mut file, expected)?;
        // The stable handle prevents redirection while copying. These checks
        // also reject replacement of the model-visible name during admission.
        lease.validate_namespace()?;
        self.pinned.validate_root_namespace()?;
        validate_directories(&self.config_root, &self.canonical_root)?;
        Ok(bytes)
    }
}

fn validate_manifest(manifest: &GoalOwnedResourcesManifest) -> io::Result<()> {
    if manifest.source_thread_id.is_empty()
        || manifest.source_thread_id.trim() != manifest.source_thread_id
    {
        return Err(invalid("Goal resource source thread is invalid"));
    }
    let preparation = uuid::Uuid::parse_str(&manifest.preparation_id)
        .map_err(|_| invalid("Goal preparation id must be a canonical UUID"))?;
    if preparation.to_string() != manifest.preparation_id {
        return Err(invalid("Goal preparation id must be a canonical UUID"));
    }
    if manifest.files.is_empty() || manifest.files.len() > MAX_GOAL_RESOURCE_FILES {
        return Err(invalid(
            "Goal resource manifest must contain 1 to 256 files",
        ));
    }
    let mut names = HashSet::new();
    for file in &manifest.files {
        if !valid_name(&file.name) || !names.insert(&file.name) {
            return Err(invalid(
                "Goal resource names must be unique owned filenames",
            ));
        }
        if file.size_bytes > MAX_GOAL_RESOURCE_BYTES {
            return Err(invalid("Goal resource exceeds 10 MiB"));
        }
        if file.sha256.len() != 64
            || !file
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(invalid(
                "Goal resource SHA-256 must be lowercase hexadecimal",
            ));
        }
    }
    Ok(())
}

fn valid_name(name: &str) -> bool {
    if name == "objective.md" {
        return true;
    }
    if let Some(index) = name
        .strip_prefix("paste-")
        .and_then(|value| value.strip_suffix(".txt"))
    {
        return valid_index(index);
    }
    let Some((index, extension)) = name
        .strip_prefix("image-")
        .and_then(|value| value.split_once('.'))
    else {
        return false;
    };
    valid_index(index)
        && (1..=8).contains(&extension.len())
        && extension
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
}

fn valid_index(index: &str) -> bool {
    !index.starts_with('0')
        && index.bytes().all(|byte| byte.is_ascii_digit())
        && index
            .parse::<u64>()
            .is_ok_and(|value| value > 0 && value <= 9_007_199_254_740_991)
}

fn validate_directories(config_root: &Path, preparation: &Path) -> io::Result<()> {
    let mut current = config_root.to_owned();
    let suffix = preparation
        .strip_prefix(config_root)
        .map_err(|_| invalid("Goal preparation escapes its configuration root"))?;
    for component in suffix.components() {
        let std::path::Component::Normal(component) = component else {
            return Err(invalid("Invalid goal preparation directory component"));
        };
        current.push(component);
        let metadata = std::fs::symlink_metadata(&current)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(invalid(
                "Goal preparation requires directories without symbolic links",
            ));
        }
    }
    if crate::fs::node_path::canonicalize_host(preparation)? != preparation {
        return Err(invalid(
            "Goal preparation directory contains a symbolic link",
        ));
    }
    Ok(())
}

fn read_bounded(file: &mut &File, expected: &GoalOwnedResourceFile) -> io::Result<Vec<u8>> {
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::with_capacity(expected.size_bytes as usize);
    file.take(expected.size_bytes + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != expected.size_bytes
        || format!("{:x}", Sha256::digest(&bytes)) != expected.sha256
    {
        return Err(invalid("Goal resource content changed"));
    }
    Ok(bytes)
}

fn create_snapshot(name: &str, bytes: &[u8]) -> io::Result<GoalOwnedReadSnapshot> {
    for _ in 0..32 {
        let directory =
            std::env::temp_dir().join(format!("klaus-goal-read-{}", uuid::Uuid::new_v4()));
        match std::fs::create_dir(&directory) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
        let snapshot = GoalOwnedReadSnapshot {
            path: directory.join(name),
            directory,
        };
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&snapshot.directory, std::fs::Permissions::from_mode(0o700))?;
        }
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        let mut file = options.open(&snapshot.path)?;
        file.write_all(bytes)?;
        file.flush()?;
        return Ok(snapshot);
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "Cannot allocate a private goal read snapshot",
    ))
}

async fn blocking<T: Send + 'static>(
    operation: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| {
            io::Error::other(format!("Goal resource validation worker failed: {error}"))
        })?
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
#[path = "owned_resources_tests.rs"]
mod tests;
