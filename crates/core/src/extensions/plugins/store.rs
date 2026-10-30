//! Versioned plugin package store.
//!
//! The store owns the on-disk package layout, not the UI lifecycle. Installed
//! plugin state can point at this store, while hosts/catalogs remain free to
//! decide how packages are discovered or downloaded.

use std::cmp::Ordering;
use std::future::Future;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::time::{SystemTime, UNIX_EPOCH};

use super::identity::PluginId;
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeEncoder, RuntimeJson,
    RuntimeObjectEncoder, RuntimeString,
};

pub const DEFAULT_PLUGIN_VERSION: &str = "local";

const CACHE_DIR: &str = "cache";
const DATA_DIR: &str = "data";
const GENERATIONS_DIR: &str = ".generations";
const GENERATION_PACKAGE_DIR: &str = "package";
const MANIFEST_FILE: &str = "plugin.json";
const KLAUS_MANIFEST_DIR: &str = ".klaus-plugin";
const CODEX_MANIFEST_DIR: &str = ".codex-plugin";
const CLAUDE_MANIFEST_DIR: &str = ".claude-plugin";

#[derive(Debug, Clone)]
pub struct PluginStore {
    home: PathBuf,
    cache_root: PathBuf,
    data_root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginInstallResult {
    pub plugin_id: PluginId,
    pub version: RuntimeString,
    pub installed_path: RuntimeString,
    pub data_path: RuntimeString,
}

impl RuntimeEncode for PluginInstallResult {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("pluginId", &self.plugin_id)?;
        object.field("version", &self.version)?;
        object.field("installedPath", &self.installed_path)?;
        object.field("dataPath", &self.data_path)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for PluginInstallResult {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                plugin_id: object.required("pluginId", &[])?,
                version: object.required("version", &[])?,
                installed_path: object.required("installedPath", &[])?,
                data_path: object.required("dataPath", &[])?,
            })
        })
    }
}

/// Node/V8 UTF-8 projection at the plugin store's native filesystem boundary.
pub(crate) fn plugin_store_host_utf8_boundary(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8())
        .expect("Node UTF-8 plugin-store host projection is valid UTF-8")
}

impl PluginStore {
    pub fn new(plugin_home: impl AsRef<Path>) -> Self {
        let home = absolutize(plugin_home.as_ref());
        Self {
            cache_root: home.join(CACHE_DIR),
            data_root: home.join(DATA_DIR),
            home,
        }
    }

    pub fn home(&self) -> &Path {
        &self.home
    }

    pub fn cache_root(&self) -> &Path {
        &self.cache_root
    }

    pub fn data_root(&self) -> &Path {
        &self.data_root
    }

    pub fn plugin_base_dir(&self, id: &PluginId) -> PathBuf {
        self.cache_root
            .join(plugin_store_host_utf8_boundary(&id.marketplace_name))
            .join(plugin_store_host_utf8_boundary(&id.plugin_name))
    }

    pub fn plugin_version_dir(
        &self,
        id: &PluginId,
        version: impl AsRef<str>,
    ) -> io::Result<PathBuf> {
        let version = normalize_version(version.as_ref())?;
        Ok(self.plugin_base_dir(id).join(version))
    }

    pub fn plugin_data_dir(&self, id: &PluginId) -> PathBuf {
        let name = id
            .plugin_name
            .concat(&RuntimeString::from("-"))
            .concat(&id.marketplace_name);
        self.data_root.join(plugin_store_host_utf8_boundary(&name))
    }

    /// Discover completed cached versions for compatibility callers. Installed
    /// state must select its exact committed package path from the ledger.
    pub async fn active_version(&self, id: &PluginId) -> io::Result<Option<String>> {
        let base = self.plugin_base_dir(id);
        let mut rd = match tokio::fs::read_dir(&base).await {
            Ok(rd) => rd,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err),
        };
        let mut versions = Vec::new();
        while let Some(entry) = rd.next_entry().await? {
            let version = entry.file_name().to_string_lossy().into_owned();
            if entry.file_type().await?.is_dir()
                && version != GENERATIONS_DIR
                && normalize_version(&version).is_ok()
                && ensure_no_symlink_ancestors(&self.cache_root, &entry.path())
                    .await
                    .is_ok()
                && ensure_plugin_manifest(&entry.path()).await.is_ok()
            {
                versions.push(version);
            }
        }
        let mut generations = match tokio::fs::read_dir(base.join(GENERATIONS_DIR)).await {
            Ok(rd) => Some(rd),
            Err(err) if err.kind() == io::ErrorKind::NotFound => None,
            Err(err) => return Err(err),
        };
        if let Some(rd) = &mut generations {
            while let Some(entry) = rd.next_entry().await? {
                if !entry.file_type().await?.is_dir() {
                    continue;
                }
                let version = entry.file_name().to_string_lossy().into_owned();
                if normalize_version(&version).is_ok()
                    && !self.completed_generations(id, &version).await?.is_empty()
                {
                    versions.push(version);
                }
            }
        }
        if versions.iter().any(|v| v == DEFAULT_PLUGIN_VERSION) {
            return Ok(Some(DEFAULT_PLUGIN_VERSION.to_string()));
        }
        versions.sort_by(|a, b| compare_versions(a, b));
        Ok(versions.pop())
    }

    /// Discover one completed cache generation; this does not choose which
    /// package is installed or committed in the ledger.
    pub async fn active_plugin_dir(&self, id: &PluginId) -> io::Result<Option<PathBuf>> {
        let Some(version) = self.active_version(id).await? else {
            return Ok(None);
        };
        let mut generations = self.completed_generations(id, &version).await?;
        generations.sort();
        if let Some(package) = generations.pop() {
            return Ok(Some(package));
        }
        Ok(Some(self.plugin_version_dir(id, version)?))
    }

    pub async fn install_dir(
        &self,
        id: &PluginId,
        version: Option<&str>,
        source_dir: impl AsRef<Path>,
    ) -> io::Result<PluginInstallResult> {
        let version = normalize_version(version.unwrap_or(DEFAULT_PLUGIN_VERSION))?;
        let source_dir = source_dir.as_ref();
        ensure_plugin_manifest(source_dir).await?;

        let data_path = self.plugin_data_dir(id);
        let version_root = self.generation_version_dir(id, &version)?;
        create_directory_tree_checked(&self.cache_root, &version_root).await?;
        if tokio::fs::canonicalize(&version_root)
            .await?
            .starts_with(tokio::fs::canonicalize(source_dir).await?)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "plugin source contains the destination generation directory",
            ));
        }
        tokio::fs::create_dir_all(&self.data_root).await?;

        // Reserve the parent exclusively before publication. A repeated version
        // receives a different generation, so captured plugin paths stay intact.
        let generation = reserve_generation_dir(&version_root).await?;
        let staging = generation.join(".staging");
        let target = generation.join(GENERATION_PACKAGE_DIR);
        let publication = async {
            copy_dir_checked(source_dir.to_path_buf(), staging.clone()).await?;
            tokio::fs::rename(&staging, &target).await
        }
        .await;
        if let Err(error) = publication {
            if let Err(cleanup) = tokio::fs::remove_dir_all(&generation).await {
                if cleanup.kind() != io::ErrorKind::NotFound {
                    return Err(io::Error::new(
                        error.kind(),
                        format!(
                            "{error}; failed to remove incomplete plugin generation: {cleanup}"
                        ),
                    ));
                }
            }
            return Err(error);
        }

        Ok(PluginInstallResult {
            plugin_id: id.clone(),
            version: RuntimeString::from(version),
            installed_path: RuntimeString::from(target.to_string_lossy().as_ref()),
            data_path: RuntimeString::from(data_path.to_string_lossy().as_ref()),
        })
    }

    /// Remove a newly attempted immutable package without touching any sibling
    /// generation, legacy version, or persistent plugin data.
    pub async fn remove_generation(
        &self,
        id: &PluginId,
        install_path: impl AsRef<Path>,
    ) -> io::Result<()> {
        let install_path = install_path.as_ref();
        if !install_path.is_absolute()
            || install_path
                .components()
                .any(|part| matches!(part, Component::CurDir | Component::ParentDir))
        {
            return Err(invalid_generation_path());
        }
        let generations_root = self.plugin_base_dir(id).join(GENERATIONS_DIR);
        let relative = install_path
            .strip_prefix(&generations_root)
            .map_err(|_| invalid_generation_path())?;
        let parts = relative.components().collect::<Vec<_>>();
        let [Component::Normal(version), Component::Normal(generation), Component::Normal(package)] =
            parts.as_slice()
        else {
            return Err(invalid_generation_path());
        };
        let version = version.to_str().ok_or_else(invalid_generation_path)?;
        normalize_version(version)?;
        if package.to_str() != Some(GENERATION_PACKAGE_DIR)
            || !is_generation_name(generation.to_str().unwrap_or_default())
        {
            return Err(invalid_generation_path());
        }
        ensure_no_symlink_ancestors(&self.cache_root, install_path).await?;
        let generation_root = install_path.parent().ok_or_else(invalid_generation_path)?;
        match tokio::fs::remove_dir_all(generation_root).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err),
        }
    }

    pub async fn uninstall(&self, id: &PluginId, remove_data: bool) -> io::Result<()> {
        match tokio::fs::remove_dir_all(self.plugin_base_dir(id)).await {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        if remove_data {
            match tokio::fs::remove_dir_all(self.plugin_data_dir(id)).await {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    fn generation_version_dir(&self, id: &PluginId, version: &str) -> io::Result<PathBuf> {
        Ok(self
            .plugin_base_dir(id)
            .join(GENERATIONS_DIR)
            .join(normalize_version(version)?))
    }

    async fn completed_generations(
        &self,
        id: &PluginId,
        version: &str,
    ) -> io::Result<Vec<PathBuf>> {
        let root = self.generation_version_dir(id, version)?;
        let mut rd = match tokio::fs::read_dir(root).await {
            Ok(rd) => rd,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(err),
        };
        let mut packages = Vec::new();
        while let Some(entry) = rd.next_entry().await? {
            if !entry.file_type().await?.is_dir()
                || !is_generation_name(&entry.file_name().to_string_lossy())
            {
                continue;
            }
            let package = entry.path().join(GENERATION_PACKAGE_DIR);
            if ensure_no_symlink_ancestors(&self.cache_root, &package)
                .await
                .is_ok()
                && ensure_plugin_manifest(&package).await.is_ok()
            {
                packages.push(package);
            }
        }
        Ok(packages)
    }
}

async fn reserve_generation_dir(version_root: &Path) -> io::Result<PathBuf> {
    for _ in 0..16 {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_nanos();
        let generation = version_root.join(format!(
            "generation-{timestamp:032x}-{:016x}",
            rand::random::<u64>()
        ));
        match tokio::fs::create_dir(&generation).await {
            Ok(()) => return Ok(generation),
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(err),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not reserve a unique plugin generation",
    ))
}

fn is_generation_name(name: &str) -> bool {
    let Some((timestamp, nonce)) = name
        .strip_prefix("generation-")
        .and_then(|suffix| suffix.split_once('-'))
    else {
        return false;
    };
    timestamp.len() == 32
        && nonce.len() == 16
        && timestamp.bytes().all(|byte| byte.is_ascii_hexdigit())
        && nonce.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn invalid_generation_path() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "plugin cleanup path is not a generated package for this plugin",
    )
}

async fn ensure_no_symlink_ancestors(root: &Path, path: &Path) -> io::Result<()> {
    ensure_within(root, path)?;
    let mut current = root.to_path_buf();
    let mut paths = vec![current.clone()];
    for component in path
        .strip_prefix(root)
        .map_err(|_| invalid_generation_path())?
        .components()
    {
        current.push(component.as_os_str());
        paths.push(current.clone());
    }
    for path in paths {
        match tokio::fs::symlink_metadata(&path).await {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "plugin generation path contains a symbolic link",
                ));
            }
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => break,
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

async fn create_directory_tree_checked(root: &Path, path: &Path) -> io::Result<()> {
    ensure_no_symlink_ancestors(root, path).await?;
    tokio::fs::create_dir_all(path).await?;
    ensure_no_symlink_ancestors(root, path).await
}

fn normalize_version(value: &str) -> io::Result<String> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.starts_with('.')
        || value.contains('/')
        || value.contains('\\')
        || value.chars().any(char::is_control)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid plugin version segment: {value}"),
        ));
    }
    Ok(value.to_string())
}

async fn ensure_plugin_manifest(source_dir: &Path) -> io::Result<()> {
    for path in manifest_candidates(source_dir) {
        match tokio::fs::metadata(&path).await {
            Ok(metadata) if metadata.is_file() => return Ok(()),
            Ok(_) => continue,
            Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "plugin package has no plugin.json manifest",
    ))
}

fn manifest_candidates(source_dir: &Path) -> [PathBuf; 4] {
    [
        source_dir.join(KLAUS_MANIFEST_DIR).join(MANIFEST_FILE),
        source_dir.join(CODEX_MANIFEST_DIR).join(MANIFEST_FILE),
        source_dir.join(CLAUDE_MANIFEST_DIR).join(MANIFEST_FILE),
        source_dir.join(MANIFEST_FILE),
    ]
}

fn copy_dir_checked(
    source: PathBuf,
    target: PathBuf,
) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send>> {
    Box::pin(async move {
        tokio::fs::create_dir_all(&target).await?;
        let mut rd = tokio::fs::read_dir(&source).await?;
        while let Some(entry) = rd.next_entry().await? {
            let file_type = entry.file_type().await?;
            let src = entry.path();
            let dst = target.join(entry.file_name());
            if file_type.is_dir() {
                copy_dir_checked(src, dst).await?;
            } else if file_type.is_file() {
                tokio::fs::copy(&src, &dst).await?;
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "plugin package contains unsupported entry: {}",
                        src.to_string_lossy()
                    ),
                ));
            }
        }
        Ok(())
    })
}

fn compare_versions(a: &str, b: &str) -> Ordering {
    let parsed_a = parse_semver_like(a);
    let parsed_b = parse_semver_like(b);
    match (parsed_a, parsed_b) {
        (Some(a), Some(b)) => a.cmp(&b),
        _ => a.cmp(b),
    }
}

fn parse_semver_like(value: &str) -> Option<(u64, u64, u64, String)> {
    let mut parts = value.splitn(2, '-');
    let core = parts.next()?;
    let suffix = parts.next().unwrap_or_default().to_string();
    let mut nums = core.split('.');
    let major = nums.next()?.parse().ok()?;
    let minor = nums.next().unwrap_or("0").parse().ok()?;
    let patch = nums.next().unwrap_or("0").parse().ok()?;
    if nums.next().is_some() {
        return None;
    }
    Some((major, minor, patch, suffix))
}

fn ensure_within(root: &Path, target: &Path) -> io::Result<()> {
    let root = absolutize(root);
    let target = absolutize(target);
    if target.starts_with(root) {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "plugin store target escapes cache root",
        ))
    }
}

fn absolutize(path: &Path) -> PathBuf {
    if path.is_absolute() {
        normalize_path(path)
    } else {
        normalize_path(
            &std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(path),
        )
    }
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extensions::command_loader::CommandLoadOptions;
    use crate::extensions::plugins::loader::load_one_plugin;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn temp_dir() -> PathBuf {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "ae-plugin-store-{}-{}-{:016x}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn plugin_dir(name: &str, version: &str) -> PathBuf {
        let dir = temp_dir();
        std::fs::write(
            dir.join("plugin.json"),
            format!(r#"{{"name":"{name}","version":"{version}"}}"#),
        )
        .unwrap();
        dir
    }

    #[tokio::test]
    async fn installs_plugin_package_under_marketplace_version_cache() {
        let home = temp_dir();
        let store = PluginStore::new(&home);
        let source = plugin_dir("docs", "1.2.3");
        std::fs::create_dir_all(source.join("skills")).unwrap();
        std::fs::write(source.join("skills").join("readme.md"), "# Readme").unwrap();
        let id = PluginId::parse("docs@bundled").unwrap();

        let result = store
            .install_dir(&id, Some("1.2.3"), &source)
            .await
            .unwrap();

        let expected = home
            .join("cache")
            .join("bundled")
            .join("docs")
            .join(GENERATIONS_DIR)
            .join("1.2.3");
        let package = PathBuf::from(plugin_store_host_utf8_boundary(&result.installed_path));
        assert_eq!(
            package.parent().and_then(Path::parent),
            Some(expected.as_path())
        );
        assert_eq!(package.file_name().unwrap(), GENERATION_PACKAGE_DIR);
        assert_eq!(result.version, "1.2.3");
        assert_eq!(
            PathBuf::from(plugin_store_host_utf8_boundary(&result.data_path)),
            home.join("data").join("docs-bundled")
        );
        assert!(package.join("skills").join("readme.md").exists());
        let installed_path = plugin_store_host_utf8_boundary(&result.installed_path);
        let loaded = load_one_plugin(&installed_path, CommandLoadOptions::default())
            .await
            .unwrap();
        assert_eq!(loaded.name, "docs");
        assert_eq!(loaded.version.as_deref(), Some("1.2.3"));
    }

    #[tokio::test]
    async fn repeated_version_publishes_immutable_generations_and_keeps_data() {
        let home = temp_dir();
        let store = PluginStore::new(&home);
        let id = PluginId::parse("docs@local").unwrap();
        let source = plugin_dir("docs", "1.0.0");
        std::fs::write(source.join("content.txt"), "original").unwrap();
        let original = store
            .install_dir(&id, Some("1.0.0"), &source)
            .await
            .unwrap();
        let original_path =
            PathBuf::from(plugin_store_host_utf8_boundary(&original.installed_path));
        let data_path = PathBuf::from(plugin_store_host_utf8_boundary(&original.data_path));
        std::fs::create_dir_all(&data_path).unwrap();
        std::fs::write(data_path.join("state.json"), "saved state").unwrap();

        std::fs::write(source.join("content.txt"), "updated").unwrap();
        let updated = store
            .install_dir(&id, Some("1.0.0"), &source)
            .await
            .unwrap();
        let updated_path = PathBuf::from(plugin_store_host_utf8_boundary(&updated.installed_path));

        assert_eq!(original.version, updated.version);
        assert_ne!(original_path, updated_path);
        assert_eq!(original.data_path, updated.data_path);
        assert_eq!(
            std::fs::read_to_string(original_path.join("content.txt")).unwrap(),
            "original"
        );
        assert_eq!(
            std::fs::read_to_string(updated_path.join("content.txt")).unwrap(),
            "updated"
        );
        assert_eq!(
            std::fs::read_to_string(data_path.join("state.json")).unwrap(),
            "saved state"
        );
        store.remove_generation(&id, &updated_path).await.unwrap();
        store.remove_generation(&id, &updated_path).await.unwrap();
        assert!(!updated_path.parent().unwrap().exists());
        assert!(original_path.join("plugin.json").exists());
        assert_eq!(
            std::fs::read_to_string(data_path.join("state.json")).unwrap(),
            "saved state"
        );
        assert_eq!(
            store.active_plugin_dir(&id).await.unwrap(),
            Some(original_path)
        );
    }

    #[tokio::test]
    async fn generation_cleanup_preserves_legacy_versions_and_other_plugins() {
        let home = temp_dir();
        let store = PluginStore::new(&home);
        let id = PluginId::parse("docs@local").unwrap();
        let legacy = store.plugin_version_dir(&id, "1.0.0").unwrap();
        std::fs::create_dir_all(&legacy).unwrap();
        std::fs::write(
            legacy.join("plugin.json"),
            r#"{"name":"docs","version":"1.0.0"}"#,
        )
        .unwrap();
        std::fs::write(legacy.join("content.txt"), "legacy").unwrap();
        let other_id = PluginId::parse("other@local").unwrap();
        let other = store
            .install_dir(&other_id, Some("1.0.0"), plugin_dir("other", "1.0.0"))
            .await
            .unwrap();
        let other_path = PathBuf::from(plugin_store_host_utf8_boundary(&other.installed_path));
        let candidate = store
            .install_dir(&id, Some("1.0.0"), plugin_dir("docs", "1.0.0"))
            .await
            .unwrap();
        let candidate_path =
            PathBuf::from(plugin_store_host_utf8_boundary(&candidate.installed_path));

        for invalid in [
            legacy.clone(),
            other_path.clone(),
            store.plugin_data_dir(&id),
            candidate_path.parent().unwrap().to_path_buf(),
            candidate_path
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .to_path_buf(),
            candidate_path.join("extra"),
            candidate_path.parent().unwrap().join("..").join("package"),
        ] {
            assert!(store.remove_generation(&id, invalid).await.is_err());
        }
        assert!(candidate_path.join("plugin.json").exists());
        store.remove_generation(&id, &candidate_path).await.unwrap();
        assert_eq!(
            std::fs::read_to_string(legacy.join("content.txt")).unwrap(),
            "legacy"
        );
        assert!(other_path.join("plugin.json").exists());
        assert_eq!(store.active_plugin_dir(&id).await.unwrap(), Some(legacy));
    }

    #[tokio::test]
    async fn compatibility_discovery_ignores_incomplete_and_invalid_generations() {
        let home = temp_dir();
        let store = PluginStore::new(&home);
        let id = PluginId::parse("docs@local").unwrap();
        let installed = store
            .install_dir(&id, Some("1.0.0"), plugin_dir("docs", "1.0.0"))
            .await
            .unwrap();
        let incomplete_version = store.generation_version_dir(&id, "99.0.0").unwrap();
        std::fs::create_dir_all(&incomplete_version).unwrap();
        let incomplete = reserve_generation_dir(&incomplete_version).await.unwrap();
        std::fs::create_dir(incomplete.join(".staging")).unwrap();
        std::fs::write(
            incomplete.join(".staging/plugin.json"),
            r#"{"name":"docs"}"#,
        )
        .unwrap();
        std::fs::create_dir_all(store.plugin_base_dir(&id).join(".invalid")).unwrap();
        std::fs::create_dir_all(store.plugin_base_dir(&id).join("100.0.0")).unwrap();
        let malformed = incomplete_version.join("not-a-generation/package");
        std::fs::create_dir_all(&malformed).unwrap();
        std::fs::write(malformed.join("plugin.json"), r#"{"name":"docs"}"#).unwrap();

        assert_eq!(
            store.active_version(&id).await.unwrap().as_deref(),
            Some("1.0.0")
        );
        assert_eq!(
            store.active_plugin_dir(&id).await.unwrap(),
            Some(PathBuf::from(plugin_store_host_utf8_boundary(
                &installed.installed_path
            )))
        );
    }

    #[tokio::test]
    async fn compatibility_discovery_sorts_completed_generation_names() {
        let home = temp_dir();
        let store = PluginStore::new(&home);
        let id = PluginId::parse("docs@local").unwrap();
        let root = store.generation_version_dir(&id, "1.0.0").unwrap();
        let mut expected = None;
        for timestamp in [1_u128, 2] {
            let package = root
                .join(format!("generation-{timestamp:032x}-0000000000000000"))
                .join(GENERATION_PACKAGE_DIR);
            std::fs::create_dir_all(&package).unwrap();
            std::fs::write(package.join("plugin.json"), r#"{"name":"docs"}"#).unwrap();
            expected = Some(package);
        }
        assert_eq!(store.active_plugin_dir(&id).await.unwrap(), expected);
    }

    #[tokio::test]
    async fn rejects_source_containing_destination_to_prevent_recursive_copy() {
        let home = temp_dir();
        let store = PluginStore::new(&home);
        let id = PluginId::parse("docs@local").unwrap();
        std::fs::write(home.join("plugin.json"), r#"{"name":"docs"}"#).unwrap();
        assert!(store.install_dir(&id, Some("1.0.0"), &home).await.is_err());
        assert!(store
            .completed_generations(&id, "1.0.0")
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn active_version_prefers_local_then_highest_semver_like_version() {
        let home = temp_dir();
        let store = PluginStore::new(&home);
        let id = PluginId::parse("docs@local").unwrap();
        store
            .install_dir(&id, Some("1.2.0"), plugin_dir("docs", "1.2.0"))
            .await
            .unwrap();
        store
            .install_dir(&id, Some("1.10.0"), plugin_dir("docs", "1.10.0"))
            .await
            .unwrap();
        assert_eq!(
            store.active_version(&id).await.unwrap().as_deref(),
            Some("1.10.0")
        );

        store
            .install_dir(&id, None, plugin_dir("docs", "local"))
            .await
            .unwrap();
        assert_eq!(
            store.active_version(&id).await.unwrap().as_deref(),
            Some(DEFAULT_PLUGIN_VERSION)
        );
    }

    #[tokio::test]
    async fn rejects_missing_manifest_and_unsafe_version_segments() {
        let home = temp_dir();
        let store = PluginStore::new(&home);
        let source = temp_dir();
        let id = PluginId::parse("docs@local").unwrap();

        assert!(store
            .install_dir(&id, Some("1.0.0"), &source)
            .await
            .is_err());
        assert!(store
            .install_dir(&id, Some("../1"), plugin_dir("docs", "1"))
            .await
            .is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_symlinks_inside_plugin_packages() {
        let home = temp_dir();
        let store = PluginStore::new(&home);
        let source = plugin_dir("docs", "1.0.0");
        let id = PluginId::parse("docs@local").unwrap();
        let previous = store
            .install_dir(&id, Some("1.0.0"), &source)
            .await
            .unwrap();
        let previous_path =
            PathBuf::from(plugin_store_host_utf8_boundary(&previous.installed_path));
        std::os::unix::fs::symlink(source.join("plugin.json"), source.join("linked.json")).unwrap();

        assert!(store
            .install_dir(&id, Some("1.0.0"), &source)
            .await
            .is_err());
        assert!(previous_path.join("plugin.json").exists());
        let version_root = store.generation_version_dir(&id, "1.0.0").unwrap();
        let generations = std::fs::read_dir(&version_root)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(generations.len(), 1);
        assert!(!generations[0].path().join(".staging").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn install_and_cleanup_refuse_symlinked_generation_paths() {
        let home = temp_dir();
        let store = PluginStore::new(&home);
        let id = PluginId::parse("docs@local").unwrap();
        let installed = store
            .install_dir(&id, Some("1.0.0"), plugin_dir("docs", "1.0.0"))
            .await
            .unwrap();
        let package = PathBuf::from(plugin_store_host_utf8_boundary(&installed.installed_path));
        let outside = temp_dir();
        std::fs::write(outside.join("sentinel"), "outside data").unwrap();
        std::fs::remove_dir_all(&package).unwrap();
        std::os::unix::fs::symlink(&outside, &package).unwrap();
        assert!(store.remove_generation(&id, &package).await.is_err());
        assert_eq!(
            std::fs::read_to_string(outside.join("sentinel")).unwrap(),
            "outside data"
        );
        assert!(package.parent().unwrap().exists());

        let unsafe_version = store.generation_version_dir(&id, "2.0.0").unwrap();
        std::os::unix::fs::symlink(&outside, &unsafe_version).unwrap();
        assert!(store
            .install_dir(&id, Some("2.0.0"), plugin_dir("docs", "2.0.0"))
            .await
            .is_err());
        assert_eq!(std::fs::read_dir(&outside).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn uninstall_removes_package_cache_and_optionally_data() {
        let home = temp_dir();
        let store = PluginStore::new(&home);
        let id = PluginId::parse("docs@local").unwrap();
        let result = store
            .install_dir(&id, Some("1.0.0"), plugin_dir("docs", "1.0.0"))
            .await
            .unwrap();
        let data_path = plugin_store_host_utf8_boundary(&result.data_path);
        std::fs::create_dir_all(&data_path).unwrap();
        std::fs::write(PathBuf::from(&data_path).join("state.json"), "{}").unwrap();

        store.uninstall(&id, true).await.unwrap();

        assert!(!PathBuf::from(plugin_store_host_utf8_boundary(&result.installed_path)).exists());
        assert!(!PathBuf::from(plugin_store_host_utf8_boundary(&result.data_path)).exists());
    }
}
