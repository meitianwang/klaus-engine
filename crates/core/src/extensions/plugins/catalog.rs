//! Host-injected plugin catalog contract.
//!
//! The engine owns the model-facing discovery/install tools and the human
//! confirmation gate. Hosts own the actual catalog source, marketplace policy,
//! download/cache behavior, and refresh strategy.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use async_trait::async_trait;

use crate::extensions::command_loader::CommandLoadOptions;
use crate::extensions::plugins::identity::PluginId;
use crate::extensions::plugins::installed::{
    default_plugin_home, read_installed_plugins, record_installed_plugin_package,
};
use crate::extensions::plugins::loader::load_one_plugin;
use crate::extensions::plugins::loader::LoadedPlugin;
use crate::extensions::plugins::store::{PluginStore, DEFAULT_PLUGIN_VERSION};
use crate::fs::node_path::runtime_string_from_native;
use crate::runtime_values::RuntimeString;
use crate::types::abort::AbortSignal;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginCatalogEntry {
    pub slug: RuntimeString,
    pub name: RuntimeString,
    pub description: RuntimeString,
    pub source: Option<RuntimeString>,
    pub version: Option<RuntimeString>,
    pub installed: bool,
    pub skills: Vec<RuntimeString>,
    pub runtime_capabilities: Vec<RuntimeString>,
}

#[derive(Clone, Default)]
pub struct PluginCatalogSearchOptions {
    pub sources: Option<Vec<RuntimeString>>,
    pub max_results: Option<usize>,
    pub signal: Option<AbortSignal>,
}

#[derive(Clone, Default)]
pub struct PluginInstallFromCatalogOptions {
    pub version: Option<RuntimeString>,
    pub install_dir: Option<String>,
    pub signal: Option<AbortSignal>,
}

#[async_trait]
pub trait PluginCatalog: Send + Sync {
    async fn search(
        &self,
        query: &RuntimeString,
        opts: PluginCatalogSearchOptions,
    ) -> Result<Vec<PluginCatalogEntry>, BoxError>;

    async fn get(
        &self,
        slug: &RuntimeString,
        signal: Option<AbortSignal>,
    ) -> Result<Option<PluginCatalogEntry>, BoxError>;

    async fn install(
        &self,
        slug: &RuntimeString,
        opts: PluginInstallFromCatalogOptions,
    ) -> Result<Option<LoadedPlugin>, BoxError>;
}

pub const KLAUS_PLUGIN_CATALOG_DIR_ENV: &str = "KLAUS_PLUGIN_CATALOG_DIR";

/// A concrete filesystem-backed plugin catalog.
///
/// Each direct child of `root` can be a real plugin package directory. Searching
/// loads those packages through the normal plugin loader; installing copies the
/// selected package into [`PluginStore`] and records it in the installed-plugin
/// ledger. This gives core a usable plugin source without hard-coding a network
/// marketplace.
#[derive(Debug, Clone)]
pub struct LocalPluginCatalog {
    root: PathBuf,
    plugin_home: PathBuf,
    marketplace: String,
}

impl LocalPluginCatalog {
    pub fn new(root: impl AsRef<Path>, plugin_home: impl AsRef<Path>) -> Self {
        Self::with_marketplace(root, plugin_home, "local")
    }

    pub fn with_marketplace(
        root: impl AsRef<Path>,
        plugin_home: impl AsRef<Path>,
        marketplace: impl Into<String>,
    ) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            plugin_home: plugin_home.as_ref().to_path_buf(),
            marketplace: marketplace.into(),
        }
    }

    async fn available_plugins(&self) -> Result<Vec<(PathBuf, LoadedPlugin)>, BoxError> {
        let mut plugins = Vec::new();
        if let Some(plugin) = load_plugin_from_dir(&self.root).await {
            plugins.push((self.root.clone(), plugin));
            return Ok(plugins);
        }

        let mut rd = match tokio::fs::read_dir(&self.root).await {
            Ok(rd) => rd,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(Box::new(err)),
        };
        while let Some(entry) = rd.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let dir = entry.path();
            if let Some(plugin) = load_plugin_from_dir(&dir).await {
                plugins.push((dir, plugin));
            }
        }
        plugins.sort_by(|(_, a), (_, b)| a.name.cmp(&b.name));
        Ok(plugins)
    }

    fn id_for(&self, plugin: &LoadedPlugin) -> Result<PluginId, BoxError> {
        PluginId::new(plugin.name.clone(), self.marketplace.clone())
            .map_err(|err| Box::new(err) as BoxError)
    }

    async fn installed_keys(&self, plugin_home: &Path) -> Result<Vec<RuntimeString>, BoxError> {
        let state = read_installed_plugins(plugin_home).await?;
        Ok(state.plugins.keys().cloned().collect())
    }
}

#[async_trait]
impl PluginCatalog for LocalPluginCatalog {
    async fn search(
        &self,
        query: &RuntimeString,
        opts: PluginCatalogSearchOptions,
    ) -> Result<Vec<PluginCatalogEntry>, BoxError> {
        check_abort(opts.signal.as_ref())?;
        if let Some(sources) = &opts.sources {
            if !sources.iter().any(|source| source == &self.marketplace) {
                return Ok(Vec::new());
            }
        }
        let installed = self.installed_keys(&self.plugin_home).await?;
        let query = lowercase_runtime(query);
        let mut out = Vec::new();
        for (_dir, plugin) in self.available_plugins().await? {
            check_abort(opts.signal.as_ref())?;
            let id = self.id_for(&plugin)?;
            if !query.is_empty() && !plugin_matches_query(&plugin, &query, &id) {
                continue;
            }
            out.push(entry_from_plugin(plugin, id, &installed));
            if opts.max_results.is_some_and(|max| out.len() >= max) {
                break;
            }
        }
        Ok(out)
    }

    async fn get(
        &self,
        slug: &RuntimeString,
        signal: Option<AbortSignal>,
    ) -> Result<Option<PluginCatalogEntry>, BoxError> {
        check_abort(signal.as_ref())?;
        let installed = self.installed_keys(&self.plugin_home).await?;
        for (_dir, plugin) in self.available_plugins().await? {
            let id = self.id_for(&plugin)?;
            if slug == &plugin.name || slug == &id.as_key() {
                return Ok(Some(entry_from_plugin(plugin, id, &installed)));
            }
        }
        Ok(None)
    }

    async fn install(
        &self,
        slug: &RuntimeString,
        opts: PluginInstallFromCatalogOptions,
    ) -> Result<Option<LoadedPlugin>, BoxError> {
        check_abort(opts.signal.as_ref())?;
        let plugin_home = opts
            .install_dir
            .as_deref()
            .map(PathBuf::from)
            .unwrap_or_else(|| self.plugin_home.clone());
        let plugins = self.available_plugins().await?;
        let catalog = IndexedPlugins::build(&self.marketplace, plugins)?;
        let Some(target_idx) = catalog.lookup(slug) else {
            return Ok(None);
        };
        let install_order = catalog.install_order(target_idx)?;
        let store = PluginStore::new(&plugin_home);
        let mut installed_target = None;
        for idx in install_order {
            check_abort(opts.signal.as_ref())?;
            let (dir, plugin, id) = catalog.plugin(idx);
            let version = if idx == target_idx {
                opts.version.clone().unwrap_or_else(|| {
                    RuntimeString::from(
                        plugin
                            .version
                            .clone()
                            .unwrap_or_else(|| DEFAULT_PLUGIN_VERSION.to_string()),
                    )
                })
            } else {
                RuntimeString::from(
                    plugin
                        .version
                        .clone()
                        .unwrap_or_else(|| DEFAULT_PLUGIN_VERSION.to_string()),
                )
            };
            let version = node_utf8_catalog_boundary(&version);
            let installed = store.install_dir(id, Some(&version), dir).await?;
            let installed_path = node_utf8_catalog_boundary(&installed.installed_path);
            let plugin = load_one_plugin(&installed_path, CommandLoadOptions::default())
                .await
                .ok_or_else(|| {
                    let err: BoxError = "installed plugin package has no loadable manifest".into();
                    err
                })?;
            record_installed_plugin_package(
                &plugin_home,
                &plugin,
                id,
                installed.version,
                Some(installed.data_path),
                RuntimeString::from(format!(
                    "catalog:{}",
                    node_utf8_catalog_boundary(&id.as_key())
                )),
            )
            .await?;
            if idx == target_idx {
                installed_target = Some(plugin);
            }
        }
        Ok(installed_target)
    }
}

struct IndexedPlugins {
    entries: Vec<(PathBuf, LoadedPlugin, PluginId)>,
    by_key: HashMap<RuntimeString, usize>,
}

impl IndexedPlugins {
    fn build(marketplace: &str, plugins: Vec<(PathBuf, LoadedPlugin)>) -> Result<Self, BoxError> {
        let mut entries = Vec::new();
        let mut by_key = HashMap::new();
        for (dir, plugin) in plugins {
            let id = PluginId::new(plugin.name.clone(), marketplace.to_string())?;
            let idx = entries.len();
            by_key.insert(RuntimeString::from(plugin.name.clone()), idx);
            by_key.insert(id.as_key(), idx);
            entries.push((dir, plugin, id));
        }
        Ok(Self { entries, by_key })
    }

    fn lookup(&self, slug: &RuntimeString) -> Option<usize> {
        self.by_key.get(slug).copied()
    }

    fn plugin(&self, idx: usize) -> (&Path, &LoadedPlugin, &PluginId) {
        let (dir, plugin, id) = &self.entries[idx];
        (dir.as_path(), plugin, id)
    }

    fn install_order(&self, target_idx: usize) -> Result<Vec<usize>, BoxError> {
        let mut visiting = HashSet::new();
        let mut visited = HashSet::new();
        let mut out = Vec::new();
        self.visit(target_idx, &mut visiting, &mut visited, &mut out)?;
        Ok(out)
    }

    fn visit(
        &self,
        idx: usize,
        visiting: &mut HashSet<usize>,
        visited: &mut HashSet<usize>,
        out: &mut Vec<usize>,
    ) -> Result<(), BoxError> {
        if visited.contains(&idx) {
            return Ok(());
        }
        if !visiting.insert(idx) {
            let (_, _, id) = self.plugin(idx);
            return Err(format!(
                "plugin dependency cycle includes {}",
                node_utf8_catalog_boundary(&id.as_key())
            )
            .into());
        }
        let (_, plugin, id) = self.plugin(idx);
        for dependency in &plugin.dependencies {
            let dep_idx = self.lookup_dependency(dependency, &id.marketplace_name)?;
            self.visit(dep_idx, visiting, visited, out)?;
        }
        visiting.remove(&idx);
        visited.insert(idx);
        out.push(idx);
        Ok(())
    }

    fn lookup_dependency(
        &self,
        dependency: &str,
        marketplace: &RuntimeString,
    ) -> Result<usize, BoxError> {
        if let Some(idx) = self.lookup(&RuntimeString::from(dependency)) {
            return Ok(idx);
        }
        if !dependency.contains('@') {
            let scoped = RuntimeString::from(dependency)
                .concat(&RuntimeString::from("@"))
                .concat(marketplace);
            if let Some(idx) = self.lookup(&scoped) {
                return Ok(idx);
            }
        }
        Err(format!("plugin dependency not found in local catalog: {dependency}").into())
    }
}

pub fn default_plugin_catalog() -> Option<Box<dyn PluginCatalog>> {
    let root = std::env::var_os(KLAUS_PLUGIN_CATALOG_DIR_ENV)?;
    Some(Box::new(LocalPluginCatalog::new(
        PathBuf::from(root),
        default_plugin_home(),
    )))
}

async fn load_plugin_from_dir(dir: &Path) -> Option<LoadedPlugin> {
    let dir = node_utf8_plugin_loader_path_boundary(&runtime_string_from_native(dir.as_os_str()));
    load_one_plugin(&dir, CommandLoadOptions::default()).await
}

fn check_abort(signal: Option<&AbortSignal>) -> Result<(), BoxError> {
    if signal.is_some_and(AbortSignal::is_cancelled) {
        Err("plugin catalog request aborted".into())
    } else {
        Ok(())
    }
}

fn plugin_matches_query(plugin: &LoadedPlugin, query: &RuntimeString, id: &PluginId) -> bool {
    scalar_matches_query(&plugin.name, query)
        || runtime_matches_query(&id.as_key(), query)
        || scalar_matches_query(plugin.description.as_deref().unwrap_or_default(), query)
        || plugin
            .keywords
            .iter()
            .any(|keyword| scalar_matches_query(keyword, query))
        || plugin
            .skills
            .iter()
            .any(|skill| runtime_matches_query(&skill.name, query))
        || plugin
            .runtime_capabilities
            .iter()
            .any(|capability| scalar_matches_query(capability, query))
}

fn scalar_matches_query(value: &str, query: &RuntimeString) -> bool {
    runtime_matches_query(&RuntimeString::from(value), query)
}

fn runtime_matches_query(value: &RuntimeString, query: &RuntimeString) -> bool {
    contains_runtime(&lowercase_runtime(value), query)
}

fn entry_from_plugin(
    plugin: LoadedPlugin,
    id: PluginId,
    installed_keys: &[RuntimeString],
) -> PluginCatalogEntry {
    let slug = id.as_key();
    let installed = installed_keys.iter().any(|key| key == &slug);
    PluginCatalogEntry {
        slug,
        name: RuntimeString::from(
            plugin
                .display_name
                .clone()
                .unwrap_or_else(|| plugin.name.clone()),
        ),
        description: RuntimeString::from(plugin.description.unwrap_or_default()),
        source: Some(id.marketplace_name),
        version: plugin.version.map(RuntimeString::from),
        installed,
        skills: plugin.skills.into_iter().map(|skill| skill.name).collect(),
        runtime_capabilities: plugin
            .runtime_capabilities
            .into_iter()
            .map(RuntimeString::from)
            .collect(),
    }
}

fn lowercase_runtime(value: &RuntimeString) -> RuntimeString {
    let mut output = Vec::with_capacity(value.len_utf16());
    let mut scalar_run = String::new();
    for point in value.code_points() {
        let Some(character) = char::from_u32(point) else {
            output.extend(scalar_run.to_lowercase().encode_utf16());
            scalar_run.clear();
            output.push(point as u16);
            continue;
        };
        scalar_run.push(character);
    }
    output.extend(scalar_run.to_lowercase().encode_utf16());
    RuntimeString::from_utf16(output)
}

fn contains_runtime(haystack: &RuntimeString, needle: &RuntimeString) -> bool {
    needle.is_empty()
        || haystack
            .as_utf16()
            .windows(needle.len_utf16())
            .any(|window| window == needle.as_utf16())
}

fn node_utf8_catalog_boundary(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8())
        .expect("Node UTF-8 plugin catalog boundary is valid UTF-8")
}

fn node_utf8_plugin_loader_path_boundary(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8())
        .expect("Node UTF-8 plugin loader path projection is valid UTF-8")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn temp_dir() -> PathBuf {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "ae-local-plugin-catalog-{}-{}-{:016x}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_plugin(root: &Path, name: &str, version: &str) -> PathBuf {
        write_plugin_with_dependencies(root, name, version, &[])
    }

    fn write_plugin_with_dependencies(
        root: &Path,
        name: &str,
        version: &str,
        dependencies: &[&str],
    ) -> PathBuf {
        let dir = root.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let dependencies = serde_json::to_string(dependencies).unwrap();
        std::fs::write(
            dir.join("plugin.json"),
            format!(
                r#"{{
                    "name":"{name}",
                    "version":"{version}",
                    "description":"{name} plugin",
                    "keywords":["{name}"],
                    "dependencies":{dependencies}
                }}"#
            ),
        )
        .unwrap();
        dir
    }

    #[tokio::test]
    async fn local_catalog_searches_real_plugin_directories() {
        let catalog_root = temp_dir();
        let plugin_home = temp_dir().join("plugins");
        write_plugin(&catalog_root, "docs", "1.0.0");
        write_plugin(&catalog_root, "browser", "1.0.0");
        let catalog = LocalPluginCatalog::new(&catalog_root, &plugin_home);

        let found = catalog
            .search(
                &RuntimeString::from("docs"),
                PluginCatalogSearchOptions {
                    max_results: Some(10),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(found.len(), 1);
        assert_eq!(found[0].slug, "docs@local");
        assert_eq!(found[0].version, Some(RuntimeString::from("1.0.0")));
        assert!(!found[0].installed);
    }

    #[tokio::test]
    async fn local_catalog_installs_into_store_and_installed_ledger() {
        let catalog_root = temp_dir();
        let plugin_home = temp_dir().join("plugins");
        write_plugin(&catalog_root, "docs", "1.2.3");
        let catalog = LocalPluginCatalog::new(&catalog_root, &plugin_home);

        let plugin = catalog
            .install(
                &RuntimeString::from("docs@local"),
                PluginInstallFromCatalogOptions::default(),
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(plugin.name, "docs");
        assert!(Path::new(&plugin.dir).join("plugin.json").exists());
        let state = read_installed_plugins(&plugin_home).await.unwrap();
        assert!(state
            .plugins
            .contains_key(&RuntimeString::from("docs@local")));
        assert_eq!(
            node_utf8_catalog_boundary(
                &state.plugins[&RuntimeString::from("docs@local")].install_path
            ),
            plugin.dir
        );
        assert_eq!(
            state.plugins[&RuntimeString::from("docs@local")]
                .version
                .as_ref(),
            Some(&RuntimeString::from("1.2.3"))
        );

        let entry = catalog
            .get(&RuntimeString::from("docs"), None)
            .await
            .unwrap()
            .unwrap();
        assert!(entry.installed);
    }

    #[tokio::test]
    async fn local_catalog_installs_dependencies_before_the_target() {
        let catalog_root = temp_dir();
        let plugin_home = temp_dir().join("plugins");
        write_plugin(&catalog_root, "helper", "1.0.0");
        write_plugin_with_dependencies(&catalog_root, "main", "1.0.0", &["helper@local"]);
        let catalog = LocalPluginCatalog::new(&catalog_root, &plugin_home);

        let plugin = catalog
            .install(
                &RuntimeString::from("main@local"),
                PluginInstallFromCatalogOptions::default(),
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(plugin.name, "main");
        let state = read_installed_plugins(&plugin_home).await.unwrap();
        assert!(state
            .plugins
            .contains_key(&RuntimeString::from("helper@local")));
        let main = state
            .plugins
            .get(&RuntimeString::from("main@local"))
            .unwrap();
        assert_eq!(main.dependencies, vec![RuntimeString::from("helper@local")]);
    }
}
