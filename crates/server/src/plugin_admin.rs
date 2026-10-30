//! Installed-plugin bootstrap and endpoint-scoped protocol administration.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tokio::sync::Mutex;

use klaus_agent_core::engine::agent::Agent;
use klaus_agent_core::engine::rewind::BoxError;
use klaus_agent_core::extensions::command_loader::CommandLoadOptions;
use klaus_agent_core::extensions::plugins::identity::PluginId;
use klaus_agent_core::extensions::plugins::installed::{
    is_builtin_record, load_installed_plugins_report, read_installed_plugins,
    read_installed_plugins_with_version, record_installed_plugin_package_if_version,
    remove_installed_plugin_files, set_installed_plugin_enabled, uninstall_installed_plugin,
    write_installed_plugins_if_version, InstalledPluginRecord, InstalledPlugins,
};
use klaus_agent_core::extensions::plugins::loader::{
    load_one_plugin, LoadedPlugin, PluginLoadReport,
};
use klaus_agent_core::extensions::plugins::store::PluginStore;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_protocol::{
    InstalledPluginLedgerMutation, ProtocolPluginAdmin, ProtocolPluginCatalog,
    ProtocolPluginCatalogEntry, ProtocolPluginCatalogRoot, ProtocolPluginContributions,
};

/// Load the installed-plugin ledger using the same core loader as the SDK.
pub async fn load_endpoint_installed_plugins(
    plugin_home: impl AsRef<Path>,
) -> io::Result<PluginLoadReport> {
    load_installed_plugins_report(plugin_home, CommandLoadOptions::default()).await
}

/// Bind one endpoint's runtime to a serialized ledger manager.
pub fn protocol_plugin_admin(
    agent: Arc<Agent>,
    plugin_home: impl Into<PathBuf>,
) -> Arc<dyn ProtocolPluginAdmin> {
    Arc::new(EndpointPluginAdmin {
        plugin_home: plugin_home.into(),
        agent,
        ledger_lock: Arc::new(Mutex::new(())),
    })
}

struct EndpointPluginAdmin {
    plugin_home: PathBuf,
    agent: Arc<Agent>,
    ledger_lock: Arc<Mutex<()>>,
}

impl EndpointPluginAdmin {
    async fn publish_plugin_generation(&self) -> Result<(), BoxError> {
        let report = load_endpoint_installed_plugins(&self.plugin_home).await?;
        self.agent
            .refresh_plugins(report)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        Ok(())
    }
}

/// Strict semantic releases only; hashes and custom version labels are unordered.
fn semantic_release(value: &str) -> Option<([u64; 3], Vec<&str>)> {
    let value = value.strip_prefix('v').unwrap_or(value);
    let (release, build) = value
        .split_once('+')
        .map_or((value, None), |(a, b)| (a, Some(b)));
    let valid =
        |id: &str| !id.is_empty() && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-');
    if build.is_some_and(|build| !build.split('.').all(valid)) {
        return None;
    }
    let (core, pre) = release
        .split_once('-')
        .map_or((release, Vec::new()), |(a, b)| (a, b.split('.').collect()));
    if pre.iter().any(|id| {
        !valid(id) || id.bytes().all(|b| b.is_ascii_digit()) && id.len() > 1 && id.starts_with('0')
    }) {
        return None;
    }
    let parts: Vec<_> = core.split('.').collect();
    if parts.len() != 3
        || parts.iter().any(|p| {
            p.is_empty()
                || !p.bytes().all(|b| b.is_ascii_digit())
                || p.len() > 1 && p.starts_with('0')
        })
    {
        return None;
    }
    Some((
        [
            parts[0].parse().ok()?,
            parts[1].parse().ok()?,
            parts[2].parse().ok()?,
        ],
        pre,
    ))
}

fn semantic_release_order(left: &str, right: &str) -> Option<Ordering> {
    let (a, ap) = semantic_release(left)?;
    let (b, bp) = semantic_release(right)?;
    if a != b {
        return Some(a.cmp(&b));
    }
    match (ap.is_empty(), bp.is_empty()) {
        (true, false) => return Some(Ordering::Greater),
        (false, true) => return Some(Ordering::Less),
        _ => {}
    }
    for (a, b) in ap.iter().zip(&bp) {
        let na = a.bytes().all(|b| b.is_ascii_digit());
        let nb = b.bytes().all(|b| b.is_ascii_digit());
        let order = match (na, nb) {
            (true, true) => a.len().cmp(&b.len()).then_with(|| a.cmp(b)),
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => a.cmp(b),
        };
        if order != Ordering::Equal {
            return Some(order);
        }
    }
    Some(ap.len().cmp(&bp.len()))
}

async fn remove_unreferenced_plugin_files(
    home: &Path,
    removed: &InstalledPluginRecord,
) -> io::Result<()> {
    let ledger = read_installed_plugins(home).await?;
    let reused = ledger.plugins.values().any(|current| {
        current.install_path == removed.install_path
            || (removed.plugin_id.is_some() && current.plugin_id == removed.plugin_id)
    });
    if !reused {
        remove_installed_plugin_files(home, removed).await?;
    }
    Ok(())
}

#[async_trait]
impl ProtocolPluginAdmin for EndpointPluginAdmin {
    async fn list_installed_plugins(&self) -> Result<InstalledPlugins, BoxError> {
        let _guard = self.ledger_lock.lock().await;
        Ok(read_installed_plugins(&self.plugin_home).await?)
    }

    fn runtime_snapshot(
        &self,
    ) -> klaus_agent_core::extensions::plugins::runtime::PluginRuntimeSnapshot {
        self.agent.plugin_runtime_snapshot()
    }

    async fn set_installed_plugin_enabled(
        &self,
        name: &str,
        enabled: bool,
    ) -> Result<Option<InstalledPluginLedgerMutation>, BoxError> {
        let _guard = self.ledger_lock.lock().await;
        let Some(plugin) = set_installed_plugin_enabled(&self.plugin_home, name, enabled).await?
        else {
            return Ok(None);
        };
        let installed_plugins = read_installed_plugins(&self.plugin_home).await?;
        let published_to_loaded_sessions = self.publish_plugin_generation().await.is_ok();
        Ok(Some(InstalledPluginLedgerMutation {
            plugin,
            installed_plugins,
            published_to_loaded_sessions,
        }))
    }

    async fn uninstall_plugin(
        &self,
        name: &str,
        remove_files: bool,
    ) -> Result<Option<InstalledPluginLedgerMutation>, BoxError> {
        let _guard = self.ledger_lock.lock().await;
        let Some(plugin) = uninstall_installed_plugin(&self.plugin_home, name, false).await? else {
            return Ok(None);
        };
        let installed_plugins = read_installed_plugins(&self.plugin_home).await?;
        let published_to_loaded_sessions = self.publish_plugin_generation().await.is_ok();
        if remove_files && published_to_loaded_sessions {
            if self.agent.has_active_plugin_contexts() {
                let agent = Arc::clone(&self.agent);
                let lock = Arc::clone(&self.ledger_lock);
                let home = self.plugin_home.clone();
                let removed = plugin.clone();
                tokio::spawn(async move {
                    agent.wait_for_loaded_plugin_contexts().await;
                    let _guard = lock.lock().await;
                    if let Err(error) = remove_unreferenced_plugin_files(&home, &removed).await {
                        eprintln!("deferred plugin file removal failed: {error}");
                    }
                });
            } else {
                remove_unreferenced_plugin_files(&self.plugin_home, &plugin).await?;
            }
        }
        Ok(Some(InstalledPluginLedgerMutation {
            plugin,
            installed_plugins,
            published_to_loaded_sessions,
        }))
    }

    async fn install_plugin(
        &self,
        marketplace: &str,
        source_dir: &str,
        version: Option<&str>,
        expected_installed_version: Option<&str>,
    ) -> Result<Option<InstalledPluginLedgerMutation>, BoxError> {
        let _guard = self.ledger_lock.lock().await;
        let (current, config_version) =
            read_installed_plugins_with_version(&self.plugin_home).await?;
        // Read the manifest from the host-resolved source to learn the plugin
        // name, which pairs with the marketplace to form the ledger key.
        let Some(source_plugin) = load_one_plugin(source_dir, CommandLoadOptions::default()).await
        else {
            return Ok(None);
        };
        let id = PluginId::new(source_plugin.name.clone(), marketplace.to_string())?;
        if let Some(expected) = expected_installed_version {
            let existing = current.plugins.get(&id.as_key()).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "Plugin is no longer installed; refresh before updating",
                )
            })?;
            let actual = existing
                .version
                .as_ref()
                .map(runtime_string_to_host)
                .unwrap_or_default();
            if actual != expected {
                return Err(io::Error::new(io::ErrorKind::InvalidInput,format!("Installed plugin version changed from {expected} to {actual}; refresh before updating")).into());
            }
        }
        let selected_version = version.or(source_plugin.version.as_deref());
        if let (Some(incoming), Some(previous)) = (
            selected_version,
            current
                .plugins
                .get(&id.as_key())
                .and_then(|record| record.version.as_ref()),
        ) {
            let previous = runtime_string_to_host(previous);
            if semantic_release_order(incoming, &previous) == Some(Ordering::Less) {
                return Err(io::Error::new(io::ErrorKind::InvalidInput,format!("Plugin update would downgrade {previous} to {incoming}; uninstall first for an explicit downgrade")).into());
            }
        }
        let store = PluginStore::new(&self.plugin_home);
        let installed = store.install_dir(&id, selected_version, source_dir).await?;
        let installed_path =
            String::from_utf8_lossy(&installed.installed_path.to_node_utf8()).into_owned();
        let Some(installed_plugin) =
            load_one_plugin(&installed_path, CommandLoadOptions::default()).await
        else {
            store
                .remove_generation(&id, Path::new(&installed_path))
                .await?;
            return Ok(None);
        };
        if installed_plugin.name != source_plugin.name
            || installed_plugin.version != source_plugin.version
        {
            store
                .remove_generation(&id, Path::new(&installed_path))
                .await?;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Plugin manifest changed while its package was being installed",
            )
            .into());
        }
        if let Err(error) = record_installed_plugin_package_if_version(
            &self.plugin_home,
            &installed_plugin,
            &id,
            installed.version,
            Some(installed.data_path),
            format!("marketplace:{marketplace}"),
            &config_version,
        )
        .await
        {
            store
                .remove_generation(&id, Path::new(&installed_path))
                .await?;
            return Err(error.into());
        }
        let installed_plugins = read_installed_plugins(&self.plugin_home).await?;
        let published_to_loaded_sessions = self.publish_plugin_generation().await.is_ok();
        let Some(plugin) = installed_plugins.plugins.get(&id.as_key()).cloned() else {
            return Ok(None);
        };
        Ok(Some(InstalledPluginLedgerMutation {
            plugin,
            installed_plugins,
            published_to_loaded_sessions,
        }))
    }

    async fn migrate_bundled_diagram(&self, source_dir: &str) -> Result<bool, BoxError> {
        let _guard = self.ledger_lock.lock().await;
        let (mut ledger, version) = read_installed_plugins_with_version(&self.plugin_home).await?;
        let builtin_key = RuntimeString::from("digram@builtin");
        let Some(builtin) = ledger.plugins.get(&builtin_key).cloned() else {
            return Ok(false);
        };
        if !is_builtin_record(&builtin) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Diagram legacy record is not builtin",
            )
            .into());
        }

        let official_id = PluginId::new("digram", "klaus-official")?;
        let official_key = official_id.as_key();
        if let Some(existing) = ledger.plugins.get(&official_key) {
            let path = runtime_string_to_host(&existing.install_path);
            if existing.source != "marketplace:klaus-official"
                || load_one_plugin(&path, CommandLoadOptions::default())
                    .await
                    .is_none_or(|plugin| plugin.name != "digram")
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Existing official Diagram installation is invalid",
                )
                .into());
            }
        } else {
            let source = Path::new(source_dir);
            let expected = self
                .plugin_home
                .join("marketplaces/klaus-official/plugins/digram");
            if tokio::fs::canonicalize(source).await? != tokio::fs::canonicalize(expected).await? {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Diagram source is outside the official marketplace",
                )
                .into());
            }
            let plugin = load_one_plugin(source_dir, CommandLoadOptions::default())
                .await
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Official Diagram has no loadable manifest",
                    )
                })?;
            if plugin.name != "digram" {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Official Diagram manifest name differs",
                )
                .into());
            }
            let store = PluginStore::new(&self.plugin_home);
            let installed = store
                .install_dir(&official_id, plugin.version.as_deref(), source)
                .await?;
            ledger.plugins.insert(
                official_key.clone(),
                InstalledPluginRecord {
                    name: RuntimeString::from("digram"),
                    plugin_id: Some(official_key),
                    marketplace: Some(RuntimeString::from("klaus-official")),
                    version: Some(installed.version),
                    install_path: installed.installed_path,
                    data_path: Some(installed.data_path),
                    source: RuntimeString::from("marketplace:klaus-official"),
                    scope: None,
                    dependencies: plugin
                        .dependencies
                        .into_iter()
                        .map(RuntimeString::from)
                        .collect(),
                    installed_at_ms: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis()
                        as u64,
                    enabled: builtin.enabled,
                },
            );
        }
        ledger.plugins.remove(&builtin_key);
        write_installed_plugins_if_version(&self.plugin_home, &ledger, &version).await?;
        Ok(true)
    }

    async fn list_plugin_catalog(
        &self,
        roots: &[ProtocolPluginCatalogRoot],
    ) -> Result<ProtocolPluginCatalog, BoxError> {
        let installed = {
            let _guard = self.ledger_lock.lock().await;
            read_installed_plugins(&self.plugin_home).await?
        };
        let mut entries: Vec<ProtocolPluginCatalogEntry> = Vec::new();
        for root in roots {
            let dir = runtime_string_to_host(&root.dir);
            let marketplace = runtime_string_to_host(&root.marketplace);
            for package in read_packages_under(&dir).await {
                // The ledger key is `<name>@<marketplace>`; a name the id rules
                // reject can never be installed, so it is not offered either.
                let Ok(id) = PluginId::new(package.name.clone(), marketplace.clone()) else {
                    continue;
                };
                let record = installed.plugins.get(&id.as_key());
                entries.push(ProtocolPluginCatalogEntry {
                    id: id.as_key(),
                    name: RuntimeString::from(package.name.clone()),
                    marketplace: root.marketplace.clone(),
                    version: package.version.clone().map(RuntimeString::from),
                    description: package.description.clone().map(RuntimeString::from),
                    installed: record.is_some(),
                    // A record with no explicit flag is enabled, matching the
                    // ledger decoder's own default.
                    enabled: record.map(|record| record.enabled).unwrap_or(false),
                    contributions: contributions_of(&package),
                });
            }
        }
        entries.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(ProtocolPluginCatalog { entries })
    }
}

/// Node/V8 UTF-8 projection at the catalog's native filesystem boundary.
fn runtime_string_to_host(value: &RuntimeString) -> String {
    String::from_utf8_lossy(&value.to_node_utf8()).into_owned()
}

/// Every loadable plugin package directly under `dir`. A root that is itself a
/// package counts as one, which is what a single-plugin marketplace looks like.
async fn read_packages_under(dir: &str) -> Vec<LoadedPlugin> {
    if let Some(plugin) = load_one_plugin(dir, CommandLoadOptions::default()).await {
        return vec![plugin];
    }
    let mut packages = Vec::new();
    let Ok(mut read_dir) = tokio::fs::read_dir(dir).await else {
        return packages;
    };
    let mut children: Vec<PathBuf> = Vec::new();
    while let Ok(Some(entry)) = read_dir.next_entry().await {
        if entry
            .file_type()
            .await
            .map(|kind| kind.is_dir())
            .unwrap_or(false)
        {
            children.push(entry.path());
        }
    }
    // Deterministic order regardless of the filesystem's readdir order.
    children.sort();
    for child in children {
        if let Some(plugin) =
            load_one_plugin(&child.to_string_lossy(), CommandLoadOptions::default()).await
        {
            packages.push(plugin);
        }
    }
    packages
}

/// The package's contribution NAMES, straight off the engine's own loader — the
/// same parse a session would do, so the plugin page and the runtime cannot
/// disagree about what a package declares.
fn contributions_of(plugin: &LoadedPlugin) -> ProtocolPluginContributions {
    // Hooks are grouped per EVENT with a count, which is what tells a user WHEN
    // they fire; a bare list of "hook" rows says nothing.
    let mut hook_counts: BTreeMap<&'static str, usize> = BTreeMap::new();
    for hook in &plugin.hooks {
        *hook_counts.entry(hook.event.wire_name()).or_insert(0) += 1;
    }
    ProtocolPluginContributions {
        skills: plugin
            .skills
            .iter()
            .map(|skill| skill.name.clone())
            .collect(),
        // Plugin agents come from `agent_loader`, whose name is a plain String
        // (the session-level `AgentDefinition` is the RuntimeString one).
        agents: plugin
            .agents
            .iter()
            .map(|agent| RuntimeString::from(agent.name.clone()))
            .collect(),
        commands: plugin
            .commands
            .iter()
            .map(|command| RuntimeString::from(command.name().to_string()))
            .collect(),
        output_styles: plugin
            .output_styles
            .iter()
            .map(|style| style.name.clone())
            .collect(),
        hooks: hook_counts
            .into_iter()
            .map(|(event, count)| RuntimeString::from(format!("{event} ({count})")))
            .collect(),
        mcp_servers: plugin
            .mcp_servers
            .iter()
            .map(|server| RuntimeString::from(server.name().to_string()))
            .collect(),
        lsp_servers: plugin
            .lsp_servers
            .keys()
            .map(|name| RuntimeString::from(name.to_string()))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use klaus_agent_core::extensions::plugins::installed::write_installed_plugins;

    fn legacy_record(enabled: bool, root: &Path) -> InstalledPluginRecord {
        InstalledPluginRecord {
            name: RuntimeString::from("digram"),
            plugin_id: Some(RuntimeString::from("digram@builtin")),
            marketplace: Some(RuntimeString::from("builtin")),
            version: None,
            install_path: RuntimeString::from(
                root.join("old-bundle").to_string_lossy().to_string(),
            ),
            data_path: None,
            source: RuntimeString::from("builtin"),
            scope: None,
            dependencies: Vec::new(),
            installed_at_ms: 1,
            enabled,
        }
    }

    async fn fixture() -> (tempfile::TempDir, EndpointPluginAdmin, PathBuf) {
        let root = tempfile::tempdir().unwrap();
        let plugin_home = root.path().join("plugins");
        let source = plugin_home.join("marketplaces/klaus-official/plugins/digram");
        tokio::fs::create_dir_all(&source).await.unwrap();
        tokio::fs::write(
            source.join("plugin.json"),
            r#"{"name":"digram","version":"1.0.0"}"#,
        )
        .await
        .unwrap();
        let mut options = crate::EndpointOptions::new(crate::test_process_context());
        options.plugin_home = Some(plugin_home.clone());
        options.load_installed_plugins = Some(false);
        let agent = crate::Endpoint::build(options).await.unwrap().agent;
        let admin = EndpointPluginAdmin {
            plugin_home,
            agent,
            ledger_lock: Arc::new(Mutex::new(())),
        };
        (root, admin, source)
    }

    #[tokio::test]
    async fn plugin_upgrade_retains_enablement_data_and_original_generation() {
        for enabled in [false, true] {
            let (_root, admin, source) = fixture().await;
            let original = admin
                .install_plugin("klaus-official", source.to_str().unwrap(), None, None)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                original.plugin.version.as_ref(),
                Some(&RuntimeString::from("1.0.0"))
            );
            admin
                .set_installed_plugin_enabled("digram@klaus-official", enabled)
                .await
                .unwrap();
            let old_path = runtime_string_to_host(&original.plugin.install_path);
            let data_path = runtime_string_to_host(original.plugin.data_path.as_ref().unwrap());
            tokio::fs::create_dir_all(Path::new(&data_path))
                .await
                .unwrap();
            tokio::fs::write(Path::new(&data_path).join("setting.txt"), "keep")
                .await
                .unwrap();
            tokio::fs::write(
                source.join("plugin.json"),
                r#"{"name":"digram","version":"1.1.0"}"#,
            )
            .await
            .unwrap();
            let updated = admin
                .install_plugin("klaus-official", source.to_str().unwrap(), None, None)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(updated.plugin.enabled, enabled);
            assert_eq!(
                updated.plugin.version.as_ref(),
                Some(&RuntimeString::from("1.1.0"))
            );
            assert_eq!(updated.plugin.data_path, original.plugin.data_path);
            assert_eq!(
                updated.plugin.installed_at_ms,
                original.plugin.installed_at_ms
            );
            assert_ne!(updated.plugin.install_path, original.plugin.install_path);
            assert_eq!(
                tokio::fs::read_to_string(Path::new(&data_path).join("setting.txt"))
                    .await
                    .unwrap(),
                "keep"
            );
            assert!(
                tokio::fs::read_to_string(Path::new(&old_path).join("plugin.json"))
                    .await
                    .unwrap()
                    .contains("1.0.0")
            );
            let restart = load_endpoint_installed_plugins(&admin.plugin_home)
                .await
                .unwrap();
            if enabled {
                assert_eq!(restart.enabled[0].version.as_deref(), Some("1.1.0"));
            } else {
                assert!(restart.enabled.is_empty());
                assert_eq!(restart.disabled.len(), 1);
            }
        }
    }

    #[tokio::test]
    async fn plugin_install_refuses_a_stale_semantic_downgrade() {
        let (_root, admin, source) = fixture().await;
        tokio::fs::write(
            source.join("plugin.json"),
            r#"{"name":"digram","version":"2.0.0"}"#,
        )
        .await
        .unwrap();
        let original = admin
            .install_plugin("klaus-official", source.to_str().unwrap(), None, None)
            .await
            .unwrap()
            .unwrap();
        tokio::fs::write(
            source.join("plugin.json"),
            r#"{"name":"digram","version":"1.1.0"}"#,
        )
        .await
        .unwrap();
        let error = admin
            .install_plugin("klaus-official", source.to_str().unwrap(), None, None)
            .await
            .err()
            .expect("a stale semantic release must be refused");
        assert!(error.to_string().contains("downgrade"));
        let installed = admin.list_installed_plugins().await.unwrap();
        assert_eq!(
            installed.plugins[&RuntimeString::from("digram@klaus-official")].install_path,
            original.plugin.install_path
        );
    }

    #[test]
    fn semantic_update_guard_orders_releases_and_leaves_hashes_unordered() {
        assert_eq!(
            semantic_release_order("v1.1.0", "v2.0.0"),
            Some(Ordering::Less)
        );
        assert_eq!(
            semantic_release_order("1.1.0", "2.0.0"),
            Some(Ordering::Less)
        );
        assert_eq!(
            semantic_release_order("1.0.0-beta.2", "1.0.0-beta.10"),
            Some(Ordering::Less)
        );
        assert_eq!(
            semantic_release_order("1.0.0", "1.0.0-rc.1"),
            Some(Ordering::Greater)
        );
        assert_eq!(
            semantic_release_order("1.0.0+new", "1.0.0+old"),
            Some(Ordering::Equal)
        );
        assert_eq!(semantic_release_order("1.0.0", "8d19641163d8"), None);
        assert_eq!(semantic_release_order("local", "1.0.0"), None);
    }

    #[tokio::test]
    async fn plugin_update_guard_cannot_resurrect_an_uninstalled_plugin() {
        let (_root, admin, source) = fixture().await;
        admin
            .install_plugin("klaus-official", source.to_str().unwrap(), None, None)
            .await
            .unwrap()
            .unwrap();
        admin
            .uninstall_plugin("digram@klaus-official", false)
            .await
            .unwrap()
            .unwrap();
        tokio::fs::write(
            source.join("plugin.json"),
            r#"{"name":"digram","version":"1.1.0"}"#,
        )
        .await
        .unwrap();
        let error = admin
            .install_plugin(
                "klaus-official",
                source.to_str().unwrap(),
                None,
                Some("1.0.0"),
            )
            .await
            .err()
            .expect("update must not install a missing record");
        assert!(error.to_string().contains("no longer installed"));
        assert!(admin
            .list_installed_plugins()
            .await
            .unwrap()
            .plugins
            .is_empty());
        assert!(admin
            .install_plugin("klaus-official", source.to_str().unwrap(), None, None)
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn plugin_update_guard_refuses_a_version_changed_at_another_endpoint() {
        let (_root, admin, source) = fixture().await;
        admin
            .install_plugin("klaus-official", source.to_str().unwrap(), None, None)
            .await
            .unwrap()
            .unwrap();
        tokio::fs::write(
            source.join("plugin.json"),
            r#"{"name":"digram","version":"1.1.0"}"#,
        )
        .await
        .unwrap();
        let current = admin
            .install_plugin("klaus-official", source.to_str().unwrap(), None, None)
            .await
            .unwrap()
            .unwrap();
        tokio::fs::write(
            source.join("plugin.json"),
            r#"{"name":"digram","version":"1.2.0"}"#,
        )
        .await
        .unwrap();
        let error = admin
            .install_plugin(
                "klaus-official",
                source.to_str().unwrap(),
                None,
                Some("1.0.0"),
            )
            .await
            .err()
            .expect("stale update version must be rejected");
        assert!(error.to_string().contains("version changed"));
        let after = admin.list_installed_plugins().await.unwrap();
        assert_eq!(
            after.plugins[&RuntimeString::from("digram@klaus-official")].install_path,
            current.plugin.install_path
        );
        assert!(admin
            .install_plugin(
                "klaus-official",
                source.to_str().unwrap(),
                None,
                Some("1.1.0")
            )
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn bundled_diagram_migrates_without_changing_disablement_or_documents() {
        let (root, admin, source) = fixture().await;
        let document = root.path().join("digram/user-diagram.digram.json");
        tokio::fs::create_dir_all(document.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&document, b"important diagram")
            .await
            .unwrap();
        let mut ledger = InstalledPlugins::default();
        ledger.plugins.insert(
            RuntimeString::from("digram@builtin"),
            legacy_record(false, root.path()),
        );
        write_installed_plugins(&admin.plugin_home, &ledger)
            .await
            .unwrap();

        assert!(admin
            .migrate_bundled_diagram(source.to_str().unwrap())
            .await
            .unwrap());
        let migrated = read_installed_plugins(&admin.plugin_home).await.unwrap();
        assert!(!migrated
            .plugins
            .contains_key(&RuntimeString::from("digram@builtin")));
        let official = &migrated.plugins[&RuntimeString::from("digram@klaus-official")];
        assert!(!official.enabled);
        assert_eq!(official.source, "marketplace:klaus-official");
        assert!(Path::new(&runtime_string_to_host(&official.install_path))
            .join("plugin.json")
            .exists());
        assert!(!admin
            .migrate_bundled_diagram(source.to_str().unwrap())
            .await
            .unwrap());
        assert_eq!(
            tokio::fs::read(&document).await.unwrap(),
            b"important diagram"
        );

        admin
            .uninstall_plugin("digram@klaus-official", true)
            .await
            .unwrap();
        assert!(!admin
            .migrate_bundled_diagram(source.to_str().unwrap())
            .await
            .unwrap());
        assert!(read_installed_plugins(&admin.plugin_home)
            .await
            .unwrap()
            .plugins
            .is_empty());
        assert_eq!(
            tokio::fs::read(&document).await.unwrap(),
            b"important diagram"
        );
    }

    #[tokio::test]
    async fn existing_official_installation_wins_over_legacy_enablement() {
        let (root, admin, source) = fixture().await;
        let mut ledger = InstalledPlugins::default();
        ledger.plugins.insert(
            RuntimeString::from("digram@builtin"),
            legacy_record(false, root.path()),
        );
        write_installed_plugins(&admin.plugin_home, &ledger)
            .await
            .unwrap();
        assert!(admin
            .migrate_bundled_diagram(source.to_str().unwrap())
            .await
            .unwrap());
        let mut ledger = read_installed_plugins(&admin.plugin_home).await.unwrap();
        ledger
            .plugins
            .get_mut(&RuntimeString::from("digram@klaus-official"))
            .unwrap()
            .enabled = true;
        ledger.plugins.insert(
            RuntimeString::from("digram@builtin"),
            legacy_record(false, root.path()),
        );
        write_installed_plugins(&admin.plugin_home, &ledger)
            .await
            .unwrap();

        assert!(admin
            .migrate_bundled_diagram(source.to_str().unwrap())
            .await
            .unwrap());
        let result = read_installed_plugins(&admin.plugin_home).await.unwrap();
        assert_eq!(result.plugins.len(), 1);
        assert!(result.plugins[&RuntimeString::from("digram@klaus-official")].enabled);
    }

    #[tokio::test]
    async fn invalid_official_source_does_not_retire_the_builtin_record() {
        let (root, admin, _source) = fixture().await;
        let mut ledger = InstalledPlugins::default();
        ledger.plugins.insert(
            RuntimeString::from("digram@builtin"),
            legacy_record(true, root.path()),
        );
        write_installed_plugins(&admin.plugin_home, &ledger)
            .await
            .unwrap();

        assert!(admin
            .migrate_bundled_diagram(root.path().to_str().unwrap())
            .await
            .is_err());
        assert_eq!(
            read_installed_plugins(&admin.plugin_home).await.unwrap(),
            ledger
        );
    }

    #[tokio::test]
    async fn an_empty_or_uninstalled_profile_is_not_automatically_granted_diagram() {
        let (_root, admin, source) = fixture().await;
        assert!(!admin
            .migrate_bundled_diagram(source.to_str().unwrap())
            .await
            .unwrap());
        assert!(read_installed_plugins(&admin.plugin_home)
            .await
            .unwrap()
            .plugins
            .is_empty());
    }
}
