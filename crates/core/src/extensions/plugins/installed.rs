//! Installed plugin state persisted under the plugin home.

use std::collections::BTreeMap;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::extensions::command_loader::CommandLoadOptions;
use crate::extensions::plugins::identity::PluginId;
use crate::extensions::plugins::loader::{
    load_one_plugin_report, LoadedPlugin, PluginDisabledEntry, PluginLoadIssue,
    PluginLoadIssueKind, PluginLoadReport,
};
use crate::extensions::plugins::store::{PluginStore, DEFAULT_PLUGIN_VERSION};
use crate::runtime_values::{
    runtime_codec, runtime_json, NodeBufferUtf8, RuntimeCodecError, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};

const INSTALLED_PLUGINS_FILE: &str = "installed_plugins.json";

/// The engine's config file holds the installed-plugin ledger under `[plugins]`
/// now, not a separate `installed_plugins.json`. The plugin home is
/// `<config home>/plugins`, so the config file is its sibling: `home.parent()`.
/// Deriving it from `home` (rather than a global) keeps the ledger isolated per
/// plugin home, which is what the test suite relies on.
fn ledger_config_path(home: &Path) -> PathBuf {
    home.parent()
        .map(|parent| parent.join(crate::config_store::CONFIG_FILE))
        .unwrap_or_else(|| home.join(crate::config_store::CONFIG_FILE))
}

pub fn default_plugin_home() -> PathBuf {
    #[allow(deprecated)] // fine on modern toolchains; `home_dir` is un-deprecated
    std::env::home_dir()
        .unwrap_or_default()
        .join(".klaus")
        .join("plugins")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstalledPluginRecord {
    pub name: RuntimeString,
    pub plugin_id: Option<RuntimeString>,
    pub marketplace: Option<RuntimeString>,
    pub version: Option<RuntimeString>,
    pub install_path: RuntimeString,
    pub data_path: Option<RuntimeString>,
    pub source: RuntimeString,
    pub scope: Option<RuntimeString>,
    pub dependencies: Vec<RuntimeString>,
    pub installed_at_ms: u64,
    pub enabled: bool,
}

/// Ledger `source` value for plugins that ship with the client and are
/// seeded by the host at startup. They can be enabled/disabled but never
/// uninstalled.
pub const BUILTIN_PLUGIN_SOURCE: &str = "builtin";

/// Whether a ledger record is a host-seeded builtin.
pub fn is_builtin_record(record: &InstalledPluginRecord) -> bool {
    record.source == BUILTIN_PLUGIN_SOURCE
}

impl RuntimeEncode for InstalledPluginRecord {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        if let Some(value) = &self.plugin_id {
            object.field("pluginId", value)?;
        }
        if let Some(value) = &self.marketplace {
            object.field("marketplace", value)?;
        }
        if let Some(value) = &self.version {
            object.field("version", value)?;
        }
        object.field("installPath", &self.install_path)?;
        if let Some(value) = &self.data_path {
            object.field("dataPath", value)?;
        }
        object.field("source", &self.source)?;
        if let Some(value) = &self.scope {
            object.field("scope", value)?;
        }
        if !self.dependencies.is_empty() {
            object.field("dependencies", &self.dependencies)?;
        }
        object.field("installedAtMs", &self.installed_at_ms)?;
        object.field("enabled", &self.enabled)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for InstalledPluginRecord {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                name: object.required("name", &[])?,
                plugin_id: object.optional("pluginId", &[])?,
                marketplace: object.optional("marketplace", &[])?,
                version: object.optional("version", &[])?,
                install_path: object.required("installPath", &[])?,
                data_path: object.optional("dataPath", &[])?,
                source: object.required("source", &[])?,
                scope: object.optional("scope", &[])?,
                dependencies: object.defaulted("dependencies", &[], Vec::new)?,
                installed_at_ms: object.required("installedAtMs", &[])?,
                enabled: object.defaulted("enabled", &[], default_enabled)?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstalledPlugins {
    pub version: u32,
    pub plugins: BTreeMap<RuntimeString, InstalledPluginRecord>,
}

impl RuntimeEncode for InstalledPlugins {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("version", &self.version)?;
        object.field("plugins", &self.plugins)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for InstalledPlugins {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                version: object.required("version", &[])?,
                plugins: object.defaulted("plugins", &[], BTreeMap::new)?,
            })
        })
    }
}

impl Default for InstalledPlugins {
    fn default() -> Self {
        Self {
            version: 2,
            plugins: BTreeMap::new(),
        }
    }
}

pub fn installed_plugins_path(home: impl AsRef<Path>) -> PathBuf {
    home.as_ref().join(INSTALLED_PLUGINS_FILE)
}

/// Read the ledger from `[plugins]` in the engine's config file. A missing file
/// or absent section is an empty ledger. The stored shape is the records map
/// keyed by id; the `version`
/// wrapper the on-disk JSON used to carry is implicit (always 2).
pub async fn read_installed_plugins(home: impl AsRef<Path>) -> io::Result<InstalledPlugins> {
    read_installed_plugins_with_version(home)
        .await
        .map(|(state, _)| state)
}

pub async fn read_installed_plugins_with_version(
    home: impl AsRef<Path>,
) -> io::Result<(InstalledPlugins, String)> {
    let path = ledger_config_path(home.as_ref());
    let document = crate::config_store::load(&path).await.map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid installed plugins state: {err}"),
        )
    })?;
    let plugins = document
        .values
        .get("plugins")
        .cloned()
        .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
    Ok((decode_installed_plugins(&plugins)?, document.version))
}

/// Decode the `[plugins]` records map (id -> record) into [`InstalledPlugins`],
/// reusing the runtime codec by wrapping it in the `{ version, plugins }` shape
/// the decoder expects.
fn decode_installed_plugins(plugins: &serde_json::Value) -> io::Result<InstalledPlugins> {
    let wrapped = serde_json::json!({ "version": 2, "plugins": plugins });
    let text = serde_json::to_string(&wrapped)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
    runtime_codec::decode_text(&NodeBufferUtf8::decode(text.as_bytes())).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid installed plugins state: {err}"),
        )
    })
}

pub async fn write_installed_plugins(
    home: impl AsRef<Path>,
    state: &InstalledPlugins,
) -> io::Result<()> {
    write_installed_plugins_inner(home.as_ref(), state, None).await
}

pub async fn write_installed_plugins_if_version(
    home: impl AsRef<Path>,
    state: &InstalledPlugins,
    expected_version: &str,
) -> io::Result<()> {
    write_installed_plugins_inner(home.as_ref(), state, Some(expected_version)).await
}

async fn write_installed_plugins_inner(
    home: &Path,
    state: &InstalledPlugins,
    expected_version: Option<&str>,
) -> io::Result<()> {
    let path = ledger_config_path(home);
    let plugins = encode_installed_plugins(state)?;
    // A whole-section replace: a removed plugin has to really disappear, which an
    // upsert merge cannot express. The engine writes its OWN config file here,
    // the same file every other config write goes through, so comments and
    // unrelated keys survive (toml_edit) and there is one writer.
    crate::config_store::apply_edits(
        &path,
        &[crate::config_store::ConfigEdit {
            key_path: "plugins".to_string(),
            value: plugins,
            merge: crate::config_store::MergeStrategy::Replace,
        }],
        expected_version,
    )
    .await
    .map(|_| ())
    .map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to write installed plugins state: {err}"),
        )
    })
}

/// The records map (id -> record) as a JSON value for the config store. Encoded
/// through the runtime codec, then the `plugins` field lifted out of the
/// `{ version, plugins }` wrapper.
fn encode_installed_plugins(state: &InstalledPlugins) -> io::Result<serde_json::Value> {
    let encoded = runtime_codec::encode_value(state).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to encode installed plugins state: {err}"),
        )
    })?;
    let plugins = encoded
        .get("plugins")
        .cloned()
        .unwrap_or_else(|| RuntimeJson::Object(Default::default()));
    let text = runtime_json::stringify_pretty(&plugins).to_node_utf8();
    serde_json::from_slice(&text)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))
}

fn node_utf8_installed_plugin_host_boundary(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8())
        .expect("Node UTF-8 installed-plugin host projection is valid UTF-8")
}

pub async fn record_installed_plugin(
    home: impl AsRef<Path>,
    plugin: &LoadedPlugin,
    source: impl Into<RuntimeString>,
) -> io::Result<()> {
    let version = plugin
        .version
        .clone()
        .map(RuntimeString::from)
        .unwrap_or_else(|| RuntimeString::from(DEFAULT_PLUGIN_VERSION));
    let plugin_id = PluginId::new(plugin.name.clone(), "local").ok();
    record_installed_plugin_inner(
        home.as_ref(),
        plugin,
        plugin_id.as_ref(),
        version,
        None,
        source.into(),
        None,
    )
    .await
}

pub async fn record_installed_plugin_package(
    home: impl AsRef<Path>,
    plugin: &LoadedPlugin,
    plugin_id: &PluginId,
    version: impl Into<RuntimeString>,
    data_path: Option<RuntimeString>,
    source: impl Into<RuntimeString>,
) -> io::Result<()> {
    record_installed_plugin_inner(
        home.as_ref(),
        plugin,
        Some(plugin_id),
        version.into(),
        data_path,
        source.into(),
        None,
    )
    .await
}

/// Publish a package only if the engine config still matches the pre-staging snapshot.
/// Package files are prepared separately; a failed compare leaves the prior record intact.
pub async fn record_installed_plugin_package_if_version(
    home: impl AsRef<Path>,
    plugin: &LoadedPlugin,
    plugin_id: &PluginId,
    version: impl Into<RuntimeString>,
    data_path: Option<RuntimeString>,
    source: impl Into<RuntimeString>,
    expected_version: &str,
) -> io::Result<()> {
    record_installed_plugin_inner(
        home.as_ref(),
        plugin,
        Some(plugin_id),
        version.into(),
        data_path,
        source.into(),
        Some(expected_version),
    )
    .await
}

async fn record_installed_plugin_inner(
    home: &Path,
    plugin: &LoadedPlugin,
    plugin_id: Option<&PluginId>,
    version: RuntimeString,
    data_path: Option<RuntimeString>,
    source: RuntimeString,
    expected_version: Option<&str>,
) -> io::Result<()> {
    let (mut state, config_version) = read_installed_plugins_with_version(home).await?;
    let key = plugin_id
        .map(PluginId::as_key)
        .unwrap_or_else(|| RuntimeString::from(plugin.name.clone()));
    let previous = state.plugins.get(&key).cloned();
    state.plugins.insert(
        key,
        InstalledPluginRecord {
            name: RuntimeString::from(plugin.name.clone()),
            plugin_id: plugin_id.map(PluginId::as_key),
            marketplace: plugin_id.map(|id| id.marketplace_name.clone()),
            version: Some(version),
            install_path: RuntimeString::from(
                PathBuf::from(&plugin.dir).to_string_lossy().into_owned(),
            ),
            data_path: previous
                .as_ref()
                .and_then(|record| record.data_path.clone())
                .or(data_path),
            source,
            scope: previous.as_ref().and_then(|record| record.scope.clone()),
            dependencies: plugin
                .dependencies
                .iter()
                .cloned()
                .map(RuntimeString::from)
                .collect(),
            installed_at_ms: previous
                .as_ref()
                .map_or_else(now_ms, |record| record.installed_at_ms),
            enabled: previous.as_ref().is_none_or(|record| record.enabled),
        },
    );
    write_installed_plugins_if_version(home, &state, expected_version.unwrap_or(&config_version))
        .await
}

pub async fn load_installed_plugins_report(
    home: impl AsRef<Path>,
    opts: CommandLoadOptions,
) -> io::Result<PluginLoadReport> {
    let home = home.as_ref();
    let state = read_installed_plugins(home).await?;
    let mut report = PluginLoadReport::default();
    for record in state.plugins.values() {
        if !record.enabled {
            report.disabled.push(PluginDisabledEntry {
                name: Some(record.name.clone()),
                dir: record.install_path.clone(),
                reason: RuntimeString::from("disabled by installed plugin state"),
            });
            continue;
        }

        let load_path = installed_record_load_path(home, record).await?;
        let mut loaded = load_one_plugin_report(&load_path, dup_command_opts(&opts)).await;
        if loaded.enabled.is_empty()
            && loaded.disabled.is_empty()
            && loaded.errors.is_empty()
            && loaded.warnings.is_empty()
        {
            loaded.errors.push(PluginLoadIssue {
                kind: PluginLoadIssueKind::InvalidManifest,
                dir: RuntimeString::from(load_path),
                message: RuntimeString::from(format!(
                    "installed plugin \"{}\" has no loadable manifest",
                    node_utf8_installed_plugin_host_boundary(&record.name)
                )),
            });
        }
        report.enabled.append(&mut loaded.enabled);
        report.disabled.append(&mut loaded.disabled);
        report.errors.append(&mut loaded.errors);
        report.warnings.append(&mut loaded.warnings);
    }
    Ok(report)
}

pub async fn set_installed_plugin_enabled(
    home: impl AsRef<Path>,
    name: &str,
    enabled: bool,
) -> io::Result<Option<InstalledPluginRecord>> {
    let home = home.as_ref();
    let mut state = read_installed_plugins(home).await?;
    let Some(key) = find_record_key(&state, name) else {
        return Ok(None);
    };
    let Some(record) = state.plugins.get_mut(&key) else {
        return Ok(None);
    };
    record.enabled = enabled;
    let out = record.clone();
    write_installed_plugins(home, &state).await?;
    Ok(Some(out))
}

pub async fn uninstall_installed_plugin(
    home: impl AsRef<Path>,
    name: &str,
    remove_files: bool,
) -> io::Result<Option<InstalledPluginRecord>> {
    let home = home.as_ref();
    let mut state = read_installed_plugins(home).await?;
    let Some(key) = find_record_key(&state, name) else {
        return Ok(None);
    };
    let Some(record) = state.plugins.remove(&key) else {
        return Ok(None);
    };
    // Builtin records are seeded by the host at startup and ship with the
    // client; uninstalling one would leave the next startup re-seeding it,
    // so refuse at the protocol level for every caller (UI, model tools,
    // future clients).
    if is_builtin_record(&record) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "plugin \"{}\" is built in and cannot be uninstalled; disable it instead",
                node_utf8_installed_plugin_host_boundary(&record.name)
            ),
        ));
    }
    let dependents = dependent_records(&state, &key, &record);
    if !dependents.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "plugin \"{}\" is required by installed plugin(s): {}",
                node_utf8_installed_plugin_host_boundary(&record.name),
                dependents.join(", ")
            ),
        ));
    }
    write_installed_plugins(home, &state).await?;
    if remove_files {
        remove_installed_plugin_files(home, &record).await?;
    }
    Ok(Some(record))
}

pub async fn remove_installed_plugin_files(
    home: &Path,
    record: &InstalledPluginRecord,
) -> io::Result<()> {
    if let Some(plugin_id) = record
        .plugin_id
        .as_ref()
        .and_then(|id| PluginId::parse_runtime(id).ok())
    {
        PluginStore::new(home).uninstall(&plugin_id, true).await
    } else {
        let install_path = PathBuf::from(node_utf8_installed_plugin_host_boundary(
            &record.install_path,
        ));
        if path_is_within_home(home, &install_path) {
            match tokio::fs::remove_dir_all(&install_path).await {
                Ok(()) => Ok(()),
                Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(err) => Err(err),
            }
        } else {
            Ok(())
        }
    }
}

async fn installed_record_load_path(
    _home: &Path,
    record: &InstalledPluginRecord,
) -> io::Result<String> {
    // Prepared/uncommitted generations must never choose the running package.
    // The engine-owned ledger is the sole activation pointer, including restarts.
    Ok(node_utf8_installed_plugin_host_boundary(
        &record.install_path,
    ))
}

fn find_record_key(state: &InstalledPlugins, name_or_id: &str) -> Option<RuntimeString> {
    let exact = RuntimeString::from(name_or_id);
    if state.plugins.contains_key(&exact) {
        return Some(exact);
    }
    state.plugins.iter().find_map(|(key, record)| {
        if record.name == name_or_id
            || record
                .plugin_id
                .as_ref()
                .is_some_and(|plugin_id| plugin_id == name_or_id)
        {
            Some(key.clone())
        } else {
            None
        }
    })
}

fn dependent_records(
    state: &InstalledPlugins,
    record_key: &RuntimeString,
    target: &InstalledPluginRecord,
) -> Vec<String> {
    state
        .plugins
        .iter()
        .filter_map(|(key, record)| {
            if key == record_key {
                return None;
            }
            let depends = record
                .dependencies
                .iter()
                .any(|dependency| dependency_matches_record(dependency, target));
            if depends {
                Some(node_utf8_installed_plugin_host_boundary(
                    record.plugin_id.as_ref().unwrap_or(&record.name),
                ))
            } else {
                None
            }
        })
        .collect()
}

fn dependency_matches_record(dependency: &RuntimeString, record: &InstalledPluginRecord) -> bool {
    dependency == &record.name || record.plugin_id.as_ref() == Some(dependency)
}

fn default_enabled() -> bool {
    true
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

fn path_is_within_home(home: &Path, target: &Path) -> bool {
    let home = absolutize(home);
    let target = if target.is_absolute() {
        normalize_path(target)
    } else {
        normalize_path(&home.join(target))
    };
    target.starts_with(home)
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

fn dup_command_opts(opts: &CommandLoadOptions) -> CommandLoadOptions {
    CommandLoadOptions {
        run_bash: opts.run_bash.clone(),
        vars: opts.vars.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn temp_dir() -> PathBuf {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "ae-installed-plugin-{}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
            now_ms()
        ));
        // The plugin home is `<config home>/plugins`; the ledger lives in the
        // SIBLING `config.toml`, so `home` must have a unique parent per test.
        dir.push("plugins");
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn record(name: &str, install_path: String) -> InstalledPluginRecord {
        InstalledPluginRecord {
            name: RuntimeString::from(name),
            plugin_id: None,
            marketplace: None,
            version: Some(RuntimeString::from("1.0.0")),
            install_path: RuntimeString::from(install_path),
            data_path: None,
            source: RuntimeString::from("catalog:docs"),
            scope: None,
            dependencies: Vec::new(),
            installed_at_ms: 1,
            enabled: true,
        }
    }

    #[tokio::test]
    async fn uninstall_refuses_builtin_records() {
        let home = temp_dir();
        let mut state = InstalledPlugins::default();
        let mut builtin = record(
            "browser",
            home.join("browser").to_string_lossy().into_owned(),
        );
        builtin.source = RuntimeString::from(BUILTIN_PLUGIN_SOURCE);
        state
            .plugins
            .insert(RuntimeString::from("browser"), builtin);
        write_installed_plugins(&home, &state).await.unwrap();

        let err = uninstall_installed_plugin(&home, "browser", false)
            .await
            .expect_err("builtin plugins cannot be uninstalled");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        // The refused uninstall leaves the record in place.
        assert!(read_installed_plugins(&home)
            .await
            .unwrap()
            .plugins
            .contains_key(&RuntimeString::from("browser")));
    }

    #[tokio::test]
    async fn installed_plugin_records_can_be_disabled_and_enabled() {
        let home = temp_dir();
        let mut state = InstalledPlugins::default();
        state.plugins.insert(
            RuntimeString::from("docs"),
            record("docs", home.join("docs").to_string_lossy().into_owned()),
        );
        write_installed_plugins(&home, &state).await.unwrap();

        let disabled = set_installed_plugin_enabled(&home, "docs", false)
            .await
            .unwrap()
            .expect("record");
        assert!(!disabled.enabled);
        assert!(
            !read_installed_plugins(&home)
                .await
                .unwrap()
                .plugins
                .get(&RuntimeString::from("docs"))
                .unwrap()
                .enabled
        );

        let enabled = set_installed_plugin_enabled(&home, "docs", true)
            .await
            .unwrap()
            .expect("record");
        assert!(enabled.enabled);
    }

    #[tokio::test]
    async fn uninstall_removes_record_and_optionally_the_plugin_dir() {
        let home = temp_dir();
        let plugin_dir = home.join("docs");
        std::fs::create_dir_all(&plugin_dir).unwrap();
        std::fs::write(plugin_dir.join("plugin.json"), "{}").unwrap();
        let mut state = InstalledPlugins::default();
        state.plugins.insert(
            RuntimeString::from("docs"),
            record("docs", plugin_dir.to_string_lossy().into_owned()),
        );
        write_installed_plugins(&home, &state).await.unwrap();

        let removed = uninstall_installed_plugin(&home, "docs", true)
            .await
            .unwrap()
            .expect("record");
        assert_eq!(removed.name, "docs");
        assert!(!plugin_dir.exists());
        assert!(!read_installed_plugins(&home)
            .await
            .unwrap()
            .plugins
            .contains_key(&RuntimeString::from("docs")));
    }

    #[tokio::test]
    async fn load_report_from_installed_state_respects_enabled_records() {
        let home = temp_dir();
        let enabled_dir = home.join("enabled");
        std::fs::create_dir_all(&enabled_dir).unwrap();
        std::fs::write(enabled_dir.join("plugin.json"), r#"{"name":"enabled"}"#).unwrap();
        let disabled_dir = home.join("disabled");
        std::fs::create_dir_all(&disabled_dir).unwrap();
        std::fs::write(disabled_dir.join("plugin.json"), r#"{"name":"disabled"}"#).unwrap();
        let broken_dir = home.join("broken");
        std::fs::create_dir_all(&broken_dir).unwrap();

        let mut state = InstalledPlugins::default();
        state.plugins.insert(
            RuntimeString::from("enabled"),
            record("enabled", enabled_dir.to_string_lossy().into_owned()),
        );
        let mut disabled = record("disabled", disabled_dir.to_string_lossy().into_owned());
        disabled.enabled = false;
        state
            .plugins
            .insert(RuntimeString::from("disabled"), disabled);
        state.plugins.insert(
            RuntimeString::from("broken"),
            record("broken", broken_dir.to_string_lossy().into_owned()),
        );
        write_installed_plugins(&home, &state).await.unwrap();

        let report = load_installed_plugins_report(
            &home,
            crate::extensions::command_loader::CommandLoadOptions::default(),
        )
        .await
        .unwrap();

        assert_eq!(report.enabled.len(), 1);
        assert_eq!(report.enabled[0].name, "enabled");
        assert_eq!(report.disabled.len(), 1);
        assert_eq!(
            report.disabled[0].name.as_ref(),
            Some(&RuntimeString::from("disabled"))
        );
        assert_eq!(report.errors.len(), 1);
        assert_eq!(
            report.errors[0].dir,
            RuntimeString::from(broken_dir.to_string_lossy())
        );
    }

    /// A ledger written by the HOST, byte for byte as it lands on disk, must load
    /// its plugin's contributions at startup.
    ///
    /// This is the regression for a boot-killing defect: the desktop shell used to
    /// own this file in its own shape (`plugins: { "<id>": [ {...} ] }` — an ARRAY
    /// per id, with an RFC3339 `installedAt`), which this decoder rejects with
    /// `ExpectedObject`. `load_installed_plugins_report` runs during endpoint
    /// startup, so that rejection was a hard exit before any session existed: a
    /// profile that had ever installed a plugin could not start the engine at all.
    /// Driving the JSON rather than the typed struct is the point — a struct-only
    /// test cannot see a schema disagreement.
    #[tokio::test]
    async fn a_host_written_ledger_loads_its_plugin_at_startup() {
        let home = temp_dir();
        let plugin_dir = home
            .join("cache")
            .join("test-market")
            .join("demo")
            .join("1.2.0");
        std::fs::create_dir_all(plugin_dir.join(".claude-plugin")).unwrap();
        std::fs::write(
            plugin_dir.join(".claude-plugin").join("plugin.json"),
            r#"{"name":"demo","version":"1.2.0","description":"a demo"}"#,
        )
        .unwrap();
        std::fs::create_dir_all(plugin_dir.join("skills").join("demo-skill")).unwrap();
        std::fs::write(
            plugin_dir
                .join("skills")
                .join("demo-skill")
                .join("SKILL.md"),
            "---\nname: demo-skill\ndescription: from the package\n---\n\nbody\n",
        )
        .unwrap();

        // Exactly the shape the host writes: every required field present, one
        // record per id, `installedAtMs` as epoch millis. The host (the shell's
        // config migration) writes it into `[plugins]` in the engine's config
        // file, so seed it there through the config store rather than the retired
        // `installed_plugins.json`.
        crate::config_store::apply_edits(
            &home
                .parent()
                .unwrap()
                .join(crate::config_store::CONFIG_FILE),
            &[crate::config_store::ConfigEdit {
                key_path: "plugins".to_string(),
                value: serde_json::json!({
                    "demo@test-market": {
                        "name": "demo",
                        "pluginId": "demo@test-market",
                        "marketplace": "test-market",
                        "version": "1.2.0",
                        "installPath": plugin_dir.to_string_lossy(),
                        "source": "marketplace:test-market",
                        "installedAtMs": 1786367310063u64,
                        "enabled": true,
                    }
                }),
                merge: crate::config_store::MergeStrategy::Replace,
            }],
            None,
        )
        .await
        .unwrap();

        // Decoding must succeed at all — this is the half that used to abort boot.
        let state = read_installed_plugins(&home).await.unwrap();
        let record = state
            .plugins
            .get(&RuntimeString::from("demo@test-market"))
            .expect("the host's ledger key survives decoding");
        assert!(record.enabled);
        assert_eq!(record.installed_at_ms, 1786367310063);

        // And the package behind it loads, with its declared contributions.
        let report = load_installed_plugins_report(
            &home,
            crate::extensions::command_loader::CommandLoadOptions::default(),
        )
        .await
        .unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(report.enabled.len(), 1);
        let loaded = &report.enabled[0];
        assert_eq!(loaded.name, "demo");
        assert_eq!(
            loaded
                .skills
                .iter()
                .map(|s| s.name.clone())
                .collect::<Vec<_>>(),
            vec![RuntimeString::from("demo-skill")],
            "the plugin's skills reach the session through the engine's own loader"
        );
    }

    #[tokio::test]
    async fn package_records_use_plugin_id_keys_and_can_be_addressed_by_name_or_id() {
        let home = temp_dir();
        let plugin_dir = home
            .join("cache")
            .join("bundled")
            .join("docs")
            .join("1.2.3");
        std::fs::create_dir_all(&plugin_dir).unwrap();
        std::fs::write(
            plugin_dir.join("plugin.json"),
            r#"{"name":"docs","version":"1.2.3"}"#,
        )
        .unwrap();
        let plugin = crate::extensions::plugins::loader::load_one_plugin(
            &plugin_dir.to_string_lossy(),
            CommandLoadOptions::default(),
        )
        .await
        .unwrap();
        let id = PluginId::parse("docs@bundled").unwrap();
        record_installed_plugin_package(
            &home,
            &plugin,
            &id,
            "1.2.3",
            Some(RuntimeString::from(
                home.join("data/docs-bundled")
                    .to_string_lossy()
                    .into_owned(),
            )),
            "marketplace:bundled",
        )
        .await
        .unwrap();

        let state = read_installed_plugins(&home).await.unwrap();
        let record = state
            .plugins
            .get(&RuntimeString::from("docs@bundled"))
            .unwrap();
        let expected_data = home
            .join("data/docs-bundled")
            .to_string_lossy()
            .into_owned();
        assert_eq!(
            record.plugin_id.as_ref(),
            Some(&RuntimeString::from("docs@bundled"))
        );
        assert_eq!(
            record.marketplace.as_ref(),
            Some(&RuntimeString::from("bundled"))
        );
        assert_eq!(
            record.data_path.as_ref(),
            Some(&RuntimeString::from(expected_data))
        );

        let disabled = set_installed_plugin_enabled(&home, "docs", false)
            .await
            .unwrap()
            .unwrap();
        assert!(!disabled.enabled);
        let enabled = set_installed_plugin_enabled(&home, "docs@bundled", true)
            .await
            .unwrap()
            .unwrap();
        assert!(enabled.enabled);
    }

    #[tokio::test]
    async fn load_report_for_package_records_uses_committed_ledger_version() {
        let home = temp_dir();
        let store = PluginStore::new(&home);
        let id = PluginId::parse("docs@local").unwrap();

        let source_v1 = temp_dir();
        std::fs::write(
            source_v1.join("plugin.json"),
            r#"{"name":"docs","version":"1.0.0"}"#,
        )
        .unwrap();
        let installed_v1 = store
            .install_dir(&id, Some("1.0.0"), &source_v1)
            .await
            .unwrap();
        let source_v2 = temp_dir();
        std::fs::write(
            source_v2.join("plugin.json"),
            r#"{"name":"docs","version":"2.0.0"}"#,
        )
        .unwrap();
        let installed_v2 = store
            .install_dir(&id, Some("2.0.0"), &source_v2)
            .await
            .unwrap();

        let mut state = InstalledPlugins::default();
        state.plugins.insert(
            id.as_key(),
            InstalledPluginRecord {
                name: RuntimeString::from("docs"),
                plugin_id: Some(id.as_key()),
                marketplace: Some(RuntimeString::from("local")),
                version: Some(RuntimeString::from("1.0.0")),
                install_path: installed_v1.installed_path.clone(),
                data_path: Some(installed_v1.data_path),
                source: RuntimeString::from("local:test"),
                scope: None,
                dependencies: Vec::new(),
                installed_at_ms: 1,
                enabled: true,
            },
        );
        write_installed_plugins(&home, &state).await.unwrap();

        let report = load_installed_plugins_report(&home, CommandLoadOptions::default())
            .await
            .unwrap();

        assert_eq!(report.enabled.len(), 1);
        assert_eq!(report.enabled[0].version.as_deref(), Some("1.0.0"));
        assert_eq!(
            report.enabled[0].dir,
            node_utf8_installed_plugin_host_boundary(&installed_v1.installed_path)
        );
        assert_ne!(installed_v1.installed_path, installed_v2.installed_path);
    }

    #[tokio::test]
    async fn failed_package_ledger_compare_keeps_old_activation_and_scoped_cleanup() {
        let home = temp_dir();
        let store = PluginStore::new(&home);
        let id = PluginId::parse("docs@local").unwrap();
        let source = temp_dir();
        std::fs::write(
            source.join("plugin.json"),
            r#"{"name":"docs","version":"1.0.0"}"#,
        )
        .unwrap();
        let first = store
            .install_dir(&id, Some("1.0.0"), &source)
            .await
            .unwrap();
        let loaded = crate::extensions::plugins::loader::load_one_plugin(
            &node_utf8_installed_plugin_host_boundary(&first.installed_path),
            CommandLoadOptions::default(),
        )
        .await
        .unwrap();
        record_installed_plugin_package(
            &home,
            &loaded,
            &id,
            first.version.clone(),
            Some(first.data_path.clone()),
            "marketplace:local",
        )
        .await
        .unwrap();
        let (mut state, expected) = read_installed_plugins_with_version(&home).await.unwrap();
        state.plugins.get_mut(&id.as_key()).unwrap().scope =
            Some(RuntimeString::from("changed while staging"));
        write_installed_plugins(&home, &state).await.unwrap();
        std::fs::write(
            source.join("plugin.json"),
            r#"{"name":"docs","version":"2.0.0"}"#,
        )
        .unwrap();
        let staged = store
            .install_dir(&id, Some("2.0.0"), &source)
            .await
            .unwrap();
        let loaded = crate::extensions::plugins::loader::load_one_plugin(
            &node_utf8_installed_plugin_host_boundary(&staged.installed_path),
            CommandLoadOptions::default(),
        )
        .await
        .unwrap();
        assert!(record_installed_plugin_package_if_version(
            &home,
            &loaded,
            &id,
            staged.version.clone(),
            Some(staged.data_path),
            "marketplace:local",
            &expected
        )
        .await
        .is_err());
        store
            .remove_generation(
                &id,
                Path::new(&node_utf8_installed_plugin_host_boundary(
                    &staged.installed_path,
                )),
            )
            .await
            .unwrap();
        let report = load_installed_plugins_report(&home, CommandLoadOptions::default())
            .await
            .unwrap();
        assert_eq!(report.enabled[0].version.as_deref(), Some("1.0.0"));
        assert!(Path::new(&node_utf8_installed_plugin_host_boundary(
            &first.installed_path
        ))
        .exists());
        assert!(!Path::new(&node_utf8_installed_plugin_host_boundary(
            &staged.installed_path
        ))
        .exists());
        assert_eq!(
            read_installed_plugins(&home).await.unwrap().plugins[&id.as_key()].scope,
            Some(RuntimeString::from("changed while staging"))
        );
    }

    #[tokio::test]
    async fn uninstall_refuses_when_another_plugin_depends_on_the_target() {
        let home = temp_dir();
        let helper_dir = home.join("helper");
        let main_dir = home.join("main");
        std::fs::create_dir_all(&helper_dir).unwrap();
        std::fs::create_dir_all(&main_dir).unwrap();

        let mut state = InstalledPlugins::default();
        state.plugins.insert(
            RuntimeString::from("helper@local"),
            InstalledPluginRecord {
                name: RuntimeString::from("helper"),
                plugin_id: Some(RuntimeString::from("helper@local")),
                marketplace: Some(RuntimeString::from("local")),
                version: Some(RuntimeString::from("1.0.0")),
                install_path: RuntimeString::from(helper_dir.to_string_lossy()),
                data_path: None,
                source: RuntimeString::from("catalog:helper@local"),
                scope: None,
                dependencies: Vec::new(),
                installed_at_ms: 1,
                enabled: true,
            },
        );
        state.plugins.insert(
            RuntimeString::from("main@local"),
            InstalledPluginRecord {
                name: RuntimeString::from("main"),
                plugin_id: Some(RuntimeString::from("main@local")),
                marketplace: Some(RuntimeString::from("local")),
                version: Some(RuntimeString::from("1.0.0")),
                install_path: RuntimeString::from(main_dir.to_string_lossy()),
                data_path: None,
                source: RuntimeString::from("catalog:main@local"),
                scope: None,
                dependencies: vec![RuntimeString::from("helper@local")],
                installed_at_ms: 1,
                enabled: true,
            },
        );
        write_installed_plugins(&home, &state).await.unwrap();

        let err = uninstall_installed_plugin(&home, "helper@local", false)
            .await
            .unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(read_installed_plugins(&home)
            .await
            .unwrap()
            .plugins
            .contains_key(&RuntimeString::from("helper@local")));
    }
}
