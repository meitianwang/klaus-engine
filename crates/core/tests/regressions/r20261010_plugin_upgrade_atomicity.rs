//! Defect: updates enabled disabled plugins and replaced captured files before commit.
//! Root cause: reinstall overwrote state and runtime chose the newest cache directory.
//! Work item: 2026-10-10 plugin marketplace update safety.

use klaus_agent_core::config_store::{self, ConfigEdit, MergeStrategy};
use klaus_agent_core::extensions::command_loader::CommandLoadOptions;
use klaus_agent_core::extensions::plugins::identity::PluginId;
use klaus_agent_core::extensions::plugins::installed::{
    load_installed_plugins_report, read_installed_plugins, record_installed_plugin_package,
    set_installed_plugin_enabled, write_installed_plugins,
};
use klaus_agent_core::extensions::plugins::loader::load_one_plugin;
use klaus_agent_core::extensions::plugins::store::{PluginInstallResult, PluginStore};
use klaus_agent_core::runtime_values::RuntimeString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

struct TestRoot(PathBuf);
impl TestRoot {
    fn new() -> Self {
        static SEQUENCE: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "plugin-upgrade-regression-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn host(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8()).unwrap()
}

async fn package(root: &Path, version: &str, contents: &str) -> PathBuf {
    let dir = root.join(format!("source-{version}-{contents}"));
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(
        dir.join("plugin.json"),
        format!(r#"{{"name":"fixture","version":"{version}"}}"#),
    )
    .await
    .unwrap();
    tokio::fs::write(dir.join("marker.txt"), contents)
        .await
        .unwrap();
    dir
}

async fn record(home: &Path, id: &PluginId, result: &PluginInstallResult) {
    let plugin = load_one_plugin(&host(&result.installed_path), CommandLoadOptions::default())
        .await
        .unwrap();
    record_installed_plugin_package(
        home,
        &plugin,
        id,
        result.version.clone(),
        Some(result.data_path.clone()),
        "marketplace:local",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn package_upgrade_preserves_disabled_state_data_scope_and_unrelated_config() {
    let root = TestRoot::new();
    let home = root.path().join("profile/plugins");
    let config = root.path().join("profile/config.toml");
    config_store::apply_edits(
        &config,
        &[ConfigEdit {
            key_path: "marketplaces.fixture".into(),
            value: serde_json::json!({"keep":"value"}),
            merge: MergeStrategy::Replace,
        }],
        None,
    )
    .await
    .unwrap();
    let id = PluginId::parse("fixture@local").unwrap();
    let store = PluginStore::new(&home);
    let v1 = store
        .install_dir(
            &id,
            Some("1.0.0"),
            package(root.path(), "1.0.0", "old").await,
        )
        .await
        .unwrap();
    record(&home, &id, &v1).await;
    set_installed_plugin_enabled(&home, "fixture@local", false)
        .await
        .unwrap();
    let mut before = read_installed_plugins(&home).await.unwrap();
    let previous = before.plugins.get_mut(&id.as_key()).unwrap();
    previous.scope = Some(RuntimeString::from("user"));
    previous.installed_at_ms = 123;
    let data_path = previous.data_path.clone().unwrap();
    tokio::fs::create_dir_all(Path::new(&host(&data_path)))
        .await
        .unwrap();
    tokio::fs::write(
        Path::new(&host(&data_path)).join("settings.txt"),
        "private setting",
    )
    .await
    .unwrap();
    write_installed_plugins(&home, &before).await.unwrap();
    let v2 = store
        .install_dir(
            &id,
            Some("1.1.0"),
            package(root.path(), "1.1.0", "new").await,
        )
        .await
        .unwrap();
    record(&home, &id, &v2).await;
    let after = read_installed_plugins(&home).await.unwrap();
    let installed = &after.plugins[&id.as_key()];
    assert!(!installed.enabled);
    assert_eq!(
        installed.version.as_ref(),
        Some(&RuntimeString::from("1.1.0"))
    );
    assert_eq!(installed.data_path.as_ref(), Some(&data_path));
    assert_eq!(installed.scope.as_ref(), Some(&RuntimeString::from("user")));
    assert_eq!(installed.installed_at_ms, 123);
    assert_eq!(
        tokio::fs::read_to_string(Path::new(&host(&data_path)).join("settings.txt"))
            .await
            .unwrap(),
        "private setting"
    );
    assert_eq!(
        config_store::load(&config).await.unwrap().values["marketplaces"]["fixture"]["keep"],
        "value"
    );
    let report = load_installed_plugins_report(&home, CommandLoadOptions::default())
        .await
        .unwrap();
    assert!(report.enabled.is_empty());
    assert_eq!(report.disabled.len(), 1);
}

#[tokio::test]
async fn unpublished_package_never_overrides_the_committed_runtime_generation() {
    let root = TestRoot::new();
    let home = root.path().join("profile/plugins");
    let id = PluginId::parse("fixture@local").unwrap();
    let store = PluginStore::new(&home);
    let v1 = store
        .install_dir(
            &id,
            Some("1.0.0"),
            package(root.path(), "1.0.0", "old").await,
        )
        .await
        .unwrap();
    record(&home, &id, &v1).await;
    let _v2 = store
        .install_dir(
            &id,
            Some("2.0.0"),
            package(root.path(), "2.0.0", "uncommitted").await,
        )
        .await
        .unwrap();
    let report = load_installed_plugins_report(&home, CommandLoadOptions::default())
        .await
        .unwrap();
    assert_eq!(report.enabled.len(), 1);
    assert_eq!(report.enabled[0].version.as_deref(), Some("1.0.0"));
    assert_eq!(report.enabled[0].dir, host(&v1.installed_path));
}

#[tokio::test]
async fn same_version_update_keeps_captured_files_immutable_until_ledger_commit() {
    let root = TestRoot::new();
    let home = root.path().join("profile/plugins");
    let id = PluginId::parse("fixture@local").unwrap();
    let store = PluginStore::new(&home);
    let v1 = store
        .install_dir(
            &id,
            Some("1.0.0"),
            package(root.path(), "1.0.0", "old").await,
        )
        .await
        .unwrap();
    record(&home, &id, &v1).await;
    let replacement = store
        .install_dir(
            &id,
            Some("1.0.0"),
            package(root.path(), "1.0.0", "replacement").await,
        )
        .await
        .unwrap();
    assert_ne!(v1.installed_path, replacement.installed_path);
    assert_eq!(
        tokio::fs::read_to_string(Path::new(&host(&v1.installed_path)).join("marker.txt"))
            .await
            .unwrap(),
        "old"
    );
    let report = load_installed_plugins_report(&home, CommandLoadOptions::default())
        .await
        .unwrap();
    assert_eq!(report.enabled[0].dir, host(&v1.installed_path));
    record(&home, &id, &replacement).await;
    let updated = load_installed_plugins_report(&home, CommandLoadOptions::default())
        .await
        .unwrap();
    assert_eq!(updated.enabled[0].dir, host(&replacement.installed_path));
    assert_eq!(
        tokio::fs::read_to_string(Path::new(&host(&v1.installed_path)).join("marker.txt"))
            .await
            .unwrap(),
        "old"
    );
}
