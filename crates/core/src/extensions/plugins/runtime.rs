//! In-memory runtime registry for loaded plugins.
//!
//! The loader owns disk parsing and remote installation. This module owns the
//! engine runtime view: which loaded plugins are registered, enabled, and what
//! permission-relevant surfaces they declare.

use std::collections::BTreeMap;

use crate::capabilities::{
    RuntimeCapabilityDescriptor, RuntimeCapabilityName, RuntimeCapabilitySourceKind,
};
use crate::extensions::plugins::loader::{
    LoadedPlugin, PluginDisabledEntry, PluginLoadIssue, PluginLoadReport,
};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeEncoder, RuntimeJson,
    RuntimeObjectEncoder, RuntimeString,
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PluginPermissionScope {
    pub mcp_servers: Vec<RuntimeString>,
    pub lsp_servers: Vec<RuntimeString>,
}

impl RuntimeEncode for PluginPermissionScope {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("mcpServers", &self.mcp_servers)?;
        object.field("lspServers", &self.lsp_servers)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for PluginPermissionScope {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                mcp_servers: object.required("mcpServers", &[])?,
                lsp_servers: object.required("lspServers", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginRuntimeEntry {
    pub name: RuntimeString,
    pub version: Option<RuntimeString>,
    pub display_name: Option<RuntimeString>,
    pub description: Option<RuntimeString>,
    pub plugin_namespace: RuntimeString,
    pub keywords: Vec<RuntimeString>,
    pub dependencies: Vec<RuntimeString>,
    pub user_config: crate::types::json::JsonObject,
    pub settings: crate::types::json::JsonObject,
    pub dir: RuntimeString,
    pub enabled: bool,
    pub permission_scope: PluginPermissionScope,
    pub has_skills: bool,
    pub runtime_capabilities: Vec<RuntimeString>,
    pub interface_capabilities: Vec<RuntimeString>,
    pub app_connectors: Vec<RuntimeString>,
}

impl RuntimeEncode for PluginRuntimeEntry {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        object.field("version", &self.version)?;
        object.field("displayName", &self.display_name)?;
        object.field("description", &self.description)?;
        object.field("pluginNamespace", &self.plugin_namespace)?;
        object.field("keywords", &self.keywords)?;
        object.field("dependencies", &self.dependencies)?;
        object.field("dir", &self.dir)?;
        object.field("enabled", &self.enabled)?;
        object.field("permissionScope", &self.permission_scope)?;
        object.field("hasSkills", &self.has_skills)?;
        object.field("runtimeCapabilities", &self.runtime_capabilities)?;
        object.field("interfaceCapabilities", &self.interface_capabilities)?;
        object.field("appConnectors", &self.app_connectors)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for PluginRuntimeEntry {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                name: object.required("name", &[])?,
                version: object.required("version", &[])?,
                display_name: object.required("displayName", &[])?,
                description: object.required("description", &[])?,
                plugin_namespace: object.required("pluginNamespace", &[])?,
                keywords: object.required("keywords", &[])?,
                dependencies: object.required("dependencies", &[])?,
                user_config: crate::types::json::JsonObject::new(),
                settings: crate::types::json::JsonObject::new(),
                dir: object.required("dir", &[])?,
                enabled: object.required("enabled", &[])?,
                permission_scope: object.required("permissionScope", &[])?,
                has_skills: object.required("hasSkills", &[])?,
                runtime_capabilities: object.required("runtimeCapabilities", &[])?,
                interface_capabilities: object.required("interfaceCapabilities", &[])?,
                app_connectors: object.required("appConnectors", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginCapabilitySummary {
    pub name: RuntimeString,
    pub display_name: RuntimeString,
    pub description: Option<RuntimeString>,
    pub has_skills: bool,
    pub mcp_server_names: Vec<RuntimeString>,
    pub lsp_server_names: Vec<RuntimeString>,
    pub runtime_capabilities: Vec<RuntimeString>,
    pub interface_capabilities: Vec<RuntimeString>,
    pub app_connectors: Vec<RuntimeString>,
    pub dependencies: Vec<RuntimeString>,
}

impl RuntimeEncode for PluginCapabilitySummary {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        object.field("displayName", &self.display_name)?;
        object.field("description", &self.description)?;
        object.field("hasSkills", &self.has_skills)?;
        object.field("mcpServerNames", &self.mcp_server_names)?;
        object.field("lspServerNames", &self.lsp_server_names)?;
        object.field("runtimeCapabilities", &self.runtime_capabilities)?;
        object.field("interfaceCapabilities", &self.interface_capabilities)?;
        object.field("appConnectors", &self.app_connectors)?;
        object.field("dependencies", &self.dependencies)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for PluginCapabilitySummary {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                name: object.required("name", &[])?,
                display_name: object.required("displayName", &[])?,
                description: object.required("description", &[])?,
                has_skills: object.required("hasSkills", &[])?,
                mcp_server_names: object.required("mcpServerNames", &[])?,
                lsp_server_names: object.required("lspServerNames", &[])?,
                runtime_capabilities: object.required("runtimeCapabilities", &[])?,
                interface_capabilities: object.required("interfaceCapabilities", &[])?,
                app_connectors: object.required("appConnectors", &[])?,
                dependencies: object.required("dependencies", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PluginRuntimeSnapshot {
    pub entries: Vec<PluginRuntimeEntry>,
    pub capability_summaries: Vec<PluginCapabilitySummary>,
    pub disabled: Vec<PluginDisabledEntry>,
    pub errors: Vec<PluginLoadIssue>,
    pub warnings: Vec<PluginLoadIssue>,
    pub needs_refresh: bool,
    pub plugin_reconnect_key: u64,
}

impl RuntimeEncode for PluginRuntimeSnapshot {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("entries", &self.entries)?;
        object.field("capabilitySummaries", &self.capability_summaries)?;
        object.field("disabled", &self.disabled)?;
        object.field("errors", &self.errors)?;
        object.field("warnings", &self.warnings)?;
        object.field("needsRefresh", &self.needs_refresh)?;
        object.field("pluginReconnectKey", &self.plugin_reconnect_key)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for PluginRuntimeSnapshot {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                entries: object.required("entries", &[])?,
                capability_summaries: object.required("capabilitySummaries", &[])?,
                disabled: object.required("disabled", &[])?,
                errors: object.required("errors", &[])?,
                warnings: object.required("warnings", &[])?,
                needs_refresh: object.required("needsRefresh", &[])?,
                plugin_reconnect_key: object.required("pluginReconnectKey", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PluginRegistrationError {
    VersionConflict {
        name: String,
        existing: Option<String>,
        incoming: Option<String>,
    },
}

#[derive(Debug, Clone, Default)]
pub struct PluginRegistry {
    entries: BTreeMap<String, PluginRuntimeEntry>,
    disabled: Vec<PluginDisabledEntry>,
    errors: Vec<PluginLoadIssue>,
    warnings: Vec<PluginLoadIssue>,
    needs_refresh: bool,
    plugin_reconnect_key: u64,
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, plugin: &LoadedPlugin) -> Result<(), PluginRegistrationError> {
        let incoming_version = plugin.version.clone().map(RuntimeString::from);
        if let Some(existing) = self.entries.get(&plugin.name) {
            if existing.version != incoming_version {
                return Err(PluginRegistrationError::VersionConflict {
                    name: plugin.name.clone(),
                    existing: existing
                        .version
                        .as_ref()
                        .map(plugin_runtime_scalar_boundary),
                    incoming: plugin.version.clone(),
                });
            }
        }

        self.entries.insert(
            plugin.name.clone(),
            PluginRuntimeEntry {
                name: RuntimeString::from(plugin.name.clone()),
                version: incoming_version,
                display_name: plugin.display_name.clone().map(RuntimeString::from),
                description: plugin.description.clone().map(RuntimeString::from),
                plugin_namespace: RuntimeString::from(plugin.plugin_namespace.clone()),
                keywords: plugin
                    .keywords
                    .iter()
                    .cloned()
                    .map(RuntimeString::from)
                    .collect(),
                dependencies: plugin
                    .dependencies
                    .iter()
                    .cloned()
                    .map(RuntimeString::from)
                    .collect(),
                user_config: plugin.user_config.clone(),
                settings: plugin.settings.clone(),
                dir: RuntimeString::from(plugin.dir.clone()),
                enabled: true,
                permission_scope: permission_scope(plugin),
                has_skills: !plugin.skills.is_empty(),
                runtime_capabilities: plugin
                    .runtime_capabilities
                    .iter()
                    .cloned()
                    .map(RuntimeString::from)
                    .collect(),
                interface_capabilities: plugin
                    .interface_capabilities
                    .iter()
                    .cloned()
                    .map(RuntimeString::from)
                    .collect(),
                app_connectors: plugin
                    .app_connectors
                    .iter()
                    .cloned()
                    .map(RuntimeString::from)
                    .collect(),
            },
        );
        Ok(())
    }

    pub fn record_load_report(
        &mut self,
        report: &PluginLoadReport,
    ) -> Result<(), PluginRegistrationError> {
        for plugin in &report.enabled {
            self.register(plugin)?;
        }
        self.record_load_issues(report);
        Ok(())
    }

    pub fn record_load_issues(&mut self, report: &PluginLoadReport) {
        self.disabled.extend(report.disabled.clone());
        self.errors.extend(report.errors.clone());
        self.warnings.extend(report.warnings.clone());
    }

    pub fn record_warnings(&mut self, warnings: impl IntoIterator<Item = PluginLoadIssue>) {
        self.warnings.extend(warnings);
    }

    pub fn replace_plugin_mcp_scope(&mut self, name: &str, mcp_servers: Vec<String>) -> bool {
        match self.entries.get_mut(name) {
            Some(entry) => {
                entry.permission_scope.mcp_servers =
                    mcp_servers.into_iter().map(RuntimeString::from).collect();
                true
            }
            None => false,
        }
    }

    pub fn replace_with_load_report(
        &mut self,
        report: &PluginLoadReport,
    ) -> Result<(), PluginRegistrationError> {
        self.entries.clear();
        self.disabled.clear();
        self.errors.clear();
        self.warnings.clear();
        self.record_load_report(report)?;
        self.complete_refresh();
        Ok(())
    }

    pub fn mark_needs_refresh(&mut self) {
        self.needs_refresh = true;
    }

    pub fn complete_refresh(&mut self) {
        if self.needs_refresh {
            self.needs_refresh = false;
            self.plugin_reconnect_key = self.plugin_reconnect_key.saturating_add(1);
        }
    }

    pub fn disable(&mut self, name: &str) -> bool {
        self.set_enabled(name, false)
    }

    pub fn enable(&mut self, name: &str) -> bool {
        self.set_enabled(name, true)
    }

    pub fn remove(&mut self, name: &str) -> bool {
        self.entries.remove(name).is_some()
    }

    pub fn get(&self, name: &str) -> Option<&PluginRuntimeEntry> {
        self.entries.get(name)
    }

    pub fn list(&self) -> Vec<PluginRuntimeEntry> {
        self.entries.values().cloned().collect()
    }

    pub fn snapshot(&self) -> PluginRuntimeSnapshot {
        let entries = self.list();
        let capability_summaries = entries
            .iter()
            .filter_map(plugin_capability_summary_from_entry)
            .collect();
        PluginRuntimeSnapshot {
            entries,
            capability_summaries,
            disabled: self.disabled.clone(),
            errors: self.errors.clone(),
            warnings: self.warnings.clone(),
            needs_refresh: self.needs_refresh,
            plugin_reconnect_key: self.plugin_reconnect_key,
        }
    }

    pub fn record_runtime_capabilities(
        &mut self,
        descriptors: impl IntoIterator<Item = RuntimeCapabilityDescriptor>,
    ) {
        for descriptor in descriptors {
            if descriptor.source.kind != RuntimeCapabilitySourceKind::Plugin {
                continue;
            }
            let Ok(source_id) = descriptor.source.id.try_to_utf8() else {
                continue;
            };
            let Some(entry) = self.entries.get_mut(&source_id) else {
                continue;
            };
            let name = RuntimeString::from(runtime_capability_name(descriptor.name));
            if !entry.runtime_capabilities.contains(&name) {
                entry.runtime_capabilities.push(name);
                entry.runtime_capabilities.sort();
            }
        }
    }

    fn set_enabled(&mut self, name: &str, enabled: bool) -> bool {
        match self.entries.get_mut(name) {
            Some(entry) => {
                entry.enabled = enabled;
                true
            }
            None => false,
        }
    }
}

fn runtime_capability_name(name: RuntimeCapabilityName) -> &'static str {
    name.as_str()
}

fn plugin_runtime_scalar_boundary(value: &RuntimeString) -> String {
    value
        .try_to_utf8()
        .expect("plugin manifest scalar validation precedes runtime registry projection")
}

fn prompt_safe_description(description: Option<&RuntimeString>) -> Option<RuntimeString> {
    let description = plugin_runtime_scalar_boundary(description?)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if description.is_empty() {
        None
    } else {
        Some(RuntimeString::from(
            description.chars().take(1024).collect::<String>(),
        ))
    }
}

fn plugin_capability_summary_from_entry(
    entry: &PluginRuntimeEntry,
) -> Option<PluginCapabilitySummary> {
    if !entry.enabled {
        return None;
    }

    let mut mcp_server_names = entry.permission_scope.mcp_servers.clone();
    mcp_server_names.sort();
    mcp_server_names.dedup();
    let mut lsp_server_names = entry.permission_scope.lsp_servers.clone();
    lsp_server_names.sort();
    lsp_server_names.dedup();
    let mut runtime_capabilities = entry.runtime_capabilities.clone();
    runtime_capabilities.sort();
    runtime_capabilities.dedup();
    let mut interface_capabilities = entry.interface_capabilities.clone();
    interface_capabilities.sort();
    interface_capabilities.dedup();
    let mut app_connectors = entry.app_connectors.clone();
    app_connectors.sort();
    app_connectors.dedup();
    let mut dependencies = entry.dependencies.clone();
    dependencies.sort();
    dependencies.dedup();

    let has_surface = entry.has_skills
        || !mcp_server_names.is_empty()
        || !lsp_server_names.is_empty()
        || !runtime_capabilities.is_empty()
        || !interface_capabilities.is_empty()
        || !app_connectors.is_empty();
    if !has_surface {
        return None;
    }

    Some(PluginCapabilitySummary {
        name: entry.name.clone(),
        display_name: entry
            .display_name
            .as_ref()
            .filter(|name| !plugin_runtime_scalar_boundary(name).trim().is_empty())
            .unwrap_or(&entry.name)
            .clone(),
        description: prompt_safe_description(entry.description.as_ref()),
        has_skills: entry.has_skills,
        mcp_server_names,
        lsp_server_names,
        runtime_capabilities,
        interface_capabilities,
        app_connectors,
        dependencies,
    })
}

fn permission_scope(plugin: &LoadedPlugin) -> PluginPermissionScope {
    let mcp_servers = plugin
        .mcp_servers
        .iter()
        .map(|server| RuntimeString::from(server.name()))
        .collect();
    let mut lsp_servers = plugin
        .lsp_servers
        .keys()
        .cloned()
        .map(RuntimeString::from)
        .collect::<Vec<_>>();
    lsp_servers.sort();
    PluginPermissionScope {
        mcp_servers,
        lsp_servers,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extensions::command_loader::CommandLoadOptions;
    use crate::extensions::plugins::loader::load_one_plugin;
    use crate::mcp::types::McpServerConfig;
    use crate::mcp::types::McpServerOptions;
    use crate::types::json::JsonObject;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-plugin-runtime-{}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        dir.push(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn plugin(name: &str, version: Option<&str>, dir: &str) -> LoadedPlugin {
        let mut lsp_servers = JsonObject::new();
        lsp_servers.insert(
            "rust".to_string(),
            serde_json::json!({ "command": "rust-analyzer" }),
        );
        LoadedPlugin {
            name: name.to_string(),
            version: version.map(String::from),
            display_name: None,
            description: None,
            plugin_namespace: name.to_string(),
            keywords: Vec::new(),
            dependencies: Vec::new(),
            user_config: JsonObject::new(),
            settings: JsonObject::new(),
            dir: dir.to_string(),
            skills: Vec::new(),
            commands: Vec::new(),
            agents: Vec::new(),
            hooks: Vec::new(),
            output_styles: Vec::new(),
            mcp_servers: vec![McpServerConfig::Stdio {
                name: "docs".to_string(),
                command: "docs-mcp".to_string(),
                args: None,
                env: None,
                options: McpServerOptions::default(),
            }],
            lsp_servers,
            runtime_capabilities: Vec::new(),
            interface_capabilities: Vec::new(),
            app_connectors: Vec::new(),
        }
    }

    #[test]
    fn registers_plugin_with_permission_scope() {
        let mut registry = PluginRegistry::new();
        registry
            .register(&plugin("docs", Some("1.0.0"), "/plugins/docs"))
            .unwrap();

        let entry = registry.get("docs").unwrap();
        assert!(entry.enabled);
        assert_eq!(entry.version.as_ref(), Some(&RuntimeString::from("1.0.0")));
        assert_eq!(
            entry.permission_scope.mcp_servers,
            vec![RuntimeString::from("docs")]
        );
        assert_eq!(
            entry.permission_scope.lsp_servers,
            vec![RuntimeString::from("rust")]
        );
    }

    #[test]
    fn snapshot_includes_prompt_safe_plugin_capability_summaries() {
        let mut registry = PluginRegistry::new();
        let mut plugin = plugin("docs", Some("1.0.0"), "/plugins/docs");
        plugin.display_name = Some("Docs Browser".to_string());
        plugin.description = Some("  Reads   docs and controls browser pages.  ".to_string());
        plugin
            .skills
            .push(crate::extensions::skills::loader::Skill {
                name: "search-docs".into(),
                description: "Search docs".into(),
                allowed_tools: None,
                body: "Search docs".into(),
                path: "/plugins/docs/skills/search-docs/SKILL.md".into(),
                ..Default::default()
            });
        plugin.runtime_capabilities = vec!["browser".to_string()];
        registry.register(&plugin).unwrap();
        registry.disable("docs");

        assert!(registry.snapshot().capability_summaries.is_empty());

        registry.enable("docs");
        let snapshot = registry.snapshot();

        assert_eq!(snapshot.capability_summaries.len(), 1);
        let summary = &snapshot.capability_summaries[0];
        assert_eq!(summary.name, "docs");
        assert_eq!(summary.display_name, "Docs Browser");
        assert_eq!(
            summary.description.as_ref(),
            Some(&RuntimeString::from(
                "Reads docs and controls browser pages."
            ))
        );
        assert!(summary.has_skills);
        assert_eq!(summary.mcp_server_names, vec![RuntimeString::from("docs")]);
        assert_eq!(summary.lsp_server_names, vec![RuntimeString::from("rust")]);
        assert_eq!(
            summary.runtime_capabilities,
            vec![RuntimeString::from("browser")]
        );
    }

    #[test]
    fn rejects_same_plugin_name_with_different_version() {
        let mut registry = PluginRegistry::new();
        registry
            .register(&plugin("docs", Some("1.0.0"), "/plugins/docs"))
            .unwrap();

        let err = registry
            .register(&plugin("docs", Some("2.0.0"), "/plugins/docs-v2"))
            .unwrap_err();

        assert_eq!(
            err,
            PluginRegistrationError::VersionConflict {
                name: "docs".to_string(),
                existing: Some("1.0.0".to_string()),
                incoming: Some("2.0.0".to_string()),
            }
        );
    }

    #[test]
    fn disables_enables_and_removes_plugins() {
        let mut registry = PluginRegistry::new();
        registry
            .register(&plugin("docs", Some("1.0.0"), "/plugins/docs"))
            .unwrap();

        assert!(registry.disable("docs"));
        assert!(!registry.get("docs").unwrap().enabled);
        assert!(registry.enable("docs"));
        assert!(registry.get("docs").unwrap().enabled);
        assert!(registry.remove("docs"));
        assert!(registry.get("docs").is_none());
        assert!(!registry.remove("docs"));
    }

    #[tokio::test]
    async fn loader_preserves_manifest_version_for_registry_conflicts() {
        let root = temp_dir();
        let plugin_dir = root.join("docs");
        std::fs::create_dir_all(&plugin_dir).unwrap();
        std::fs::write(
            plugin_dir.join("plugin.json"),
            r#"{"name":"docs","version":"1.2.3"}"#,
        )
        .unwrap();

        let loaded = load_one_plugin(plugin_dir.to_str().unwrap(), CommandLoadOptions::default())
            .await
            .unwrap();

        assert_eq!(loaded.version.as_deref(), Some("1.2.3"));
    }

    #[test]
    fn runtime_snapshot_exposes_plugin_load_errors_warnings_and_disabled_entries() {
        let mut registry = PluginRegistry::new();
        registry
            .record_load_report(&crate::extensions::plugins::loader::PluginLoadReport {
                enabled: vec![plugin("docs", Some("1.0.0"), "/plugins/docs")],
                disabled: vec![crate::extensions::plugins::loader::PluginDisabledEntry {
                    name: Some("legacy".into()),
                    dir: "/plugins/legacy".into(),
                    reason: "disabled by manifest".into(),
                }],
                errors: vec![crate::extensions::plugins::loader::PluginLoadIssue {
                    kind: crate::extensions::plugins::loader::PluginLoadIssueKind::InvalidManifest,
                    dir: "/plugins/bad".into(),
                    message: "name required".into(),
                }],
                warnings: vec![crate::extensions::plugins::loader::PluginLoadIssue {
                    kind: crate::extensions::plugins::loader::PluginLoadIssueKind::UnsupportedRuntimeCapability,
                    dir: "/plugins/warn".into(),
                    message: "unknown runtime capability".into(),
                }],
            })
            .unwrap();

        let snapshot = registry.snapshot();

        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.disabled.len(), 1);
        assert_eq!(
            snapshot.disabled[0].name.as_ref(),
            Some(&RuntimeString::from("legacy"))
        );
        assert_eq!(snapshot.errors.len(), 1);
        assert_eq!(
            snapshot.errors[0].kind,
            crate::extensions::plugins::loader::PluginLoadIssueKind::InvalidManifest
        );
        assert_eq!(snapshot.warnings.len(), 1);
    }

    #[test]
    fn runtime_snapshot_wire_projection_omits_plugin_config_values() {
        let mut registry = PluginRegistry::new();
        let mut plugin = plugin("docs", Some("1.0.0"), "/plugins/docs");
        plugin.user_config.insert(
            "apiToken".to_string(),
            serde_json::json!({ "value": "user-config-secret" }),
        );
        plugin.settings.insert(
            "authorization".to_string(),
            serde_json::json!("settings-secret"),
        );
        registry.register(&plugin).unwrap();

        let encoded = runtime_codec::encode_value(&registry.snapshot()).unwrap();
        let serialized = crate::runtime_values::runtime_json::stringify(&encoded);
        let entry = encoded
            .get("entries")
            .and_then(RuntimeJson::as_array)
            .and_then(|entries| entries.first())
            .and_then(RuntimeJson::as_object)
            .unwrap();

        assert_eq!(
            entry.get("name").and_then(RuntimeJson::as_string),
            Some(&RuntimeString::from("docs"))
        );
        assert!(entry.get("userConfig").is_none());
        assert!(entry.get("settings").is_none());
        assert!(!serialized.contains("user-config-secret"));
        assert!(!serialized.contains("settings-secret"));
    }

    #[test]
    fn replace_load_report_drops_stale_plugin_state_for_hot_reload() {
        let mut registry = PluginRegistry::new();
        registry
            .record_load_report(&crate::extensions::plugins::loader::PluginLoadReport {
                enabled: vec![plugin("old", Some("1.0.0"), "/plugins/old")],
                disabled: Vec::new(),
                errors: vec![crate::extensions::plugins::loader::PluginLoadIssue {
                    kind: crate::extensions::plugins::loader::PluginLoadIssueKind::InvalidJson,
                    dir: "/plugins/bad".into(),
                    message: "invalid JSON manifest".into(),
                }],
                warnings: Vec::new(),
            })
            .unwrap();

        registry
            .replace_with_load_report(&crate::extensions::plugins::loader::PluginLoadReport {
                enabled: vec![plugin("new", Some("2.0.0"), "/plugins/new")],
                disabled: Vec::new(),
                errors: Vec::new(),
                warnings: Vec::new(),
            })
            .unwrap();

        let snapshot = registry.snapshot();
        assert_eq!(
            snapshot
                .entries
                .iter()
                .map(|entry| entry.name.clone())
                .collect::<Vec<_>>(),
            vec![RuntimeString::from("new")]
        );
        assert!(snapshot.errors.is_empty());
    }

    #[test]
    fn refresh_state_tracks_needs_refresh_and_reconnect_generation() {
        let mut registry = PluginRegistry::new();
        assert!(!registry.snapshot().needs_refresh);
        assert_eq!(registry.snapshot().plugin_reconnect_key, 0);

        registry.mark_needs_refresh();
        let snapshot = registry.snapshot();
        assert!(snapshot.needs_refresh);
        assert_eq!(snapshot.plugin_reconnect_key, 0);

        registry
            .replace_with_load_report(&crate::extensions::plugins::loader::PluginLoadReport {
                enabled: vec![plugin("fresh", Some("1.0.0"), "/plugins/fresh")],
                disabled: Vec::new(),
                errors: Vec::new(),
                warnings: Vec::new(),
            })
            .unwrap();

        let snapshot = registry.snapshot();
        assert!(!snapshot.needs_refresh);
        assert_eq!(snapshot.plugin_reconnect_key, 1);
    }
}
