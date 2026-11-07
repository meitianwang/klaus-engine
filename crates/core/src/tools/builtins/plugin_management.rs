//! Installed-plugin management tools.
//!
//! These tools are only added to the model-visible tool set when the host
//! injects a plugin home. Listing is read-only; enable/disable/uninstall mutate
//! the persisted installed-plugin ledger and tell the host to refresh plugin
//! state before expecting running sessions to see the change.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use serde_json::{json, Map as SchemaMap, Value as SchemaValue};

use crate::extensions::plugins::installed::{
    read_installed_plugins, set_installed_plugin_enabled, uninstall_installed_plugin,
    InstalledPluginRecord,
};
use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimeString};
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::permission::{PermissionReason, PermissionResult};
use crate::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};

pub enum PluginManageMutation {
    SetEnabled { name: String, enabled: bool },
    Uninstall { name: String, remove_files: bool },
}

pub struct PluginManageOutcome {
    pub record: InstalledPluginRecord,
    pub live: bool,
}

pub type PluginManageFuture =
    Pin<Box<dyn Future<Output = Result<Option<PluginManageOutcome>, String>> + Send>>;
pub type PluginManageFn = Arc<dyn Fn(PluginManageMutation) -> PluginManageFuture + Send + Sync>;

#[derive(Clone)]
pub struct PluginManagementToolOptions {
    pub home: String,
    pub handler: Option<Arc<OnceLock<PluginManageFn>>>,
}

pub fn create_plugin_list_tool(opts: PluginManagementToolOptions) -> Arc<dyn Tool> {
    Arc::new(PluginListTool {
        input_schema: empty_object_schema(),
        home: PathBuf::from(opts.home),
    })
}

pub fn create_plugin_manage_tool(opts: PluginManagementToolOptions) -> Arc<dyn Tool> {
    Arc::new(PluginManageTool {
        input_schema: manage_input_schema(),
        home: PathBuf::from(opts.home),
        handler: opts.handler,
    })
}

struct PluginListTool {
    input_schema: JsonSchema,
    home: PathBuf,
}

struct PluginManageTool {
    input_schema: JsonSchema,
    home: PathBuf,
    handler: Option<Arc<OnceLock<PluginManageFn>>>,
}

fn empty_object_schema() -> JsonSchema {
    let mut schema = SchemaMap::new();
    schema.insert("type".to_string(), json!("object"));
    schema.insert("properties".to_string(), json!({}));
    schema.insert("additionalProperties".to_string(), json!(false));
    schema
}

fn manage_input_schema() -> JsonSchema {
    let mut name = SchemaMap::new();
    name.insert("type".to_string(), json!("string"));
    name.insert(
        "description".to_string(),
        json!("Installed plugin name or pluginId from plugin_list"),
    );

    let mut action = SchemaMap::new();
    action.insert("type".to_string(), json!("string"));
    action.insert(
        "enum".to_string(),
        json!(["enable", "disable", "uninstall"]),
    );
    action.insert(
        "description".to_string(),
        json!("Lifecycle action to apply to the installed plugin"),
    );

    let mut remove_files = SchemaMap::new();
    remove_files.insert("type".to_string(), json!("boolean"));
    remove_files.insert(
        "description".to_string(),
        json!("For uninstall only: also delete the installed plugin package/data when it is inside the plugin home"),
    );

    let mut properties = SchemaMap::new();
    properties.insert("name".to_string(), SchemaValue::Object(name));
    properties.insert("action".to_string(), SchemaValue::Object(action));
    properties.insert("removeFiles".to_string(), SchemaValue::Object(remove_files));

    let mut schema = SchemaMap::new();
    schema.insert("type".to_string(), json!("object"));
    schema.insert("properties".to_string(), SchemaValue::Object(properties));
    schema.insert("required".to_string(), json!(["name", "action"]));
    schema.insert("additionalProperties".to_string(), json!(false));
    schema
}

fn record_json(record: &InstalledPluginRecord) -> RuntimeJson {
    let optional = |value: &Option<RuntimeString>| {
        value
            .as_ref()
            .cloned()
            .map(RuntimeJson::String)
            .unwrap_or(RuntimeJson::Null)
    };
    let mut output = RuntimeObject::new();
    output.insert("name", RuntimeJson::String(record.name.clone()));
    output.insert("pluginId", optional(&record.plugin_id));
    output.insert("marketplace", optional(&record.marketplace));
    output.insert("version", optional(&record.version));
    output.insert(
        "installPath",
        RuntimeJson::String(record.install_path.clone()),
    );
    output.insert("dataPath", optional(&record.data_path));
    output.insert("source", RuntimeJson::String(record.source.clone()));
    output.insert("scope", optional(&record.scope));
    output.insert(
        "dependencies",
        RuntimeJson::Array(
            record
                .dependencies
                .iter()
                .cloned()
                .map(RuntimeJson::String)
                .collect(),
        ),
    );
    output.insert("installedAtMs", RuntimeJson::from(record.installed_at_ms));
    output.insert("enabled", RuntimeJson::Bool(record.enabled));
    RuntimeJson::Object(output)
}

fn input_name(input: &Json) -> RuntimeString {
    input
        .get("name")
        .and_then(RuntimeJson::as_string)
        .cloned()
        .unwrap_or_default()
}

fn input_action(input: &Json) -> RuntimeString {
    input
        .get("action")
        .and_then(RuntimeJson::as_string)
        .cloned()
        .unwrap_or_default()
}

fn remove_files(input: &Json) -> bool {
    input
        .get("removeFiles")
        .and_then(RuntimeJson::as_bool)
        .unwrap_or(false)
}

fn list_output(
    plugins: Vec<RuntimeJson>,
    enabled_count: usize,
    disabled_count: usize,
) -> RuntimeJson {
    let mut output = RuntimeObject::new();
    output.insert("count", RuntimeJson::from(plugins.len()));
    output.insert("enabledCount", RuntimeJson::from(enabled_count));
    output.insert("disabledCount", RuntimeJson::from(disabled_count));
    output.insert("plugins", RuntimeJson::Array(plugins));
    RuntimeJson::Object(output)
}

#[allow(clippy::too_many_arguments)]
fn manage_output(
    name: Option<RuntimeString>,
    action: Option<RuntimeString>,
    changed_key: &str,
    changed: bool,
    enabled: Option<bool>,
    remove_files: Option<bool>,
    needs_refresh: bool,
    plugin: Option<&InstalledPluginRecord>,
) -> RuntimeJson {
    let mut output = RuntimeObject::new();
    if let Some(name) = name {
        output.insert("name", RuntimeJson::String(name));
    }
    if let Some(action) = action {
        output.insert("action", RuntimeJson::String(action));
    }
    output.insert(changed_key, RuntimeJson::Bool(changed));
    if let Some(enabled) = enabled {
        output.insert("enabled", RuntimeJson::Bool(enabled));
    }
    if let Some(remove_files) = remove_files {
        output.insert("removeFiles", RuntimeJson::Bool(remove_files));
    }
    output.insert("needsRefresh", RuntimeJson::Bool(needs_refresh));
    if let Some(plugin) = plugin {
        output.insert("plugin", record_json(plugin));
    }
    RuntimeJson::Object(output)
}

fn node_utf8_plugin_ledger_boundary(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8())
        .expect("Node UTF-8 installed-plugin ledger projection is valid UTF-8")
}

#[async_trait]
impl Tool for PluginListTool {
    fn static_name(&self) -> &str {
        "plugin_list"
    }

    fn static_description(&self) -> &str {
        "List installed plugins from the host-configured plugin home, including version, source, install path, and enabled state."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    fn summarize(&self, _input: &Json) -> Option<RuntimeString> {
        Some(RuntimeString::from("Listing installed plugins"))
    }

    async fn call(
        &self,
        _input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let state = match read_installed_plugins(&self.home).await {
            Ok(state) => state,
            Err(err) => {
                return Ok(text_result(
                    list_output(Vec::new(), 0, 0),
                    format!("Failed to read installed plugin state: {err}"),
                    true,
                ));
            }
        };
        let plugins = state.plugins.values().map(record_json).collect::<Vec<_>>();
        let enabled_count = state.plugins.values().filter(|p| p.enabled).count();
        let disabled_count = plugins.len().saturating_sub(enabled_count);
        let text = if plugins.is_empty() {
            "No plugins are installed.".to_string()
        } else {
            format!(
                "Found {} installed plugin(s): {} enabled, {} disabled.",
                plugins.len(),
                enabled_count,
                disabled_count
            )
        };
        Ok(text_result(
            list_output(plugins, enabled_count, disabled_count),
            text,
            false,
        ))
    }
}

impl PluginManageTool {
    async fn mutate(
        &self,
        mutation: PluginManageMutation,
    ) -> Result<Option<PluginManageOutcome>, String> {
        if let Some(handler) = self.handler.as_ref().and_then(|slot| slot.get()) {
            return handler(mutation).await;
        }
        match mutation {
            PluginManageMutation::SetEnabled { name, enabled } => {
                set_installed_plugin_enabled(&self.home, &name, enabled)
                    .await
                    .map_err(|error| error.to_string())
                    .map(|record| {
                        record.map(|record| PluginManageOutcome {
                            record,
                            live: false,
                        })
                    })
            }
            PluginManageMutation::Uninstall { name, remove_files } => {
                uninstall_installed_plugin(&self.home, &name, remove_files)
                    .await
                    .map_err(|error| error.to_string())
                    .map(|record| {
                        record.map(|record| PluginManageOutcome {
                            record,
                            live: false,
                        })
                    })
            }
        }
    }
}

#[async_trait]
impl Tool for PluginManageTool {
    fn static_name(&self) -> &str {
        "plugin_manage"
    }

    fn static_description(&self) -> &str {
        "Enable, disable, or uninstall one installed plugin by name. This mutates the installed-plugin ledger; the host should refresh plugin state afterwards."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    fn is_destructive(&self, input: &Json) -> Option<bool> {
        Some(input_action(input) == "uninstall")
    }

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        Some(
            RuntimeString::from("Managing plugin ")
                .concat(&input_name(input))
                .concat(&RuntimeString::from(": "))
                .concat(&input_action(input)),
        )
    }

    async fn check_permissions(
        &self,
        input: &Json,
        _ctx: &ToolContext,
    ) -> Option<PermissionResult> {
        let name = input_name(input);
        let action = input_action(input);
        let (prefix, suffix) = match &action {
            action if action == "enable" => ("Allow enabling plugin \"", "\"?"),
            action if action == "disable" => ("Allow disabling plugin \"", "\"?"),
            action if action == "uninstall" && remove_files(input) => {
                ("Allow uninstalling plugin \"", "\" and deleting its files?")
            }
            action if action == "uninstall" => ("Allow uninstalling plugin \"", "\"?"),
            _ => ("Allow changing plugin \"", "\"?"),
        };
        let message = RuntimeString::from(prefix)
            .concat(&name)
            .concat(&RuntimeString::from(suffix));
        Some(PermissionResult::Ask {
            message,
            updated_input: None,
            reason: PermissionReason::ToolSelfCheck,
            suggestions: None,
        })
    }

    async fn call(
        &self,
        input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let name = input_name(&input);
        let action = input_action(&input);
        if name.is_empty() {
            return Ok(text_result(
                manage_output(None, None, "updated", false, None, None, false, None),
                "Plugin name is required.",
                true,
            ));
        }

        match &action {
            action_value if action_value == "enable" || action_value == "disable" => {
                let enabled = action == "enable";
                let ledger_name = node_utf8_plugin_ledger_boundary(&name);
                match self
                    .mutate(PluginManageMutation::SetEnabled {
                        name: ledger_name,
                        enabled,
                    })
                    .await
                {
                    Ok(Some(outcome)) => {
                        let record = outcome.record;
                        Ok(text_result(
                            manage_output(
                                Some(record.name.clone()),
                                Some(action.clone()),
                                "updated",
                                true,
                                Some(record.enabled),
                                None,
                                !outcome.live,
                                Some(&record),
                            ),
                            RuntimeString::from(if record.enabled {
                                "Enabled plugin \""
                            } else {
                                "Disabled plugin \""
                            })
                            .concat(&record.name)
                            .concat(&RuntimeString::from(if outcome.live {
                                "\". Available in loaded sessions by the next turn."
                            } else {
                                "\". Refresh plugin state before expecting running sessions to see the change."
                            })),
                            false,
                        ))
                    }
                    Ok(None) => Ok(text_result(
                        manage_output(
                            Some(name.clone()),
                            Some(action.clone()),
                            "updated",
                            false,
                            None,
                            None,
                            false,
                            None,
                        ),
                        RuntimeString::from("Plugin \"")
                            .concat(&name)
                            .concat(&RuntimeString::from("\" is not installed.")),
                        true,
                    )),
                    Err(err) => Ok(text_result(
                        manage_output(
                            Some(name.clone()),
                            Some(action.clone()),
                            "updated",
                            false,
                            None,
                            None,
                            false,
                            None,
                        ),
                        RuntimeString::from("Failed to update plugin \"")
                            .concat(&name)
                            .concat(&RuntimeString::from(format!("\": {err}"))),
                        true,
                    )),
                }
            }
            action_value if action_value == "uninstall" => {
                let remove = remove_files(&input);
                let ledger_name = node_utf8_plugin_ledger_boundary(&name);
                match self
                    .mutate(PluginManageMutation::Uninstall {
                        name: ledger_name,
                        remove_files: remove,
                    })
                    .await
                {
                    Ok(Some(outcome)) => {
                        let record = outcome.record;
                        Ok(text_result(
                            manage_output(
                                Some(record.name.clone()),
                                Some(action.clone()),
                                "removed",
                                true,
                                None,
                                Some(remove),
                                !outcome.live,
                                Some(&record),
                            ),
                            RuntimeString::from("Uninstalled plugin \"")
                                .concat(&record.name)
                                .concat(&RuntimeString::from(if outcome.live {
                                    "\". Available in loaded sessions by the next turn."
                                } else {
                                    "\". Refresh plugin state before expecting running sessions to see the change."
                                })),
                            false,
                        ))
                    }
                    Ok(None) => Ok(text_result(
                        manage_output(
                            Some(name.clone()),
                            Some(action.clone()),
                            "removed",
                            false,
                            None,
                            None,
                            false,
                            None,
                        ),
                        RuntimeString::from("Plugin \"")
                            .concat(&name)
                            .concat(&RuntimeString::from("\" is not installed.")),
                        true,
                    )),
                    Err(err) => Ok(text_result(
                        manage_output(
                            Some(name.clone()),
                            Some(action.clone()),
                            "removed",
                            false,
                            None,
                            None,
                            false,
                            None,
                        ),
                        RuntimeString::from("Failed to uninstall plugin \"")
                            .concat(&name)
                            .concat(&RuntimeString::from(format!("\": {err}"))),
                        true,
                    )),
                }
            }
            _ => Ok(text_result(
                manage_output(
                    Some(name),
                    Some(action),
                    "updated",
                    false,
                    None,
                    None,
                    false,
                    None,
                ),
                "Invalid plugin action. Use enable, disable, or uninstall.",
                true,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extensions::plugins::installed::{
        read_installed_plugins, write_installed_plugins, InstalledPlugins,
    };
    use crate::runtime_values::runtime_json;
    use crate::tools::builtins::test_ctx::make_ctx;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn temp_dir() -> PathBuf {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "ae-plugin-management-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn record(name: &str, install_path: String) -> InstalledPluginRecord {
        InstalledPluginRecord {
            name: RuntimeString::from(name),
            plugin_id: None,
            marketplace: None,
            version: Some(RuntimeString::from("1.2.3")),
            install_path: RuntimeString::from(install_path),
            data_path: None,
            source: RuntimeString::from("catalog:docs"),
            scope: None,
            dependencies: Vec::new(),
            installed_at_ms: 12,
            enabled: true,
        }
    }

    fn exact_json(source: &str) -> RuntimeJson {
        runtime_json::parse(&RuntimeString::from(source)).expect("valid exact JSON fixture")
    }

    fn native_path(path: &std::path::Path) -> String {
        path.to_str()
            .expect("temporary test path is valid UTF-8")
            .to_string()
    }

    #[tokio::test]
    async fn list_returns_installed_plugin_state() {
        let home = temp_dir().join("plugins");
        let plugin_dir = home.join("docs");
        std::fs::create_dir_all(&plugin_dir).unwrap();
        let mut state = InstalledPlugins::default();
        state.plugins.insert(
            RuntimeString::from("docs"),
            record("docs", native_path(&plugin_dir)),
        );
        write_installed_plugins(&home, &state).await.unwrap();

        let tool = create_plugin_list_tool(PluginManagementToolOptions {
            home: native_path(&home),
            handler: None,
        });
        let result = tool
            .call(exact_json("{}"), &make_ctx("."), None)
            .await
            .unwrap();

        assert_eq!(result.serializable.is_error, Some(false));
        assert_eq!(
            result.serializable.output.get("count"),
            Some(&RuntimeJson::from(1))
        );
        assert_eq!(
            result.serializable.output["plugins"][0]["name"],
            RuntimeJson::String(RuntimeString::from("docs"))
        );
        assert_eq!(
            result.serializable.output["plugins"][0]["enabled"],
            RuntimeJson::Bool(true)
        );
    }

    #[tokio::test]
    async fn manage_uses_endpoint_handler_when_present() {
        let home = temp_dir().join("plugins");
        let slot = Arc::new(OnceLock::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&calls);
        let plugin = record("docs", native_path(&home.join("docs")));
        let handler: PluginManageFn = Arc::new(move |mutation| {
            let count = Arc::clone(&count);
            let mut plugin = plugin.clone();
            Box::pin(async move {
                assert!(matches!(
                    mutation,
                    PluginManageMutation::SetEnabled { enabled: false, .. }
                ));
                count.fetch_add(1, Ordering::SeqCst);
                plugin.enabled = false;
                Ok(Some(PluginManageOutcome {
                    record: plugin,
                    live: true,
                }))
            })
        });
        slot.set(handler).ok().unwrap();
        let tool = create_plugin_manage_tool(PluginManagementToolOptions {
            home: native_path(&home),
            handler: Some(slot),
        });
        let result = tool
            .call(
                exact_json(r#"{"name":"docs","action":"disable"}"#),
                &make_ctx("."),
                None,
            )
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            result.serializable.output["needsRefresh"],
            RuntimeJson::Bool(false)
        );
        assert_eq!(
            result.serializable.output["enabled"],
            RuntimeJson::Bool(false)
        );
        assert!(
            !home.exists(),
            "tool did not write the ledger behind the endpoint"
        );
    }

    #[tokio::test]
    async fn manage_updates_enablement_and_uninstalls() {
        let home = temp_dir().join("plugins");
        let plugin_dir = home.join("docs");
        std::fs::create_dir_all(&plugin_dir).unwrap();
        std::fs::write(plugin_dir.join("plugin.json"), "{}").unwrap();
        let mut state = InstalledPlugins::default();
        state.plugins.insert(
            RuntimeString::from("docs"),
            record("docs", native_path(&plugin_dir)),
        );
        write_installed_plugins(&home, &state).await.unwrap();

        let tool = create_plugin_manage_tool(PluginManagementToolOptions {
            home: native_path(&home),
            handler: None,
        });

        let disabled = tool
            .call(
                exact_json(r#"{"name":"docs","action":"disable"}"#),
                &make_ctx("."),
                None,
            )
            .await
            .unwrap();
        assert_eq!(disabled.serializable.is_error, Some(false));
        assert_eq!(
            disabled.serializable.output["enabled"],
            RuntimeJson::Bool(false)
        );
        assert!(
            !read_installed_plugins(&home)
                .await
                .unwrap()
                .plugins
                .get(&RuntimeString::from("docs"))
                .unwrap()
                .enabled
        );

        let enabled = tool
            .call(
                exact_json(r#"{"name":"docs","action":"enable"}"#),
                &make_ctx("."),
                None,
            )
            .await
            .unwrap();
        assert_eq!(enabled.serializable.is_error, Some(false));
        assert_eq!(
            enabled.serializable.output["enabled"],
            RuntimeJson::Bool(true)
        );

        let removed = tool
            .call(
                exact_json(r#"{"name":"docs","action":"uninstall","removeFiles":true}"#),
                &make_ctx("."),
                None,
            )
            .await
            .unwrap();
        assert_eq!(removed.serializable.is_error, Some(false));
        assert_eq!(
            removed.serializable.output["removed"],
            RuntimeJson::Bool(true)
        );
        assert!(!plugin_dir.exists());
        assert!(read_installed_plugins(&home)
            .await
            .unwrap()
            .plugins
            .is_empty());
    }
}
