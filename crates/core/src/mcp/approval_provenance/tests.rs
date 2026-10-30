use std::sync::Arc;

use serde_json::json;

use super::*;
use crate::extensions::command_loader::CommandLoadOptions;
use crate::extensions::plugins::loader::load_one_plugin;
use crate::mcp::config_file::{load_mcp_server_configs, LoadMcpConfigsOptions};
use crate::mcp::tool_wrapper::{wrap_mcp_tool, McpToolCall, WrapMcpToolOptions};
use crate::mcp::types::{McpCallResult, McpToolDescriptor};
use crate::types::permission::ApprovalMcpRegistration;

struct FixtureDirectory(PathBuf);

impl FixtureDirectory {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("klaus-mcp-provenance-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for FixtureDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn metadata(config: &McpServerConfig) -> ApprovalActionMetadata {
    let name = RuntimeString::from("read_records");
    let call: McpToolCall = Arc::new(|_, _, _, _| Box::pin(async { Ok(McpCallResult::default()) }));
    let tool = wrap_mcp_tool(
        config.name(),
        McpToolDescriptor {
            name: name.clone(),
            title: None,
            description: None,
            input_schema: None,
            annotations: None,
            meta: None,
        },
        call,
        Some(WrapMcpToolOptions {
            approval_registration: Some(ApprovalMcpRegistration {
                native_registration: PreparedMcpRegistration::from_config(
                    Arc::new(config.clone()),
                    &name,
                ),
                ..Default::default()
            }),
            ..Default::default()
        }),
    );
    tool.approval_metadata()
        .expect("the native wrapper captures metadata")
}

fn fixture_config(home: &Path, user_file: &Path, user: Value, effective: Value) -> McpServerConfig {
    let mut config = parse_mcp_server_entry("records", &effective).unwrap();
    config.options_mut().native_origin = Some(NativeMcpOrigin::configuration(
        home.to_path_buf(),
        user_file.to_path_buf(),
        Some(user),
        effective,
    ));
    config
}

fn use_fixture_home(config: &mut McpServerConfig, fixture_home: &Path) {
    match Arc::make_mut(&mut config.options_mut().native_origin.as_mut().unwrap().source) {
        NativeMcpSource::Configuration { home, .. } | NativeMcpSource::Plugin { home, .. } => {
            *home = fixture_home.to_path_buf();
        }
    }
}

#[tokio::test]
async fn native_user_configuration_binds_exact_registered_tool() {
    let directory = FixtureDirectory::new();
    let user_file = directory.path().join("mcp.json");
    std::fs::write(&user_file, "{}").unwrap();
    let entry = json!({"command":"records-server","args":["--read"],"env":{"ACCOUNT":"owner"}});
    let config = fixture_config(directory.path(), &user_file, entry.clone(), entry);
    let captured = metadata(&config);
    let trusted = trusted_tool_context(&captured).await.unwrap();
    assert_eq!(trusted.server, "records");
    assert_eq!(trusted.connector_id, None);
    assert_eq!(
        trusted.source,
        TrustedToolSource::UserConfiguration(user_file.display().to_string())
    );

    let mut replaced = captured.clone();
    let ApprovalActionSource::Mcp(tool) = &mut replaced.source else {
        unreachable!()
    };
    tool.tool_name = "different_tool".into();
    assert!(trusted_tool_context(&replaced).await.is_none());
    let mut replaced = captured.clone();
    let ApprovalActionSource::Mcp(tool) = &mut replaced.source else {
        unreachable!()
    };
    tool.server = "different_server".into();
    assert!(trusted_tool_context(&replaced).await.is_none());
}

#[tokio::test]
async fn changed_effective_raw_entry_is_not_attested_by_parsed_equality() {
    let directory = FixtureDirectory::new();
    let user_file = directory.path().join("mcp.json");
    std::fs::write(&user_file, "{}").unwrap();
    let user = json!({"command":"records-server","futureOption":"user"});
    let effective = json!({"command":"records-server","futureOption":"project"});
    let config = fixture_config(directory.path(), &user_file, user, effective);
    assert!(trusted_tool_context(&metadata(&config)).await.is_none());
}

#[tokio::test]
async fn prepared_configuration_requires_full_options_environment_and_auth_equality() {
    let directory = FixtureDirectory::new();
    let user_file = directory.path().join("mcp.json");
    std::fs::write(&user_file, "{}").unwrap();
    let entry = json!({"command":"records-server","env":{"ACCOUNT":"owner"}});
    let mut config = fixture_config(directory.path(), &user_file, entry.clone(), entry);
    config.options_mut().always_load = Some(true);
    assert!(trusted_tool_context(&metadata(&config)).await.is_none());
    config.options_mut().always_load = None;
    if let McpServerConfig::Stdio { env, .. } = &mut config {
        *env = parse_mcp_server_entry(
            "records",
            &json!({"command":"records-server","env":{"ACCOUNT":"other"}}),
        )
        .and_then(|config| match config {
            McpServerConfig::Stdio { env, .. } => env,
            _ => None,
        });
    }
    assert!(trusted_tool_context(&metadata(&config)).await.is_none());

    let entry = json!({"type":"http","url":"https://records.example/mcp","headers":{"Authorization":"original"}});
    let mut remote = fixture_config(directory.path(), &user_file, entry.clone(), entry);
    if let McpServerConfig::Http { headers, .. } = &mut remote {
        headers
            .as_mut()
            .unwrap()
            .insert("Authorization".into(), "changed".into());
    }
    assert!(trusted_tool_context(&metadata(&remote)).await.is_none());
}

#[tokio::test]
async fn native_file_loader_preserves_the_selected_named_raw_layer() {
    let directory = FixtureDirectory::new();
    let home = directory.path().join("home");
    let project = directory.path().join("project");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(project.join(".klaus")).unwrap();
    std::fs::write(
        home.join("mcp.json"),
        r#"{"mcpServers":{"records":{"command":"records-server","note":"user"}}}"#,
    )
    .unwrap();
    std::fs::write(
        project.join(".klaus/mcp.json"),
        r#"{"mcpServers":{"records":{"command":"records-server","note":"project"}}}"#,
    )
    .unwrap();
    let mut configs = load_mcp_server_configs(&LoadMcpConfigsOptions {
        cwd: project.display().to_string(),
        user_config_dir: Some(home.display().to_string()),
    })
    .await;
    assert_eq!(configs.len(), 1);
    use_fixture_home(&mut configs[0], &home);
    assert!(trusted_tool_context(&metadata(&configs[0])).await.is_none());
    std::fs::remove_file(project.join(".klaus/mcp.json")).unwrap();
    let mut configs = load_mcp_server_configs(&LoadMcpConfigsOptions {
        cwd: project.display().to_string(),
        user_config_dir: Some(home.display().to_string()),
    })
    .await;
    use_fixture_home(&mut configs[0], &home);
    assert!(trusted_tool_context(&metadata(&configs[0])).await.is_some());
}

#[tokio::test]
async fn manually_parsed_origin_claims_remain_external() {
    let config = parse_mcp_server_entry(
        "records",
        &json!({
            "command":"records-server",
            "nativeOrigin":{"source":"configuration","home":"/home/user"},
            "trusted":true,
            "connectorId":"claimed-connector"
        }),
    )
    .unwrap();
    assert!(config.options().native_origin.is_none());
    assert!(
        PreparedMcpRegistration::from_config(Arc::new(config.clone()), &"read_records".into())
            .is_none()
    );
    assert!(trusted_tool_context(&metadata(&config)).await.is_none());
}

#[tokio::test]
async fn native_plugin_registration_retains_its_real_root_and_declaration() {
    let directory = FixtureDirectory::new();
    let root = directory.path().join("plugins/records");
    std::fs::create_dir_all(root.join(".klaus-plugin")).unwrap();
    std::fs::write(
        root.join(".klaus-plugin/plugin.json"),
        r#"{"name":"records","mcpServers":"servers.json"}"#,
    )
    .unwrap();
    std::fs::write(
        root.join("servers.json"),
        r#"{"mcpServers":{"records":{"command":"records-server"}}}"#,
    )
    .unwrap();
    let plugin = load_one_plugin(root.to_str().unwrap(), CommandLoadOptions::default())
        .await
        .unwrap();
    let mut config = plugin.mcp_servers.into_iter().next().unwrap();
    use_fixture_home(&mut config, directory.path());
    let trusted = trusted_tool_context(&metadata(&config)).await.unwrap();
    assert_eq!(
        trusted.source,
        TrustedToolSource::UserConfiguration(root.display().to_string())
    );
    std::fs::write(root.join(".klaus-plugin/plugin.json"), "invalid JSON").unwrap();
    assert!(trusted_tool_context(&metadata(&config)).await.is_none());
}

#[cfg(unix)]
#[tokio::test]
async fn native_receipts_reject_user_and_plugin_symlink_escapes() {
    let home = FixtureDirectory::new();
    let outside = FixtureDirectory::new();
    let external_user = outside.path().join("mcp.json");
    std::fs::write(&external_user, "{}").unwrap();
    let user_file = home.path().join("mcp.json");
    std::os::unix::fs::symlink(&external_user, &user_file).unwrap();
    let entry = json!({"command":"records-server"});
    let config = fixture_config(home.path(), &user_file, entry.clone(), entry);
    assert!(trusted_tool_context(&metadata(&config)).await.is_none());

    let root = home.path().join("plugins/records");
    std::fs::create_dir_all(root.join(".klaus-plugin")).unwrap();
    std::fs::write(
        root.join(".klaus-plugin/plugin.json"),
        r#"{"name":"records","mcpServers":"servers.json"}"#,
    )
    .unwrap();
    let declaration = outside.path().join("servers.json");
    std::fs::write(&declaration, r#"{"records":{"command":"records-server"}}"#).unwrap();
    std::os::unix::fs::symlink(&declaration, root.join("servers.json")).unwrap();
    let plugin = load_one_plugin(root.to_str().unwrap(), CommandLoadOptions::default())
        .await
        .unwrap();
    let mut config = plugin.mcp_servers.into_iter().next().unwrap();
    use_fixture_home(&mut config, home.path());
    assert!(trusted_tool_context(&metadata(&config)).await.is_none());

    let external_root = outside.path().join("external-plugin");
    std::fs::create_dir_all(external_root.join(".klaus-plugin")).unwrap();
    std::fs::write(
        external_root.join(".klaus-plugin/plugin.json"),
        r#"{"name":"external","mcpServers":{"records":{"command":"records-server"}}}"#,
    )
    .unwrap();
    let linked_root = home.path().join("linked-plugin");
    std::os::unix::fs::symlink(&external_root, &linked_root).unwrap();
    let plugin = load_one_plugin(linked_root.to_str().unwrap(), CommandLoadOptions::default())
        .await
        .unwrap();
    let mut config = plugin.mcp_servers.into_iter().next().unwrap();
    use_fixture_home(&mut config, home.path());
    assert!(trusted_tool_context(&metadata(&config)).await.is_none());

    let manifest_root = home.path().join("linked-manifest");
    std::fs::create_dir_all(manifest_root.join(".klaus-plugin")).unwrap();
    std::os::unix::fs::symlink(
        external_root.join(".klaus-plugin/plugin.json"),
        manifest_root.join(".klaus-plugin/plugin.json"),
    )
    .unwrap();
    let plugin = load_one_plugin(
        manifest_root.to_str().unwrap(),
        CommandLoadOptions::default(),
    )
    .await
    .unwrap();
    let mut config = plugin.mcp_servers.into_iter().next().unwrap();
    use_fixture_home(&mut config, home.path());
    assert!(trusted_tool_context(&metadata(&config)).await.is_none());
}

#[test]
fn native_receipt_binding_requires_the_full_selected_configuration() {
    let entry = json!({"command":"records-server","env":{"ACCOUNT":"owner"}});
    let native = fixture_config(
        Path::new("/home/user/.klaus"),
        Path::new("/home/user/.klaus/mcp.json"),
        entry.clone(),
        entry.clone(),
    );
    let exact = parse_mcp_server_entry("records", &entry).unwrap();
    let changed = parse_mcp_server_entry(
        "records",
        &json!({"command":"records-server","env":{"ACCOUNT":"other"}}),
    )
    .unwrap();
    let mut configs = vec![exact, changed];
    bind_matching_file_origins(&mut configs, &[native]);
    assert!(configs[0].options().native_origin.is_some());
    assert!(configs[1].options().native_origin.is_none());
}
