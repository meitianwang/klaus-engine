use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use klaus_agent_core::engine::agent::{Agent, AgentOptions, StartSessionOptions};
use klaus_agent_core::engine::deps::EngineDepsOverrides;
use klaus_agent_core::engine::rewind::BoxError;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::extensions::plugins::installed::{
    read_installed_plugins, write_installed_plugins, InstalledPluginRecord, InstalledPlugins,
};
use klaus_agent_core::extensions::plugins::loader::PluginLoadReport;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::testing::fake_provider;
use klaus_agent_protocol::{
    CreateSession, CreateSessionFuture, PluginActivationScope, ProtocolClient,
    ProtocolClientOptions, ProtocolServer, ProtocolServerOptions,
};
use klaus_agent_server::{load_endpoint_installed_plugins, protocol_plugin_admin};

fn temp_dir() -> PathBuf {
    static SEQ: AtomicUsize = AtomicUsize::new(0);
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "klaus-stdio-plugins-{}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    // The engine keeps the installed-plugin ledger in `[plugins]` of the config
    // file BESIDE the plugin home (`home.parent()/config.toml`), so the home needs
    // a unique parent per fixture or parallel tests would share one ledger.
    dir.push("plugins");
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn installed_record(name: &str, install_path: &Path, enabled: bool) -> InstalledPluginRecord {
    InstalledPluginRecord {
        name: runtime_string(name),
        plugin_id: None,
        marketplace: Some(runtime_string("local")),
        version: Some(runtime_string("1.0.0")),
        install_path: runtime_path_string(install_path),
        data_path: None,
        source: runtime_string("marketplace:local"),
        scope: Some(runtime_string("user")),
        dependencies: Vec::new(),
        installed_at_ms: 1,
        enabled,
    }
}

fn runtime_string(value: &str) -> RuntimeString {
    RuntimeString::from(value)
}

fn runtime_path_string(path: &Path) -> RuntimeString {
    runtime_string(
        path.to_str()
            .expect("stdio plugin fixture path must be a Unicode scalar string"),
    )
}

async fn plugin_home_fixture() -> PathBuf {
    let home = temp_dir();
    let enabled = home.join("enabled");
    let disabled = home.join("disabled");
    let broken = home.join("broken");
    for dir in [&enabled, &disabled, &broken] {
        std::fs::create_dir_all(dir).unwrap();
    }
    std::fs::write(
        enabled.join("plugin.json"),
        r#"{"name":"enabled","version":"1.0.0","mcpServers":{"docs":{"command":"/usr/bin/true"}}}"#,
    )
    .unwrap();
    for path in [
        enabled.join("skills/lookup"),
        enabled.join("agents"),
        enabled.join("commands"),
        enabled.join("hooks"),
    ] {
        std::fs::create_dir_all(path).unwrap();
    }
    std::fs::write(
        enabled.join("skills/lookup/SKILL.md"),
        "---\nname: lookup\n---\nLook up docs.",
    )
    .unwrap();
    std::fs::write(
        enabled.join("agents/docs.md"),
        "---\nname: docs-agent\ndescription: Reads docs\n---\nRead docs.",
    )
    .unwrap();
    std::fs::write(
        enabled.join("commands/docs.md"),
        "---\ndescription: Read docs\n---\nRead docs.",
    )
    .unwrap();
    std::fs::write(
        enabled.join("hooks/hooks.json"),
        r#"{"PostToolUse":[{"hooks":[{"type":"command","command":"true"}]}]}"#,
    )
    .unwrap();
    std::fs::write(
        disabled.join("plugin.json"),
        r#"{"name":"disabled","version":"1.0.0"}"#,
    )
    .unwrap();

    let mut plugins = BTreeMap::new();
    plugins.insert(
        runtime_string("enabled@local"),
        installed_record("enabled", &enabled, true),
    );
    plugins.insert(
        runtime_string("disabled@local"),
        installed_record("disabled", &disabled, false),
    );
    plugins.insert(
        runtime_string("broken@local"),
        installed_record("broken", &broken, true),
    );
    write_installed_plugins(
        &home,
        &InstalledPlugins {
            version: 2,
            plugins,
        },
    )
    .await
    .unwrap();
    home
}

fn agent_with_report(report: PluginLoadReport) -> Arc<Agent> {
    Arc::new(Agent::new(AgentOptions {
        process_context: test_process_context(),
        provider: Arc::new(fake_provider()),
        config: None,
        tools: None,
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        cwd: None,
        deps: EngineDepsOverrides::default(),
        compactor: None,
        approval_handler: None,
        approval_reviewer: None,
        elicit_handler: None,
        credential_store: None,
        hooks: Vec::new(),
        observer: Some(None),
        logger: None,
        pricing: None,
        agents: None,
        include_default_agents: None,
        skills: None,
        output_styles: None,
        workflows: None,
        mcp_catalog: None,
        plugin_catalog: None,
        lsp_servers: None,
        agent_memory: None,
        mcp_clients: None,
        mcp_server_configs: Vec::new(),
        mcp_tools: None,
        refresh_tools: None,
        system_prompt_provider: None,
        plugins: Vec::new(),
        plugin_load_report: Some(report),
    }))
}

fn test_process_context() -> RuntimeProcessContext {
    // The supervisor is process state bound to one runtime, so it is owned by the
    // test binary (see `all.rs`) rather than minted here: a second owner in this
    // file would race the other integration file for the same global slot.
    crate::shared_process::context()
}

fn unreachable_create_session() -> CreateSession {
    Arc::new(|_| {
        Box::pin(async move {
            Err(Box::new(std::io::Error::other(
                "plugin endpoint ops must not bind a session",
            )) as BoxError)
        }) as CreateSessionFuture
    })
}

fn start_endpoint(
    agent: Arc<Agent>,
    plugin_home: PathBuf,
) -> (ProtocolClient, tokio::task::JoinHandle<()>) {
    let (client_side, server_side) = tokio::io::duplex(64 * 1024);
    let (client_input, client_output) = tokio::io::split(client_side);
    let (server_input, server_output) = tokio::io::split(server_side);
    let server = Arc::new(ProtocolServer::new(ProtocolServerOptions {
        create_session: unreachable_create_session(),
        lifecycle: None,
        plugin_admin: Some(protocol_plugin_admin(agent, plugin_home)),
        credential_store: None,
        input: Box::new(server_input),
        output: Box::new(server_output),
    }));
    let handle = tokio::spawn(server.start());
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: Box::new(client_input),
        output: Box::new(client_output),
    });
    (client, handle)
}

#[tokio::test]
async fn stdio_bootstraps_installed_plugins() {
    let home = plugin_home_fixture().await;
    let report = load_endpoint_installed_plugins(&home).await.unwrap();
    let runtime = agent_with_report(report).plugin_runtime_snapshot();

    assert_eq!(runtime.entries.len(), 1);
    assert_eq!(runtime.entries[0].name, "enabled");
    assert_eq!(runtime.disabled.len(), 1);
    assert_eq!(
        runtime.disabled[0].name.as_ref(),
        Some(&runtime_string("disabled"))
    );
    assert_eq!(runtime.errors.len(), 1);
    // The reported directory is a host path, so its last separator is the host's; assert on
    // the component instead of pinning one platform's spelling.
    let broken_dir = runtime.errors[0]
        .dir
        .try_to_utf8()
        .expect("the fixture plugin directory is a scalar host path");
    assert_eq!(
        std::path::Path::new(&broken_dir).file_name(),
        Some(std::ffi::OsStr::new("broken")),
        "{broken_dir}"
    );

    std::fs::remove_dir_all(home).unwrap();
}

#[tokio::test]
async fn plugin_admin_publishes_to_loaded_sessions_at_next_turn() {
    let home = plugin_home_fixture().await;
    let initial_agent = agent_with_report(load_endpoint_installed_plugins(&home).await.unwrap());
    let startup_runtime = initial_agent.plugin_runtime_snapshot();
    let existing = initial_agent.start_session(StartSessionOptions::default());
    let second = initial_agent.start_session(StartSessionOptions::default());
    let dropped = initial_agent.start_session(StartSessionOptions::default());
    drop(dropped);
    let existing_runtime = existing.extension_runtime_snapshot();
    assert!(existing
        .visible_skill_names()
        .contains(&runtime_string("lookup")));
    assert!(existing
        .agent_definition_names()
        .contains(&runtime_string("docs-agent")));
    assert_eq!(existing_runtime.hooks.len(), 1);
    let (client, handle) = start_endpoint(initial_agent.clone(), home.clone());
    let held_turn = existing.run_streamed(
        Input::Text("Already admitted".into()),
        TurnOptions::default(),
    );

    let installed = client.list_installed_plugins().await.unwrap();
    assert_eq!(installed.plugins.len(), 3);
    assert_eq!(client.get_plugin_runtime().await.unwrap(), startup_runtime);

    let disabled = client
        .set_installed_plugin_enabled("enabled@local".to_string(), false)
        .await
        .unwrap();
    assert!(disabled.persisted);
    assert!(!disabled.applied_to_current_session);
    assert_eq!(disabled.activation_scope, PluginActivationScope::NextTurn);
    assert!(!disabled.restart_required);
    assert!(!disabled.plugin.enabled);
    assert_eq!(disabled.installed_plugins.plugins.len(), 3);
    assert!(disabled.runtime.entries.is_empty());
    assert!(disabled
        .runtime
        .disabled
        .iter()
        .any(|entry| entry.name.as_ref() == Some(&runtime_string("enabled"))));
    assert_eq!(client.get_plugin_runtime().await.unwrap(), disabled.runtime);
    assert_eq!(existing.extension_runtime_snapshot(), existing_runtime);
    assert!(existing
        .visible_skill_names()
        .contains(&runtime_string("lookup")));
    drop(held_turn);
    let next_turn =
        existing.run_streamed(Input::Text("After disable".into()), TurnOptions::default());
    assert_eq!(
        existing.extension_runtime_snapshot().plugin_runtime,
        disabled.runtime
    );
    assert!(!existing
        .visible_skill_names()
        .contains(&runtime_string("lookup")));
    assert!(!existing
        .agent_definition_names()
        .contains(&runtime_string("docs-agent")));
    assert!(existing.extension_runtime_snapshot().hooks.is_empty());
    drop(next_turn);
    let second_turn = second.run_streamed(
        Input::Text("Second loaded session".into()),
        TurnOptions::default(),
    );
    assert_eq!(
        second.extension_runtime_snapshot().plugin_runtime,
        disabled.runtime
    );
    drop(second_turn);
    let after_disable = initial_agent.start_session(StartSessionOptions::default());
    assert_eq!(
        after_disable.extension_runtime_snapshot().plugin_runtime,
        disabled.runtime
    );
    assert!(!after_disable
        .visible_skill_names()
        .contains(&runtime_string("lookup")));
    assert!(!after_disable
        .agent_definition_names()
        .contains(&runtime_string("docs-agent")));
    assert!(after_disable.extension_runtime_snapshot().hooks.is_empty());
    assert!(startup_runtime.entries.iter().any(|p| p.name == "enabled"));
    let persisted = read_installed_plugins(&home).await.unwrap();
    assert!(
        !persisted
            .plugins
            .get(&runtime_string("enabled@local"))
            .expect("enabled plugin record must remain installed")
            .enabled
    );

    let removed = client
        .uninstall_plugin("disabled@local".to_string(), true)
        .await
        .unwrap();
    assert_eq!(removed.plugin.name, "disabled");
    assert_eq!(removed.installed_plugins.plugins.len(), 2);
    assert!(removed.runtime.entries.is_empty());
    assert_eq!(removed.runtime.disabled.len(), 1);
    assert!(!home.join("disabled").exists());

    let reenabled = client
        .set_installed_plugin_enabled("enabled@local".to_string(), true)
        .await
        .unwrap();
    assert_eq!(reenabled.runtime.entries.len(), 1);
    assert_eq!(reenabled.runtime.entries[0].name, "enabled");
    assert_eq!(
        reenabled.runtime.entries[0].permission_scope.mcp_servers,
        vec![runtime_string("docs")]
    );
    let after_enable = initial_agent.start_session(StartSessionOptions::default());
    assert_eq!(
        after_enable.extension_runtime_snapshot().plugin_runtime,
        reenabled.runtime
    );
    assert!(after_enable
        .visible_skill_names()
        .contains(&runtime_string("lookup")));
    assert!(after_enable
        .agent_definition_names()
        .contains(&runtime_string("docs-agent")));
    assert_eq!(after_enable.extension_runtime_snapshot().hooks.len(), 1);
    let enabled_turn =
        existing.run_streamed(Input::Text("After enable".into()), TurnOptions::default());
    assert_eq!(
        existing.extension_runtime_snapshot().plugin_runtime,
        reenabled.runtime
    );
    assert!(existing
        .visible_skill_names()
        .contains(&runtime_string("lookup")));
    assert!(existing
        .agent_definition_names()
        .contains(&runtime_string("docs-agent")));
    assert_eq!(existing.extension_runtime_snapshot().hooks.len(), 1);
    drop(enabled_turn);

    let source = home.parent().unwrap().join("new-source");
    std::fs::create_dir_all(&source).unwrap();
    std::fs::write(
        source.join("plugin.json"),
        r#"{"name":"fresh","version":"1.0.0"}"#,
    )
    .unwrap();
    std::fs::create_dir_all(source.join("skills/fresh-skill")).unwrap();
    std::fs::write(
        source.join("skills/fresh-skill/SKILL.md"),
        "---\nname: fresh-skill\n---\nUse the new skill.",
    )
    .unwrap();
    let installed = client
        .install_plugin(
            "local".to_string(),
            source.to_str().unwrap().to_string(),
            Some("1.0.0".to_string()),
            None,
        )
        .await
        .unwrap();
    assert_eq!(installed.activation_scope, PluginActivationScope::NextTurn);
    assert!(!installed.restart_required);
    assert!(installed
        .runtime
        .entries
        .iter()
        .any(|entry| entry.name == "fresh"));
    let install_turn =
        existing.run_streamed(Input::Text("After install".into()), TurnOptions::default());
    assert!(existing
        .visible_skill_names()
        .contains(&runtime_string("fresh-skill")));
    let removed_fresh = client
        .uninstall_plugin("fresh@local".to_string(), true)
        .await
        .unwrap();
    assert_eq!(
        removed_fresh.activation_scope,
        PluginActivationScope::NextTurn
    );
    assert!(!removed_fresh.restart_required);
    assert!(!removed_fresh
        .runtime
        .entries
        .iter()
        .any(|entry| entry.name == "fresh"));
    let installed_path = PathBuf::from(removed_fresh.plugin.install_path.try_to_utf8().unwrap());
    assert!(
        installed_path.exists(),
        "the active turn retains its plugin files"
    );
    drop(install_turn);
    tokio::time::timeout(Duration::from_secs(5), async {
        while installed_path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("plugin files are removed after the active turn exits");
    let uninstall_turn = existing.run_streamed(
        Input::Text("After uninstall".into()),
        TurnOptions::default(),
    );
    assert_eq!(
        existing.extension_runtime_snapshot().plugin_runtime,
        removed_fresh.runtime
    );
    assert!(!existing
        .visible_skill_names()
        .contains(&runtime_string("fresh-skill")));
    drop(uninstall_turn);
    std::fs::remove_dir_all(source).unwrap();

    client
        .set_installed_plugin_enabled("enabled@local".to_string(), false)
        .await
        .unwrap();
    client.shutdown().await.unwrap();
    handle.await.unwrap();
    existing.dispose().await;
    second.dispose().await;
    after_disable.dispose().await;
    after_enable.dispose().await;

    let restarted_agent = agent_with_report(load_endpoint_installed_plugins(&home).await.unwrap());
    let restarted_runtime = restarted_agent.plugin_runtime_snapshot();
    let (restarted_client, restarted_handle) = start_endpoint(restarted_agent, home.clone());
    assert!(restarted_runtime.entries.is_empty());
    assert_eq!(restarted_runtime.disabled.len(), 1);
    assert_eq!(
        restarted_runtime.disabled[0].name.as_ref(),
        Some(&runtime_string("enabled"))
    );
    assert_eq!(restarted_runtime.errors.len(), 1);
    assert_eq!(
        restarted_client.get_plugin_runtime().await.unwrap(),
        restarted_runtime
    );

    restarted_client.shutdown().await.unwrap();
    restarted_handle.await.unwrap();
    std::fs::remove_dir_all(home).unwrap();
}
