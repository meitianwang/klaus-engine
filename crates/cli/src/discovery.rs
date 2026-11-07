//! Host-side extension discovery.
//!
//! The engine does not read extensions off disk. It is handed skills, sub-agent
//! definitions, command hooks and MCP servers by whoever is hosting it, because
//! "what is on disk" is a host policy question (which roots, whose trust, which
//! scopes win) and the engine has no business guessing. That makes discovery the
//! CLI's job, exactly as it is the desktop shell's job.
//!
//! The roots below therefore ARE the CLI's contract, not a re-derivation of some
//! other client's: project scope under `<cwd>/.klaus`, user scope under the
//! engine's config home, then whatever `--skills-dir` / `--agents-dir` add. First
//! writer of a name wins, so a project file shadows a user file of the same name.
//!
//! Everything found here crosses to the engine over the wire in
//! [`ProtocolSessionInit`], never as an in-process handle. A discovery result the
//! wire cannot express is dropped and reported, not silently downgraded: an MCP
//! server the CLI cannot describe on the wire would otherwise look connected and
//! do nothing.

use klaus_agent_core::engine::agent_definitions::{load_agent_definitions, AgentDefinition};
use klaus_agent_core::extensions::hooks_loader::load_hooks_json;
use klaus_agent_core::extensions::skills::loader::{discover_skills, Skill};
use klaus_agent_core::fs::paths::config_home;
use klaus_agent_core::hooks::types::{Hook, HookHandler};
use klaus_agent_core::mcp::config_file::{load_mcp_server_configs, LoadMcpConfigsOptions};
use klaus_agent_core::mcp::types::{McpOAuthOption, McpServerConfig, McpServerOptions};
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_protocol::{
    ProtocolCommandHook, ProtocolMcpOAuthOption, ProtocolMcpServerInit, ProtocolSessionInit,
};

use std::collections::{BTreeMap, HashSet};

/// What the CLI found on disk for one session, plus what it had to leave behind.
#[derive(Default)]
pub struct Discovered {
    pub skills: Vec<Skill>,
    pub agents: Vec<AgentDefinition>,
    pub command_hooks: Vec<ProtocolCommandHook>,
    pub mcp_servers: Vec<ProtocolMcpServerInit>,
    /// Human-readable reasons a discovered item did not make it onto the wire.
    /// Surfaced as diagnostics rather than dropped, because a silently missing
    /// extension is indistinguishable from a broken one.
    pub skipped: Vec<String>,
}

/// Which roots discovery reads, in precedence order.
pub struct DiscoveryOptions {
    pub cwd: String,
    pub skills_dirs: Vec<String>,
    pub agents_dirs: Vec<String>,
}

/// Read every root and return what the session init can carry.
pub async fn discover(opts: &DiscoveryOptions) -> Discovered {
    let mut found = Discovered {
        skills: discover_skills(&opts.cwd, &opts.skills_dirs).await,
        agents: discover_agents(&opts.cwd, &opts.agents_dirs).await,
        ..Default::default()
    };
    for hook in discover_hooks(&opts.cwd).await {
        match command_hook(&hook) {
            Some(wire) => found.command_hooks.push(wire),
            None => found.skipped.push(
                "hook: an in-process callback handler has no wire form and was skipped".to_string(),
            ),
        }
    }
    let (servers, skipped) = mcp_servers(&opts.cwd).await;
    found.mcp_servers = servers;
    found.skipped.extend(skipped);
    found
}

/// The MCP servers on disk in their wire form, plus a reason for each one that
/// could not be expressed.
///
/// Split out of [`discover`] because `mcp reload` re-reads exactly this and
/// nothing else: reloading servers must not also re-read skills, since replacing
/// the session's skill set is not what the caller asked for.
pub async fn mcp_servers(cwd: &str) -> (Vec<ProtocolMcpServerInit>, Vec<String>) {
    let configs = load_mcp_server_configs(&LoadMcpConfigsOptions {
        cwd: cwd.to_string(),
        user_config_dir: None,
    })
    .await;
    let mut servers = Vec::new();
    let mut skipped = Vec::new();
    for config in &configs {
        match mcp_server(config) {
            Some(wire) => servers.push(wire),
            None => skipped.push(format!(
                "mcp server {}: transport `{}` cannot be declared over the wire and was skipped",
                config.name(),
                config.transport()
            )),
        }
    }
    (servers, skipped)
}

/// Fold discovery into a session init, leaving every other field alone.
pub fn apply(init: &mut ProtocolSessionInit, found: Discovered) {
    init.skills = found.skills;
    init.agents = found.agents;
    init.command_hooks = found.command_hooks;
    if !found.mcp_servers.is_empty() {
        init.host_capabilities
            .get_or_insert_with(Default::default)
            .mcp_servers = found.mcp_servers;
    }
}

/// Sub-agent definitions from the project root, the user config home, then the
/// caller's extra dirs. Deduped by name, first root wins.
///
/// This reads through [`load_agent_definitions`] rather than the reduced loader
/// in `extensions::agent_loader`: the canonical parser produces exactly the type
/// the session init carries, so nothing has to be re-mapped on the way to the
/// wire, and it understands the fields the flat loader drops (`memory`,
/// `isolation`, `maxTurns`, `background`, `criticalSystemReminder`).
async fn discover_agents(cwd: &str, extra_dirs: &[String]) -> Vec<AgentDefinition> {
    let mut dirs = vec![
        format!("{cwd}/.klaus/agents"),
        format!("{}/agents", config_home()),
    ];
    dirs.extend(extra_dirs.iter().cloned());
    let mut seen: HashSet<RuntimeString> = HashSet::new();
    let mut result = Vec::new();
    for dir in &dirs {
        for agent in load_agent_definitions(dir).await {
            if seen.insert(agent.name.clone()) {
                result.push(agent);
            }
        }
    }
    result
}

/// Command hooks from `<configHome>/hooks.json` then `<cwd>/.klaus/hooks.json`.
/// Both files load: hooks are additive by design (a project hook does not
/// replace a user hook, it runs alongside it), so there is nothing to dedupe.
async fn discover_hooks(cwd: &str) -> Vec<Hook> {
    let mut hooks = load_hooks_json(&format!("{}/hooks.json", config_home())).await;
    hooks.extend(load_hooks_json(&format!("{cwd}/.klaus/hooks.json")).await);
    hooks
}

/// A hook's wire form, or `None` for a handler the wire cannot express.
fn command_hook(hook: &Hook) -> Option<ProtocolCommandHook> {
    let HookHandler::Command {
        command,
        timeout_ms,
    } = &hook.handler
    else {
        return None;
    };
    Some(ProtocolCommandHook {
        event: hook.event,
        matcher: hook.matcher.as_deref().map(RuntimeString::from),
        command: RuntimeString::from(command.as_str()),
        timeout_ms: *timeout_ms,
        is_async: hook.is_async,
    })
}

/// An MCP server's wire form, or `None` for a transport that only exists inside
/// a host process (`in-process`, `host-managed`). The CLI hosts no such server,
/// so one appearing here came from a config file that expected a different host.
fn mcp_server(config: &McpServerConfig) -> Option<ProtocolMcpServerInit> {
    match config {
        McpServerConfig::Stdio {
            name,
            command,
            args,
            env,
            options,
        } => Some(ProtocolMcpServerInit::Stdio {
            name: RuntimeString::from(name.as_str()),
            command: RuntimeString::from(command.as_str()),
            args: args.as_ref().map(|args| {
                args.iter()
                    .map(|arg| RuntimeString::from(arg.as_str()))
                    .collect()
            }),
            env: env.clone(),
            always_load: options.always_load,
            disabled_tools: disabled_tools(options),
        }),
        McpServerConfig::Http {
            name,
            url,
            headers,
            oauth,
            options,
        } => Some(ProtocolMcpServerInit::Http {
            name: RuntimeString::from(name.as_str()),
            url: RuntimeString::from(url.as_str()),
            headers: wire_headers(headers.as_ref()),
            oauth: wire_oauth(oauth.as_ref()),
            always_load: options.always_load,
            disabled_tools: disabled_tools(options),
        }),
        McpServerConfig::Sse {
            name,
            url,
            headers,
            oauth,
            options,
        } => Some(ProtocolMcpServerInit::Sse {
            name: RuntimeString::from(name.as_str()),
            url: RuntimeString::from(url.as_str()),
            headers: wire_headers(headers.as_ref()),
            oauth: wire_oauth(oauth.as_ref()),
            always_load: options.always_load,
            disabled_tools: disabled_tools(options),
        }),
        McpServerConfig::InProcess { .. } | McpServerConfig::HostManaged { .. } => None,
    }
}

fn disabled_tools(options: &McpServerOptions) -> Option<Vec<RuntimeString>> {
    options.disabled_tools.as_ref().map(|tools| {
        tools
            .iter()
            .map(|tool| RuntimeString::from(tool.as_str()))
            .collect()
    })
}

fn wire_headers(
    headers: Option<&std::collections::HashMap<String, String>>,
) -> Option<BTreeMap<RuntimeString, RuntimeString>> {
    headers.map(|headers| {
        headers
            .iter()
            .map(|(key, value)| {
                (
                    RuntimeString::from(key.as_str()),
                    RuntimeString::from(value.as_str()),
                )
            })
            .collect()
    })
}

/// The wire spells "use the server's advertised scopes" as `true`, where the
/// engine's own config type spells it as a unit variant.
fn wire_oauth(oauth: Option<&McpOAuthOption>) -> Option<ProtocolMcpOAuthOption> {
    oauth.map(|oauth| match oauth {
        McpOAuthOption::Advertised => ProtocolMcpOAuthOption::Advertised(true),
        McpOAuthOption::Object { scopes } => ProtocolMcpOAuthOption::Object {
            scopes: scopes.as_ref().map(|scopes| {
                scopes
                    .iter()
                    .map(|scope| RuntimeString::from(scope.as_str()))
                    .collect()
            }),
        },
    })
}
