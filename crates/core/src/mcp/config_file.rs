//! On-disk declaration of MCP servers — the user-facing way to add an MCP
//! server without writing host code. A friendly `mcpServers` map keyed by
//! server name lives in one of three scopes; each entry infers its transport
//! from its fields (`command` ⇒ stdio; `url` + `type: "sse"` ⇒ sse; `url` ⇒
//! http).
//!
//! Three scopes, by storage + sharing:
//!  - `local`  : private to you AND this project — `~/.klaus/projects.json`
//!    under `projects[<abs cwd>].mcpServers`. Not committed, no trust gate.
//!  - `project`: committed/shared with the repo — `<cwd>/.klaus/mcp.json`.
//!    Trust-gated (see [`crate::mcp::mcp_trust`]).
//!  - `user`   : private to you, ALL projects — `~/.klaus/mcp.json`. Not
//!    committed, no trust gate.
//!
//! Connect-time precedence (higher wins on a name clash):
//! local > project (approved) > user.

use crate::context::env_info::{current_platform, Platform};
use crate::fs::node_path::{dirname_runtime, project_runtime_path, resolve_runtime};
use crate::fs::paths::config_home;
use crate::mcp::types::{McpOAuthOption, McpServerConfig, McpServerOptions};
use crate::permission::bash::node_path::{dirname, resolve};
use crate::runtime_process::RuntimeEnv;
use crate::runtime_values::{
    runtime_json, NodeBufferUtf8, RuntimeJson, RuntimeObject, RuntimePath, RuntimeString,
};
use crate::types::json::JsonObject;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::io;

/// Where a declared MCP server lives. Three scopes, by storage + sharing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpScope {
    Local,
    Project,
    User,
}

/// Project-scope MCP config path: `<cwd>/.klaus/mcp.json`.
pub fn project_mcp_config_path(cwd: &str) -> String {
    join(&[cwd, ".klaus", "mcp.json"])
}

/// User-scope MCP config path: `~/.klaus/mcp.json` (honors `$KLAUS_CONFIG_DIR`).
pub fn user_mcp_config_path() -> String {
    join(&[&config_home(), "mcp.json"])
}

/// Local-scope store path: `~/.klaus/projects.json`. Holds per-project PRIVATE
/// servers keyed by the project's absolute path. Lives in the user's config
/// home (not the repo), so local servers are never committed or shared.
pub fn local_mcp_store_path(user_config_dir: Option<&str>) -> String {
    let home = config_home();
    let dir = user_config_dir.unwrap_or(&home);
    join(&[dir, "projects.json"])
}

/// The project key under which a cwd's local servers are stored (absolute,
/// normalized path).
fn local_project_key(cwd: &str) -> String {
    resolve(&[cwd])
}

/// Read the raw local store object; `None` on missing/unreadable/invalid-JSON
/// or a non-object (array/scalar) top level.
async fn read_local_store(user_config_dir: Option<&str>) -> Option<Value> {
    let bytes = tokio::fs::read(local_mcp_store_path(user_config_dir))
        .await
        .ok()?;
    let text = String::from_utf8_lossy(&bytes);
    let parsed: Value = serde_json::from_str(&text).ok()?;
    if parsed.is_object() {
        Some(parsed)
    } else {
        None
    }
}

/// Raw local-scope server entries for `cwd`, keyed by name.
pub async fn list_local_mcp_server_entries(cwd: &str, user_config_dir: Option<&str>) -> JsonObject {
    let store = match read_local_store(user_config_dir).await {
        Some(s) => s,
        None => return Map::new(),
    };
    store
        .get("projects")
        .and_then(|p| p.get(local_project_key(cwd)))
        .and_then(|proj| proj.get("mcpServers"))
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default()
}

/// Add/replace a local-scope server for `cwd` (validated, like
/// [`add_mcp_server_entry`]).
pub async fn add_local_mcp_server_entry(
    cwd: &str,
    name: &str,
    entry: &Value,
    user_config_dir: Option<&str>,
) -> io::Result<()> {
    if parse_mcp_server_entry(name, entry).is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            invalid_server_msg(name),
        ));
    }
    let mut store = read_local_store(user_config_dir)
        .await
        .unwrap_or_else(|| Value::Object(Map::new()));
    let key = local_project_key(cwd);
    let store_obj = ensure_object(&mut store);
    let projects = ensure_object(store_obj.entry("projects").or_insert(Value::Null));
    let project = ensure_object(projects.entry(key).or_insert(Value::Null));
    let servers = ensure_object(project.entry("mcpServers").or_insert(Value::Null));
    servers.insert(name.to_string(), entry.clone());
    write_local_store(&store, user_config_dir).await
}

/// Remove a local-scope server for `cwd`. Returns false if absent.
pub async fn remove_local_mcp_server_entry(
    cwd: &str,
    name: &str,
    user_config_dir: Option<&str>,
) -> io::Result<bool> {
    let mut store = match read_local_store(user_config_dir).await {
        Some(s) => s,
        None => return Ok(false),
    };
    let key = local_project_key(cwd);
    let servers = store
        .get_mut("projects")
        .and_then(|p| p.get_mut(&key))
        .and_then(|proj| proj.get_mut("mcpServers"))
        .and_then(Value::as_object_mut);
    let servers = match servers {
        Some(s) if s.contains_key(name) => s,
        _ => return Ok(false),
    };
    servers.remove(name);
    write_local_store(&store, user_config_dir).await?;
    Ok(true)
}

async fn write_local_store(store: &Value, user_config_dir: Option<&str>) -> io::Result<()> {
    let path = local_mcp_store_path(user_config_dir);
    tokio::fs::create_dir_all(dirname(&path)).await?;
    let json = serde_json::to_string_pretty(store).map_err(io::Error::other)?;
    tokio::fs::write(&path, format!("{json}\n")).await
}

/// The `platform` key a friendly entry may use to vary its transport per OS.
const PLATFORM_KEY: &str = "platform";

/// Fold an entry's `platform` overrides into the entry itself.
///
/// A server that ships an executable has to name a different program per OS, and
/// a declaration can only carry one `command`. Rather than make every caller
/// branch, an entry may carry a `platform` map keyed by `win32` / `darwin` /
/// `linux` whose values override the base fields. The merge is shallow — an
/// override replaces a base key wholesale instead of deep-merging into it — so
/// `args` in an override is the whole argument list, not an append. Shallow is
/// the predictable rule here: a deep merge would let an override inherit base
/// arguments the platform must not receive.
///
/// An entry whose transport only exists under `platform` and whose base carries
/// no `command`/`url` resolves to nothing on every other platform. That is the
/// intended shape for platform-specific servers: the declaration is absent, so
/// the server is never registered there, while the plugin's other components
/// still load.
fn resolve_platform_entry(raw: &Value, platform: Platform) -> Option<Value> {
    let base = raw.as_object()?;
    let overrides = match base.get(PLATFORM_KEY) {
        Some(Value::Object(map)) => map,
        // Absent or malformed: the base entry stands as written. A non-object
        // here is a declaration error, not a reason to drop the server.
        _ => return Some(raw.clone()),
    };
    let key = platform.as_str();
    let Some(Value::Object(patch)) = overrides.get(key) else {
        // No override for this platform: drop the `platform` key itself so the
        // transport dispatch below cannot mistake it for a field, and keep the
        // base entry.
        let mut base_without_platform = base.clone();
        base_without_platform.remove(PLATFORM_KEY);
        return Some(Value::Object(base_without_platform));
    };
    let mut merged = base.clone();
    merged.remove(PLATFORM_KEY);
    for (field, value) in patch {
        merged.insert(field.clone(), value.clone());
    }
    Some(Value::Object(merged))
}

/// Map one friendly file entry to an engine [`McpServerConfig`], inferring the
/// transport from its fields (`command` ⇒ stdio; `url` + `type: "sse"` ⇒ sse;
/// `url` ⇒ http). Returns `None` for a shape the engine can't connect from JSON
/// (e.g. an in-process server, which has no JSON representation). Non-string
/// `env`/`header` values are dropped (consistent with `args`) rather than cast
/// through unchecked. This is the single source of truth for the
/// friendly→config mapping (the plugin manifest loader reuses it).
pub fn parse_mcp_server_entry(name: &str, raw: &Value) -> Option<McpServerConfig> {
    parse_mcp_server_entry_on_platform(name, raw, current_platform())
}

/// [`parse_mcp_server_entry`] against an explicit platform, so a declaration
/// carrying `platform` overrides can be exercised for an OS other than the one
/// running the check.
pub fn parse_mcp_server_entry_on_platform(
    name: &str,
    raw: &Value,
    platform: Platform,
) -> Option<McpServerConfig> {
    let effective = resolve_platform_entry(raw, platform)?;
    parse_mcp_server_entry_resolved(name, &effective)
}

fn parse_mcp_server_entry_resolved(name: &str, raw: &Value) -> Option<McpServerConfig> {
    let r = raw.as_object()?;
    // `alwaysLoad: true` opts the server's tools out of deferred loading
    // (eager-load all its schemas).
    let always_load = if r.get("alwaysLoad") == Some(&Value::Bool(true)) {
        Some(true)
    } else {
        None
    };
    // `enabled: false` is the entry's OWN off switch, so the file a client edits
    // is the whole answer to "is this server on". Only `false` is material:
    // absent means enabled, and materializing `Some(true)` would make an
    // untouched entry compare unequal to a host-built one of the same server.
    let enabled = if r.get("enabled") == Some(&Value::Bool(false)) {
        Some(false)
    } else {
        None
    };
    // Per-TOOL off switches, same idea one level down: the names the user turned
    // off on this server. Absent and empty both mean "nothing denied", so an
    // emptied list does not survive as a meaningless `Some([])`.
    let disabled_tools = string_list(r.get("disabledTools"));
    // `hostManaged: true` declares a server the HOST application owns and
    // binds at runtime; its endpoint never appears in JSON, so the engine
    // keeps the declaration for attribution and enablement but never
    // connects it. Checked first: a host-managed entry carries no
    // command/url, and one that wrongly carries both is still host-managed.
    if r.get("hostManaged") == Some(&Value::Bool(true)) {
        return Some(McpServerConfig::HostManaged {
            name: name.to_string(),
            options: McpServerOptions {
                always_load,
                enabled,
                disabled_tools,
                ..Default::default()
            },
        });
    }
    if let Some(command) = r.get("command").and_then(Value::as_str) {
        let env = string_env_record(r.get("env"));
        let args = match r.get("args") {
            Some(Value::Array(arr)) => Some(
                arr.iter()
                    .filter_map(|a| a.as_str().map(String::from))
                    .collect(),
            ),
            _ => None,
        };
        return Some(McpServerConfig::Stdio {
            name: name.to_string(),
            command: command.to_string(),
            args,
            env,
            options: McpServerOptions {
                always_load,
                enabled,
                disabled_tools,
                ..Default::default()
            },
        });
    }
    if let Some(url) = r.get("url").and_then(Value::as_str) {
        let is_sse = r.get("type").and_then(Value::as_str) == Some("sse");
        let headers = string_record(r.get("headers"));
        let oauth = parse_oauth_option(r.get("oauth"));
        let name = name.to_string();
        let url = url.to_string();
        return Some(if is_sse {
            McpServerConfig::Sse {
                name,
                url,
                headers,
                oauth,
                options: McpServerOptions {
                    always_load,
                    enabled,
                    disabled_tools: disabled_tools.clone(),
                    ..Default::default()
                },
            }
        } else {
            McpServerConfig::Http {
                name,
                url,
                headers,
                oauth,
                options: McpServerOptions {
                    always_load,
                    enabled,
                    disabled_tools,
                    ..Default::default()
                },
            }
        });
    }
    None
}

/// A friendly string array (`disabledTools`), non-strings dropped like `args`.
/// An empty result is `None`: the field's absence and an emptied list are the
/// same statement, and materializing `Some([])` would make two equal configs
/// compare unequal.
fn string_list(raw: Option<&Value>) -> Option<Vec<String>> {
    let values: Vec<String> = raw?
        .as_array()?
        .iter()
        .filter_map(|value| value.as_str().map(String::from))
        .collect();
    (!values.is_empty()).then_some(values)
}

/// Exact-runtime counterpart used by catalog and runtime ingress. The friendly
/// JSON remains exact through validation and persistence; projection into the
/// still-scalar `McpServerConfig` happens only when constructing the concrete
/// transport configuration.
pub fn parse_mcp_server_entry_exact(
    name: &RuntimeString,
    raw: &RuntimeJson,
) -> Option<McpServerConfig> {
    let r = raw.as_object()?;
    let always_load = (r.get("alwaysLoad") == Some(&RuntimeJson::Bool(true))).then_some(true);
    let enabled = (r.get("enabled") == Some(&RuntimeJson::Bool(false))).then_some(false);
    if r.get("hostManaged") == Some(&RuntimeJson::Bool(true)) {
        return Some(McpServerConfig::HostManaged {
            name: project_runtime_transport_string(name),
            options: McpServerOptions {
                always_load,
                enabled,
                ..Default::default()
            },
        });
    }
    if let Some(command) = r.get("command").and_then(RuntimeJson::as_string) {
        let env = runtime_env_record(r.get("env"));
        let args = match r.get("args") {
            Some(RuntimeJson::Array(values)) => Some(
                values
                    .iter()
                    .filter_map(RuntimeJson::as_string)
                    .map(project_runtime_transport_string)
                    .collect(),
            ),
            _ => None,
        };
        return Some(McpServerConfig::Stdio {
            name: project_runtime_transport_string(name),
            command: project_runtime_transport_string(command),
            args,
            env,
            options: McpServerOptions {
                always_load,
                enabled,
                ..Default::default()
            },
        });
    }
    if let Some(url) = r.get("url").and_then(RuntimeJson::as_string) {
        let is_sse = r
            .get("type")
            .and_then(RuntimeJson::as_string)
            .is_some_and(|kind| kind == "sse");
        let headers = runtime_string_record(r.get("headers"));
        let oauth = parse_oauth_option_exact(r.get("oauth"));
        let name = project_runtime_transport_string(name);
        let url = project_runtime_transport_string(url);
        return Some(if is_sse {
            McpServerConfig::Sse {
                name,
                url,
                headers,
                oauth,
                options: McpServerOptions {
                    always_load,
                    enabled,
                    ..Default::default()
                },
            }
        } else {
            McpServerConfig::Http {
                name,
                url,
                headers,
                oauth,
                options: McpServerOptions {
                    always_load,
                    enabled,
                    ..Default::default()
                },
            }
        });
    }
    None
}

fn parse_oauth_option_exact(raw: Option<&RuntimeJson>) -> Option<McpOAuthOption> {
    match raw {
        Some(RuntimeJson::Bool(true)) => Some(McpOAuthOption::Advertised),
        Some(RuntimeJson::Object(object)) => match object.get("scopes") {
            Some(RuntimeJson::Array(values)) => Some(McpOAuthOption::Object {
                scopes: Some(
                    values
                        .iter()
                        .filter_map(RuntimeJson::as_string)
                        .map(project_runtime_transport_string)
                        .collect(),
                ),
            }),
            _ => Some(McpOAuthOption::Object { scopes: None }),
        },
        _ => None,
    }
}

fn runtime_string_record(raw: Option<&RuntimeJson>) -> Option<HashMap<String, String>> {
    let RuntimeJson::Object(object) = raw? else {
        return None;
    };
    let values = object
        .entries()
        .filter_map(|(key, value)| {
            value.as_string().map(|value| {
                (
                    project_runtime_transport_string(&key),
                    project_runtime_transport_string(value),
                )
            })
        })
        .collect::<HashMap<_, _>>();
    (!values.is_empty()).then_some(values)
}

fn runtime_env_record(raw: Option<&RuntimeJson>) -> Option<RuntimeEnv> {
    let RuntimeJson::Object(object) = raw? else {
        return None;
    };
    let mut environment = RuntimeEnv::new();
    for (key, value) in object.entries() {
        if let Some(value) = value.as_string() {
            environment.insert(key, value.clone());
        }
    }
    (!environment.is_empty()).then_some(environment)
}

fn project_runtime_transport_string(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8())
        .expect("Node UTF-8 MCP transport projection is valid UTF-8")
}

/// Parse the friendly `oauth` field for a remote server: `true` → use the
/// server's advertised scopes; `{ scopes: [...] }` → request those scopes;
/// anything else → `None` (no OAuth).
fn parse_oauth_option(raw: Option<&Value>) -> Option<McpOAuthOption> {
    match raw {
        Some(Value::Bool(true)) => Some(McpOAuthOption::Advertised),
        Some(Value::Object(obj)) => match obj.get("scopes") {
            Some(Value::Array(arr)) => {
                let scopes = arr
                    .iter()
                    .filter_map(|s| s.as_str().map(String::from))
                    .collect();
                Some(McpOAuthOption::Object {
                    scopes: Some(scopes),
                })
            }
            _ => Some(McpOAuthOption::Object { scopes: None }),
        },
        _ => None,
    }
}

/// Keep only string-valued keys of an object; `None` if none (matches manifest
/// env/header handling).
fn string_record(raw: Option<&Value>) -> Option<HashMap<String, String>> {
    let obj = match raw {
        Some(Value::Object(o)) => o,
        _ => return None,
    };
    let mut out: HashMap<String, String> = HashMap::new();
    for (k, v) in obj {
        if let Some(s) = v.as_str() {
            out.insert(k.clone(), s.to_string());
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

fn string_env_record(raw: Option<&Value>) -> Option<RuntimeEnv> {
    let obj = match raw {
        Some(Value::Object(object)) => object,
        _ => return None,
    };
    let mut environment = RuntimeEnv::new();
    for (key, value) in obj {
        if let Some(value) = value.as_str() {
            environment.insert(
                RuntimeString::from(key.as_str()),
                RuntimeString::from(value),
            );
        }
    }
    (!environment.is_empty()).then_some(environment)
}

/// Parse a config file at `path`; `None` on missing/unreadable/invalid-JSON
/// (never throws). A non-null object or array is accepted (a missing
/// `mcpServers` then reads as "no servers"); a scalar/null top level is
/// rejected.
async fn read_config_file(path: &str) -> Option<Value> {
    let bytes = tokio::fs::read(path).await.ok()?;
    let text = String::from_utf8_lossy(&bytes);
    let parsed: Value = serde_json::from_str(&text).ok()?;
    match parsed {
        Value::Object(_) | Value::Array(_) => Some(parsed),
        _ => None,
    }
}

/// Options for [`load_mcp_server_configs`].
#[derive(Debug, Clone)]
pub struct LoadMcpConfigsOptions {
    /// Project root whose `.klaus/mcp.json` is read (project scope).
    pub cwd: String,
    /// Override the user config dir (defaults to [`config_home`]). Injectable
    /// for tests.
    pub user_config_dir: Option<String>,
}

/// Load the connect-ready MCP servers from the user, project and local files.
/// A same-named project entry overrides the user one (project-level servers are
/// the repo's intent) and a local entry overrides both. Unparseable entries are
/// skipped, and so are entries the file itself turns off with
/// `"enabled": false` — "connect-ready" means what it says, so a caller never
/// has to re-apply the enable state the file already declares.
///
/// The PROJECT trust gate is NOT applied here (see [`crate::mcp::mcp_trust`]):
/// it is a user-facing decision that belongs to the host that can ask, and the
/// host applies it before it hands servers to a session.
pub async fn load_mcp_server_configs(opts: &LoadMcpConfigsOptions) -> Vec<McpServerConfig> {
    let user_path = match opts.user_config_dir.as_deref() {
        Some(d) => join(&[d, "mcp.json"]),
        None => user_mcp_config_path(),
    };
    let mut merged: Vec<(String, McpServerConfig)> = Vec::new();
    // User scope: always loads (not trust-gated).
    let user_file = read_config_file(&user_path).await;
    let user_entries = user_file
        .as_ref()
        .and_then(|file| file.get("mcpServers"))
        .and_then(Value::as_object);
    let attach_origin = |cfg: &mut McpServerConfig, name: &str, raw: &Value| {
        cfg.options_mut().native_origin = Some(
            crate::mcp::approval_provenance::NativeMcpOrigin::configuration(
                config_home().into(),
                user_path.clone().into(),
                user_entries.and_then(|entries| entries.get(name)).cloned(),
                raw.clone(),
            ),
        );
    };
    if let Some(servers) = user_file
        .as_ref()
        .and_then(|f| f.get("mcpServers"))
        .and_then(Value::as_object)
    {
        for (name, raw) in servers {
            if let Some(mut cfg) = parse_mcp_server_entry(name, raw) {
                attach_origin(&mut cfg, name, raw);
                merge_set(&mut merged, name.clone(), cfg);
            }
        }
    }
    // Project scope: loads unconditionally (trust gate removed). A same-named
    // project entry wins over user — project-level servers are the repo's intent.
    let project_file = read_config_file(&project_mcp_config_path(&opts.cwd)).await;
    if let Some(servers) = project_file
        .as_ref()
        .and_then(|f| f.get("mcpServers"))
        .and_then(Value::as_object)
    {
        for (name, raw) in servers {
            if let Some(mut cfg) = parse_mcp_server_entry(name, raw) {
                attach_origin(&mut cfg, name, raw);
                merge_set(&mut merged, name.clone(), cfg);
            }
        }
    }
    // Local scope: always loads (private per-project), highest precedence.
    let local_entries =
        list_local_mcp_server_entries(&opts.cwd, opts.user_config_dir.as_deref()).await;
    for (name, raw) in &local_entries {
        if let Some(mut cfg) = parse_mcp_server_entry(name, raw) {
            attach_origin(&mut cfg, name, raw);
            merge_set(&mut merged, name.clone(), cfg);
        }
    }
    merged
        .into_iter()
        .map(|(_, c)| c)
        .filter(McpServerConfig::is_enabled)
        .collect()
}

/// With the trust gate removed, there are no "pending" project servers — all
/// project-declared servers connect unconditionally. This function exists for
/// API compatibility but always returns an empty list.
pub async fn pending_project_mcp_servers(_cwd: &str) -> Vec<String> {
    Vec::new()
}

/// List the raw (friendly) server entries in one config file, keyed by name.
pub async fn list_mcp_server_entries(path: &str) -> JsonObject {
    let file = read_config_file(path).await;
    file.as_ref()
        .and_then(|f| f.get("mcpServers"))
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default()
}

/// Add or replace one server entry in a config file (creating the file + parent
/// dir if needed). `entry` is the friendly shape; it is validated through
/// [`parse_mcp_server_entry`] first so a bogus shape is rejected before it's
/// written.
pub async fn add_mcp_server_entry(path: &str, name: &str, entry: &Value) -> io::Result<()> {
    if parse_mcp_server_entry(name, entry).is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            invalid_server_msg(name),
        ));
    }
    let mut file = read_config_file(path)
        .await
        .unwrap_or_else(|| Value::Object(Map::new()));
    let file_obj = ensure_object(&mut file);
    let servers = ensure_object(file_obj.entry("mcpServers").or_insert(Value::Null));
    servers.insert(name.to_string(), entry.clone());
    tokio::fs::create_dir_all(dirname(path)).await?;
    let json = serde_json::to_string_pretty(&file).map_err(io::Error::other)?;
    tokio::fs::write(path, format!("{json}\n")).await
}

/// Exact local-scope insert used by runtime catalog data. No serde value or
/// scalar string participates in the JSON read/modify/write path.
pub async fn add_local_mcp_server_entry_exact(
    cwd: &RuntimePath,
    name: &RuntimeString,
    entry: &RuntimeJson,
    user_config_dir: Option<&RuntimePath>,
) -> io::Result<()> {
    if parse_mcp_server_entry_exact(name, entry).is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid exact MCP server: need a command or url field",
        ));
    }
    let path = local_mcp_store_runtime_path(user_config_dir);
    let mut store = read_runtime_json_file(&path)
        .await
        .unwrap_or_else(|| RuntimeJson::Object(RuntimeObject::new()));
    let project_key = resolve_runtime(cwd, std::slice::from_ref(cwd))
        .map_err(|error| io::Error::other(format!("{error:?}")))?
        .into_runtime_string();
    let store_object = ensure_runtime_object(&mut store);
    let projects = ensure_runtime_child_object(store_object, "projects");
    let project = ensure_runtime_child_object(projects, project_key);
    let servers = ensure_runtime_child_object(project, "mcpServers");
    servers.insert(name.clone(), entry.clone());
    write_runtime_json_file(&path, &store).await
}

/// Exact config-file insert used by runtime catalog data.
pub async fn add_mcp_server_entry_exact(
    path: &RuntimePath,
    name: &RuntimeString,
    entry: &RuntimeJson,
) -> io::Result<()> {
    if parse_mcp_server_entry_exact(name, entry).is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid exact MCP server: need a command or url field",
        ));
    }
    let mut file = read_runtime_json_file(path)
        .await
        .unwrap_or_else(|| RuntimeJson::Object(RuntimeObject::new()));
    let file_object = ensure_runtime_object(&mut file);
    let servers = ensure_runtime_child_object(file_object, "mcpServers");
    servers.insert(name.clone(), entry.clone());
    write_runtime_json_file(path, &file).await
}

/// Exact local entries for verification and exact runtime consumers.
pub async fn list_local_mcp_server_entries_exact(
    cwd: &RuntimePath,
    user_config_dir: Option<&RuntimePath>,
) -> RuntimeObject {
    let Some(store) = read_runtime_json_file(&local_mcp_store_runtime_path(user_config_dir)).await
    else {
        return RuntimeObject::new();
    };
    let Ok(project_key) = resolve_runtime(cwd, std::slice::from_ref(cwd)) else {
        return RuntimeObject::new();
    };
    store
        .get("projects")
        .and_then(|projects| projects.get(project_key.into_runtime_string()))
        .and_then(|project| project.get("mcpServers"))
        .and_then(RuntimeJson::as_object)
        .cloned()
        .unwrap_or_default()
}

pub fn project_mcp_config_path_exact(cwd: &RuntimePath) -> RuntimePath {
    resolve_runtime(
        cwd,
        &[
            cwd.clone(),
            RuntimePath::from(".klaus"),
            RuntimePath::from("mcp.json"),
        ],
    )
    .expect("an explicit MCP cwd cannot fail resolution")
}

pub fn user_mcp_config_path_exact() -> RuntimePath {
    let home = RuntimePath::from(config_home());
    resolve_runtime(&home, &[home.clone(), RuntimePath::from("mcp.json")])
        .expect("the configured MCP home cannot fail resolution")
}

fn local_mcp_store_runtime_path(user_config_dir: Option<&RuntimePath>) -> RuntimePath {
    let home;
    let dir = match user_config_dir {
        Some(dir) => dir,
        None => {
            home = RuntimePath::from(config_home());
            &home
        }
    };
    resolve_runtime(dir, &[dir.clone(), RuntimePath::from("projects.json")])
        .expect("the configured MCP store directory cannot fail resolution")
}

async fn read_runtime_json_file(path: &RuntimePath) -> Option<RuntimeJson> {
    let native = project_runtime_path(path, "MCP exact JSON read").ok()?;
    let bytes = tokio::fs::read(native).await.ok()?;
    let parsed = runtime_json::parse(&NodeBufferUtf8::decode(&bytes)).ok()?;
    matches!(parsed, RuntimeJson::Object(_) | RuntimeJson::Array(_)).then_some(parsed)
}

async fn write_runtime_json_file(path: &RuntimePath, value: &RuntimeJson) -> io::Result<()> {
    let native = project_runtime_path(path, "MCP exact JSON write")
        .map_err(|error| io::Error::other(format!("{error:?}")))?;
    let parent = dirname_runtime(path);
    let native_parent = project_runtime_path(&parent, "MCP exact JSON parent")
        .map_err(|error| io::Error::other(format!("{error:?}")))?;
    tokio::fs::create_dir_all(native_parent).await?;
    let text = runtime_json::stringify_pretty(value).concat(&RuntimeString::from("\n"));
    tokio::fs::write(native, text.to_node_utf8()).await
}

fn ensure_runtime_object(value: &mut RuntimeJson) -> &mut RuntimeObject {
    if !matches!(value, RuntimeJson::Object(_)) {
        *value = RuntimeJson::Object(RuntimeObject::new());
    }
    value
        .as_object_mut()
        .expect("runtime object installed immediately above")
}

fn ensure_runtime_child_object(
    object: &mut RuntimeObject,
    key: impl Into<RuntimeString>,
) -> &mut RuntimeObject {
    let key = key.into();
    if !object.contains_key(key.clone()) {
        object.insert(key.clone(), RuntimeJson::Object(RuntimeObject::new()));
    }
    let value = object
        .get_mut(key)
        .expect("runtime child inserted immediately above");
    ensure_runtime_object(value)
}

/// Remove one server entry from a config file. Returns false if the file or
/// name was absent.
pub async fn remove_mcp_server_entry(path: &str, name: &str) -> io::Result<bool> {
    let mut file = match read_config_file(path).await {
        Some(f) => f,
        None => return Ok(false),
    };
    let servers = file.get_mut("mcpServers").and_then(Value::as_object_mut);
    let servers = match servers {
        Some(s) if s.contains_key(name) => s,
        _ => return Ok(false),
    };
    servers.remove(name);
    let json = serde_json::to_string_pretty(&file).map_err(io::Error::other)?;
    tokio::fs::write(path, format!("{json}\n")).await?;
    Ok(true)
}

// ---- helpers ---------------------------------------------------------------

/// The rejection message for a shape lacking both `command` and `url`.
fn invalid_server_msg(name: &str) -> String {
    format!("invalid MCP server \"{name}\": need a \"command\" (stdio) or \"url\" (http/sse) field")
}

/// Insertion-ordered upsert: replace the value at an existing name (keeping its
/// position), else append. Mirrors a name-keyed insertion-ordered map.
fn merge_set(merged: &mut Vec<(String, McpServerConfig)>, name: String, cfg: McpServerConfig) {
    if let Some(slot) = merged.iter_mut().find(|(n, _)| *n == name) {
        slot.1 = cfg;
    } else {
        merged.push((name, cfg));
    }
}

/// Coerce a JSON value to a mutable object, replacing a non-object in place with
/// a fresh empty object first.
fn ensure_object(v: &mut Value) -> &mut Map<String, Value> {
    if !v.is_object() {
        *v = Value::Object(Map::new());
    }
    v.as_object_mut().unwrap()
}

/// POSIX `path.join`: concatenate the non-empty segments and normalize.
fn join(parts: &[&str]) -> String {
    let mut joined: Option<String> = None;
    for part in parts {
        if part.is_empty() {
            continue;
        }
        joined = Some(match joined {
            None => (*part).to_string(),
            Some(j) => format!("{j}/{part}"),
        });
    }
    match joined {
        None => ".".to_string(),
        Some(j) => normalize_posix(&j),
    }
}

/// Normalize a POSIX path: collapse `.`/`..` and duplicate slashes, preserving
/// a leading `/` for absolute paths.
fn normalize_posix(path: &str) -> String {
    let is_abs = path.starts_with('/');
    let mut out: Vec<&str> = Vec::new();
    for seg in path.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                if let Some(last) = out.last() {
                    if *last != ".." {
                        out.pop();
                        continue;
                    }
                }
                if !is_abs {
                    out.push("..");
                }
            }
            s => out.push(s),
        }
    }
    let joined = out.join("/");
    if is_abs {
        format!("/{joined}")
    } else if joined.is_empty() {
        ".".to_string()
    } else {
        joined
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn environment(entries: &[(&str, &str)]) -> RuntimeEnv {
        let mut environment = RuntimeEnv::new();
        for (key, value) in entries {
            environment.insert(RuntimeString::from(*key), RuntimeString::from(*value));
        }
        environment
    }

    /// A fresh, unique temp dir (atomic counter + pid + nanos); never mutates
    /// cwd. Cleaned up by the caller.
    fn make_temp_dir() -> std::path::PathBuf {
        let base = std::env::temp_dir();
        let uniq = format!(
            "mcp-cfg-{}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = base.join(uniq);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    // ---- parse_mcp_server_entry ------------------------------------------

    #[test]
    fn host_managed_entries_parse_without_an_address() {
        let parsed = parse_mcp_server_entry("klaus-browser", &json!({ "hostManaged": true }))
            .expect("host-managed declaration parses");
        assert_eq!(parsed.transport(), "host-managed");
        assert_eq!(parsed.name(), "klaus-browser");
        assert!(parsed
            .dedup_signature()
            .expect("host-managed carries a name-based signature")
            .starts_with("host-managed:"));
        // Without the flag, an address-less object still parses to nothing.
        assert!(parse_mcp_server_entry("addressless", &json!({})).is_none());
    }

    #[test]
    fn infers_stdio_from_a_command_keeping_string_args_and_env_only() {
        let got = parse_mcp_server_entry(
            "x",
            &json!({
                "command": "npx",
                "args": ["-y", "srv", 42],
                "env": { "B": "b", "2": "two", "1": "one", "A": "1", "ignored": 2 }
            }),
        );
        assert_eq!(
            got,
            Some(McpServerConfig::Stdio {
                name: "x".to_string(),
                command: "npx".to_string(),
                args: Some(vec!["-y".to_string(), "srv".to_string()]),
                env: Some(environment(&[
                    ("B", "b"),
                    ("2", "two"),
                    ("1", "one"),
                    ("A", "1"),
                ])),
                options: McpServerOptions::default(),
            })
        );

        let Some(McpServerConfig::Stdio { env: Some(env), .. }) = got else {
            panic!("expected stdio environment")
        };
        assert_eq!(
            env.entries()
                .map(|(key, value)| (key, value.clone()))
                .collect::<Vec<_>>(),
            vec![
                (RuntimeString::from("1"), RuntimeString::from("one")),
                (RuntimeString::from("2"), RuntimeString::from("two")),
                (RuntimeString::from("B"), RuntimeString::from("b")),
                (RuntimeString::from("A"), RuntimeString::from("1")),
            ]
        );
    }

    #[test]
    fn exact_stdio_env_preserves_property_order_and_overwrite_slot() {
        let raw = runtime_json::parse(&RuntimeString::from(
            r#"{"command":"node","env":{"B":"first","A":"a","B":"last","ignored":7}}"#,
        ))
        .unwrap();
        let config = parse_mcp_server_entry_exact(&RuntimeString::from("ordered"), &raw)
            .expect("exact stdio config");
        let McpServerConfig::Stdio { env: Some(env), .. } = config else {
            panic!("expected ordered stdio environment")
        };

        assert_eq!(
            env.entries()
                .map(|(key, value)| (key, value.clone()))
                .collect::<Vec<_>>(),
            vec![
                (RuntimeString::from("B"), RuntimeString::from("last")),
                (RuntimeString::from("A"), RuntimeString::from("a")),
            ]
        );
    }

    #[test]
    fn exact_write_validation_ignores_platform_overrides() {
        // The exact path only validates a local-scope WRITE; every connect path
        // (plugin load, and reading the user/project/local files) goes through
        // `parse_mcp_server_entry`, which is where a `platform` override is
        // resolved. So an entry carrying a base transport validates here and is
        // platform-selected later — the two paths disagree by design, and this
        // pins which one wins so a future change cannot flip it silently.
        //
        // The exact path deliberately stays serde-free, so it does not duplicate
        // the override merge; a declaration that names a transport ONLY under
        // `platform` is rejected at write time rather than mis-parsed. That is the
        // safe direction, but it is a real limit on the shape a local-scope write
        // accepts.
        let with_base = runtime_json::parse(&RuntimeString::from(
            r#"{"command":"/bin/sh","args":["run.sh"],"platform":{"win32":{"command":"cmd.exe"}}}"#,
        ))
        .unwrap();
        let config = parse_mcp_server_entry_exact(&RuntimeString::from("srv"), &with_base)
            .expect("a base transport validates even when overrides are present");
        assert_eq!(
            config,
            McpServerConfig::Stdio {
                name: "srv".to_string(),
                // The BASE command, not the win32 override: resolution happens on
                // the connect path, not here.
                command: "/bin/sh".to_string(),
                args: Some(vec!["run.sh".to_string()]),
                env: None,
                options: McpServerOptions::default(),
            },
            "the exact path must keep the base transport untouched"
        );

        let platform_only = runtime_json::parse(&RuntimeString::from(
            r#"{"platform":{"win32":{"command":"cmd.exe"}}}"#,
        ))
        .unwrap();
        assert_eq!(
            parse_mcp_server_entry_exact(&RuntimeString::from("srv"), &platform_only),
            None,
            "a transport named only under `platform` is rejected at write time"
        );
    }

    #[test]
    fn infers_http_from_a_url_sse_only_when_type_sse() {
        assert_eq!(
            parse_mcp_server_entry("h", &json!({ "url": "https://a/mcp" })),
            Some(McpServerConfig::Http {
                name: "h".to_string(),
                url: "https://a/mcp".to_string(),
                headers: None,
                oauth: None,
                options: McpServerOptions::default(),
            })
        );
        assert_eq!(
            parse_mcp_server_entry("s", &json!({ "url": "https://a/sse", "type": "sse" })),
            Some(McpServerConfig::Sse {
                name: "s".to_string(),
                url: "https://a/sse".to_string(),
                headers: None,
                oauth: None,
                options: McpServerOptions::default(),
            })
        );
    }

    #[test]
    fn carries_oauth_true_and_scopes_onto_remote_configs_else_absent() {
        match parse_mcp_server_entry("o", &json!({ "url": "https://a/mcp", "oauth": true }))
            .unwrap()
        {
            McpServerConfig::Http { oauth, .. } => {
                assert_eq!(oauth, Some(McpOAuthOption::Advertised))
            }
            other => panic!("expected http, got {other:?}"),
        }
        match parse_mcp_server_entry(
            "o",
            &json!({ "url": "https://a/mcp", "oauth": { "scopes": ["read", 42, "write"] } }),
        )
        .unwrap()
        {
            McpServerConfig::Http { oauth, .. } => assert_eq!(
                oauth,
                Some(McpOAuthOption::Object {
                    scopes: Some(vec!["read".to_string(), "write".to_string()])
                })
            ),
            other => panic!("expected http, got {other:?}"),
        }
        // No oauth field when absent or falsey.
        match parse_mcp_server_entry("n", &json!({ "url": "https://a/mcp" })).unwrap() {
            McpServerConfig::Http { oauth, .. } => assert!(oauth.is_none()),
            other => panic!("expected http, got {other:?}"),
        }
        match parse_mcp_server_entry("n", &json!({ "url": "https://a/mcp", "oauth": false }))
            .unwrap()
        {
            McpServerConfig::Http { oauth, .. } => assert!(oauth.is_none()),
            other => panic!("expected http, got {other:?}"),
        }
    }

    #[test]
    fn rejects_shapes_with_neither_command_nor_url() {
        assert!(parse_mcp_server_entry("bad", &json!({ "foo": 1 })).is_none());
        assert!(parse_mcp_server_entry("bad", &Value::Null).is_none());
        assert!(parse_mcp_server_entry("bad", &json!("nope")).is_none());
    }

    /// The entry's own `enabled: false` is the off switch, and it has to survive
    /// the parse or the file stops being the answer to "is this server on" for
    /// every client that reads it. Only `false` is material: an untouched entry
    /// leaves the option absent so it still compares equal to a host-built config.
    #[test]
    fn carries_the_entrys_own_off_switch_into_options() {
        let disabled = parse_mcp_server_entry("off", &json!({ "command": "c", "enabled": false }))
            .expect("stdio entry");
        assert!(!disabled.is_enabled());
        let disabled_remote = parse_mcp_server_entry(
            "off",
            &json!({ "type": "sse", "url": "https://a/mcp", "enabled": false }),
        )
        .expect("sse entry");
        assert!(!disabled_remote.is_enabled());
        for raw in [
            json!({ "command": "c" }),
            json!({ "command": "c", "enabled": true }),
        ] {
            let cfg = parse_mcp_server_entry("on", &raw).expect("stdio entry");
            assert!(cfg.is_enabled());
            assert_eq!(cfg.options().enabled, None);
        }
    }

    /// `disabledTools` is the per-tool off switch on the same entry, so a file a
    /// client hand-edits (or a plugin ships) filters the same tools the desktop
    /// switch does. Absent, empty and all-non-string collapse to `None` — an empty
    /// deny list is not a deny list.
    #[test]
    fn carries_the_entrys_per_tool_deny_list_into_options() {
        let cfg = parse_mcp_server_entry(
            "srv",
            &json!({ "command": "c", "disabledTools": ["a", 7, "b"] }),
        )
        .expect("stdio entry");
        assert_eq!(
            cfg.options().disabled_tools,
            Some(vec!["a".to_string(), "b".to_string()]),
            "non-strings are dropped like args"
        );
        let remote = parse_mcp_server_entry(
            "srv",
            &json!({ "url": "https://a/mcp", "disabledTools": ["a"] }),
        )
        .expect("http entry");
        assert_eq!(remote.options().disabled_tools, Some(vec!["a".to_string()]));
        for raw in [
            json!({ "command": "c" }),
            json!({ "command": "c", "disabledTools": [] }),
            json!({ "command": "c", "disabledTools": [7] }),
            json!({ "command": "c", "disabledTools": "a" }),
        ] {
            let cfg = parse_mcp_server_entry("srv", &raw).expect("stdio entry");
            assert_eq!(cfg.options().disabled_tools, None, "{raw}");
        }
    }

    /// `load_mcp_server_configs` promises CONNECT-READY servers, so a disabled
    /// entry never reaches the caller — no caller has to re-read the enable
    /// state, and a scope that disables a name it inherited wins.
    #[tokio::test]
    async fn disabled_entries_are_withheld_from_the_connect_ready_set() {
        let dir = make_temp_dir();
        let dir_s = dir.to_string_lossy().to_string();
        let user_dir_s = dir.join("user").to_string_lossy().to_string();
        let user_mcp = join(&[&user_dir_s, "mcp.json"]);

        add_mcp_server_entry(&user_mcp, "on", &json!({ "command": "c" }))
            .await
            .unwrap();
        add_mcp_server_entry(
            &user_mcp,
            "off",
            &json!({ "command": "c", "enabled": false }),
        )
        .await
        .unwrap();
        // A LOCAL entry has the highest precedence, so its off switch also turns
        // off the same-named user server it overrides.
        add_mcp_server_entry(&user_mcp, "shadowed", &json!({ "command": "c" }))
            .await
            .unwrap();
        add_local_mcp_server_entry(
            &dir_s,
            "shadowed",
            &json!({ "command": "c", "enabled": false }),
            Some(&user_dir_s),
        )
        .await
        .unwrap();

        let opts = LoadMcpConfigsOptions {
            cwd: dir_s.clone(),
            user_config_dir: Some(user_dir_s.clone()),
        };
        let names: Vec<String> = load_mcp_server_configs(&opts)
            .await
            .iter()
            .map(|c| c.name().to_string())
            .collect();
        assert_eq!(names, vec!["on".to_string()]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- platform overrides ---------------------------------------------

    #[test]
    fn platform_override_replaces_the_base_transport_fields() {
        // The shape a bundled server uses when its executable differs per OS: a
        // POSIX base and a Windows override naming `cmd.exe` and a batch script.
        let entry = json!({
            "command": "/bin/sh",
            "args": ["/plugins/rea/launcher/run-mcp.sh"],
            "env": { "SHARED": "1" },
            "platform": {
                "win32": {
                    "command": "cmd.exe",
                    "args": ["/d", "/s", "/c", "C:\\plugins\\rea\\launcher\\run-mcp.cmd"]
                }
            }
        });

        let darwin = parse_mcp_server_entry_on_platform("rea", &entry, Platform::Darwin)
            .expect("posix base parses on darwin");
        assert_eq!(
            darwin,
            McpServerConfig::Stdio {
                name: "rea".to_string(),
                command: "/bin/sh".to_string(),
                args: Some(vec!["/plugins/rea/launcher/run-mcp.sh".to_string()]),
                // The override does not name `env`, so the base value survives;
                // the merge is per field, not per entry.
                env: Some(environment(&[("SHARED", "1")])),
                options: McpServerOptions::default(),
            }
        );

        let win32 = parse_mcp_server_entry_on_platform("rea", &entry, Platform::Win32)
            .expect("override parses on win32");
        assert_eq!(
            win32,
            McpServerConfig::Stdio {
                name: "rea".to_string(),
                command: "cmd.exe".to_string(),
                args: Some(vec![
                    "/d".to_string(),
                    "/s".to_string(),
                    "/c".to_string(),
                    "C:\\plugins\\rea\\launcher\\run-mcp.cmd".to_string(),
                ]),
                env: Some(environment(&[("SHARED", "1")])),
                options: McpServerOptions::default(),
            }
        );

        // Linux has no override, so it gets the base entry untouched.
        assert_eq!(
            parse_mcp_server_entry_on_platform("rea", &entry, Platform::Linux),
            Some(darwin),
            "an unlisted platform falls back to the base entry"
        );
    }

    #[test]
    fn platform_only_entry_resolves_to_nothing_elsewhere() {
        // A server that cannot run at all off one platform: no base transport,
        // so every other platform yields no declaration rather than a broken one.
        let entry = json!({
            "platform": {
                "darwin": { "command": "/usr/bin/osascript", "args": ["-l", "JavaScript"] }
            }
        });

        let darwin = parse_mcp_server_entry_on_platform("mac-only", &entry, Platform::Darwin)
            .expect("the listed platform gets a transport");
        assert_eq!(darwin.transport(), "stdio");

        for platform in [Platform::Win32, Platform::Linux] {
            assert_eq!(
                parse_mcp_server_entry_on_platform("mac-only", &entry, platform),
                None,
                "{platform:?} has no base transport, so the server is not declared there"
            );
        }
    }

    #[test]
    fn platform_key_never_leaks_into_the_parsed_entry() {
        // `platform` is consumed by resolution. If it reached transport dispatch
        // it would be an unknown field on a stdio entry, and an entry carrying
        // only `platform` plus `url` could dispatch on the wrong branch.
        let entry = json!({
            "url": "https://example.test/mcp",
            "platform": { "linux": { "type": "sse" } }
        });
        assert_eq!(
            parse_mcp_server_entry_on_platform("srv", &entry, Platform::Darwin)
                .map(|c| c.transport().to_string()),
            Some("http".to_string())
        );
        assert_eq!(
            parse_mcp_server_entry_on_platform("srv", &entry, Platform::Linux)
                .map(|c| c.transport().to_string()),
            Some("sse".to_string())
        );
    }

    #[test]
    fn platform_override_arguments_replace_rather_than_append() {
        // Shallow merge: an override's `args` is the whole list. A deep merge
        // would hand the platform base arguments it must not receive.
        let entry = json!({
            "command": "node",
            "args": ["--base-flag", "server.js"],
            "platform": { "win32": { "args": ["server.js"] } }
        });
        let win32 = parse_mcp_server_entry_on_platform("srv", &entry, Platform::Win32)
            .expect("win32 parses");
        assert_eq!(
            win32,
            McpServerConfig::Stdio {
                name: "srv".to_string(),
                command: "node".to_string(),
                args: Some(vec!["server.js".to_string()]),
                env: None,
                options: McpServerOptions::default(),
            }
        );
    }

    #[test]
    fn malformed_platform_value_leaves_the_base_entry_standing() {
        // A declaration error in `platform` must not silently drop a working
        // server, and the key must still be removed before dispatch.
        for malformed in [json!("win32"), json!(42), json!(["win32"]), json!(null)] {
            let entry = json!({ "command": "node", "args": ["s.js"], "platform": malformed });
            let parsed = parse_mcp_server_entry_on_platform("srv", &entry, Platform::Win32)
                .expect("a malformed platform map is ignored, not fatal");
            assert_eq!(parsed.transport(), "stdio");
        }

        // A non-object override under a real platform key is likewise ignored.
        let entry = json!({
            "command": "node",
            "platform": { "win32": "not-an-object" }
        });
        assert_eq!(
            parse_mcp_server_entry_on_platform("srv", &entry, Platform::Win32)
                .map(|c| c.transport().to_string()),
            Some("stdio".to_string())
        );
    }

    #[test]
    fn host_managed_wins_over_a_platform_override() {
        // `hostManaged` is checked before transport fields, so an override that
        // also carries a command cannot turn a host-owned server into a spawned
        // process on some platforms only.
        let entry = json!({
            "hostManaged": true,
            "platform": { "win32": { "command": "cmd.exe" } }
        });
        for platform in [Platform::Darwin, Platform::Linux, Platform::Win32] {
            let parsed = parse_mcp_server_entry_on_platform("klaus-x", &entry, platform)
                .expect("host-managed parses on every platform");
            assert_eq!(parsed.transport(), "host-managed", "{platform:?}");
        }
    }

    // ---- config file load / edit -----------------------------------------

    #[tokio::test]
    async fn project_servers_load_unconditionally_and_win_over_user() {
        let dir = make_temp_dir();
        let dir_s = dir.to_string_lossy().to_string();
        let user_dir_s = dir.join("user").to_string_lossy().to_string();
        let user_mcp = join(&[&user_dir_s, "mcp.json"]);
        let proj_mcp = project_mcp_config_path(&dir_s);

        add_mcp_server_entry(&user_mcp, "shared", &json!({ "command": "user-cmd" }))
            .await
            .unwrap();
        add_mcp_server_entry(&user_mcp, "onlyUser", &json!({ "command": "u" }))
            .await
            .unwrap();
        add_mcp_server_entry(&proj_mcp, "shared", &json!({ "url": "https://proj/mcp" }))
            .await
            .unwrap();
        add_mcp_server_entry(&proj_mcp, "onlyProj", &json!({ "command": "p" }))
            .await
            .unwrap();

        // There is no approval step: the trust gate is gone, so a project server is
        // never withheld and nothing is ever pending.
        assert!(pending_project_mcp_servers(&dir_s).await.is_empty());

        let opts = LoadMcpConfigsOptions {
            cwd: dir_s.clone(),
            user_config_dir: Some(user_dir_s.clone()),
        };
        let configs = load_mcp_server_configs(&opts).await;
        let mut names: Vec<String> = configs.iter().map(|c| c.name().to_string()).collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                "onlyProj".to_string(),
                "onlyUser".to_string(),
                "shared".to_string()
            ]
        );
        // A same-named project entry WINS over the user's: the repo's declaration is
        // the intent for that checkout, so `shared` resolves to the project's http
        // server rather than the user's stdio one.
        let shared = configs.iter().find(|c| c.name() == "shared").unwrap();
        match shared {
            McpServerConfig::Http { url, .. } => assert_eq!(url, "https://proj/mcp"),
            other => panic!("expected http, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn add_list_remove_roundtrips_and_pretty_prints_json() {
        let dir = make_temp_dir();
        let dir_s = dir.to_string_lossy().to_string();
        let path = project_mcp_config_path(&dir_s);
        add_mcp_server_entry(&path, "srv", &json!({ "command": "run", "args": ["--x"] }))
            .await
            .unwrap();
        assert_eq!(
            Value::Object(list_mcp_server_entries(&path).await),
            json!({ "srv": { "command": "run", "args": ["--x"] } })
        );
        let text = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(text.contains("\n  \"mcpServers\""));

        assert!(remove_mcp_server_entry(&path, "srv").await.unwrap());
        assert_eq!(
            Value::Object(list_mcp_server_entries(&path).await),
            json!({})
        );
        assert!(!remove_mcp_server_entry(&path, "missing").await.unwrap());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn rejects_an_invalid_entry_before_writing() {
        let dir = make_temp_dir();
        let dir_s = dir.to_string_lossy().to_string();
        let path = project_mcp_config_path(&dir_s);
        let err = add_mcp_server_entry(&path, "bad", &json!({ "nope": 1 }))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("invalid MCP server"));
        assert_eq!(
            Value::Object(list_mcp_server_entries(&path).await),
            json!({})
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn returns_empty_when_no_files_exist() {
        let dir = make_temp_dir();
        let dir_s = dir.to_string_lossy().to_string();
        let opts = LoadMcpConfigsOptions {
            cwd: dir_s.clone(),
            user_config_dir: Some(join(&[&dir_s, "nope"])),
        };
        assert!(load_mcp_server_configs(&opts).await.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn local_scope_private_per_project_highest_precedence() {
        let dir = make_temp_dir();
        let dir_s = dir.to_string_lossy().to_string();
        let user_dir_s = dir.join("user").to_string_lossy().to_string();
        add_mcp_server_entry(
            &join(&[&user_dir_s, "mcp.json"]),
            "shared",
            &json!({ "command": "user" }),
        )
        .await
        .unwrap();
        add_mcp_server_entry(
            &project_mcp_config_path(&dir_s),
            "shared",
            &json!({ "command": "proj" }),
        )
        .await
        .unwrap();
        add_local_mcp_server_entry(
            &dir_s,
            "shared",
            &json!({ "url": "https://local/mcp" }),
            Some(&user_dir_s),
        )
        .await
        .unwrap();
        add_local_mcp_server_entry(
            &dir_s,
            "onlyLocal",
            &json!({ "command": "L" }),
            Some(&user_dir_s),
        )
        .await
        .unwrap();

        assert_eq!(
            Value::Object(list_local_mcp_server_entries(&dir_s, Some(&user_dir_s)).await),
            json!({ "shared": { "url": "https://local/mcp" }, "onlyLocal": { "command": "L" } })
        );

        let opts = LoadMcpConfigsOptions {
            cwd: dir_s.clone(),
            user_config_dir: Some(user_dir_s.clone()),
        };
        let configs = load_mcp_server_configs(&opts).await;
        let mut names: Vec<String> = configs.iter().map(|c| c.name().to_string()).collect();
        names.sort();
        assert_eq!(names, vec!["onlyLocal".to_string(), "shared".to_string()]);
        // local `shared` (http) wins over approved-project (stdio) and user (stdio)
        let shared = configs.iter().find(|c| c.name() == "shared").unwrap();
        match shared {
            McpServerConfig::Http { url, .. } => assert_eq!(url, "https://local/mcp"),
            other => panic!("expected http, got {other:?}"),
        }

        assert!(
            remove_local_mcp_server_entry(&dir_s, "onlyLocal", Some(&user_dir_s))
                .await
                .unwrap()
        );
        assert!(
            !remove_local_mcp_server_entry(&dir_s, "nope", Some(&user_dir_s))
                .await
                .unwrap()
        );
        let remaining = list_local_mcp_server_entries(&dir_s, Some(&user_dir_s)).await;
        let keys: Vec<String> = remaining.keys().cloned().collect();
        assert_eq!(keys, vec!["shared".to_string()]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn exact_local_store_roundtrips_surrogate_server_names_and_values() {
        let dir = make_temp_dir();
        let user_dir = dir.join("user");
        let cwd = RuntimePath::from(
            dir.to_str()
                .expect("temporary test directory is valid UTF-8"),
        );
        let user_config_dir = RuntimePath::from(
            user_dir
                .to_str()
                .expect("temporary user directory is valid UTF-8"),
        );
        let name = RuntimeString::from_utf16(vec![b's' as u16, 0xd800]);
        let entry = runtime_json::parse(&RuntimeString::from(
            r#"{"command":"node","env":{"EXACT":"value\udc00"}}"#,
        ))
        .expect("valid exact config fixture");

        add_local_mcp_server_entry_exact(&cwd, &name, &entry, Some(&user_config_dir))
            .await
            .unwrap();
        let entries = list_local_mcp_server_entries_exact(&cwd, Some(&user_config_dir)).await;

        assert_eq!(entries.get(name.clone()), Some(&entry));
        assert_eq!(
            entries
                .get(name.clone())
                .and_then(|value| value.get("env"))
                .and_then(|value| value.get("EXACT"))
                .and_then(RuntimeJson::as_string)
                .and_then(|value| value.as_utf16().last()),
            Some(&0xdc00)
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
