//! Trust gate for PROJECT-scope MCP servers.
//!
//! A server declared in a repo's committed project config (`<cwd>/.klaus/mcp.json`)
//! is NOT connected until the user approves it, so cloning a hostile repo can't
//! silently wire a third-party MCP server (and its code/credentials) into your
//! environment.
//!
//! Decision model, in a fixed priority order:
//!  - three states per server name: `rejected` > `approved` > `pending`;
//!  - decisions are three fields — `enabledMcpjsonServers` / `disabledMcpjsonServers` /
//!    `enableAllProjectMcpServers` — appended union-dedup (never removed), enable-all one-way;
//!  - **pending = not connected** (the core safety rule);
//!  - the gate covers ONLY project scope (user/local are private-by-construction, not gated).
//!
//! Decisions live in a project-local PRIVATE settings file,
//! `<cwd>/.klaus/settings.local.json`, which MUST be gitignored — an approval is your
//! machine's choice, never committed or imposed on teammates.
//!
//! The host drives the prompt: the core computes the pending set, the host asks the user
//! however it likes, and records the outcome here. With no host prompt (non-interactive),
//! pending servers simply stay pending = not connected.

use serde_json::{Map, Value};
use std::collections::HashSet;
use std::path::Path;

/// A settings object read from / written to `settings.local.json`.
type SettingsMap = Map<String, Value>;

/// Trust status of a project-scope server, judged in a fixed priority order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpServerStatus {
    Approved,
    Rejected,
    Pending,
}

/// The three trust fields.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct McpTrustState {
    pub enabled_mcpjson_servers: Vec<String>,
    pub disabled_mcpjson_servers: Vec<String>,
    pub enable_all_project_mcp_servers: bool,
}

/// Outcome of a host approval prompt for a batch of pending project servers.
#[derive(Debug, Clone, Default)]
pub struct McpTrustDecisions {
    /// Names the user approved → appended to `enabledMcpjsonServers`.
    pub approved: Option<Vec<String>>,
    /// Names the user rejected → appended to `disabledMcpjsonServers`.
    pub rejected: Option<Vec<String>>,
    /// "Approve all (incl. future)" → sets `enableAllProjectMcpServers` (one-way).
    pub enable_all_future: Option<bool>,
}

/// Project-local PRIVATE settings file holding MCP trust decisions:
/// `<cwd>/.klaus/settings.local.json`. MUST be gitignored (per-user, never committed).
pub fn project_local_settings_path(cwd: &str) -> String {
    Path::new(cwd)
        .join(".klaus")
        .join("settings.local.json")
        .to_string_lossy()
        .into_owned()
}

/// Normalize a server name for trust comparison — fold case (and trim), so the same
/// server under a different casing can't slip past a recorded decision.
pub fn normalize_mcp_server_name(name: &str) -> String {
    name.trim().to_lowercase()
}

/// Read the raw settings object (preserving unrelated keys); `None` on missing/invalid.
async fn read_local_settings(cwd: &str) -> Option<SettingsMap> {
    let text = tokio::fs::read_to_string(project_local_settings_path(cwd))
        .await
        .ok()?;
    match serde_json::from_str::<Value>(&text).ok()? {
        Value::Object(m) => Some(m),
        _ => None,
    }
}

/// Keep only the string elements of a value that is expected to be an array.
fn string_array(v: Option<&Value>) -> Vec<String> {
    match v {
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(|x| x.as_str().map(str::to_string))
            .collect(),
        _ => Vec::new(),
    }
}

/// Read the three trust fields from `settings.local.json` (tolerant of missing/garbage).
pub async fn read_mcp_trust_state(cwd: &str) -> McpTrustState {
    let s = match read_local_settings(cwd).await {
        Some(s) => s,
        None => return McpTrustState::default(),
    };
    McpTrustState {
        enabled_mcpjson_servers: string_array(s.get("enabledMcpjsonServers")),
        disabled_mcpjson_servers: string_array(s.get("disabledMcpjsonServers")),
        enable_all_project_mcp_servers: s.get("enableAllProjectMcpServers")
            == Some(&Value::Bool(true)),
    }
}

/// Compute a project server's trust status: disabled wins first (→ rejected), then
/// enabled OR enable-all (→ approved), else pending.
pub fn project_mcp_server_status(name: &str, state: &McpTrustState) -> McpServerStatus {
    let n = normalize_mcp_server_name(name);
    if state
        .disabled_mcpjson_servers
        .iter()
        .any(|s| normalize_mcp_server_name(s) == n)
    {
        return McpServerStatus::Rejected;
    }
    if state.enable_all_project_mcp_servers
        || state
            .enabled_mcpjson_servers
            .iter()
            .any(|s| normalize_mcp_server_name(s) == n)
    {
        return McpServerStatus::Approved;
    }
    McpServerStatus::Pending
}

/// Union-append (dedupe by normalized name, preserve original casing of first occurrence).
fn union_append(existing: &[String], add: &[String]) -> Vec<String> {
    let mut seen: HashSet<String> = existing
        .iter()
        .map(|s| normalize_mcp_server_name(s))
        .collect();
    let mut out = existing.to_vec();
    for name in add {
        let n = normalize_mcp_server_name(name);
        if !seen.contains(&n) {
            seen.insert(n);
            out.push(name.clone());
        }
    }
    out
}

/// Record a host approval outcome into `settings.local.json`: approved → append to
/// `enabledMcpjsonServers`, rejected → append to `disabledMcpjsonServers` (both
/// union-dedup, never removed), `enableAllFuture` → set `enableAllProjectMcpServers`=true
/// (one-way). Other keys in the file are preserved.
pub async fn record_mcp_trust_decisions(
    cwd: &str,
    decisions: &McpTrustDecisions,
) -> std::io::Result<()> {
    let mut settings = read_local_settings(cwd).await.unwrap_or_default();
    let empty: Vec<String> = Vec::new();
    let enabled = union_append(
        &string_array(settings.get("enabledMcpjsonServers")),
        decisions.approved.as_deref().unwrap_or(&empty),
    );
    let disabled = union_append(
        &string_array(settings.get("disabledMcpjsonServers")),
        decisions.rejected.as_deref().unwrap_or(&empty),
    );
    settings.insert(
        "enabledMcpjsonServers".to_string(),
        Value::Array(enabled.into_iter().map(Value::String).collect()),
    );
    settings.insert(
        "disabledMcpjsonServers".to_string(),
        Value::Array(disabled.into_iter().map(Value::String).collect()),
    );
    if decisions.enable_all_future == Some(true) {
        settings.insert("enableAllProjectMcpServers".to_string(), Value::Bool(true));
    }
    write_local_settings(cwd, &settings).await
}

/// Clear the three trust fields (preserving other settings keys). The keys are removed
/// entirely from the file.
pub async fn reset_project_mcp_choices(cwd: &str) -> std::io::Result<()> {
    let mut settings = match read_local_settings(cwd).await {
        Some(s) => s,
        None => return Ok(()),
    };
    settings.remove("enabledMcpjsonServers");
    settings.remove("disabledMcpjsonServers");
    settings.remove("enableAllProjectMcpServers");
    write_local_settings(cwd, &settings).await
}

async fn write_local_settings(cwd: &str, settings: &SettingsMap) -> std::io::Result<()> {
    let path = project_local_settings_path(cwd);
    if let Some(parent) = Path::new(&path).parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let body = format!(
        "{}\n",
        serde_json::to_string_pretty(settings).expect("settings serialize to JSON")
    );
    tokio::fs::write(&path, body).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct Tmp {
        path: std::path::PathBuf,
    }

    impl Tmp {
        fn new(prefix: &str) -> Self {
            let n = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let uniq = format!(
                "{prefix}-{}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                n
            );
            let path = std::env::temp_dir().join(uniq);
            std::fs::create_dir_all(&path).unwrap();
            Tmp { path }
        }
        fn str(&self) -> String {
            self.path.to_string_lossy().into_owned()
        }
    }

    impl Drop for Tmp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn state(over: McpTrustState) -> McpTrustState {
        over
    }

    fn approved(names: &[&str]) -> McpTrustDecisions {
        McpTrustDecisions {
            approved: Some(names.iter().map(|s| s.to_string()).collect()),
            ..Default::default()
        }
    }

    // ---- projectMcpServerStatus (priority) -------------------------------------

    #[test]
    fn defaults_to_pending() {
        assert_eq!(
            project_mcp_server_status("srv", &McpTrustState::default()),
            McpServerStatus::Pending
        );
    }

    #[test]
    fn enabled_is_approved_and_enable_all_is_approved() {
        assert_eq!(
            project_mcp_server_status(
                "srv",
                &state(McpTrustState {
                    enabled_mcpjson_servers: vec!["srv".into()],
                    ..Default::default()
                })
            ),
            McpServerStatus::Approved
        );
        assert_eq!(
            project_mcp_server_status(
                "srv",
                &state(McpTrustState {
                    enable_all_project_mcp_servers: true,
                    ..Default::default()
                })
            ),
            McpServerStatus::Approved
        );
    }

    #[test]
    fn disabled_is_rejected() {
        assert_eq!(
            project_mcp_server_status(
                "srv",
                &state(McpTrustState {
                    disabled_mcpjson_servers: vec!["srv".into()],
                    ..Default::default()
                })
            ),
            McpServerStatus::Rejected
        );
    }

    #[test]
    fn disabled_wins_over_enabled_and_over_enable_all() {
        assert_eq!(
            project_mcp_server_status(
                "srv",
                &state(McpTrustState {
                    disabled_mcpjson_servers: vec!["srv".into()],
                    enabled_mcpjson_servers: vec!["srv".into()],
                    ..Default::default()
                })
            ),
            McpServerStatus::Rejected
        );
        assert_eq!(
            project_mcp_server_status(
                "srv",
                &state(McpTrustState {
                    disabled_mcpjson_servers: vec!["srv".into()],
                    enable_all_project_mcp_servers: true,
                    ..Default::default()
                })
            ),
            McpServerStatus::Rejected
        );
    }

    #[test]
    fn compares_case_insensitively() {
        assert_eq!(
            project_mcp_server_status(
                "SRV",
                &state(McpTrustState {
                    enabled_mcpjson_servers: vec!["srv".into()],
                    ..Default::default()
                })
            ),
            McpServerStatus::Approved
        );
        assert_eq!(
            project_mcp_server_status(
                "srv",
                &state(McpTrustState {
                    disabled_mcpjson_servers: vec!["SRV".into()],
                    ..Default::default()
                })
            ),
            McpServerStatus::Rejected
        );
    }

    // ---- trust state persistence -----------------------------------------------

    #[tokio::test]
    async fn records_approve_reject_as_union_dedup_append_enable_all_one_way() {
        let dir = Tmp::new("mcp-trust");
        let cwd = dir.str();
        record_mcp_trust_decisions(
            &cwd,
            &McpTrustDecisions {
                approved: Some(vec!["a".into()]),
                rejected: Some(vec!["b".into()]),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        record_mcp_trust_decisions(
            &cwd,
            &McpTrustDecisions {
                approved: Some(vec!["a".into(), "c".into()]),
                enable_all_future: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let s = read_mcp_trust_state(&cwd).await;
        assert_eq!(
            s.enabled_mcpjson_servers,
            vec!["a".to_string(), "c".to_string()]
        );
        assert_eq!(s.disabled_mcpjson_servers, vec!["b".to_string()]);
        assert!(s.enable_all_project_mcp_servers);
        // A later record without enable_all_future must NOT turn it back off (one-way).
        record_mcp_trust_decisions(&cwd, &approved(&["d"]))
            .await
            .unwrap();
        assert!(
            read_mcp_trust_state(&cwd)
                .await
                .enable_all_project_mcp_servers
        );
    }

    #[tokio::test]
    async fn preserves_unrelated_keys() {
        let dir = Tmp::new("mcp-trust");
        let cwd = dir.str();
        let path = project_local_settings_path(&cwd);
        record_mcp_trust_decisions(&cwd, &approved(&["x"]))
            .await
            .unwrap();
        let mut raw: Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        raw.as_object_mut()
            .unwrap()
            .insert("theme".into(), json!("dark"));
        std::fs::write(&path, serde_json::to_string(&raw).unwrap()).unwrap();
        record_mcp_trust_decisions(&cwd, &approved(&["y"]))
            .await
            .unwrap();
        let out: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(out.get("theme"), Some(&json!("dark")));
        assert_eq!(out.get("enabledMcpjsonServers"), Some(&json!(["x", "y"])));
    }

    #[tokio::test]
    async fn reset_clears_the_three_fields_but_keeps_other_keys() {
        let dir = Tmp::new("mcp-trust");
        let cwd = dir.str();
        let path = project_local_settings_path(&cwd);
        record_mcp_trust_decisions(
            &cwd,
            &McpTrustDecisions {
                approved: Some(vec!["a".into()]),
                rejected: Some(vec!["b".into()]),
                enable_all_future: Some(true),
            },
        )
        .await
        .unwrap();
        let mut raw: Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        raw.as_object_mut()
            .unwrap()
            .insert("theme".into(), json!("dark"));
        std::fs::write(&path, serde_json::to_string(&raw).unwrap()).unwrap();

        reset_project_mcp_choices(&cwd).await.unwrap();
        let s = read_mcp_trust_state(&cwd).await;
        assert_eq!(s, McpTrustState::default());
        let out: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(out.get("theme"), Some(&json!("dark")));
        assert!(out.get("enabledMcpjsonServers").is_none());
    }

    #[tokio::test]
    async fn tolerates_a_missing_or_garbage_settings_file() {
        let dir = Tmp::new("mcp-trust");
        let cwd = dir.str();
        // Missing file → empty state.
        assert_eq!(read_mcp_trust_state(&cwd).await, McpTrustState::default());
        // Create the .klaus dir (via a record) then corrupt the file with non-JSON.
        record_mcp_trust_decisions(&cwd, &approved(&["x"]))
            .await
            .unwrap();
        std::fs::write(project_local_settings_path(&cwd), "not json").unwrap();
        assert_eq!(read_mcp_trust_state(&cwd).await, McpTrustState::default());
        assert!(reset_project_mcp_choices(&cwd).await.is_ok());
    }
}
