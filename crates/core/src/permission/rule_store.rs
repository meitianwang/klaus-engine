//! Durable permission rules, owned by the engine.
//!
//! A rule the user grants with a NON-session destination outlives the session, so
//! the engine keeps it itself: `<config home>/permission-rules.json` is read when
//! a session starts and appended to when a durable grant is approved.
//!
//! Why the engine and not the host: when the host persisted these for the engine,
//! one fact had two owners. The host wrote its own file, then had to re-inject the
//! rules through session config, so a rule granted mid-session only took effect in
//! the NEXT session, and the host's rule list and the engine's evaluation could
//! drift with nothing to catch it. The host still decides WHERE the config home is
//! (`KLAUS_CONFIG_DIR`); the content is the engine's.

use crate::fs::paths::config_home;
use crate::permission::rules::{format_rule_string, parse_rule_string};
use crate::types::permission::{PermissionBehavior, PermissionRule};
use std::io;
use std::path::{Path, PathBuf};

const RULES_FILE: &str = "permission-rules.json";

/// `<config home>/permission-rules.json`.
pub fn default_rules_path() -> PathBuf {
    PathBuf::from(config_home()).join(RULES_FILE)
}

/// The three durable rule classes, as the rule STRINGS the config grammar uses
/// (`Read`, `Bash(git status)`, `mcp__server__*`). Keep the stored strings intact
/// for management and legacy cleanup. The live durable layer parses them
/// separately from project config and session-only grants.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DurableRules {
    pub allow: Vec<String>,
    pub deny: Vec<String>,
    pub ask: Vec<String>,
}

impl DurableRules {
    fn class_mut(&mut self, behavior: PermissionBehavior) -> &mut Vec<String> {
        match behavior {
            PermissionBehavior::Allow => &mut self.allow,
            PermissionBehavior::Deny => &mut self.deny,
            PermissionBehavior::Ask => &mut self.ask,
        }
    }

    fn is_empty(&self) -> bool {
        self.allow.is_empty() && self.deny.is_empty() && self.ask.is_empty()
    }
}

/// The rule string for a parsed rule — the inverse of `parse_rule_string`. An MCP
/// wildcard rule keeps its original text (`tool_name` holds it verbatim), so it
/// round-trips instead of collapsing into an exact tool reference.
pub fn rule_to_string(rule: &PermissionRule) -> String {
    let tool = String::from_utf16_lossy(rule.tool_name.as_utf16());
    if rule.tool_name_prefix.is_some() {
        return tool;
    }
    let prefix = rule
        .argument_prefix
        .as_ref()
        .map(|prefix| String::from_utf16_lossy(prefix.as_utf16()));
    format_rule_string(&tool, prefix.as_deref())
}

/// Semantic identity for dedup, matching the session-state rule key: lowercase
/// tool, absent prefix ≡ empty prefix. An unparseable string keeps its raw form
/// as identity so a hand-edited file never silently loses a line.
fn rule_key(rule: &str) -> (String, String) {
    match parse_rule_string(rule) {
        Some(parsed) => {
            let tool = String::from_utf16_lossy(parsed.tool_name.as_utf16()).to_lowercase();
            let prefix = parsed
                .argument_prefix
                .map(|value| String::from_utf16_lossy(value.as_utf16()))
                .unwrap_or_default();
            (tool, prefix)
        }
        None => (rule.trim().to_lowercase(), String::new()),
    }
}

fn string_list(value: Option<&serde_json::Value>) -> Vec<String> {
    value
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|rule| !rule.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn rules_from_object(object: &serde_json::Value) -> DurableRules {
    DurableRules {
        allow: string_list(object.get("allow")),
        deny: string_list(object.get("deny")),
        ask: string_list(object.get("ask")),
    }
}

/// Read the store. A missing file, unreadable file, or unparseable content all
/// resolve to "no durable rules": permission state must never fail a session
/// start, and an empty list is the safe reading (every action keeps asking).
pub async fn load(path: &Path) -> DurableRules {
    let Ok(raw) = tokio::fs::read_to_string(path).await else {
        return DurableRules::default();
    };
    match serde_json::from_str::<serde_json::Value>(&raw) {
        Ok(value) => rules_from_object(&value),
        Err(_) => DurableRules::default(),
    }
}

/// Administration must distinguish an absent store from unreadable or corrupt
/// data. Session startup retains its separate historical fallback above.
pub async fn load_strict(path: &Path) -> io::Result<DurableRules> {
    let raw = match tokio::fs::read_to_string(path).await {
        Ok(raw) => raw,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(DurableRules::default()),
        Err(error) => return Err(error),
    };
    let value: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if !value.is_object()
        || ["allow", "deny", "ask"].iter().any(|key| {
            value.get(key).is_some_and(|entries| {
                entries
                    .as_array()
                    .is_none_or(|entries| entries.iter().any(|entry| !entry.is_string()))
            })
        })
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid permission rule store shape",
        ));
    }
    Ok(rules_from_object(&value))
}

async fn store(path: &Path, rules: &DurableRules) -> io::Result<()> {
    use tokio::io::AsyncWriteExt;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let body = serde_json::json!({
        "allow": rules.allow,
        "deny": rules.deny,
        "ask": rules.ask,
    });
    let mut text =
        serde_json::to_string_pretty(&body).expect("a string-list object always serializes");
    text.push('\n');
    let temp = path.with_file_name(format!(".permission-rules-{}.tmp", uuid::Uuid::new_v4()));
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(&temp).await?;
    let written = async {
        file.write_all(text.as_bytes()).await?;
        file.sync_all().await
    }
    .await;
    drop(file);
    if let Err(error) = written {
        let _ = tokio::fs::remove_file(&temp).await;
        return Err(error);
    }
    if let Err(error) = tokio::fs::rename(&temp, path).await {
        let _ = tokio::fs::remove_file(&temp).await;
        return Err(error);
    }
    Ok(())
}

/// Append rules to one class, deduped. Returns the store as it now stands so a
/// caller can answer with it instead of reading the file again.
pub async fn add(
    path: &Path,
    behavior: PermissionBehavior,
    rules: &[PermissionRule],
) -> io::Result<DurableRules> {
    // Validate the complete batch before loading or changing the file. A
    // structured rule that cannot survive serialization must not be reported
    // as saved while the session's parser silently drops it.
    let texts: Vec<String> = rules.iter().map(rule_to_string).collect();
    if texts.iter().any(|text| parse_rule_string(text).is_none()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "permission rule cannot be represented by the rule grammar",
        ));
    }
    let mut next = load_strict(path).await?;
    let target = next.class_mut(behavior);
    for text in texts {
        let key = rule_key(&text);
        if !target.iter().any(|existing| rule_key(existing) == key) {
            target.push(text);
        }
    }
    store(path, &next).await?;
    Ok(next)
}

/// Drop one rule from one class (identity is the semantic key, so the caller may
/// pass the rule as displayed). Returns the store as it now stands.
pub async fn remove(
    path: &Path,
    behavior: PermissionBehavior,
    rule: &str,
) -> io::Result<DurableRules> {
    let mut next = load_strict(path).await?;
    let key = rule_key(rule);
    let target = next.class_mut(behavior);
    target.retain(|existing| rule_key(existing) != key);
    store(path, &next).await?;
    Ok(next)
}

/// One-time adoption of rules a HOST used to persist on the engine's behalf: when
/// the store does not exist yet and `legacy` carries a `permissions` block with
/// rule lists, those rules become the store's initial content. Without this the
/// ownership move would silently drop every rule the user had already saved.
pub async fn adopt_legacy(legacy: &Path, path: &Path) -> io::Result<Option<DurableRules>> {
    if tokio::fs::try_exists(path).await.unwrap_or(false) {
        return Ok(None);
    }
    let Ok(raw) = tokio::fs::read_to_string(legacy).await else {
        return Ok(None);
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&raw) else {
        return Ok(None);
    };
    let Some(permissions) = value.get("permissions") else {
        return Ok(None);
    };
    let adopted = rules_from_object(permissions);
    if adopted.is_empty() {
        return Ok(None);
    }
    store(path, &adopted).await?;
    Ok(Some(adopted))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::RuntimeString;

    fn rule(tool: &str, prefix: Option<&str>) -> PermissionRule {
        PermissionRule {
            tool_name: RuntimeString::from(tool),
            argument_prefix: prefix.map(RuntimeString::from),
            tool_name_prefix: None,
        }
    }

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "klaus-rule-store-{tag}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// The round trip that matters: what `add` writes, `parse_rule_string` can
    /// read back, because the same strings are handed to a session as config.
    #[tokio::test]
    async fn added_rules_round_trip_through_the_config_grammar() {
        let dir = temp_dir("round-trip");
        let path = dir.join("permission-rules.json");

        add(
            &path,
            PermissionBehavior::Allow,
            &[rule("Bash", Some("git status"))],
        )
        .await
        .unwrap();
        let stored = add(&path, PermissionBehavior::Deny, &[rule("Write", None)])
            .await
            .unwrap();

        assert_eq!(stored.allow, vec!["Bash(git status)".to_string()]);
        assert_eq!(stored.deny, vec!["Write".to_string()]);
        assert_eq!(load(&path).await, stored);
        assert!(parse_rule_string("Bash(git status)").is_some());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Approving the same command twice must not grow the file, and removal takes
    /// the rule as displayed.
    #[tokio::test]
    async fn dedupes_by_semantic_key_and_removes_by_display_text() {
        let dir = temp_dir("dedupe");
        let path = dir.join("permission-rules.json");

        add(
            &path,
            PermissionBehavior::Allow,
            &[rule("Bash", Some("git status"))],
        )
        .await
        .unwrap();
        // Same rule, different case on the tool name: one entry, not two.
        let stored = add(
            &path,
            PermissionBehavior::Allow,
            &[rule("bash", Some("git status"))],
        )
        .await
        .unwrap();
        assert_eq!(stored.allow.len(), 1);

        let after = remove(&path, PermissionBehavior::Allow, "Bash(git status)")
            .await
            .unwrap();
        assert!(after.allow.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A corrupt file is not a session-start failure: it reads as "no rules".
    #[tokio::test]
    async fn unreadable_content_reads_as_no_rules() {
        let dir = temp_dir("corrupt");
        let path = dir.join("permission-rules.json");
        std::fs::write(&path, b"{not json").unwrap();
        assert_eq!(load(&path).await, DurableRules::default());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The rules a host had already saved for the user come across on first run,
    /// and a second call is a no-op (the store is authoritative from then on).
    #[tokio::test]
    async fn adopts_host_persisted_rules_once() {
        let dir = temp_dir("adopt");
        let legacy = dir.join("settings.json");
        let path = dir.join("permission-rules.json");
        std::fs::write(
            &legacy,
            br#"{ "model": "m", "permissions": { "allow": ["Read"], "ask": ["Bash(rm)"] } }"#,
        )
        .unwrap();

        let adopted = adopt_legacy(&legacy, &path).await.unwrap().unwrap();
        assert_eq!(adopted.allow, vec!["Read".to_string()]);
        assert_eq!(adopted.ask, vec!["Bash(rm)".to_string()]);

        // Second run: the store already exists, so the legacy block is ignored.
        std::fs::write(&legacy, br#"{ "permissions": { "allow": ["Write"] } }"#).unwrap();
        assert!(adopt_legacy(&legacy, &path).await.unwrap().is_none());
        assert_eq!(load(&path).await.allow, vec!["Read".to_string()]);
        std::fs::remove_dir_all(&dir).ok();
    }
}
