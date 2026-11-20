//! Catalog notifications for the static fallback tool protocol.
//!
//! Wires without a native deferred-tool channel freeze their serialized tool
//! list to the direct tools plus the fixed `tool_search` / `call_mcp_tool`
//! entries. Folded-pool changes — MCP servers connecting, dropping, or
//! redefining tools — are announced as engine-generated meta messages that
//! join the session history, so the announcement bytes ride the cached prefix
//! exactly once and every replay reuses identical content and position.
//!
//! The history itself is the ledger: prior notifications are re-scanned each
//! turn, which makes retries idempotent and lets cold resumes and compaction
//! rebuild the announced set from whatever survived.

use crate::engine::query::request::static_deferred_dispatch_active;
use crate::runtime_values::RuntimeString;
use crate::schemas::config::ResolvedConfig;
use crate::schemas::content::{ContentBlock, TextBlock};
use crate::schemas::message::{Message, UserMessage};
use crate::tools::pool::to_tool_spec;
use crate::types::tool::{Tool, ToolExposure};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Phase marker identifying a catalog notification inside the history.
pub(crate) const CATALOG_PHASE: &str = "deferred-tool-catalog";

const UUID_PREFIX: &str = "deferred-catalog-";

/// Byte budget for one notification. Over budget, individually listed
/// additions collapse into per-server summaries; the full catalog stays
/// reachable through keyword search and exact `select:` refetches.
const NOTIFICATION_BUDGET_BYTES: usize = 12 * 1024;

const GUIDANCE: &str = "Deferred tools are available via tool_search; their schemas are not loaded. To use one, load it with query \"select:<name>[,<name>...]\" — the result carries its full input schema — then call it through call_mcp_tool with {\"name\": \"<tool name>\", \"arguments\": {...}}.";

/// One pending catalog announcement, diffed against the history ledger.
pub(crate) struct CatalogDelta {
    added: Vec<(RuntimeString, RuntimeString)>,
    updated: Vec<(RuntimeString, RuntimeString)>,
    removed: Vec<RuntimeString>,
}

/// Build the catalog notification for this turn, when the session runs the
/// static fallback protocol and the folded pool drifted from the announced
/// ledger. `None` keeps the history untouched.
pub(crate) fn catalog_notification_message(
    config: &ResolvedConfig,
    history: &[Message],
    tools: &[Arc<dyn Tool>],
    native_active: bool,
) -> Option<Message> {
    if !static_deferred_dispatch_active(config, tools, true, native_active) {
        return None;
    }
    let ledger = scan_ledger(history);
    let current = folded_catalog(tools);
    let delta = diff(&ledger, &current)?;
    let text = render(&delta);
    let uuid = notification_uuid(&text);
    if history
        .iter()
        .any(|message| message_uuid(message).is_some_and(|existing| existing == uuid))
    {
        // Same content already announced (a retry of this exact turn state).
        return None;
    }
    Some(Message::User(UserMessage {
        question_answer: None,
        uuid,
        content: vec![ContentBlock::Text(TextBlock {
            text,
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: Some(RuntimeString::from(CATALOG_PHASE)),
        })],
        is_meta: Some(true),
    }))
}

fn message_uuid(message: &Message) -> Option<RuntimeString> {
    match message {
        Message::User(user) => Some(user.uuid.clone()),
        Message::Assistant(assistant) => Some(assistant.uuid.clone()),
        Message::ToolResult(result) => Some(result.uuid.clone()),
        Message::System(system) => Some(system.uuid.clone()),
        Message::CompactBoundary(boundary) => Some(boundary.uuid.clone()),
    }
}

fn is_catalog_notification(message: &Message) -> bool {
    matches!(message, Message::User(user)
        if user.is_meta == Some(true)
        && user.content.len() == 1
        && matches!(&user.content[0], ContentBlock::Text(text)
            if text.phase.as_ref().is_some_and(|phase| phase == CATALOG_PHASE)))
}

/// The announced state, reconstructed from every persisted notification in
/// order: additions and updates set an entry, removals retire it.
fn scan_ledger(history: &[Message]) -> BTreeMap<RuntimeString, RuntimeString> {
    let mut ledger: BTreeMap<RuntimeString, RuntimeString> = BTreeMap::new();
    for message in history
        .iter()
        .filter(|message| is_catalog_notification(message))
    {
        let Message::User(user) = message else {
            continue;
        };
        let ContentBlock::Text(text) = &user.content[0] else {
            continue;
        };
        let Ok(body) = text.text.try_to_utf8() else {
            continue;
        };
        for line in body.lines() {
            if let Some(entry) = line.strip_prefix("+ ") {
                if let Some((name, description)) = split_entry(entry) {
                    ledger.insert(name, description);
                }
            } else if let Some(entry) = line.strip_prefix("* ") {
                if let Some((name, description)) = split_entry(entry) {
                    ledger.insert(name, description);
                }
            } else if let Some(name) = line.strip_prefix("- ") {
                ledger.remove(&RuntimeString::from(name.trim()));
            }
        }
    }
    ledger
}

fn split_entry(entry: &str) -> Option<(RuntimeString, RuntimeString)> {
    let (name, description) = entry.split_once(':')?;
    let name = name.trim();
    if name.is_empty() {
        return None;
    }
    Some((
        RuntimeString::from(name),
        RuntimeString::from(description.trim()),
    ))
}

/// The folded pool as the ledger sees it: every enabled `Deferred` tool with
/// its one-line rendered description, in stable name order.
fn folded_catalog(tools: &[Arc<dyn Tool>]) -> BTreeMap<RuntimeString, RuntimeString> {
    let mut catalog = BTreeMap::new();
    for tool in tools {
        if tool.exposure() != ToolExposure::Deferred || !tool.is_enabled() {
            continue;
        }
        let spec = to_tool_spec(tool.as_ref());
        let description = spec
            .description
            .try_to_utf8()
            .unwrap_or_default()
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        catalog.insert(spec.name.clone(), RuntimeString::from(description));
    }
    catalog
}

fn diff(
    ledger: &BTreeMap<RuntimeString, RuntimeString>,
    current: &BTreeMap<RuntimeString, RuntimeString>,
) -> Option<CatalogDelta> {
    let added = current
        .iter()
        .filter(|(name, _)| !ledger.contains_key(*name))
        .map(|(name, description)| (name.clone(), description.clone()))
        .collect::<Vec<_>>();
    let updated = current
        .iter()
        .filter(|(name, description)| {
            ledger
                .get(*name)
                .is_some_and(|announced| announced != *description)
        })
        .map(|(name, description)| (name.clone(), description.clone()))
        .collect::<Vec<_>>();
    let removed = ledger
        .keys()
        .filter(|name| !current.contains_key(*name))
        .cloned()
        .collect::<Vec<_>>();
    if added.is_empty() && updated.is_empty() && removed.is_empty() {
        return None;
    }
    Some(CatalogDelta {
        added,
        updated,
        removed,
    })
}

fn render(delta: &CatalogDelta) -> RuntimeString {
    let mut text =
        RuntimeString::from("<deferred-tool-catalog>\n").concat(&RuntimeString::from(GUIDANCE));
    let mut body = RuntimeString::from("");
    if !delta.added.is_empty() {
        body = body.concat(&RuntimeString::from("\n\nNow available:"));
        for (name, description) in &delta.added {
            body = body
                .concat(&RuntimeString::from("\n+ "))
                .concat(name)
                .concat(&RuntimeString::from(": "))
                .concat(description);
        }
    }
    if !delta.updated.is_empty() {
        body = body.concat(&RuntimeString::from(
            "\n\nUpdated (re-fetch with select: before calling):",
        ));
        for (name, description) in &delta.updated {
            body = body
                .concat(&RuntimeString::from("\n* "))
                .concat(name)
                .concat(&RuntimeString::from(": "))
                .concat(description);
        }
    }
    if !delta.removed.is_empty() {
        body = body.concat(&RuntimeString::from(
            "\n\nNo longer available (do not call):",
        ));
        for name in &delta.removed {
            body = body.concat(&RuntimeString::from("\n- ")).concat(name);
        }
    }
    text = text.concat(&body);
    let oversized = text
        .try_to_utf8()
        .is_ok_and(|rendered| rendered.len() > NOTIFICATION_BUDGET_BYTES);
    if oversized && !delta.added.is_empty() {
        // Whole-list collapse into per-server summaries: additions were the
        // oversized section, and keyword search still reaches every name.
        let mut grouped: BTreeMap<RuntimeString, usize> = BTreeMap::new();
        for (name, _) in &delta.added {
            grouped
                .entry(server_group(name))
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }
        let mut summary = RuntimeString::from(
            "\n\nNow available (listed by service to fit the budget; search by capability keywords or load exact names with select:):",
        );
        for (group, count) in &grouped {
            summary = summary
                .concat(&RuntimeString::from("\n+ "))
                .concat(group)
                .concat(&RuntimeString::from(": "))
                .concat(&RuntimeString::from(count.to_string()))
                .concat(&RuntimeString::from(" tools"));
        }
        let mut text =
            RuntimeString::from("<deferred-tool-catalog>\n").concat(&RuntimeString::from(GUIDANCE));
        text = text.concat(&summary);
        if !delta.updated.is_empty() || !delta.removed.is_empty() {
            text = text.concat(&body_after_added(&body));
        }
        return text.concat(&RuntimeString::from("\n</deferred-tool-catalog>"));
    }
    text.concat(&RuntimeString::from("\n</deferred-tool-catalog>"))
}

/// The updated/removed tail of the rendered body, without the additions
/// section (used when additions collapse into summaries).
fn body_after_added(body: &RuntimeString) -> RuntimeString {
    let Ok(rendered) = body.try_to_utf8() else {
        return RuntimeString::from("");
    };
    let tail_start = rendered
        .find("\n\nUpdated (")
        .or_else(|| rendered.find("\n\nNo longer available ("));
    match tail_start {
        Some(index) => RuntimeString::from(rendered[index..].to_string()),
        None => RuntimeString::from(""),
    }
}

/// `mcp__server__tool` groups under `mcp__server__*`; everything else groups
/// under its bare name prefix bucket.
fn server_group(name: &RuntimeString) -> RuntimeString {
    let Ok(name) = name.try_to_utf8() else {
        return RuntimeString::from("other");
    };
    if let Some(rest) = name.strip_prefix("mcp__") {
        if let Some((server, _)) = rest.split_once("__") {
            return RuntimeString::from(format!("mcp__{server}__*"));
        }
    }
    RuntimeString::from("other tools")
}

fn notification_uuid(text: &RuntimeString) -> RuntimeString {
    let mut digest = Sha256::new();
    for unit in text.as_utf16() {
        digest.update(unit.to_le_bytes());
    }
    let hash = digest.finalize();
    let hex: String = hash
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect();
    RuntimeString::from(format!("{UUID_PREFIX}{hex}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::builtins::tool_search::{create_tool_search_tool, ExposureOverrideTool};
    use crate::types::errors::ToolError;
    use crate::types::json::{Json, JsonSchema};
    use crate::types::tool::{ToolContext, ToolProgress, ToolResult};
    use async_trait::async_trait;
    use serde_json::Map;

    struct FakeTool {
        name: String,
        description: String,
        schema: JsonSchema,
    }

    fn fake(name: &str, description: &str, deferred: bool) -> Arc<dyn Tool> {
        let tool: Arc<dyn Tool> = Arc::new(FakeTool {
            name: name.to_string(),
            description: description.to_string(),
            schema: Map::new(),
        });
        if deferred {
            ExposureOverrideTool::wrap(tool, ToolExposure::Deferred)
        } else {
            tool
        }
    }

    #[async_trait]
    impl Tool for FakeTool {
        fn static_name(&self) -> &str {
            &self.name
        }
        fn static_description(&self) -> &str {
            &self.description
        }
        fn static_input_schema(&self) -> &JsonSchema {
            &self.schema
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
        async fn call(
            &self,
            _input: Json,
            _ctx: &ToolContext,
            _on_progress: Option<ToolProgress>,
        ) -> Result<ToolResult, ToolError> {
            unreachable!("catalog fixture tools are never called")
        }
    }

    fn config() -> ResolvedConfig {
        crate::schemas::config::default_config()
    }

    fn pool(deferred: &[(&str, &str)]) -> Vec<Arc<dyn Tool>> {
        let mut tools = vec![create_tool_search_tool(None), fake("read", "Read", false)];
        tools.extend(
            deferred
                .iter()
                .map(|(name, description)| fake(name, description, true)),
        );
        tools
    }

    fn notification_text(message: &Message) -> String {
        let Message::User(user) = message else {
            panic!("notification must be a user meta message");
        };
        assert_eq!(user.is_meta, Some(true));
        let ContentBlock::Text(text) = &user.content[0] else {
            panic!("notification must be a text block");
        };
        assert!(text
            .phase
            .as_ref()
            .is_some_and(|phase| phase == CATALOG_PHASE));
        text.text.try_to_utf8().unwrap()
    }

    #[test]
    fn folded_catalog_is_announced_only_on_the_fallback_protocol() {
        let tools = pool(&[("mcp__docs__read", "Read a document by ID.")]);
        let message = catalog_notification_message(&config(), &[], &tools, false)
            .expect("fallback protocol announces the folded catalog");
        let text = notification_text(&message);
        assert!(text.contains("<deferred-tool-catalog>"));
        assert!(text.contains("+ mcp__docs__read: Read a document by ID."));
        assert!(text.contains("call_mcp_tool"));
        assert!(text.contains("select:<name>"));
        // Direct tools never enter the announcement.
        assert!(!text.contains("+ read"));
        // A native deferred channel keeps its own wire-native announcement.
        assert!(catalog_notification_message(&config(), &[], &tools, true).is_none());
        // Without a folded pool there is nothing to announce.
        assert!(catalog_notification_message(&config(), &[], &pool(&[]), false).is_none());
    }

    #[test]
    fn the_history_ledger_makes_repeat_turns_idempotent() {
        let tools = pool(&[("mcp__docs__read", "Read a document by ID.")]);
        let first = catalog_notification_message(&config(), &[], &tools, false).unwrap();
        let history = vec![first.clone()];
        // Same pool, same announced ledger: nothing new to say.
        assert!(catalog_notification_message(&config(), &history, &tools, false).is_none());
        // A new server arriving appends exactly its own delta.
        let grown = pool(&[
            ("mcp__docs__read", "Read a document by ID."),
            ("mcp__cal__events", "List events."),
        ]);
        let second = catalog_notification_message(&config(), &history, &grown, false).unwrap();
        let text = notification_text(&second);
        assert!(text.contains("+ mcp__cal__events: List events."));
        assert!(!text.contains("mcp__docs__read"));
    }

    #[test]
    fn removals_and_updates_render_their_own_sections() {
        let tools = pool(&[
            ("mcp__docs__read", "Read a document by ID."),
            ("mcp__docs__write", "Write a document."),
        ]);
        let first = catalog_notification_message(&config(), &[], &tools, false).unwrap();
        let history = vec![first];
        // read is gone; write changed its description.
        let changed = pool(&[("mcp__docs__write", "Write or overwrite a document.")]);
        let second = catalog_notification_message(&config(), &history, &changed, false).unwrap();
        let text = notification_text(&second);
        assert!(text.contains("- mcp__docs__read"));
        assert!(text.contains("* mcp__docs__write: Write or overwrite a document."));
        assert!(text.contains("re-fetch"));
        // The ledger absorbs both sections: a third turn is silent again.
        let history2 = {
            let mut h = history;
            h.push(second);
            h
        };
        assert!(catalog_notification_message(&config(), &history2, &changed, false).is_none());
    }

    #[test]
    fn oversized_catalogs_collapse_into_service_summaries() {
        let entries: Vec<(String, String)> = (0..40)
            .flat_map(|server| {
                (0..20).map(move |tool| {
                    (
                        format!("mcp__srv{server}__tool{tool}"),
                        format!(
                            "A quite lengthy capability description for tool {tool} of service {server} that pads the catalog entry beyond the notification budget when multiplied across the whole fleet of servers."
                        ),
                    )
                })
            })
            .collect();
        let refs: Vec<(&str, &str)> = entries
            .iter()
            .map(|(name, description)| (name.as_str(), description.as_str()))
            .collect();
        let tools = pool(&refs);
        let message = catalog_notification_message(&config(), &[], &tools, false).unwrap();
        let text = notification_text(&message);
        // Individual listings collapsed into per-service summary lines.
        assert!(text.contains("listed by service to fit the budget"));
        assert!(text.contains("+ mcp__srv7__*: 20 tools"));
        assert!(!text.contains("mcp__srv7__tool3:"));
        // Exact refetch and keyword search stay reachable.
        assert!(text.contains("select:"));
        assert!(text.len() < NOTIFICATION_BUDGET_BYTES + 2048);
    }
}
