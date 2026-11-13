//! Explicit resource-reference parsing and turn-start capability resolution.
//!
//! When a user selects an inline `[label](klaus:plugin/<id>)`,
//! `[label](klaus:skill/<id>)`, or `[label](klaus:mcp/<id>)` token in the
//! composer (or supplies a `<referenced-resources>` JSON envelope), the engine
//! resolves the referenced plugin, skill, or MCP server before the first model
//! sampling step of the turn:
//! - Small MCP tool sets belonging to the referenced plugin or server are
//!   auto-surfaced so the model can invoke them on turn 1 without a separate
//!   `tool_search` round-trip; oversized tool sets stay deferred to protect the
//!   context window while advertising exact server names.
//! - Single-skill inline plugins (and explicit skill references) preload their
//!   expanded `SKILL.md` instructions once per conversation; multi-skill plugins
//!   advertise their exact `<namespace>:<skill>` entries so the model selects
//!   the right one intentionally.

use std::collections::HashSet;
use std::sync::Arc;

use serde::Deserialize;

use crate::extensions::plugins::runtime::PluginRuntimeEntry;
use crate::extensions::skills::loader::{catalog_skill_name, Skill};
use crate::runtime_values::RuntimeString;
use crate::types::tool::{Tool, ToolExposure};

/// Maximum number of MCP tools from one explicitly referenced plugin or MCP
/// server that are auto-surfaced into the active tool declarations for the turn.
/// Larger inventories remain behind `tool_search` so wide toolboxes never crowd
/// out the prompt or hit provider tool-count ceilings.
pub const AUTO_SURFACE_PLUGIN_TOOLS_MAX: usize = 16;

const REFERENCED_OPEN: &str = "<referenced-resources>";
const REFERENCED_CLOSE: &str = "</referenced-resources>";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExplicitResourceKind {
    Plugin,
    Skill,
    Mcp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplicitResourceReference {
    pub kind: ExplicitResourceKind,
    pub name: String,
    pub id: Option<String>,
    pub path: Option<String>,
}

impl ExplicitResourceReference {
    /// Primary lookup key (`id` when present and non-blank, otherwise `name`).
    pub fn key(&self) -> &str {
        self.id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| self.name.trim())
    }

    /// Human-readable label for status and capability headers.
    pub fn display_label(&self) -> &str {
        let name = self.name.trim();
        if !name.is_empty() {
            name
        } else {
            self.key()
        }
    }
}

#[derive(Deserialize)]
struct RawReferencedResource {
    kind: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    path: Option<String>,
}

/// Fast substring precheck before running full reference extraction.
pub fn has_explicit_resource_reference(text: &str) -> bool {
    text.contains(REFERENCED_OPEN)
        || text.contains("(klaus:plugin/")
        || text.contains("(klaus:skill/")
        || text.contains("(klaus:mcp/")
}

/// Extract deduplicated plugin, skill, and MCP references from user message text.
///
/// Parses structured `<referenced-resources>` JSON blocks first, then scans
/// prose outside fenced code blocks for `[label](klaus:<kind>/<encoded-id>)`
/// inline links so standalone Markdown references work identically.
pub fn extract_explicit_resource_references(text: &str) -> Vec<ExplicitResourceReference> {
    let mut out = Vec::new();
    let mut seen: HashSet<(ExplicitResourceKind, String)> = HashSet::new();

    let mut push_unique = |item: ExplicitResourceReference| {
        let canonical = canonical_dedup_key(item.kind, item.key(), item.path.as_deref());
        if !canonical.is_empty() && seen.insert((item.kind, canonical)) {
            out.push(item);
        }
    };

    for block in extract_referenced_resource_blocks(text) {
        for line in block.lines().rev() {
            let trimmed = line.trim();
            if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
                continue;
            }
            if let Ok(items) = serde_json::from_str::<Vec<RawReferencedResource>>(trimmed) {
                for raw in items {
                    let Some(kind) = parse_kind(&raw.kind) else {
                        continue;
                    };
                    let id = raw
                        .id
                        .map(|v| v.trim().to_string())
                        .filter(|v| !v.is_empty());
                    let path = raw
                        .path
                        .map(|v| v.trim().to_string())
                        .filter(|v| !v.is_empty());
                    let name = raw.name.trim().to_string();
                    if name.is_empty() && id.is_none() && path.is_none() {
                        continue;
                    }
                    push_unique(ExplicitResourceReference {
                        kind,
                        name,
                        id,
                        path,
                    });
                }
                break;
            }
        }
    }

    let stripped = strip_fenced_code_blocks_and_envelopes(text);
    for inline in scan_inline_klaus_links(&stripped) {
        push_unique(inline);
    }

    out
}

fn parse_kind(raw: &str) -> Option<ExplicitResourceKind> {
    match raw.trim() {
        "plugin" => Some(ExplicitResourceKind::Plugin),
        "skill" => Some(ExplicitResourceKind::Skill),
        "mcp" => Some(ExplicitResourceKind::Mcp),
        _ => None,
    }
}

fn canonical_dedup_key(kind: ExplicitResourceKind, key: &str, path: Option<&str>) -> String {
    let trimmed = key.trim();
    match kind {
        ExplicitResourceKind::Plugin => normalize_plugin_slug(trimmed).to_ascii_lowercase(),
        ExplicitResourceKind::Skill => {
            if !trimmed.is_empty() {
                trimmed.to_string()
            } else {
                path.unwrap_or_default().trim().to_string()
            }
        }
        ExplicitResourceKind::Mcp => trimmed.to_string(),
    }
}

/// Strip `@<marketplace>` suffixes from `<plugin>@<marketplace>` identifiers.
pub fn normalize_plugin_slug(raw: &str) -> &str {
    let trimmed = raw.trim();
    match trimmed.split_once('@') {
        Some((plugin, _)) if !plugin.trim().is_empty() => plugin.trim(),
        _ => trimmed,
    }
}

fn extract_referenced_resource_blocks(text: &str) -> Vec<&str> {
    let mut blocks = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find(REFERENCED_OPEN) {
        let after_open = &rest[start + REFERENCED_OPEN.len()..];
        let Some(end) = after_open.find(REFERENCED_CLOSE) else {
            break;
        };
        blocks.push(&after_open[..end]);
        rest = &after_open[end + REFERENCED_CLOSE.len()..];
    }
    blocks
}

fn strip_fenced_code_blocks_and_envelopes(text: &str) -> String {
    let mut without_envelopes = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find(REFERENCED_OPEN) {
        without_envelopes.push_str(&rest[..start]);
        let after_open = &rest[start + REFERENCED_OPEN.len()..];
        if let Some(end) = after_open.find(REFERENCED_CLOSE) {
            rest = &after_open[end + REFERENCED_CLOSE.len()..];
        } else {
            rest = "";
            break;
        }
    }
    without_envelopes.push_str(rest);

    let mut out = String::with_capacity(without_envelopes.len());
    let mut in_fence: Option<&str> = None;
    for line in without_envelopes.lines() {
        let trimmed = line.trim_start();
        if let Some(marker) = in_fence {
            if trimmed.starts_with(marker) {
                in_fence = None;
            }
            continue;
        }
        if trimmed.starts_with("```") {
            in_fence = Some("```");
            continue;
        }
        if trimmed.starts_with("~~~") {
            in_fence = Some("~~~");
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    out
}

fn scan_inline_klaus_links(text: &str) -> Vec<ExplicitResourceReference> {
    let mut found = Vec::new();
    let bytes = text.as_bytes();
    let mut idx = 0;
    while idx < bytes.len() {
        let Some(rel_open) = text[idx..].find('[') else {
            break;
        };
        let label_start = idx + rel_open + 1;
        let Some(rel_close) = text[label_start..].find("](klaus:") else {
            idx = label_start;
            continue;
        };
        let label_end = label_start + rel_close;
        let target_start = label_end + "](klaus:".len();
        let Some(rel_paren_end) = text[target_start..].find(')') else {
            break;
        };
        let target_end = target_start + rel_paren_end;
        let label = text[label_start..label_end].trim();
        let target = &text[target_start..target_end];
        if let Some((kind_str, rest)) = target.split_once('/') {
            if let Some(kind) = parse_kind(kind_str) {
                let (encoded_id, query) = match rest.split_once('?') {
                    Some((left, q)) => (left, Some(q)),
                    None => (rest, None),
                };
                let decoded_id = percent_decode(encoded_id);
                let decoded_path = query.and_then(|q| {
                    q.split('&').find_map(|pair| {
                        let (k, v) = pair.split_once('=')?;
                        (k == "path").then(|| percent_decode(v))
                    })
                });
                if !decoded_id.trim().is_empty() {
                    found.push(ExplicitResourceReference {
                        kind,
                        name: if label.is_empty() {
                            decoded_id.clone()
                        } else {
                            label.to_string()
                        },
                        id: Some(decoded_id),
                        path: decoded_path.filter(|p| !p.trim().is_empty()),
                    });
                }
            }
        }
        idx = target_end + 1;
    }
    found
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                out.push((hi << 4) | lo);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Match an explicit plugin reference against the session's plugin runtime entries.
pub fn match_plugin_runtime_entry<'a>(
    entries: &'a [PluginRuntimeEntry],
    reference: &ExplicitResourceReference,
) -> Option<&'a PluginRuntimeEntry> {
    let raw_key = reference.key();
    let slug = normalize_plugin_slug(raw_key);
    if !slug.is_empty() {
        if let Some(exact) = entries.iter().find(|entry| {
            entry
                .name
                .try_to_utf8()
                .is_ok_and(|n| n.eq_ignore_ascii_case(slug) || n.eq_ignore_ascii_case(raw_key))
                || entry.plugin_namespace.try_to_utf8().is_ok_and(|ns| {
                    ns.eq_ignore_ascii_case(slug) || ns.eq_ignore_ascii_case(raw_key)
                })
        }) {
            return Some(exact);
        }
    }
    let display = reference.name.trim();
    if !display.is_empty() {
        if let Some(by_display) = entries.iter().find(|entry| {
            entry
                .display_name
                .as_ref()
                .and_then(|d| d.try_to_utf8().ok())
                .is_some_and(|d| d.trim().eq_ignore_ascii_case(display))
        }) {
            return Some(by_display);
        }
    }
    None
}

fn sanitize_mcp_server_segment(name: &str) -> String {
    name.trim()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

/// Collect MCP tools in `tools` belonging to any of `server_names`.
///
/// Returns `(all_matching_tool_names, deferred_tool_names_to_surface)`.
pub fn collect_mcp_tools_for_servers(
    tools: &[Arc<dyn Tool>],
    server_names: &[RuntimeString],
) -> (Vec<RuntimeString>, Vec<RuntimeString>) {
    let prefixes: Vec<String> = server_names
        .iter()
        .filter_map(|server| server.try_to_utf8().ok())
        .map(|server| format!("mcp__{}__", sanitize_mcp_server_segment(&server)))
        .collect();
    if prefixes.is_empty() {
        return (Vec::new(), Vec::new());
    }
    let mut all_names = Vec::new();
    let mut deferred_names = Vec::new();
    for tool in tools {
        let name = tool.name();
        let Ok(utf8) = name.try_to_utf8() else {
            continue;
        };
        if prefixes.iter().any(|prefix| utf8.starts_with(prefix)) {
            all_names.push(name.clone());
            if tool.exposure() == ToolExposure::Deferred {
                deferred_names.push(name);
            }
        }
    }
    (all_names, deferred_names)
}

/// Return skills belonging to `entry` (matched by `plugin_namespace` or `plugin_skill_names`).
pub fn skills_for_plugin<'a>(
    skills: &'a [Skill],
    entry: &PluginRuntimeEntry,
    plugin_skill_names: &HashSet<RuntimeString>,
) -> Vec<&'a Skill> {
    skills
        .iter()
        .filter(|skill| {
            skill.plugin_namespace.as_ref() == Some(&entry.plugin_namespace)
                || skill.plugin_namespace.as_ref() == Some(&entry.name)
                || plugin_skill_names.contains(&skill.name)
        })
        .collect()
}

/// Compact single-line summary of a skill for multi-skill plugin capability cards.
pub fn summarize_skill_line(skill: &Skill) -> String {
    let qualified = catalog_skill_name(skill)
        .try_to_utf8()
        .unwrap_or_else(|_| String::from("<skill>"));
    let blurb_src = skill
        .short_description
        .as_ref()
        .filter(|s| s.try_to_utf8().is_ok_and(|t| !t.trim().is_empty()))
        .unwrap_or(&skill.description);
    let blurb = blurb_src
        .try_to_utf8()
        .unwrap_or_default()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let capped = if blurb.chars().count() > 140 {
        let prefix: String = blurb.chars().take(139).collect();
        format!("{prefix}…")
    } else {
        blurb
    };
    if capped.is_empty() {
        format!("`{qualified}`")
    } else {
        format!("`{qualified}` — {capped}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_plugin_skill_and_mcp_from_referenced_resources_envelope() {
        let input = concat!(
            "用 [演示文稿](klaus:plugin/ppt%40klaus-official) 给 klaus 做一个产品介绍\n\n",
            "<referenced-resources>\n",
            "The user selected these inline references.\n",
            "[{\"kind\":\"plugin\",\"name\":\"演示文稿\",\"id\":\"ppt@klaus-official\"},",
            "{\"kind\":\"skill\",\"name\":\"pdf\",\"id\":\"pdf\",\"path\":\"/skills/pdf/SKILL.md\"},",
            "{\"kind\":\"mcp\",\"name\":\"GitHub\",\"id\":\"github\"},",
            "{\"kind\":\"file\",\"name\":\"README.md\",\"path\":\"README.md\"}]\n",
            "</referenced-resources>"
        );
        let refs = extract_explicit_resource_references(input);
        assert_eq!(refs.len(), 3);
        assert_eq!(refs[0].kind, ExplicitResourceKind::Plugin);
        assert_eq!(refs[0].id.as_deref(), Some("ppt@klaus-official"));
        assert_eq!(normalize_plugin_slug(refs[0].key()), "ppt");
        assert_eq!(refs[1].kind, ExplicitResourceKind::Skill);
        assert_eq!(refs[1].key(), "pdf");
        assert_eq!(refs[1].path.as_deref(), Some("/skills/pdf/SKILL.md"));
        assert_eq!(refs[2].kind, ExplicitResourceKind::Mcp);
        assert_eq!(refs[2].key(), "github");
    }

    #[test]
    fn extracts_standalone_inline_klaus_links_and_skips_fenced_code_blocks() {
        let input = concat!(
            "Use [逆向工程](klaus:plugin/reverse-engineering%40klaus-official) here.\n",
            "```markdown\n",
            "Ignore [Fake](klaus:plugin/not-real%40klaus-official) inside code fences.\n",
            "```\n",
            "And [iphone-use](klaus:skill/iphoneuse%3Aiphone-use?path=%2Ftmp%2FSKILL.md)."
        );
        let refs = extract_explicit_resource_references(input);
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].kind, ExplicitResourceKind::Plugin);
        assert_eq!(
            refs[0].id.as_deref(),
            Some("reverse-engineering@klaus-official")
        );
        assert_eq!(refs[1].kind, ExplicitResourceKind::Skill);
        assert_eq!(refs[1].id.as_deref(), Some("iphoneuse:iphone-use"));
        assert_eq!(refs[1].path.as_deref(), Some("/tmp/SKILL.md"));
    }
}
