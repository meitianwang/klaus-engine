//! Tool-name normalization: the pipeline that turns raw MCP server/tool names
//! into model-visible callable names that are unique, <= 64 bytes, and free of
//! characters the Responses API rejects.

use std::collections::{HashMap, HashSet};

use sha1::{Digest, Sha1};

use crate::runtime_values::RuntimeString;

/// The separator between namespace (server) and tool name in the model-visible
/// callable name. Matches the legacy `mcp__<server>__<tool>` convention.
pub const MCP_TOOL_NAME_DELIMITER: &str = "__";

/// Hard upper bound on the combined namespace + delimiter + tool name.
const MAX_TOOL_NAME_LENGTH: usize = 64;

/// Length of the SHA1 hash suffix used to disambiguate collisions.
const CALLABLE_NAME_HASH_LEN: usize = 12;

/// The legacy prefix for MCP tool names. When `prefix_mcp_tool_names` is true,
/// every server that is NOT in `non_prefixed_servers` gets this prepended to its
/// namespace portion.
const LEGACY_MCP_TOOL_NAME_PREFIX: &str = "mcp__";

/// Sanitize a raw name for the Responses API: only `[a-zA-Z0-9_-]` are valid,
/// everything else becomes `_`.
fn sanitize_responses_api_tool_name(name: &str) -> String {
    name.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn sha1_hex(s: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(s.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn callable_name_hash_suffix(raw_identity: &str) -> String {
    let hash = sha1_hex(raw_identity);
    format!("_{}", &hash[..CALLABLE_NAME_HASH_LEN])
}

fn append_hash_suffix(value: &str, raw_identity: &str) -> String {
    format!("{value}{}", callable_name_hash_suffix(raw_identity))
}

fn append_namespace_hash_suffix(namespace: &str, raw_identity: &str) -> String {
    if let Some(ns) = namespace.strip_suffix(MCP_TOOL_NAME_DELIMITER) {
        format!(
            "{}{}{}",
            ns,
            callable_name_hash_suffix(raw_identity),
            MCP_TOOL_NAME_DELIMITER
        )
    } else {
        append_hash_suffix(namespace, raw_identity)
    }
}

fn truncate_name(value: &str, max_len: usize) -> String {
    value.chars().take(max_len).collect()
}

fn fit_callable_parts_with_hash(
    namespace: &str,
    tool_name: &str,
    raw_identity: &str,
    reserved_len: usize,
) -> (String, String) {
    let suffix = callable_name_hash_suffix(raw_identity);
    let max_tool_len = MAX_TOOL_NAME_LENGTH.saturating_sub(namespace.len() + reserved_len);
    if max_tool_len >= suffix.len() {
        let prefix_len = max_tool_len - suffix.len();
        return (
            namespace.to_string(),
            format!("{}{}", truncate_name(tool_name, prefix_len), suffix),
        );
    }
    let max_namespace_len = MAX_TOOL_NAME_LENGTH.saturating_sub(suffix.len() + reserved_len);
    (truncate_name(namespace, max_namespace_len), suffix)
}

fn unique_callable_parts(
    namespace: &str,
    tool_name: &str,
    raw_identity: &str,
    used_names: &mut HashSet<String>,
    reserved_len: usize,
) -> (String, String) {
    let model_name = format!("{namespace}{tool_name}");
    if model_name.len() + reserved_len <= MAX_TOOL_NAME_LENGTH && used_names.insert(model_name) {
        return (namespace.to_string(), tool_name.to_string());
    }
    let mut attempt = 0_u32;
    loop {
        let hash_input = if attempt == 0 {
            raw_identity.to_string()
        } else {
            format!("{raw_identity}\0{attempt}")
        };
        let (ns, tn) =
            fit_callable_parts_with_hash(namespace, tool_name, &hash_input, reserved_len);
        let model_name = format!("{ns}{tn}");
        if used_names.insert(model_name) {
            return (ns, tn);
        }
        attempt = attempt.saturating_add(1);
    }
}

fn callable_namespace_with_prefix(namespace: &str, prefix: bool) -> String {
    if !prefix || namespace.starts_with(LEGACY_MCP_TOOL_NAME_PREFIX) {
        namespace.to_string()
    } else {
        format!("{LEGACY_MCP_TOOL_NAME_PREFIX}{namespace}")
    }
}

/// One tool descriptor before normalization, carrying raw identity.
struct Candidate {
    /// The sanitized namespace (server portion).
    callable_namespace: String,
    /// The sanitized tool name.
    callable_name: String,
    /// Composite raw identity for the namespace (server+connector).
    raw_namespace_identity: String,
    /// Composite raw identity for the full tool.
    raw_tool_identity: String,
}

/// Options for [`normalize_tool_names`].
pub struct NormalizeOptions<'a> {
    /// Whether to prepend `mcp__` to namespaces.
    pub prefix_mcp_tool_names: bool,
    /// Server names that are exempt from the `mcp__` prefix (host-managed).
    pub non_prefixed_servers: &'a [String],
}

impl Default for NormalizeOptions<'_> {
    fn default() -> Self {
        Self {
            prefix_mcp_tool_names: true,
            non_prefixed_servers: &[],
        }
    }
}

/// Input: `(server_name, tool_name)` pairs in the order they should appear.
/// Output: `(model_visible_name, raw_tool_name)` pairs in the same order.
///
/// Every output name is guaranteed unique and <= 64 bytes.
pub fn normalize_tool_names(
    tools: &[(String, String)],
    opts: &NormalizeOptions<'_>,
) -> Vec<(RuntimeString, RuntimeString)> {
    let mut seen_raw_names: HashSet<String> = HashSet::new();
    let mut candidates: Vec<(usize, Candidate)> = Vec::new();

    for (index, (server, tool)) in tools.iter().enumerate() {
        let raw_namespace_identity = format!("{server}\0");
        let raw_tool_identity = format!("{raw_namespace_identity}\0{tool}");
        if !seen_raw_names.insert(raw_tool_identity.clone()) {
            // Duplicate tool — skip silently.
            continue;
        }
        let callable_namespace = callable_namespace_with_prefix(
            &format!(
                "{}{}",
                sanitize_responses_api_tool_name(server),
                MCP_TOOL_NAME_DELIMITER
            ),
            opts.prefix_mcp_tool_names && !opts.non_prefixed_servers.contains(server),
        );
        let callable_name = sanitize_responses_api_tool_name(tool);
        candidates.push((
            index,
            Candidate {
                callable_namespace,
                callable_name,
                raw_namespace_identity,
                raw_tool_identity,
            },
        ));
    }

    // Phase 1: Detect namespace collisions.
    let mut ns_identities: HashMap<String, HashSet<String>> = HashMap::new();
    for (_, c) in &candidates {
        ns_identities
            .entry(c.callable_namespace.clone())
            .or_default()
            .insert(c.raw_namespace_identity.clone());
    }
    let colliding_ns: HashSet<String> = ns_identities
        .into_iter()
        .filter_map(|(ns, ids)| (ids.len() > 1).then_some(ns))
        .collect();
    for (_, c) in &mut candidates {
        if colliding_ns.contains(&c.callable_namespace) {
            c.callable_namespace =
                append_namespace_hash_suffix(&c.callable_namespace, &c.raw_namespace_identity);
        }
    }

    // Phase 2: Detect tool-name collisions within each namespace.
    let mut tool_identities: HashMap<(String, String), HashSet<String>> = HashMap::new();
    for (_, c) in &candidates {
        tool_identities
            .entry((c.callable_namespace.clone(), c.callable_name.clone()))
            .or_default()
            .insert(c.raw_tool_identity.clone());
    }
    let colliding_tools: HashSet<(String, String)> = tool_identities
        .into_iter()
        .filter_map(|(key, ids)| (ids.len() > 1).then_some(key))
        .collect();
    for (_, c) in &mut candidates {
        if colliding_tools.contains(&(c.callable_namespace.clone(), c.callable_name.clone())) {
            c.callable_name = append_hash_suffix(&c.callable_name, &c.raw_tool_identity);
        }
    }

    // Sort by raw identity for determinism.
    candidates.sort_by(|a, b| a.1.raw_tool_identity.cmp(&b.1.raw_tool_identity));

    // Phase 3: Final uniqueness pass with length enforcement.
    let mut used_names: HashSet<String> = HashSet::new();
    let mut results: Vec<(usize, RuntimeString)> = Vec::new();
    for (original_index, c) in candidates {
        let (ns, tn) = unique_callable_parts(
            &c.callable_namespace,
            &c.callable_name,
            &c.raw_tool_identity,
            &mut used_names,
            MCP_TOOL_NAME_DELIMITER.len(),
        );
        let model_name = RuntimeString::from(format!("{ns}{tn}"));
        results.push((original_index, model_name));
    }

    // Re-sort by original index to preserve input order.
    results.sort_by_key(|(idx, _)| *idx);

    results
        .into_iter()
        .map(|(idx, model_name)| {
            let raw_tool = RuntimeString::from(tools[idx].1.as_str());
            (model_name, raw_tool)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_prefixed_naming() {
        let tools = vec![("github".to_string(), "list_repos".to_string())];
        let result = normalize_tool_names(&tools, &NormalizeOptions::default());
        assert_eq!(result.len(), 1);
        let name = result[0].0.try_to_utf8().unwrap();
        assert_eq!(name, "mcp__github__list_repos");
        assert!(name.len() <= MAX_TOOL_NAME_LENGTH);
    }

    #[test]
    fn non_prefixed_server() {
        let non_prefixed = vec!["computer-use".to_string()];
        let tools = vec![("computer-use".to_string(), "click".to_string())];
        let result = normalize_tool_names(
            &tools,
            &NormalizeOptions {
                prefix_mcp_tool_names: true,
                non_prefixed_servers: &non_prefixed,
            },
        );
        let name = result[0].0.try_to_utf8().unwrap();
        assert_eq!(name, "computer-use__click");
    }

    #[test]
    fn long_names_are_truncated_and_hashed() {
        let long_server = "a".repeat(50);
        let long_tool = "b".repeat(50);
        let tools = vec![(long_server, long_tool)];
        let result = normalize_tool_names(&tools, &NormalizeOptions::default());
        let name = result[0].0.try_to_utf8().unwrap();
        assert!(name.len() <= MAX_TOOL_NAME_LENGTH, "got {}", name.len());
    }

    #[test]
    fn duplicate_tools_are_deduplicated() {
        let tools = vec![
            ("srv".to_string(), "tool".to_string()),
            ("srv".to_string(), "tool".to_string()),
        ];
        let result = normalize_tool_names(&tools, &NormalizeOptions::default());
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn colliding_namespaces_get_hash_suffix() {
        // Two different servers that sanitize to the same namespace
        let tools = vec![
            ("srv.a".to_string(), "t".to_string()),
            ("srv/a".to_string(), "t".to_string()),
        ];
        let result = normalize_tool_names(&tools, &NormalizeOptions::default());
        assert_eq!(result.len(), 2);
        let n0 = result[0].0.try_to_utf8().unwrap();
        let n1 = result[1].0.try_to_utf8().unwrap();
        assert_ne!(n0, n1);
        assert!(n0.len() <= MAX_TOOL_NAME_LENGTH);
        assert!(n1.len() <= MAX_TOOL_NAME_LENGTH);
    }
}
