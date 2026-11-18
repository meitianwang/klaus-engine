//! Wire-boundary prompt and tool-identifier projection scoped exclusively to
//! the Antigravity adapter.
//!
//! Only serialized outbound `GenerateContent` payloads and inbound streamed
//! responses pass through this module; session state, rollout transcripts,
//! permission checks, and every other model source remain untouched.

use klaus_agent_core::provider::gemini::{GenerateContentResponse, Part};
use klaus_agent_core::runtime_values::{RuntimeJson, RuntimeObject, RuntimeString};
use std::collections::{HashMap, HashSet};

const KLAUS_IDENTITY_PREFIX: &str = "You are Klaus. You and the user share one workspace, and your job is to collaborate with them until their intended goal is completely handled.";
const WIRE_IDENTITY_PREFIX: &str = "You are an expert AI coding assistant with extreme attention to detail. You are pair programming with a USER to solve a task in their workspace.";

/// Exact phrase substitutions applied to harness instructions and tool schema
/// descriptions. Every pattern targets prose or protocol markers rather than
/// filesystem path segments, so paths such as `/workspace/klaus` or `~/.klaus/`
/// stay intact for shell and file tools.
const PROSE_REPLACEMENTS: &[(&str, &str)] = &[
    ("::klaus-inline-vis", "::inline-vis-frame"),
    (":klaus-file-cite", ":workspace-file-cite"),
    ("<klaus-delegation>", "<peer-delegation>"),
    ("</klaus-delegation>", "</peer-delegation>"),
    (
        "<!-- klaus-steered-message -->",
        "<!-- user-steered-message -->",
    ),
    ("### Klaus desktop context", "### Desktop host context"),
    (
        "You are running inside Klaus on",
        "You are running inside the desktop host on",
    ),
    ("in the Klaus desktop app", "in the desktop app"),
    ("Klaus's built-in browser", "The built-in browser"),
    ("same Klaus account", "same workspace account"),
    ("a Klaus account", "a host account"),
    ("refer to Klaus chats", "refer to workspace chats"),
    ("Klaus chats", "workspace chats"),
    ("Klaus supports persistent", "The host supports persistent"),
    ("saved prompt to Klaus", "saved prompt to the assistant"),
    (
        "Operate Klaus on the user's behalf",
        "Operate the host app on the user's behalf",
    ),
    ("Read Klaus documentation", "Read host documentation"),
    ("how Klaus works", "how the host app works"),
    (
        "Klaus product self-knowledge",
        "Host product self-knowledge",
    ),
    ("official Klaus manual", "official host manual"),
    ("reusable Klaus skill", "reusable workspace skill"),
    (
        "reusable Klaus slash command",
        "reusable workspace slash command",
    ),
    ("Klaus local scheduled tasks", "local scheduled tasks"),
    ("Klaus skill", "workspace skill"),
    (
        "Additional Klaus history record",
        "Additional session history record",
    ),
    ("Sent by Klaus from another chat", "Sent from another chat"),
    ("signed in to Klaus", "signed in to the host app"),
    ("in Klaus?", "in the host app?"),
    ("covers Klaus", "covers the host app"),
    (
        "in the codex *** Begin Patch format",
        "in the *** Begin Patch format",
    ),
    ("Klaus's", "the host app's"),
    ("inside Klaus", "inside the host app"),
    ("with Klaus", "with the assistant"),
    ("from Klaus", "from the host app"),
    ("by Klaus", "by the host app"),
    ("Klaus desktop", "desktop host"),
    ("Klaus engine", "agent engine"),
    ("Klaus client", "desktop client"),
    ("Klaus session", "workspace session"),
    ("Klaus workspace", "current workspace"),
];

/// Reversible directive markers that the model may emit in assistant text or
/// structured tool arguments and that the desktop renderer expects in canonical
/// form.
const REVERSIBLE_WIRE_MARKERS: &[(&str, &str)] = &[
    ("::inline-vis-frame", "::klaus-inline-vis"),
    (":workspace-file-cite", ":klaus-file-cite"),
    ("<peer-delegation>", "<klaus-delegation>"),
    ("</peer-delegation>", "</klaus-delegation>"),
];

#[derive(Clone, Debug, Default)]
pub(super) struct ToolProjection {
    internal_to_wire: HashMap<String, String>,
    wire_to_internal: HashMap<String, String>,
}

impl ToolProjection {
    fn from_declared_names<I, S>(names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let declared: Vec<String> = names.into_iter().map(|s| s.as_ref().to_string()).collect();
        let mut occupied: HashSet<String> = declared.iter().cloned().collect();
        let mut internal_to_wire = HashMap::new();
        let mut wire_to_internal = HashMap::new();

        for name in &declared {
            let Some(base_wire) = candidate_wire_tool_name(name) else {
                continue;
            };
            let mut candidate = base_wire.clone();
            let mut suffix = 2usize;
            while occupied.contains(&candidate) {
                candidate = format!("{base_wire}_{suffix}");
                suffix += 1;
            }
            occupied.insert(candidate.clone());
            internal_to_wire.insert(name.clone(), candidate.clone());
            wire_to_internal.insert(candidate, name.clone());
        }

        Self {
            internal_to_wire,
            wire_to_internal,
        }
    }

    fn register_ad_hoc(&mut self, internal: &str) -> Option<String> {
        if let Some(existing) = self.internal_to_wire.get(internal) {
            return Some(existing.clone());
        }
        let base_wire = candidate_wire_tool_name(internal)?;
        let mut candidate = base_wire.clone();
        let mut suffix = 2usize;
        while self.wire_to_internal.contains_key(&candidate) {
            candidate = format!("{base_wire}_{suffix}");
            suffix += 1;
        }
        self.internal_to_wire
            .insert(internal.to_string(), candidate.clone());
        self.wire_to_internal
            .insert(candidate.clone(), internal.to_string());
        Some(candidate)
    }

    fn map_outbound_text(&self, input: &str, replace_identity: bool) -> String {
        let mut out = if replace_identity && input.contains(KLAUS_IDENTITY_PREFIX) {
            input.replacen(KLAUS_IDENTITY_PREFIX, WIRE_IDENTITY_PREFIX, 1)
        } else {
            input.to_string()
        };
        for &(from, to) in PROSE_REPLACEMENTS {
            if out.contains(from) {
                out = out.replace(from, to);
            }
        }
        for (internal, wire) in &self.internal_to_wire {
            if out.contains(internal.as_str()) {
                out = replace_identifier_tokens(&out, internal, wire);
            }
        }
        scrub_standalone_klaus_prose(&out)
    }

    pub(super) fn restore_response(
        &self,
        response: &mut GenerateContentResponse,
        restorer: &mut TextRestorer,
        is_terminal_chunk: bool,
    ) {
        let Some(candidates) = response.candidates.as_mut() else {
            return;
        };
        for candidate in candidates.iter_mut() {
            let candidate_finishing = is_terminal_chunk || candidate.finish_reason.is_some();
            let Some(content) = candidate.content.as_mut() else {
                continue;
            };
            let has_function_call = content.parts.iter().any(|p| p.function_call.is_some());
            let mut last_visible_text_idx = None;
            for (idx, part) in content.parts.iter().enumerate() {
                if part.thought != Some(true)
                    && part.thought_signature.is_none()
                    && part.text.is_some()
                {
                    last_visible_text_idx = Some(idx);
                }
            }
            for (idx, part) in content.parts.iter_mut().enumerate() {
                // Signed thought parts must remain byte-identical so their
                // cryptographic `thoughtSignature` verifies on subsequent turns.
                if part.thought == Some(true) || part.thought_signature.is_some() {
                    if let Some(call) = part.function_call.as_mut() {
                        self.restore_function_call(&mut call.name, &mut call.args);
                    }
                    continue;
                }
                if let Some(call) = part.function_call.as_mut() {
                    self.restore_function_call(&mut call.name, &mut call.args);
                }
                if let Some(text_ref) = part.text.as_mut() {
                    if let Ok(utf8) = text_ref.try_to_utf8() {
                        let flush_now = (candidate_finishing || has_function_call)
                            && last_visible_text_idx == Some(idx);
                        let restored = restorer.push(&utf8, flush_now);
                        *text_ref = RuntimeString::from(restored);
                    }
                }
            }
            if (candidate_finishing || has_function_call) && last_visible_text_idx.is_none() {
                let tail = restorer.flush();
                if !tail.is_empty() {
                    content.parts.push(Part {
                        text: Some(RuntimeString::from(tail)),
                        ..Part::default()
                    });
                }
            }
        }
    }

    fn restore_function_call(
        &self,
        name: &mut Option<RuntimeString>,
        args: &mut Option<RuntimeJson>,
    ) {
        let caller_name = if let Some(name_ref) = name.as_mut() {
            if let Ok(raw_name) = name_ref.try_to_utf8() {
                if let Some(internal) = self.wire_to_internal.get(&raw_name) {
                    *name_ref = RuntimeString::from(internal.as_str());
                    internal.clone()
                } else {
                    raw_name
                }
            } else {
                String::new()
            }
        } else {
            String::new()
        };
        if let Some(args_ref) = args.as_mut() {
            let restore_tool_identifiers_in_code = caller_name == "REPL";
            self.restore_args_json(args_ref, restore_tool_identifiers_in_code);
        }
    }

    fn restore_args_json(&self, value: &mut RuntimeJson, restore_code_identifiers: bool) {
        match value {
            RuntimeJson::String(s) => {
                if let Ok(utf8) = s.try_to_utf8() {
                    let mut out = restore_wire_markers(&utf8);
                    if restore_code_identifiers {
                        for (wire, internal) in &self.wire_to_internal {
                            if out.contains(wire.as_str()) {
                                out = replace_identifier_tokens(&out, wire, internal);
                            }
                        }
                    }
                    if out != utf8 {
                        *s = RuntimeString::from(out);
                    }
                }
            }
            RuntimeJson::Array(items) => {
                for item in items.iter_mut() {
                    self.restore_args_json(item, restore_code_identifiers);
                }
            }
            RuntimeJson::Object(obj) => {
                for child in obj.values_mut() {
                    self.restore_args_json(child, restore_code_identifiers);
                }
            }
            _ => {}
        }
    }
}

fn candidate_wire_tool_name(name: &str) -> Option<String> {
    match name {
        "klaus_query" => return Some("app_state_query".to_string()),
        "klaus_action" => return Some("app_state_action".to_string()),
        _ => {}
    }
    if let Some(rest) = name.strip_prefix("klaus_") {
        return Some(format!("host_{rest}"));
    }
    if name.to_ascii_lowercase().contains("klaus") {
        let mut out = String::with_capacity(name.len());
        let bytes = name.as_bytes();
        let mut i = 0usize;
        while i < bytes.len() {
            if i + 5 <= bytes.len() && bytes[i..i + 5].eq_ignore_ascii_case(b"klaus") {
                out.push_str("host");
                i += 5;
            } else if let Some(ch) = name[i..].chars().next() {
                out.push(ch);
                i += ch.len_utf8();
            } else {
                break;
            }
        }
        return Some(out);
    }
    None
}

fn is_ident_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-' || byte == b'/'
}

/// Replace `from` with `to` only when not embedded inside a larger identifier
/// or path segment (for example `/klaus_query_logs` or `my_klaus_query`).
fn replace_identifier_tokens(text: &str, from: &str, to: &str) -> String {
    if from.is_empty() || !text.contains(from) {
        return text.to_string();
    }
    let bytes = text.as_bytes();
    let needle = from.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut cursor = 0usize;
    while let Some(rel) = text[cursor..].find(from) {
        let start = cursor + rel;
        let end = start + needle.len();
        let left_boundary = start == 0 || !is_ident_byte(bytes[start - 1]);
        let right_boundary = end == bytes.len() || !is_ident_byte(bytes[end]);
        if left_boundary && right_boundary {
            out.push_str(&text[cursor..start]);
            out.push_str(to);
        } else {
            out.push_str(&text[cursor..end]);
        }
        cursor = end;
    }
    out.push_str(&text[cursor..]);
    out
}

fn restore_wire_markers(text: &str) -> String {
    let mut out = text.to_string();
    for &(wire, canonical) in REVERSIBLE_WIRE_MARKERS {
        if out.contains(wire) {
            out = out.replace(wire, canonical);
        }
    }
    out
}

/// Stateful streaming restorer that holds back only a trailing partial prefix
/// of a reversible wire marker so multi-chunk SSE splits never leak the wire
/// form to the renderer.
#[derive(Clone, Debug, Default)]
pub(super) struct TextRestorer {
    pending: String,
}

impl TextRestorer {
    pub(super) fn push(&mut self, delta: &str, flush_all: bool) -> String {
        if self.pending.is_empty() && delta.is_empty() {
            return String::new();
        }
        self.pending.push_str(delta);
        let replaced = restore_wire_markers(&self.pending);
        if flush_all {
            self.pending.clear();
            return replaced;
        }
        let holdback = trailing_marker_prefix_len(&replaced);
        if holdback == 0 {
            self.pending.clear();
            replaced
        } else {
            let split_at = replaced.len() - holdback;
            let emit = replaced[..split_at].to_string();
            self.pending = replaced[split_at..].to_string();
            emit
        }
    }

    pub(super) fn flush(&mut self) -> String {
        if self.pending.is_empty() {
            return String::new();
        }
        let tail = std::mem::take(&mut self.pending);
        restore_wire_markers(&tail)
    }
}

fn trailing_marker_prefix_len(text: &str) -> usize {
    let max_check = REVERSIBLE_WIRE_MARKERS
        .iter()
        .map(|(wire, _)| wire.len().saturating_sub(1))
        .max()
        .unwrap_or(0)
        .min(text.len());
    for len in (1..=max_check).rev() {
        let start = text.len() - len;
        if !text.is_char_boundary(start) {
            continue;
        }
        let suffix = &text[start..];
        if REVERSIBLE_WIRE_MARKERS
            .iter()
            .any(|(wire, _)| wire.starts_with(suffix))
        {
            return len;
        }
    }
    0
}

/// Project an outbound `GenerateContent` request object in place and return the
/// reverse mapping needed to restore tool calls and markers in streamed chunks.
pub(super) fn sanitize_outbound_request(request: &mut RuntimeJson) -> ToolProjection {
    let Some(root) = request.as_object_mut() else {
        return ToolProjection::default();
    };

    let mut declared_names = Vec::new();
    if let Some(tools) = root.get("tools").and_then(RuntimeJson::as_array) {
        for tool in tools {
            if let Some(decls) = tool
                .get("functionDeclarations")
                .and_then(RuntimeJson::as_array)
            {
                for decl in decls {
                    if let Some(name) = decl
                        .get("name")
                        .and_then(RuntimeJson::as_string)
                        .and_then(|s| s.try_to_utf8().ok())
                    {
                        declared_names.push(name);
                    }
                }
            }
        }
    }
    let mut projection = ToolProjection::from_declared_names(declared_names);

    // Ensure historical calls to renamed tools (for example after tool-set
    // changes mid-session) also receive a deterministic bidirectional entry.
    if let Some(contents) = root.get("contents").and_then(RuntimeJson::as_array) {
        let mut extra_names = Vec::new();
        for content in contents {
            if let Some(parts) = content.get("parts").and_then(RuntimeJson::as_array) {
                for part in parts {
                    for key in ["functionCall", "functionResponse"] {
                        if let Some(name) = part
                            .get(key)
                            .and_then(|obj| obj.get("name"))
                            .and_then(RuntimeJson::as_string)
                            .and_then(|s| s.try_to_utf8().ok())
                        {
                            extra_names.push(name);
                        }
                    }
                }
            }
        }
        for name in extra_names {
            let _ = projection.register_ad_hoc(&name);
        }
    }

    // 1. Rewrite `systemInstruction` parts.
    if let Some(sys_parts) = root
        .get_mut("systemInstruction")
        .and_then(RuntimeJson::as_object_mut)
        .and_then(|sys| sys.get_mut("parts"))
        .and_then(RuntimeJson::as_array_mut)
    {
        for (idx, part) in sys_parts.iter_mut().enumerate() {
            if let Some(obj) = part.as_object_mut() {
                if let Some(text) = obj
                    .get("text")
                    .and_then(RuntimeJson::as_string)
                    .and_then(|s| s.try_to_utf8().ok())
                {
                    let rewritten = projection.map_outbound_text(&text, idx == 0);
                    if rewritten != text {
                        obj.insert("text", RuntimeJson::String(RuntimeString::from(rewritten)));
                    }
                }
            }
        }
    }

    // 2. Rewrite `tools[].functionDeclarations` names and descriptions.
    if let Some(tools) = root.get_mut("tools").and_then(RuntimeJson::as_array_mut) {
        for tool in tools.iter_mut() {
            if let Some(decls) = tool
                .as_object_mut()
                .and_then(|t| t.get_mut("functionDeclarations"))
                .and_then(RuntimeJson::as_array_mut)
            {
                for decl in decls.iter_mut() {
                    if let Some(decl_obj) = decl.as_object_mut() {
                        if let Some(name) = decl_obj
                            .get("name")
                            .and_then(RuntimeJson::as_string)
                            .and_then(|s| s.try_to_utf8().ok())
                        {
                            if let Some(wire) = projection.internal_to_wire.get(&name) {
                                decl_obj.insert(
                                    "name",
                                    RuntimeJson::String(RuntimeString::from(wire.as_str())),
                                );
                            }
                        }
                        scrub_schema_descriptions(decl_obj, &projection);
                    }
                }
            }
        }
    }

    // 3. Rewrite `toolConfig.functionCallingConfig.allowedFunctionNames`.
    if let Some(allowed) = root
        .get_mut("toolConfig")
        .and_then(RuntimeJson::as_object_mut)
        .and_then(|tc| tc.get_mut("functionCallingConfig"))
        .and_then(RuntimeJson::as_object_mut)
        .and_then(|fcc| fcc.get_mut("allowedFunctionNames"))
        .and_then(RuntimeJson::as_array_mut)
    {
        for entry in allowed.iter_mut() {
            if let Some(name) = entry
                .as_string()
                .and_then(|s| s.try_to_utf8().ok())
                .and_then(|name| projection.internal_to_wire.get(&name).cloned())
            {
                *entry = RuntimeJson::String(RuntimeString::from(name));
            }
        }
    }

    // 4. Rewrite historical `functionCall` / `functionResponse` names and
    // harness markers inside non-thought `contents` parts.
    if let Some(contents) = root.get_mut("contents").and_then(RuntimeJson::as_array_mut) {
        for content in contents.iter_mut() {
            let Some(parts) = content
                .as_object_mut()
                .and_then(|c| c.get_mut("parts"))
                .and_then(RuntimeJson::as_array_mut)
            else {
                continue;
            };
            for part in parts.iter_mut() {
                let Some(part_obj) = part.as_object_mut() else {
                    continue;
                };
                let is_signed_or_thought = part_obj.get("thought").and_then(RuntimeJson::as_bool)
                    == Some(true)
                    || part_obj.get("thoughtSignature").is_some();
                for call_key in ["functionCall", "functionResponse"] {
                    if let Some(call_obj) = part_obj
                        .get_mut(call_key)
                        .and_then(RuntimeJson::as_object_mut)
                    {
                        if let Some(name) = call_obj
                            .get("name")
                            .and_then(RuntimeJson::as_string)
                            .and_then(|s| s.try_to_utf8().ok())
                        {
                            if let Some(wire) = projection.internal_to_wire.get(&name) {
                                call_obj.insert(
                                    "name",
                                    RuntimeJson::String(RuntimeString::from(wire.as_str())),
                                );
                            }
                        }
                    }
                }
                if is_signed_or_thought {
                    continue;
                }
                if let Some(text) = part_obj
                    .get("text")
                    .and_then(RuntimeJson::as_string)
                    .and_then(|s| s.try_to_utf8().ok())
                {
                    let rewritten = map_content_markers(&text, &projection);
                    if rewritten != text {
                        part_obj
                            .insert("text", RuntimeJson::String(RuntimeString::from(rewritten)));
                    }
                }
            }
        }
    }

    projection
}

fn map_content_markers(text: &str, projection: &ToolProjection) -> String {
    if text.contains("<system-reminder>") || text.contains("<skill-catalog:") {
        return projection.map_outbound_text(text, false);
    }
    let mut out = text.to_string();
    for &(from, to) in &[
        ("::klaus-inline-vis", "::inline-vis-frame"),
        (":klaus-file-cite", ":workspace-file-cite"),
        ("<klaus-delegation>", "<peer-delegation>"),
        ("</klaus-delegation>", "</peer-delegation>"),
        (
            "<!-- klaus-steered-message -->",
            "<!-- user-steered-message -->",
        ),
    ] {
        if out.contains(from) {
            out = out.replace(from, to);
        }
    }
    out
}

/// Scrub residual standalone capitalized `Klaus` tokens in prose without
/// altering filesystem paths (`/workspace/klaus`, `~/.klaus/`, `/Applications/Klaus.app/...`)
/// or compound identifiers (`klaus_query`, `klaus-agent`).
fn scrub_standalone_klaus_prose(text: &str) -> String {
    if !text.contains("Klaus") {
        return text.to_string();
    }
    let bytes = text.as_bytes();
    let needle = b"Klaus";
    let mut out = String::with_capacity(text.len());
    let mut cursor = 0usize;
    while let Some(rel) = text[cursor..].find("Klaus") {
        let start = cursor + rel;
        let end = start + needle.len();
        let left_blocked = start > 0
            && (bytes[start - 1].is_ascii_alphanumeric()
                || matches!(bytes[start - 1], b'/' | b'\\' | b'.' | b'-' | b'_' | b':'));
        let right_blocked = end < bytes.len()
            && (bytes[end].is_ascii_alphanumeric()
                || matches!(bytes[end], b'/' | b'\\' | b'-' | b'_')
                || text[end..].starts_with(".app"));
        if left_blocked || right_blocked {
            out.push_str(&text[cursor..end]);
        } else {
            out.push_str(&text[cursor..start]);
            if out.ends_with("the ") || out.ends_with("The ") || out.ends_with("a ") {
                out.push_str("host app");
            } else {
                out.push_str("the host app");
            }
        }
        cursor = end;
    }
    out.push_str(&text[cursor..]);
    out
}

fn scrub_schema_descriptions(obj: &mut RuntimeObject, projection: &ToolProjection) {
    if let Some(desc) = obj
        .get("description")
        .and_then(RuntimeJson::as_string)
        .and_then(|s| s.try_to_utf8().ok())
    {
        let rewritten = projection.map_outbound_text(&desc, false);
        if rewritten != desc {
            obj.insert(
                "description",
                RuntimeJson::String(RuntimeString::from(rewritten)),
            );
        }
    }
    let keys: Vec<String> = obj
        .keys()
        .filter_map(|k| k.try_to_utf8().ok())
        .filter(|k| k != "description" && k != "name")
        .collect();
    for key in keys {
        if let Some(child) = obj.get_mut(&key) {
            scrub_schema_value(child, projection);
        }
    }
}

fn scrub_schema_value(value: &mut RuntimeJson, projection: &ToolProjection) {
    match value {
        RuntimeJson::Object(obj) => scrub_schema_descriptions(obj, projection),
        RuntimeJson::Array(items) => {
            for item in items.iter_mut() {
                scrub_schema_value(item, projection);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use klaus_agent_core::provider::gemini::{Candidate, Content, FunctionCall};
    use klaus_agent_core::runtime_values::runtime_json;
    use serde_json::{json, Value};

    fn parse_runtime(val: Value) -> RuntimeJson {
        runtime_json::parse(&RuntimeString::from(val.to_string())).unwrap()
    }

    fn to_serde(val: &RuntimeJson) -> Value {
        let mut bytes = Vec::new();
        runtime_json::write_to(val, &mut bytes).unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[test]
    fn outbound_projection_rewrites_identity_and_klaus_tools_while_preserving_workspace_paths() {
        let mut req = parse_runtime(json!({
            "systemInstruction": {
                "parts": [{
                    "text": "You are Klaus. You and the user share one workspace, and your job is to collaborate with them until their intended goal is completely handled.\nPrimary working directory: /Users/dev/workspace/klaus\nSkills: /Users/dev/.klaus/skills\nUse `klaus_query` before `klaus_action`.\nEmit ::klaus-inline-vis{path=\"/Users/dev/.klaus/visualizations/1/chart.html\"} and :klaus-file-cite[app.rs]{path=\"/Users/dev/workspace/klaus/src/app.rs#L10\"}."
                }]
            },
            "tools": [{
                "functionDeclarations": [
                    {
                        "name": "klaus_query",
                        "description": "Read Klaus documentation and live application state. Use it when the user asks how Klaus works.",
                        "parameters": {
                            "type": "OBJECT",
                            "properties": {
                                "key": { "type": "STRING", "description": "Surface in Klaus" }
                            }
                        }
                    },
                    {
                        "name": "klaus_action",
                        "description": "Operate Klaus on the user's behalf. Discover actions with klaus_query first.",
                        "parameters": { "type": "OBJECT" }
                    },
                    {
                        "name": "apply_patch",
                        "description": "Apply a multi-file patch (add/update/delete) in the codex *** Begin Patch format.",
                        "parameters": { "type": "OBJECT" }
                    },
                    {
                        "name": "bash",
                        "description": "Execute a shell command.",
                        "parameters": { "type": "OBJECT" }
                    }
                ]
            }],
            "toolConfig": {
                "functionCallingConfig": {
                    "mode": "ANY",
                    "allowedFunctionNames": ["klaus_query", "bash"]
                }
            },
            "contents": [
                {
                    "role": "user",
                    "parts": [{ "text": "<!-- klaus-steered-message -->\nCheck /Users/dev/workspace/klaus/README.md" }]
                },
                {
                    "role": "model",
                    "parts": [
                        { "thought": true, "text": "Klaus internal signed thought", "thoughtSignature": "sig-123" },
                        { "functionCall": { "name": "klaus_query", "args": { "key": "docs" } }, "thoughtSignature": "sig-call" }
                    ]
                },
                {
                    "role": "user",
                    "parts": [
                        { "functionResponse": { "name": "klaus_query", "response": { "ok": true } } }
                    ]
                }
            ]
        }));

        let projection = sanitize_outbound_request(&mut req);
        let out = to_serde(&req);
        let sys_text = out["systemInstruction"]["parts"][0]["text"]
            .as_str()
            .unwrap();

        assert!(sys_text.starts_with(WIRE_IDENTITY_PREFIX));
        assert!(!sys_text.contains("You are Klaus"));
        assert!(sys_text.contains("/Users/dev/workspace/klaus"));
        assert!(sys_text.contains("/Users/dev/.klaus/skills"));
        assert!(sys_text.contains("/Users/dev/.klaus/visualizations/1/chart.html"));
        assert!(sys_text.contains("`app_state_query`"));
        assert!(sys_text.contains("`app_state_action`"));
        assert!(sys_text.contains("::inline-vis-frame{"));
        assert!(sys_text.contains(":workspace-file-cite["));

        let decls = out["tools"][0]["functionDeclarations"].as_array().unwrap();
        assert_eq!(decls[0]["name"], "app_state_query");
        assert!(!decls[0]["description"].as_str().unwrap().contains("Klaus"));
        assert!(!decls[0]["parameters"]["properties"]["key"]["description"]
            .as_str()
            .unwrap()
            .contains("Klaus"));
        assert_eq!(decls[1]["name"], "app_state_action");
        assert!(decls[1]["description"]
            .as_str()
            .unwrap()
            .contains("app_state_query"));
        assert_eq!(decls[2]["name"], "apply_patch");
        assert_eq!(
            decls[2]["description"],
            "Apply a multi-file patch (add/update/delete) in the *** Begin Patch format."
        );
        assert_eq!(decls[3]["name"], "bash");

        assert_eq!(
            out["toolConfig"]["functionCallingConfig"]["allowedFunctionNames"],
            json!(["app_state_query", "bash"])
        );
        assert_eq!(
            out["contents"][0]["parts"][0]["text"],
            "<!-- user-steered-message -->\nCheck /Users/dev/workspace/klaus/README.md"
        );
        // Signed thought text MUST remain byte-identical!
        assert_eq!(
            out["contents"][1]["parts"][0]["text"],
            "Klaus internal signed thought"
        );
        assert_eq!(
            out["contents"][1]["parts"][1]["functionCall"]["name"],
            "app_state_query"
        );
        assert_eq!(
            out["contents"][2]["parts"][0]["functionResponse"]["name"],
            "app_state_query"
        );

        // Inbound restoration reverses functionCall name and REPL code references.
        let mut restorer = TextRestorer::default();
        let mut resp = GenerateContentResponse {
            model_version: None,
            usage_metadata: None,
            candidates: Some(vec![Candidate {
                content: Some(Content {
                    role: "model".into(),
                    parts: vec![
                        Part {
                            thought: Some(true),
                            text: Some("::inline-vis-frame must stay untouched in thought".into()),
                            thought_signature: Some("sig".into()),
                            ..Part::default()
                        },
                        Part {
                            function_call: Some(FunctionCall {
                                id: None,
                                name: Some("app_state_query".into()),
                                args: Some(parse_runtime(json!({"key": "docs"}))),
                            }),
                            ..Part::default()
                        },
                    ],
                }),
                finish_reason: Some("STOP".into()),
                grounding_metadata: None,
            }]),
        };
        projection.restore_response(&mut resp, &mut restorer, true);
        let candidates = resp.candidates.unwrap();
        let parts = &candidates[0].content.as_ref().unwrap().parts;
        assert_eq!(
            parts[0].text.as_ref().unwrap(),
            "::inline-vis-frame must stay untouched in thought"
        );
        assert_eq!(
            parts[1]
                .function_call
                .as_ref()
                .unwrap()
                .name
                .as_ref()
                .unwrap(),
            "klaus_query"
        );
    }

    #[test]
    fn streaming_text_restorer_reassembles_markers_across_every_chunk_boundary() {
        let wire = "Here is the chart:\n::inline-vis-frame{path=\"/tmp/demo.html\"}\nAnd ref :workspace-file-cite[main.rs]{path=\"/tmp/main.rs#L5\"} done.";
        let expected = "Here is the chart:\n::klaus-inline-vis{path=\"/tmp/demo.html\"}\nAnd ref :klaus-file-cite[main.rs]{path=\"/tmp/main.rs#L5\"} done.";

        for split in 0..=wire.len() {
            let mut restorer = TextRestorer::default();
            let mut out = String::new();
            out.push_str(&restorer.push(&wire[..split], false));
            out.push_str(&restorer.push(&wire[split..], true));
            assert_eq!(out, expected, "failed at split index {split}");
        }

        // Byte-by-byte streaming also reconstructs exact canonical text.
        let mut restorer = TextRestorer::default();
        let mut out = String::new();
        for ch in wire.chars() {
            let mut buf = [0u8; 4];
            out.push_str(&restorer.push(ch.encode_utf8(&mut buf), false));
        }
        out.push_str(&restorer.flush());
        assert_eq!(out, expected);
    }
}
