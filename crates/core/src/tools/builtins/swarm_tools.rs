//! `TeamCreate` / `TeamDelete` / `ListPeers` / `SendMessage` /
//! `WaitForTeammates` — multi-agent coordination. Ordinary subagents receive
//! messages through their task registry without requiring a team. Agents in a named team
//! coordinate through a shared file-backed mailbox; an agent's team+peer
//! identity is declared at session start or spawn (`TeamCreate` may bootstrap a
//! top-level session that has none). Present only when the host enables swarm
//! coordination.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::runtime_values::{runtime_json, RuntimeJson, RuntimeObject, RuntimeString};
use crate::schemas::subagent_message::{nonblank, validate_text_message};
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{
    SubagentMessageRequest, Tool, ToolContext, ToolExposure, ToolProgress, ToolResult,
    ValidationResult,
};

/// A `{ ok: false, message }` result flagged as an error — the shared "swarm
/// unavailable / not usable" response.
fn runtime_object(fields: impl IntoIterator<Item = (&'static str, RuntimeJson)>) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    for (key, value) in fields {
        object.insert(key, value);
    }
    RuntimeJson::Object(object)
}

fn join_runtime_strings(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let Some(mut output) = values.first().cloned() else {
        return RuntimeString::default();
    };
    let separator = RuntimeString::from(separator);
    for value in &values[1..] {
        output = output.concat(&separator).concat(value);
    }
    output
}

fn no_swarm(message: impl Into<RuntimeString>) -> ToolResult {
    let message = message.into();
    let output = runtime_object([
        ("ok", RuntimeJson::Bool(false)),
        ("message", RuntimeJson::String(message.clone())),
    ]);
    text_result(output, message, true)
}

/// The default "swarm unavailable" message.
const SWARM_DISABLED: &str = "Swarm is not enabled for this session.";
/// The "no team yet" message for team-only operations.
const NO_TEAM: &str = "This session is not in a team yet. Call TeamCreate first.";

/// An empty object schema (`ListPeers` / `WaitForTeammates` take no arguments).
fn empty_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {},
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

// ---- TeamCreate ------------------------------------------------------------

fn team_create_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "team_name": {
                "type": "string",
                "description": "Name for the new team to create."
            },
            "description": {
                "type": "string",
                "description": "Team description/purpose."
            },
            "agent_type": {
                "type": "string",
                "description": "Type/role of the team lead (e.g., \"researcher\", \"test-runner\"). Used for team file and inter-agent coordination."
            }
        },
        "required": ["team_name"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

/// Create a team using the TeamCreate contract.
pub struct TeamCreateTool {
    input_schema: JsonSchema,
}

/// Construct the `TeamCreate` tool.
pub fn create_team_create_tool() -> Arc<dyn Tool> {
    Arc::new(TeamCreateTool {
        input_schema: team_create_input_schema(),
    })
}

#[async_trait]
impl Tool for TeamCreateTool {
    fn static_name(&self) -> &str {
        "TeamCreate"
    }

    fn static_description(&self) -> &str {
        "Create a new team for coordinating multiple agents"
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn input_schema_property_order(
        &self,
        object_path: &[String],
    ) -> Option<&'static [&'static str]> {
        object_path
            .is_empty()
            .then_some(&["team_name", "description", "agent_type"])
    }

    fn static_search_hint(&self) -> Option<&str> {
        Some("create a multi-agent swarm team")
    }

    fn exposure(&self) -> ToolExposure {
        ToolExposure::Deferred
    }

    fn max_result_size_chars(&self) -> Option<u64> {
        Some(100_000)
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

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let swarm = match &ctx.swarm {
            Some(swarm) => swarm,
            None => return Ok(no_swarm(SWARM_DISABLED)),
        };
        let team_name = input
            .get("team_name")
            .and_then(RuntimeJson::as_str)
            .cloned();
        if team_name.as_ref().is_none_or(|team_name| {
            team_name.as_utf16().iter().all(|unit| {
                matches!(
                    unit,
                    0x0009..=0x000d
                        | 0x0020
                        | 0x00a0
                        | 0x1680
                        | 0x2000..=0x200a
                        | 0x2028
                        | 0x2029
                        | 0x202f
                        | 0x205f
                        | 0x3000
                        | 0xfeff
                )
            })
        }) {
            let message = RuntimeString::from("team_name is required for TeamCreate");
            return Ok(text_result(
                runtime_object([
                    ("ok", RuntimeJson::Bool(false)),
                    ("message", RuntimeJson::String(message.clone())),
                ]),
                message,
                true,
            ));
        }
        let description = input
            .get("description")
            .and_then(RuntimeJson::as_str)
            .cloned();
        let agent_type = input
            .get("agent_type")
            .and_then(RuntimeJson::as_str)
            .cloned();
        let created = swarm
            .create(
                team_name.expect("validated TeamCreate team_name"),
                description,
                agent_type,
            )
            .await;
        let output = runtime_object([
            (
                "team_name",
                RuntimeJson::String(created.identity.team.clone()),
            ),
            (
                "team_file_path",
                RuntimeJson::String(created.team_file_path),
            ),
            ("lead_agent_id", RuntimeJson::String(created.lead_agent_id)),
        ]);
        let text = runtime_json::stringify(&output);
        Ok(text_result(output, text, false))
    }
}

// ---- TeamDelete ------------------------------------------------------------

fn team_delete_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "team": {
                "type": "string",
                "description": "The team to delete."
            },
            "force": {
                "type": "boolean",
                "default": false,
                "description": "Delete even if other peers are still active (their mailboxes are lost)."
            }
        },
        "required": ["team", "force"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

/// Delete a swarm team and its mailboxes.
pub struct TeamDeleteTool {
    input_schema: JsonSchema,
}

/// Construct the `TeamDelete` tool.
pub fn create_team_delete_tool() -> Arc<dyn Tool> {
    Arc::new(TeamDeleteTool {
        input_schema: team_delete_input_schema(),
    })
}

#[async_trait]
impl Tool for TeamDeleteTool {
    fn static_name(&self) -> &str {
        "TeamDelete"
    }

    fn static_description(&self) -> &str {
        "Delete a swarm team and its mailboxes. Refuses if other peers are still active unless force is set."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn static_search_hint(&self) -> Option<&str> {
        Some("disband a team and remove its mailboxes")
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

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(true)
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let swarm = match &ctx.swarm {
            Some(swarm) => swarm,
            None => return Ok(no_swarm(SWARM_DISABLED)),
        };
        let team = input
            .get("team")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        let force = input
            .get("force")
            .and_then(RuntimeJson::as_bool)
            .unwrap_or(false);
        let result = swarm.remove(&team, force).await;
        if !result.removed {
            let out_message = result
                .reason
                .clone()
                .unwrap_or_else(|| RuntimeString::from("Team was not removed."));
            let text = result.reason.clone().unwrap_or_else(|| {
                RuntimeString::from("Team \"")
                    .concat(&team)
                    .concat(&RuntimeString::from("\" was not removed."))
            });
            return Ok(text_result(
                runtime_object([
                    ("ok", RuntimeJson::Bool(false)),
                    ("removed", RuntimeJson::Bool(false)),
                    ("message", RuntimeJson::String(out_message)),
                ]),
                text,
                true,
            ));
        }
        let message = RuntimeString::from("Removed team \"")
            .concat(&team)
            .concat(&RuntimeString::from("\"."));
        Ok(text_result(
            runtime_object([
                ("ok", RuntimeJson::Bool(true)),
                ("removed", RuntimeJson::Bool(true)),
                ("message", RuntimeJson::String(message.clone())),
            ]),
            message.clone(),
            false,
        ))
    }
}

// ---- ListPeers -------------------------------------------------------------

/// List the peers in this agent's swarm team.
pub struct ListPeersTool {
    input_schema: JsonSchema,
}

/// Construct the `ListPeers` tool.
pub fn create_list_peers_tool() -> Arc<dyn Tool> {
    Arc::new(ListPeersTool {
        input_schema: empty_input_schema(),
    })
}

#[async_trait]
impl Tool for ListPeersTool {
    fn static_name(&self) -> &str {
        "ListPeers"
    }

    fn static_description(&self) -> &str {
        "List the peers in this agent's swarm team, with their activity status."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn static_search_hint(&self) -> Option<&str> {
        Some("list the other agents in my team")
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    async fn call(
        &self,
        _input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let swarm = match &ctx.swarm {
            Some(swarm) => swarm,
            None => return Ok(no_swarm(SWARM_DISABLED)),
        };
        let current = match swarm.current() {
            Some(current) => current,
            None => return Ok(no_swarm(NO_TEAM)),
        };
        let peers = swarm.peers().await;
        let lines: Vec<RuntimeString> = peers
            .iter()
            .map(|p| {
                let you = if p.peer == current.peer { " (you)" } else { "" };
                let idle = if p.active { "" } else { " (idle)" };
                RuntimeString::from("- ")
                    .concat(&p.peer)
                    .concat(&RuntimeString::from(you))
                    .concat(&RuntimeString::from(idle))
            })
            .collect();
        let peers_json: Vec<RuntimeJson> = peers
            .iter()
            .map(|p| {
                runtime_object([
                    ("peer", RuntimeJson::String(p.peer.clone())),
                    ("lastSeen", RuntimeJson::from(p.last_seen)),
                    ("active", RuntimeJson::Bool(p.active)),
                ])
            })
            .collect();
        let message = RuntimeString::from(format!("{} peer(s) in team \"", peers.len()))
            .concat(&current.team)
            .concat(&RuntimeString::from("\"."));
        let output = runtime_object([
            ("ok", RuntimeJson::Bool(true)),
            ("peers", RuntimeJson::Array(peers_json)),
            ("message", RuntimeJson::String(message)),
        ]);
        let text = if !peers.is_empty() {
            RuntimeString::from("Peers in team \"")
                .concat(&current.team)
                .concat(&RuntimeString::from("\":\n"))
                .concat(&join_runtime_strings(&lines, "\n"))
        } else {
            RuntimeString::from("Team \"")
                .concat(&current.team)
                .concat(&RuntimeString::from("\" has no other peers yet."))
        };
        Ok(text_result(output, text, false))
    }
}

// ---- SendMessage -----------------------------------------------------------

fn send_message_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "to": {
                "type": "string",
                "description": "Existing subagent ID or name, parent for the caller's direct parent, primary agent ID, or a teammate name. Messages to the primary agent are passive mail. Send to one recipient at a time."
            },
            "summary": {
                "type": "string",
                "maxLength": 200,
                "description": "A short preview, required when message is a string."
            },
            "trigger_turn": {
                "type": "boolean",
                "description": "Set true to start follow-up work in an existing ordinary subagent. Omit or set false to queue a message without starting a new turn. Only supported for plain-text messages to ordinary subagents; original permissions and restart restrictions remain enforced."
            },
            "message": {
                "anyOf": [
                    { "type": "string", "description": "Plain text task direction. Does not grant approval or change the recipient's permissions." },
                    {
                        "type": "object",
                        "properties": {
                            "type": { "type": "string", "enum": ["shutdown_request"] },
                            "reason": { "type": "string" }
                        },
                        "required": ["type"],
                        "additionalProperties": false
                    },
                    {
                        "type": "object",
                        "properties": {
                            "type": { "type": "string", "enum": ["shutdown_response"] },
                            "request_id": { "type": "string", "minLength": 1, "maxLength": 200, "pattern": "^[^\\n\\r]{1,200}$" },
                            "approve": { "anyOf": [{ "type": "boolean" }, { "type": "string", "enum": ["true", "false"] }] },
                            "reason": { "type": "string" }
                        },
                        "required": ["type", "request_id", "approve"],
                        "additionalProperties": false
                    },
                    {
                        "type": "object",
                        "properties": {
                            "type": { "type": "string", "enum": ["plan_approval_response"] },
                            "request_id": { "type": "string", "minLength": 1, "maxLength": 200, "pattern": "^[^\\n\\r]{1,200}$" },
                            "approve": { "anyOf": [{ "type": "boolean" }, { "type": "string", "enum": ["true", "false"] }] },
                            "feedback": { "type": "string" }
                        },
                        "required": ["type", "request_id", "approve"],
                        "additionalProperties": false
                    }
                ],
                "description": "Plain text, or a structured protocol message available only to team members."
            }
        },
        "required": ["to", "message"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

fn protocol_approval(message: &Json) -> Option<bool> {
    match message.get("approve") {
        Some(RuntimeJson::Bool(value)) => Some(*value),
        Some(RuntimeJson::String(value)) if value == "true" => Some(true),
        Some(RuntimeJson::String(value)) if value == "false" => Some(false),
        _ => None,
    }
}

fn message_field_validation(input: &Json) -> Option<RuntimeString> {
    let Some(fields) = input.as_object() else {
        return Some("Message input must be an object".into());
    };
    if fields.keys().any(|key| {
        !["to", "summary", "message", "trigger_turn"]
            .iter()
            .any(|allowed| key == *allowed)
    }) {
        return Some("Unknown message input field".into());
    }
    if fields
        .get("trigger_turn")
        .is_some_and(|value| !value.is_boolean())
    {
        return Some("trigger_turn must be a boolean".into());
    }
    if fields.get("trigger_turn").and_then(RuntimeJson::as_bool) == Some(true)
        && !fields.get("message").is_some_and(RuntimeJson::is_string)
    {
        return Some("trigger_turn requires a plain-text message to an ordinary subagent".into());
    }
    if let Some(message) = input.get("message").filter(|message| message.is_object()) {
        let Some(kind) = message.get("type").and_then(RuntimeJson::as_str) else {
            return Some("Structured messages require a supported type".into());
        };
        let allowed: &[&str] = match kind.try_to_utf8().as_deref() {
            Ok("shutdown_request") => &["type", "reason"],
            Ok("shutdown_response") => &["type", "request_id", "approve", "reason"],
            Ok("plan_approval_response") => &["type", "request_id", "approve", "feedback"],
            _ => return Some("Unsupported structured team-protocol message type".into()),
        };
        if message
            .as_object()
            .expect("validated object")
            .keys()
            .any(|key| !allowed.iter().any(|allowed| key == *allowed))
        {
            return Some("Unknown structured team-protocol field".into());
        }
    }
    None
}

fn send_message_validation(input: &Json, ctx: &ToolContext) -> Option<RuntimeString> {
    if let Some(error) = message_field_validation(input) {
        return Some(error);
    }
    let Some(to) = input.get("to").and_then(RuntimeJson::as_str) else {
        return Some("to must be a nonempty subagent ID or name".into());
    };
    if !nonblank(to) {
        return Some("to must not be empty".into());
    }
    if to == "*" {
        return Some("Broadcast is not supported; send a message per recipient.".into());
    }
    if to.as_utf16().contains(&u16::from(b'@')) {
        return Some("to must be an unqualified subagent ID or teammate name.".into());
    }
    if let Some(summary) = input.get("summary") {
        let Some(summary) = summary.as_str() else {
            return Some("summary must be a string".into());
        };
        if summary.as_utf16().len() > 200 {
            return Some("summary must contain at most 200 characters".into());
        }
    }
    let Some(message) = input.get("message") else {
        return Some("message is required".into());
    };
    if let Some(text) = message.as_str() {
        let Some(summary) = input.get("summary").and_then(RuntimeJson::as_str) else {
            return Some("summary is required when message is a string".into());
        };
        return validate_text_message(to, text, summary).err();
    }
    if !message.is_object() {
        return Some("message must be plain text or a structured team-protocol object".into());
    }
    let Some(identity) = ctx.swarm.as_ref().and_then(|swarm| swarm.current()) else {
        return Some("Structured team-protocol messages require an active team.".into());
    };
    let Some(kind) = message.get("type").and_then(RuntimeJson::as_str) else {
        return Some("Structured messages require a supported type".into());
    };
    for key in ["reason", "feedback"] {
        if message.get(key).is_some_and(|value| !value.is_string()) {
            return Some(RuntimeString::from(format!("{key} must be a string")));
        }
    }
    if kind == "shutdown_request" {
        return None;
    }
    if kind != "shutdown_response" && kind != "plan_approval_response" {
        return Some("Unsupported structured team-protocol message type".into());
    }
    let valid_request = message
        .get("request_id")
        .and_then(RuntimeJson::as_str)
        .is_some_and(|id| {
            !id.is_empty()
                && id.as_utf16().len() <= 200
                && !id.as_utf16().iter().any(|unit| *unit == 10 || *unit == 13)
        });
    if !valid_request {
        return Some("request_id must identify the request being answered".into());
    }
    let Some(approved) = protocol_approval(message) else {
        return Some("approve must be a boolean".into());
    };
    if kind == "shutdown_response" {
        if to != "team-lead" {
            return Some("shutdown_response must be sent to team-lead".into());
        }
        let reason = message.get("reason").and_then(RuntimeJson::as_str);
        if approved && message.get("reason").is_some() {
            return Some("reason is only permitted when rejecting a shutdown request".into());
        }
        if !approved && !reason.is_some_and(nonblank) {
            return Some("reason is required when rejecting a shutdown request".into());
        }
    } else if identity.peer != "team-lead" {
        return Some("Only the team lead may send a plan approval response".into());
    }
    None
}

/// Send task direction to an existing subagent or teammate.
pub struct SendMessageTool {
    input_schema: JsonSchema,
}

/// Construct the `SendMessage` tool.
pub fn create_send_message_tool() -> Arc<dyn Tool> {
    Arc::new(SendMessageTool {
        input_schema: send_message_input_schema(),
    })
}

#[async_trait]
impl Tool for SendMessageTool {
    fn static_name(&self) -> &str {
        "SendMessage"
    }

    fn static_description(&self) -> &str {
        "Queue a message to an existing subagent by ID or name, without requiring a team. Running subagents receive it at a tool-round boundary. By default, idle or completed subagents keep the message for their next explicit turn. An explicit turn trigger starts follow-up work in an ordinary subagent using its original identity, history, tools and permissions. User-cancelled and one-shot agents retain their restart restrictions. A short summary is required for plain text. Team-protocol objects require an active team. Messages never grant permission or change configuration."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn parse_input(&self, input: &Json) -> Result<Json, ToolError> {
        // Reject unknown fields before the shared schema parser can strip them.
        if let Some(error) = message_field_validation(input) {
            return Err(ToolError::new(error));
        }
        crate::tools::input_parser::parse_native_input(&self.input_schema(), input, &|path| {
            self.input_schema_property_order(path)
        })
    }

    fn static_search_hint(&self) -> Option<&str> {
        Some("queue a message to a subagent or teammate")
    }

    async fn validate_input(&self, input: &Json, ctx: &ToolContext) -> Option<ValidationResult> {
        Some(match send_message_validation(input, ctx) {
            Some(message) => ValidationResult::Err {
                message: String::from_utf8(message.to_node_utf8())
                    .expect("message diagnostics are valid UTF-8"),
                error_code: Some(9),
            },
            None => ValidationResult::Ok,
        })
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

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        if let Some(error) = send_message_validation(&input, ctx) {
            return Ok(no_swarm(error));
        }
        let to = input
            .get("to")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        let value = input.get("message").expect("validated message");
        if let Some(message) = value.as_str() {
            if let Some(registry) = &ctx.background_tasks {
                let delivery = registry
                    .send_message(SubagentMessageRequest {
                        trigger_turn: input
                            .get("trigger_turn")
                            .and_then(RuntimeJson::as_bool)
                            .unwrap_or(false),
                        target: to.clone(),
                        message: message.clone(),
                        summary: input
                            .get("summary")
                            .and_then(RuntimeJson::as_str)
                            .cloned()
                            .expect("validated plain-text summary"),
                        sender_agent_id: registry.caller_agent_id(),
                        user_initiated: false,
                    })
                    .await;
                if let Some(delivery) = delivery {
                    return Ok(match delivery {
                        Err(error) => no_swarm(error),
                        Ok(delivery) => {
                            let text = RuntimeString::from(if delivery.resumed {
                                "Subagent resumed with message: "
                            } else {
                                "Message queued for agent: "
                            })
                            .concat(&delivery.agent_id);
                            text_result(
                                runtime_object([
                                    ("ok", RuntimeJson::Bool(true)),
                                    ("delivery", RuntimeJson::from("queued")),
                                    ("agentId", RuntimeJson::String(delivery.agent_id.clone())),
                                    (
                                        "recipients",
                                        RuntimeJson::Array(vec![RuntimeJson::String(
                                            delivery.agent_id,
                                        )]),
                                    ),
                                    ("resumed", RuntimeJson::Bool(delivery.resumed)),
                                    ("message", RuntimeJson::String(text.clone())),
                                ]),
                                text,
                                false,
                            )
                        }
                    });
                }
            }
        }
        if input.get("trigger_turn").and_then(RuntimeJson::as_bool) == Some(true) {
            return Ok(no_swarm("Starting follow-up work requires an existing ordinary subagent; teammate mail does not start a stopped runtime."));
        }
        let Some(swarm) = &ctx.swarm else {
            return Ok(no_swarm(
                "No subagent or teammate matches the requested recipient.",
            ));
        };
        if swarm.current().is_none() {
            return Ok(no_swarm(
                "No subagent or teammate matches the requested recipient.",
            ));
        }
        // The roster carries a liveness window, not just names. A teammate whose
        // loop died with the parent process still has a persisted mailbox and a
        // roster entry, so name-matching alone reports "sent" for a message nobody
        // is reading. Record the recipient's liveness so the result can say which
        // case it is.
        let peers = swarm.peers().await;
        let recipient = peers.iter().find(|peer| peer.peer == to);
        if recipient.is_none() {
            return Ok(no_swarm(
                "No subagent or teammate matches the requested recipient.",
            ));
        }
        let recipient_active = recipient.is_some_and(|peer| peer.active);
        let message = if let Some(text) = value.as_str() {
            text.clone()
        } else {
            let mut frame = value.clone();
            if let Some(approved) = protocol_approval(value) {
                frame
                    .as_object_mut()
                    .expect("validated protocol object")
                    .insert("approve", RuntimeJson::Bool(approved));
            }
            runtime_json::stringify(&frame)
        };
        let result = swarm.send(&to, &message).await;
        let recipients = result.recipients;
        if recipients.is_empty() {
            return Ok(no_swarm(
                "No subagent or teammate matches the requested recipient.",
            ));
        }
        let who = if to == "*" {
            RuntimeString::from(format!("{} teammate(s)", recipients.len()))
        } else {
            RuntimeString::from("\"")
                .concat(&to)
                .concat(&RuntimeString::from("\""))
        };
        let has_recipients = !recipients.is_empty();
        // A mailbox write always succeeds, so "Sent" alone would hide whether any
        // live peer is going to read it. Say so when the recipient is not active.
        let output_message =
            RuntimeString::from("Sent to ")
                .concat(&who)
                .concat(&RuntimeString::from(if recipient_active {
                    "."
                } else {
                    " (not currently running; queued in its mailbox for the next run)."
                }));
        let output = runtime_object([
            ("ok", RuntimeJson::Bool(true)),
            ("delivery", RuntimeJson::from("queued")),
            (
                "recipients",
                RuntimeJson::Array(recipients.into_iter().map(RuntimeJson::String).collect()),
            ),
            ("message", RuntimeJson::String(output_message)),
        ]);
        let text = if has_recipients {
            RuntimeString::from("Message sent to ")
                .concat(&who)
                .concat(&RuntimeString::from(if recipient_active {
                    "."
                } else {
                    ", but that teammate is not currently running. The message is queued in its mailbox and will be delivered the next time it runs; it will not act on it now."
                }))
        } else {
            RuntimeString::from("No recipients (no other peers in the team).")
        };
        Ok(text_result(output, text, false))
    }
}

// ---- WaitForTeammates ------------------------------------------------------

/// Block until every persistent teammate this session spawned is idle or done.
pub struct WaitForTeammatesTool {
    input_schema: JsonSchema,
}

/// Construct the `WaitForTeammates` tool.
pub fn create_wait_for_teammates_tool() -> Arc<dyn Tool> {
    Arc::new(WaitForTeammatesTool {
        input_schema: empty_input_schema(),
    })
}

#[async_trait]
impl Tool for WaitForTeammatesTool {
    fn static_name(&self) -> &str {
        "WaitForTeammates"
    }

    fn static_description(&self) -> &str {
        "Block until every persistent teammate you spawned is idle (waiting) or finished. Wait when the next step depends on those results; continue useful independent work when it is available."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn static_search_hint(&self) -> Option<&str> {
        Some("wait for my teammates to finish their current work")
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    async fn call(
        &self,
        _input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let bg = match &ctx.background_tasks {
            Some(bg) => bg,
            None => {
                return Ok(text_result(
                    runtime_object([
                        ("ok", RuntimeJson::Bool(false)),
                        ("message", RuntimeJson::String(RuntimeString::default())),
                    ]),
                    "Teammates are not available in this context.",
                    true,
                ))
            }
        };
        let idle = bg.wait_for_idle();
        tokio::pin!(idle);
        loop {
            let mailbox_pending = match &ctx.swarm {
                Some(swarm) => swarm.has_pending_input().await.map_err(ToolError::new)?,
                None => false,
            };
            let pending = bg.has_pending_input() || mailbox_pending;
            if pending || ctx.signal.is_cancelled() {
                let reason = if pending {
                    "input_pending"
                } else {
                    "cancelled"
                };
                let message = if pending {
                    "New input is waiting."
                } else {
                    "Waiting was cancelled."
                };
                return Ok(text_result(
                    runtime_object([
                        ("ok", RuntimeJson::Bool(true)),
                        ("reason", RuntimeJson::from(reason)),
                        ("message", RuntimeJson::from(message)),
                    ]),
                    message,
                    false,
                ));
            }
            tokio::select! {
                _ = &mut idle => break,
                _ = ctx.signal.cancelled() => {},
                _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {},
            }
        }
        Ok(text_result(
            runtime_object([
                ("ok", RuntimeJson::Bool(true)),
                (
                    "message",
                    RuntimeJson::String(RuntimeString::from("All teammates are idle.")),
                ),
            ]),
            "All teammates are idle.",
            false,
        ))
    }
}

/// All swarm tools, included when the host enables swarm coordination.
pub fn create_swarm_tools() -> Vec<Arc<dyn Tool>> {
    vec![
        create_team_create_tool(),
        create_team_delete_tool(),
        create_list_peers_tool(),
        create_send_message_tool(),
        create_wait_for_teammates_tool(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_process::RuntimeEnv;
    use crate::runtime_values::{runtime_json, RuntimePath};
    use crate::schemas::content::OutputBlock;
    use crate::types::abort::AbortSignal;
    use crate::types::events::ToolDomainEvent;
    use crate::types::file_state::{FileStateCache, FileStateEntry, RecordOptions};
    use crate::types::permission::PermissionContext;
    use crate::types::sandbox::{
        ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
    };
    use crate::types::tool::{
        BackgroundTaskHandle, BackgroundTaskRegistry, SubagentMessageResult, SwarmController,
        SwarmCreateResult, SwarmIdentity, SwarmPeerInfo, SwarmRemoveResult, SwarmSendResult,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc as StdArc;

    macro_rules! rjson {
        ($($json:tt)+) => {{
            runtime_json::parse(&RuntimeString::from(stringify!($($json)+)))
                .expect("test JSON literal must parse as exact runtime JSON")
        }};
    }

    fn rs(value: &str) -> RuntimeString {
        RuntimeString::from(value)
    }

    struct NoopFileState;

    impl FileStateCache for NoopFileState {
        fn get(&self, _path: &RuntimePath) -> Option<FileStateEntry> {
            None
        }
        fn record(&self, _path: &RuntimePath, _mtime_ms: f64, _opts: RecordOptions) {}
        fn has(&self, _path: &RuntimePath) -> bool {
            false
        }
        fn entries(&self) -> Vec<(RuntimePath, FileStateEntry)> {
            Vec::new()
        }
        fn delete(&self, _path: &RuntimePath) {}
        fn clear(&self) {}
        fn clone_box(&self) -> Box<dyn FileStateCache> {
            Box::new(NoopFileState)
        }
    }

    struct NoopSandbox;

    impl SandboxBackend for NoopSandbox {
        fn kind(&self) -> SandboxKind {
            SandboxKind::Process
        }
        fn is_available(&self) -> bool {
            false
        }
        fn transform(&self, req: &ExecRequest, _policy: &SandboxPolicy) -> SpawnSpec {
            SpawnSpec {
                file: req.argv.first().cloned().unwrap_or_default(),
                args: req.argv.iter().skip(1).cloned().collect(),
                cwd: req.cwd.clone(),
                env: RuntimeEnv::new(),
            }
        }
    }

    /// A mock swarm controller returning canned identity / roster / outcomes.
    struct MockSwarm {
        identity: Option<SwarmIdentity>,
        peers: Vec<SwarmPeerInfo>,
        remove_result: SwarmRemoveResult,
        send_result: SwarmSendResult,
        sends: AtomicUsize,
    }

    impl Default for MockSwarm {
        fn default() -> Self {
            MockSwarm {
                identity: Some(SwarmIdentity {
                    team: rs("alpha"),
                    peer: rs("me"),
                }),
                peers: Vec::new(),
                remove_result: SwarmRemoveResult {
                    removed: true,
                    reason: None,
                },
                send_result: SwarmSendResult {
                    recipients: Vec::new(),
                },
                sends: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl SwarmController for MockSwarm {
        fn current(&self) -> Option<SwarmIdentity> {
            self.identity.clone()
        }
        async fn create(
            &self,
            team_name: RuntimeString,
            _description: Option<RuntimeString>,
            _agent_type: Option<RuntimeString>,
        ) -> SwarmCreateResult {
            let lead_agent_id = rs("team-lead@").concat(&team_name);
            SwarmCreateResult {
                identity: SwarmIdentity {
                    team: team_name,
                    peer: rs("team-lead"),
                },
                team_file_path: rs("/tmp/team/config.json"),
                lead_agent_id,
            }
        }
        async fn peers(&self) -> Vec<SwarmPeerInfo> {
            self.peers.clone()
        }
        async fn send(&self, _to: &RuntimeString, _body: &RuntimeString) -> SwarmSendResult {
            self.sends.fetch_add(1, Ordering::SeqCst);
            self.send_result.clone()
        }
        async fn remove(&self, _team: &RuntimeString, _force: bool) -> SwarmRemoveResult {
            self.remove_result.clone()
        }
    }

    /// A mock background-task registry that records whether `wait_for_idle` ran.
    struct MockBackground {
        waited: StdArc<AtomicBool>,
    }

    #[async_trait]
    impl BackgroundTaskRegistry for MockBackground {
        fn get(&self, _agent_id: &RuntimeString) -> Option<BackgroundTaskHandle> {
            None
        }
        fn list(&self) -> Vec<BackgroundTaskHandle> {
            Vec::new()
        }
        fn stop(&self, _agent_id: &RuntimeString) -> bool {
            false
        }
        async fn wait_for_idle(&self) {
            self.waited.store(true, Ordering::SeqCst);
        }
    }

    fn base_ctx() -> ToolContext {
        ToolContext {
            async_question: None,
            background_processes: Arc::new(
                crate::command_runtime::background::BackgroundCommandRegistry::new(),
            ),
            process_context: crate::runtime_process::test_runtime_process_context(),
            cwd: ".".into(),
            signal: AbortSignal::new(),
            file_state: Arc::new(NoopFileState),
            permission: PermissionContext {
                allow_rules: Vec::new(),
                deny_rules: Vec::new(),
                ask_rules: Vec::new(),
                additional_writable_roots: Vec::new(),
                internal_writable_roots: Vec::new(),
                pinned_memory_root: None,
                should_avoid_prompts: false,
            },
            sandbox: Arc::new(NoopSandbox),
            planning_read_only: false,
            approval_policy: None,
            sandbox_mode: None,
            approvals_reviewer: None,
            web_search_access: None,
            network_allowed: None,
            sandbox_retry_on_denial: None,
            request_approval: None,
            request_network_access: None,
            enqueue_background_notice: None,
            record_rollout_event: None,
            emit: std::sync::Arc::new(|_e: ToolDomainEvent| Box::pin(async {})),
            native_pdf: None,
            vision: None,
            shell_env: None,
            depth: None,
            forward_subagent_text: None,
            spawn_sub_agent: None,
            spawn_background_agent: None,
            spawn_workflow_agent: None,
            surface_tools: None,
            activate_skill_paths: None,
            tool_catalog: None,
            elicit: None,
            plan_file: None,
            apply_skill_context: None,
            run_bash: None,
            mcp_clients: None,
            background_tasks: None,
            task_list_id: None,
            task_plan: None,
            worktree: None,
            workflow_journal: None,
            swarm: None,
            cron: None,
            goal: None,
            goal_generation: None,
            goal_owned_resources: None,
            lsp: None,
            browser: None,
            browser_tool_mapping: None,
            run_hook: None,
            invoke_tool: None,
            advisor: None,
        }
    }

    fn text_of(res: &ToolResult) -> RuntimeString {
        match &res.serializable.model_content[0] {
            OutputBlock::Text(t) => t.text.clone(),
            _ => panic!("expected a text block"),
        }
    }

    // ---- metadata + schemas --------------------------------------------------

    #[test]
    fn tools_expose_expected_metadata_and_flags() {
        let create = create_team_create_tool();
        assert_eq!(create.name(), "TeamCreate");
        assert_eq!(
            create.search_hint(),
            Some(rs("create a multi-agent swarm team"))
        );
        assert_eq!(create.exposure(), ToolExposure::Deferred);
        assert_eq!(create.max_result_size_chars(), Some(100_000));
        assert!(!create.is_read_only(&rjson!({})));
        assert!(!create.is_concurrency_safe(&rjson!({})));
        assert_eq!(create.is_destructive(&rjson!({})), Some(false));

        let delete = create_team_delete_tool();
        assert_eq!(delete.name(), "TeamDelete");
        assert_eq!(delete.is_destructive(&rjson!({})), Some(true));

        let list = create_list_peers_tool();
        assert_eq!(list.name(), "ListPeers");
        assert!(list.is_read_only(&rjson!({})));
        assert!(list.is_concurrency_safe(&rjson!({})));

        let send = create_send_message_tool();
        assert_eq!(send.name(), "SendMessage");
        assert!(!send.is_read_only(&rjson!({})));

        let wait = create_wait_for_teammates_tool();
        assert_eq!(wait.name(), "WaitForTeammates");
        assert!(wait.is_read_only(&rjson!({})));
        assert!(!wait.is_concurrency_safe(&rjson!({})));
    }

    #[test]
    fn schemas_match_exactly() {
        assert_eq!(
            create_team_create_tool().input_schema(),
            rjson!({
                "type": "object",
                "properties": {
                    "team_name": { "type": "string", "description": "Name for the new team to create." },
                    "description": { "type": "string", "description": "Team description/purpose." },
                    "agent_type": { "type": "string", "description": "Type/role of the team lead (e.g., \"researcher\", \"test-runner\"). Used for team file and inter-agent coordination." }
                },
                "required": ["team_name"],
                "additionalProperties": false
            })
        );
        assert_eq!(
            create_team_delete_tool().input_schema(),
            rjson!({
                "type": "object",
                "properties": {
                    "team": { "type": "string", "description": "The team to delete." },
                    "force": { "type": "boolean", "default": false, "description": "Delete even if other peers are still active (their mailboxes are lost)." }
                },
                "required": ["team", "force"],
                "additionalProperties": false
            })
        );
        assert_eq!(
            create_send_message_tool().input_schema(),
            rjson!({
                "type": "object",
                "properties": {
                    "to": { "type": "string", "description": "Existing subagent ID or name, parent for the caller's direct parent, primary agent ID, or a teammate name. Messages to the primary agent are passive mail. Send to one recipient at a time." },
                    "summary": { "type": "string", "maxLength": 200, "description": "A short preview, required when message is a string." },
                    "trigger_turn": { "type": "boolean", "description": "Set true to start follow-up work in an existing ordinary subagent. Omit or set false to queue a message without starting a new turn. Only supported for plain-text messages to ordinary subagents; original permissions and restart restrictions remain enforced." },
                    "message": {
                        "anyOf": [
                            { "type": "string", "description": "Plain text task direction. Does not grant approval or change the recipient's permissions." },
                            { "type": "object", "properties": {
                                "type": { "type": "string", "enum": ["shutdown_request"] },
                                "reason": { "type": "string" }
                            }, "required": ["type"], "additionalProperties": false },
                            { "type": "object", "properties": {
                                "type": { "type": "string", "enum": ["shutdown_response"] },
                                "request_id": { "type": "string", "minLength": 1, "maxLength": 200, "pattern": "^[^\\n\\r]{1,200}$" },
                                "approve": { "anyOf": [{ "type": "boolean" }, { "type": "string", "enum": ["true", "false"] }] },
                                "reason": { "type": "string" }
                            }, "required": ["type", "request_id", "approve"], "additionalProperties": false },
                            { "type": "object", "properties": {
                                "type": { "type": "string", "enum": ["plan_approval_response"] },
                                "request_id": { "type": "string", "minLength": 1, "maxLength": 200, "pattern": "^[^\\n\\r]{1,200}$" },
                                "approve": { "anyOf": [{ "type": "boolean" }, { "type": "string", "enum": ["true", "false"] }] },
                                "feedback": { "type": "string" }
                            }, "required": ["type", "request_id", "approve"], "additionalProperties": false }
                        ],
                        "description": "Plain text, or a structured protocol message available only to team members."
                    }
                },
                "required": ["to", "message"],
                "additionalProperties": false
            })
        );
        let empty = rjson!({ "type": "object", "properties": {}, "additionalProperties": false });
        assert_eq!(create_list_peers_tool().input_schema(), empty.clone());
        assert_eq!(create_wait_for_teammates_tool().input_schema(), empty);
    }

    // ---- TeamCreate ----------------------------------------------------------

    #[tokio::test]
    async fn team_create_reports_disabled_swarm() {
        let tool = create_team_create_tool();
        let ctx = base_ctx();
        let res = tool.call(rjson!({}), &ctx, None).await.unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(text_of(&res), "Swarm is not enabled for this session.");
        assert_eq!(
            res.serializable.output,
            rjson!({ "ok": false, "message": "Swarm is not enabled for this session." })
        );
    }

    #[tokio::test]
    async fn team_create_creates_and_registers() {
        let tool = create_team_create_tool();
        let mut ctx = base_ctx();
        ctx.swarm = Some(Arc::new(MockSwarm::default()));
        let res = tool
            .call(
                rjson!({ "team_name": "alpha", "description": "purpose", "agent_type": "researcher" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable.output,
            rjson!({ "team_name": "alpha", "team_file_path": "/tmp/team/config.json", "lead_agent_id": "team-lead@alpha" })
        );
        assert_eq!(
            text_of(&res),
            r#"{"team_name":"alpha","team_file_path":"/tmp/team/config.json","lead_agent_id":"team-lead@alpha"}"#
        );
    }

    #[tokio::test]
    async fn team_create_direct_call_rejects_missing_and_whitespace_team_cleanly() {
        let tool = create_team_create_tool();
        let mut ctx = base_ctx();
        ctx.swarm = Some(Arc::new(MockSwarm::default()));
        let mut unicode_whitespace = RuntimeObject::new();
        unicode_whitespace.insert(
            "team_name",
            RuntimeJson::String(RuntimeString::from_utf16([0x0020, 0x0009, 0x000a, 0xfeff])),
        );
        for input in [
            rjson!({}),
            rjson!({ "team_name": "" }),
            RuntimeJson::Object(unicode_whitespace),
        ] {
            let result = tool.call(input, &ctx, None).await.unwrap();
            assert_eq!(result.serializable.is_error, Some(true));
            assert_eq!(
                result.serializable.output,
                rjson!({ "ok": false, "message": "team_name is required for TeamCreate" })
            );
            assert_eq!(text_of(&result), "team_name is required for TeamCreate");
        }
    }

    #[tokio::test]
    async fn team_create_preserves_lone_surrogates_and_output_property_order() {
        let tool = create_team_create_tool();
        let mut ctx = base_ctx();
        ctx.swarm = Some(Arc::new(MockSwarm::default()));
        let team = RuntimeString::from_utf16(vec![u16::from(b't'), 0xd800]);
        let mut input = RuntimeObject::new();
        input.insert("team_name", RuntimeJson::String(team.clone()));

        let result = tool
            .call(RuntimeJson::Object(input), &ctx, None)
            .await
            .unwrap();
        assert_eq!(
            result.serializable.output["team_name"]
                .as_str()
                .unwrap()
                .as_utf16(),
            team.as_utf16()
        );
        assert_eq!(
            result
                .serializable
                .output
                .as_object()
                .unwrap()
                .keys()
                .collect::<Vec<_>>(),
            vec![rs("team_name"), rs("team_file_path"), rs("lead_agent_id")]
        );
    }

    // ---- TeamDelete ----------------------------------------------------------

    #[tokio::test]
    async fn team_delete_removes() {
        let tool = create_team_delete_tool();
        let mut ctx = base_ctx();
        ctx.swarm = Some(Arc::new(MockSwarm::default()));
        let res = tool
            .call(rjson!({ "team": "alpha", "force": false }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable.output,
            rjson!({ "ok": true, "removed": true, "message": "Removed team \"alpha\"." })
        );
        assert_eq!(text_of(&res), "Removed team \"alpha\".");
    }

    #[tokio::test]
    async fn team_delete_refused_uses_reason_then_fallbacks() {
        // With a reason, both output.message and text use it.
        let tool = create_team_delete_tool();
        let mut ctx = base_ctx();
        ctx.swarm = Some(Arc::new(MockSwarm {
            remove_result: SwarmRemoveResult {
                removed: false,
                reason: Some(rs(
                    "1 other peer(s) still active: bob. Pass force to remove anyway.",
                )),
            },
            ..MockSwarm::default()
        }));
        let res = tool
            .call(rjson!({ "team": "alpha", "force": false }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "1 other peer(s) still active: bob. Pass force to remove anyway."
        );

        // Without a reason, output falls back to "Team was not removed." and text
        // to the team-named variant.
        let mut ctx2 = base_ctx();
        ctx2.swarm = Some(Arc::new(MockSwarm {
            remove_result: SwarmRemoveResult {
                removed: false,
                reason: None,
            },
            ..MockSwarm::default()
        }));
        let res2 = tool
            .call(rjson!({ "team": "alpha", "force": false }), &ctx2, None)
            .await
            .unwrap();
        assert_eq!(
            res2.serializable.output,
            rjson!({ "ok": false, "removed": false, "message": "Team was not removed." })
        );
        assert_eq!(text_of(&res2), "Team \"alpha\" was not removed.");
    }

    // ---- ListPeers -----------------------------------------------------------

    #[tokio::test]
    async fn list_peers_requires_swarm_then_identity() {
        let tool = create_list_peers_tool();
        let ctx = base_ctx();
        let res = tool.call(rjson!({}), &ctx, None).await.unwrap();
        assert_eq!(text_of(&res), "Swarm is not enabled for this session.");

        let mut ctx2 = base_ctx();
        ctx2.swarm = Some(Arc::new(MockSwarm {
            identity: None,
            ..MockSwarm::default()
        }));
        let res2 = tool.call(rjson!({}), &ctx2, None).await.unwrap();
        assert_eq!(res2.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res2),
            "This session is not in a team yet. Call TeamCreate first."
        );
    }

    #[tokio::test]
    async fn list_peers_formats_you_and_idle() {
        let tool = create_list_peers_tool();
        let mut ctx = base_ctx();
        ctx.swarm = Some(Arc::new(MockSwarm {
            peers: vec![
                SwarmPeerInfo {
                    peer: rs("me"),
                    last_seen: 5,
                    active: true,
                },
                SwarmPeerInfo {
                    peer: rs("bob"),
                    last_seen: 3,
                    active: false,
                },
            ],
            ..MockSwarm::default()
        }));
        let res = tool.call(rjson!({}), &ctx, None).await.unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            text_of(&res),
            "Peers in team \"alpha\":\n- me (you)\n- bob (idle)"
        );
        assert_eq!(
            res.serializable.output,
            rjson!({
                "ok": true,
                "peers": [
                    { "peer": "me", "lastSeen": 5, "active": true },
                    { "peer": "bob", "lastSeen": 3, "active": false }
                ],
                "message": "2 peer(s) in team \"alpha\"."
            })
        );
    }

    #[tokio::test]
    async fn list_peers_handles_an_empty_roster() {
        let tool = create_list_peers_tool();
        let mut ctx = base_ctx();
        ctx.swarm = Some(Arc::new(MockSwarm::default()));
        let res = tool.call(rjson!({}), &ctx, None).await.unwrap();
        assert_eq!(text_of(&res), "Team \"alpha\" has no other peers yet.");
    }

    // ---- SendMessage ---------------------------------------------------------

    #[tokio::test]
    async fn unknown_recipient_is_rejected_without_requiring_a_team() {
        let tool = create_send_message_tool();
        let ctx = base_ctx();
        let res = tool
            .call(
                rjson!({ "to": "bob", "message": "hi", "summary": "Short greeting" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            text_of(&res),
            "No subagent or teammate matches the requested recipient."
        );

        let mut ctx2 = base_ctx();
        ctx2.swarm = Some(Arc::new(MockSwarm {
            identity: None,
            ..MockSwarm::default()
        }));
        let res2 = tool
            .call(
                rjson!({ "to": "bob", "message": "hi", "summary": "Short greeting" }),
                &ctx2,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            text_of(&res2),
            "No subagent or teammate matches the requested recipient."
        );
    }

    #[tokio::test]
    async fn send_message_to_a_single_peer() {
        let tool = create_send_message_tool();
        let mut ctx = base_ctx();
        ctx.swarm = Some(Arc::new(MockSwarm {
            peers: vec![SwarmPeerInfo {
                peer: rs("bob"),
                last_seen: 1,
                active: true,
            }],
            send_result: SwarmSendResult {
                recipients: vec![rs("bob")],
            },
            ..MockSwarm::default()
        }));
        let res = tool
            .call(
                rjson!({ "to": "bob", "message": "hi", "summary": "Short greeting" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable.output,
            rjson!({ "ok": true, "delivery": "queued", "recipients": ["bob"], "message": "Sent to \"bob\"." })
        );
        assert_eq!(text_of(&res), "Message sent to \"bob\".");
    }

    /// A teammate whose loop died with the parent process still has a roster
    /// entry and a persisted mailbox, so the write succeeds — but reporting a
    /// plain "sent" would let the caller believe someone is about to act on it.
    /// The result must say the recipient is not running and when the mail lands.
    #[tokio::test]
    async fn send_message_to_an_inactive_peer_says_nobody_is_reading_it() {
        let tool = create_send_message_tool();
        let mut ctx = base_ctx();
        ctx.swarm = Some(Arc::new(MockSwarm {
            peers: vec![SwarmPeerInfo {
                peer: rs("bob"),
                last_seen: 1,
                active: false,
            }],
            send_result: SwarmSendResult {
                recipients: vec![rs("bob")],
            },
            ..MockSwarm::default()
        }));
        let res = tool
            .call(
                rjson!({ "to": "bob", "message": "hi", "summary": "Short greeting" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        // The write still succeeded, so this is not an error.
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable.output,
            rjson!({ "ok": true, "delivery": "queued", "recipients": ["bob"], "message": "Sent to \"bob\" (not currently running; queued in its mailbox for the next run)." })
        );
        let text = text_of(&res).try_to_utf8().unwrap();
        assert!(
            text.contains("not currently running"),
            "the reply must not imply a live recipient: {text}"
        );
        assert!(
            text.contains("next time it runs"),
            "the reply must say when the mail is actually read: {text}"
        );
    }

    #[tokio::test]
    async fn send_message_rejects_broadcast_even_with_a_team() {
        let tool = create_send_message_tool();
        let mut ctx = base_ctx();
        ctx.swarm = Some(Arc::new(MockSwarm {
            send_result: SwarmSendResult {
                recipients: vec![rs("bob"), rs("cara")],
            },
            ..MockSwarm::default()
        }));
        let res = tool
            .call(
                rjson!({ "to": "*", "message": "team-wide", "summary": "Broadcast attempt" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            res.serializable.output,
            rjson!({ "ok": false, "message": "Broadcast is not supported; send a message per recipient." })
        );
        assert_eq!(res.serializable.is_error, Some(true));
    }

    #[tokio::test]
    async fn send_message_does_not_claim_delivery_to_an_unknown_peer() {
        let tool = create_send_message_tool();
        let mut ctx = base_ctx();
        ctx.swarm = Some(Arc::new(MockSwarm::default()));
        let res = tool
            .call(
                rjson!({ "to": "missing", "message": "hello?", "summary": "Missing recipient" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            text_of(&res),
            "No subagent or teammate matches the requested recipient."
        );
        assert_eq!(res.serializable.is_error, Some(true));
    }

    // ---- WaitForTeammates ----------------------------------------------------

    struct MessageRegistry {
        reject: bool,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl BackgroundTaskRegistry for MessageRegistry {
        fn get(&self, _id: &RuntimeString) -> Option<BackgroundTaskHandle> {
            None
        }
        fn list(&self) -> Vec<BackgroundTaskHandle> {
            Vec::new()
        }
        fn stop(&self, _id: &RuntimeString) -> bool {
            false
        }
        async fn wait_for_idle(&self) {}
        async fn send_message(
            &self,
            request: SubagentMessageRequest,
        ) -> Option<Result<SubagentMessageResult, RuntimeString>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(request.target, "agent-1");
            assert_eq!(request.message, "Continue the investigation");
            assert_eq!(request.summary, "Continue investigation");
            assert!(!request.user_initiated);
            if self.reject {
                Some(Err("The user cancelled this agent.".into()))
            } else {
                Some(Ok(SubagentMessageResult {
                    agent_id: request.target,
                    resumed: request.trigger_turn,
                }))
            }
        }
    }

    #[tokio::test]
    async fn plain_message_queues_for_an_ordinary_agent_without_a_team() {
        let mut ctx = base_ctx();
        ctx.background_tasks = Some(Arc::new(MessageRegistry {
            reject: false,
            calls: AtomicUsize::new(0),
        }));
        let result = create_send_message_tool().call(
            rjson!({"to":"agent-1","summary":"Continue investigation","message":"Continue the investigation"}),
            &ctx, None,
        ).await.unwrap();
        assert_eq!(result.serializable.is_error, Some(false));
        assert_eq!(
            result.serializable.output.get("delivery"),
            Some(&RuntimeJson::from("queued"))
        );
        assert_eq!(
            result.serializable.output.get("resumed"),
            Some(&RuntimeJson::Bool(false))
        );
        assert_eq!(
            result.serializable.output.get("recipients"),
            Some(&rjson!(["agent-1"]))
        );
    }

    #[tokio::test]
    async fn an_explicit_turn_trigger_activates_existing_work_without_a_spawner() {
        let mut ctx = base_ctx();
        ctx.background_tasks = Some(Arc::new(MessageRegistry {
            reject: false,
            calls: AtomicUsize::new(0),
        }));
        assert!(ctx.spawn_sub_agent.is_none() && ctx.spawn_background_agent.is_none());
        let tool = create_send_message_tool();
        let input = tool
            .parse_input(&rjson!({
                "to":"agent-1", "summary":"Continue investigation",
                "message":"Continue the investigation", "trigger_turn":true
            }))
            .unwrap();
        assert_eq!(input.get("trigger_turn"), Some(&RuntimeJson::Bool(true)));
        let result = tool.call(input, &ctx, None).await.unwrap();
        assert_eq!(result.serializable.is_error, Some(false));
        assert_eq!(
            result.serializable.output.get("resumed"),
            Some(&RuntimeJson::Bool(true))
        );
        for invalid in [rjson!("true"), rjson!(1), RuntimeJson::Null] {
            let mut invalid_input = rjson!({
                "to":"agent-1", "summary":"Continue investigation",
                "message":"Continue the investigation", "trigger_turn":false
            });
            if let RuntimeJson::Object(fields) = &mut invalid_input {
                fields.insert("trigger_turn", invalid);
            }
            assert!(tool.parse_input(&invalid_input).is_err());
        }
        assert!(tool
            .parse_input(&rjson!({
                "to":"agent-1", "message":{"type":"shutdown_request"}, "trigger_turn":true
            }))
            .is_err());
    }

    #[tokio::test]
    async fn rendered_message_and_followup_controls_keep_distinct_turn_activation() {
        let registry = Arc::new(MessageRegistry {
            reject: false,
            calls: AtomicUsize::new(0),
        });
        let provider = crate::types::provider::ProviderInfo {
            name: "openai".into(),
            base_url: None,
            env_key: None,
            default_model: "fake-model".into(),
        };
        for (name, expected) in [
            ("send_message", "Message queued for agent: agent-1"),
            ("followup_task", "Subagent resumed with message: agent-1"),
        ] {
            let registry = registry.clone();
            let factory = Arc::new(move || {
                let mut ctx = base_ctx();
                ctx.sandbox_mode = Some(crate::types::permission::SandboxMode::DangerFullAccess);
                ctx.approval_policy = Some(crate::types::permission::ApprovalPolicy::Never);
                ctx.background_tasks = Some(registry.clone());
                ctx
            });
            let result = super::super::task::dispatch_native_message_tool(
                create_send_message_tool(), name,
                rjson!({"target":"agent-1", "summary":"Continue investigation", "message":"Continue the investigation"}),
                factory, Some(provider.clone()),
            ).await;
            assert!(!result.is_error);
            assert!(result.content.iter().any(|block| matches!(block,
                OutputBlock::Text(text) if text.text.contains(expected)
            )));
        }
    }

    #[tokio::test]
    async fn cancelled_agent_rejection_cannot_fall_through_to_team_delivery() {
        let mut ctx = base_ctx();
        ctx.background_tasks = Some(Arc::new(MessageRegistry {
            reject: true,
            calls: AtomicUsize::new(0),
        }));
        ctx.swarm = Some(Arc::new(MockSwarm {
            peers: vec![SwarmPeerInfo {
                peer: rs("agent-1"),
                last_seen: 1,
                active: true,
            }],
            send_result: SwarmSendResult {
                recipients: vec![rs("agent-1")],
            },
            ..Default::default()
        }));
        let result = create_send_message_tool().call(
            rjson!({"to":"agent-1","summary":"Continue investigation","message":"Continue the investigation"}),
            &ctx, None,
        ).await.unwrap();
        assert_eq!(result.serializable.is_error, Some(true));
        assert_eq!(text_of(&result), "The user cancelled this agent.");
    }

    fn message_context(
        registry: Arc<MessageRegistry>,
        swarm: Arc<MockSwarm>,
        spawns: Arc<AtomicUsize>,
    ) -> ToolContext {
        let mut ctx = base_ctx();
        ctx.sandbox_mode = Some(crate::types::permission::SandboxMode::DangerFullAccess);
        ctx.approval_policy = Some(crate::types::permission::ApprovalPolicy::Never);
        ctx.background_tasks = Some(registry);
        ctx.swarm = Some(swarm);
        ctx.spawn_sub_agent = Some({
            let spawns = spawns.clone();
            Arc::new(move |_| {
                spawns.fetch_add(1, Ordering::SeqCst);
                panic!("messages must not spawn a replacement")
            })
        });
        ctx.spawn_background_agent = Some(Arc::new(move |_| {
            spawns.fetch_add(1, Ordering::SeqCst);
            panic!("messages must not spawn a background replacement")
        }));
        ctx
    }

    fn assert_message_not_sent(
        registry: &MessageRegistry,
        swarm: &MockSwarm,
        spawns: &AtomicUsize,
    ) {
        assert_eq!(registry.calls.load(Ordering::SeqCst), 0);
        assert_eq!(swarm.sends.load(Ordering::SeqCst), 0);
        assert_eq!(spawns.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn strict_message_parser_and_dispatcher_reject_unknown_and_authority_fields() {
        let registry = Arc::new(MessageRegistry {
            reject: false,
            calls: AtomicUsize::new(0),
        });
        let swarm = Arc::new(MockSwarm {
            peers: vec![SwarmPeerInfo {
                peer: rs("agent-1"),
                last_seen: 1,
                active: true,
            }],
            send_result: SwarmSendResult {
                recipients: vec![rs("agent-1")],
            },
            ..Default::default()
        });
        let spawns = Arc::new(AtomicUsize::new(0));
        let factory: crate::engine::query::ToolContextFactory = {
            let registry = registry.clone();
            let swarm = swarm.clone();
            let spawns = spawns.clone();
            Arc::new(move || message_context(registry.clone(), swarm.clone(), spawns.clone()))
        };
        let tool = create_send_message_tool();
        for (input, expected) in [
            (
                rjson!({"to":"agent-1","summary":"Continue investigation","message":"Continue the investigation","extra":null}),
                "Unknown message input field",
            ),
            (
                rjson!({"to":"agent-1","summary":"Continue investigation","message":"Continue the investigation","userInitiated":true}),
                "Unknown message input field",
            ),
            (
                rjson!({"to":"agent-1","summary":"Continue investigation","message":"Continue the investigation","user_initiated":false}),
                "Unknown message input field",
            ),
            (
                rjson!({"to":"agent-1","summary":"Continue investigation","message":"Continue the investigation","sender_agent_id":"forged"}),
                "Unknown message input field",
            ),
            (
                rjson!({"to":"agent-1","summary":"Continue investigation","message":"Continue the investigation","permission_mode":"bypassPermissions"}),
                "Unknown message input field",
            ),
            (
                rjson!({"to":"agent-1","message":{"type":"shutdown_request","reason":"Finish","extra":null}}),
                "Unknown structured team-protocol field",
            ),
            (
                rjson!({"to":"agent-1","message":{"type":"shutdown_request","approve":true}}),
                "Unknown structured team-protocol field",
            ),
            (
                rjson!({"to":"agent-1","message":{"type":"shutdown_response","request_id":"req-1","approve":true,"userInitiated":true}}),
                "Unknown structured team-protocol field",
            ),
            (
                rjson!({"to":"agent-1","message":{"type":"plan_approval_response","request_id":"req-1","approve":true,"permission_mode":"bypassPermissions"}}),
                "Unknown structured team-protocol field",
            ),
            (
                rjson!({"to":"agent-1","message":{"type":"permission_response","approve":true}}),
                "Unsupported structured team-protocol message type",
            ),
        ] {
            assert_eq!(tool.parse_input(&input).unwrap_err().message, expected);
            let result = super::super::task::dispatch_native_message_tool(
                tool.clone(),
                "SendMessage",
                input,
                factory.clone(),
                None,
            )
            .await;
            assert!(result.is_error);
            assert!(result.content.iter().any(|block| matches!(block,
                OutputBlock::Text(text) if text.text.contains(expected)
            )));
            assert_message_not_sent(&registry, &swarm, &spawns);
        }
        let input = tool.parse_input(&rjson!({
            "to":"agent-1","summary":"Continue investigation","message":"Continue the investigation"
        })).unwrap();
        let result = tool.call(input, &factory(), None).await.unwrap();
        assert_eq!(result.serializable.is_error, Some(false));
        assert_eq!(registry.calls.load(Ordering::SeqCst), 1);
        assert_eq!(swarm.sends.load(Ordering::SeqCst), 0);
        let result = super::super::task::dispatch_native_message_tool(
            tool,
            "SendMessage",
            rjson!({"to":"agent-1","message":{"type":"shutdown_request","reason":"Finish"}}),
            factory,
            None,
        )
        .await;
        assert!(!result.is_error);
        assert_eq!(registry.calls.load(Ordering::SeqCst), 1);
        assert_eq!(swarm.sends.load(Ordering::SeqCst), 1);
        assert_eq!(spawns.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn explicit_preview_cannot_deliver_blank_body_through_parser_or_dispatcher() {
        let registry = Arc::new(MessageRegistry {
            reject: false,
            calls: AtomicUsize::new(0),
        });
        let swarm = Arc::new(MockSwarm {
            peers: vec![SwarmPeerInfo {
                peer: rs("agent-1"),
                last_seen: 1,
                active: true,
            }],
            send_result: SwarmSendResult {
                recipients: vec![rs("agent-1")],
            },
            ..Default::default()
        });
        let spawns = Arc::new(AtomicUsize::new(0));
        let factory: crate::engine::query::ToolContextFactory = {
            let registry = registry.clone();
            let swarm = swarm.clone();
            let spawns = spawns.clone();
            Arc::new(move || message_context(registry.clone(), swarm.clone(), spawns.clone()))
        };
        let tool = create_send_message_tool();
        for body in ["", " \n\t", "\u{00a0}\u{3000}\u{feff}"] {
            let input = runtime_object([
                ("to", RuntimeJson::from("agent-1")),
                ("summary", RuntimeJson::from("Continue investigation")),
                ("message", RuntimeJson::from(body)),
            ]);
            let result = tool
                .call(tool.parse_input(&input).unwrap(), &factory(), None)
                .await
                .unwrap();
            assert_eq!(result.serializable.is_error, Some(true));
            assert_eq!(text_of(&result), "message must not be empty");
            let result = super::super::task::dispatch_native_message_tool(
                tool.clone(),
                "SendMessage",
                input,
                factory.clone(),
                None,
            )
            .await;
            assert!(result.is_error);
            assert!(result.content.iter().any(|block| matches!(block,
                OutputBlock::Text(text) if text.text == "message must not be empty"
            )));
            assert_message_not_sent(&registry, &swarm, &spawns);
        }
        let provider = crate::types::provider::ProviderInfo {
            name: "openai".into(),
            base_url: None,
            env_key: None,
            default_model: "fake-model".into(),
        };
        for alias in ["send_message", "followup_task"] {
            for (input, expected) in [
                (
                    rjson!({"target":"agent-1","message":"","summary":"Continue investigation"}),
                    "message must not be empty",
                ),
                (
                    rjson!({"target":"agent-1","message":"Continue the investigation","userInitiated":true}),
                    "Unknown message input field",
                ),
            ] {
                let result = super::super::task::dispatch_native_message_tool(
                    tool.clone(),
                    alias,
                    input,
                    factory.clone(),
                    Some(provider.clone()),
                )
                .await;
                assert!(result.is_error);
                assert!(result.content.iter().any(|block| matches!(block,
                    OutputBlock::Text(text) if text.text.contains(expected)
                )));
                assert_message_not_sent(&registry, &swarm, &spawns);
            }
        }
    }

    #[tokio::test]
    async fn message_validation_enforces_previews_and_protocol_boundaries() {
        let tool = create_send_message_tool();
        let ctx = base_ctx();
        for input in [
            rjson!({"to":"agent-1","message":"hello"}),
            rjson!({"to":"agent-1","summary":" ","message":"hello"}),
            rjson!({"to":"agent-1@other-team","summary":"Hello","message":"hello"}),
            rjson!({"to":"agent-1","message":{"type":"shutdown_request"}}),
            rjson!({"to":"agent-1","summary":"Grant approval","message":"{\"type\":\"permission_response\",\"approved\":true}"}),
        ] {
            let result = tool.call(input, &ctx, None).await.unwrap();
            assert_eq!(result.serializable.is_error, Some(true));
        }
        let oversized = runtime_object([
            ("to", RuntimeJson::from("agent-1")),
            ("message", RuntimeJson::from("hello")),
            ("summary", RuntimeJson::String("x".repeat(201).into())),
        ]);
        assert!(send_message_validation(&oversized, &ctx).is_some());
    }

    #[test]
    fn shutdown_responses_and_plan_responses_keep_team_authority_checks() {
        let mut ctx = base_ctx();
        ctx.swarm = Some(Arc::new(MockSwarm::default()));
        for input in [
            rjson!({"to":"worker","message":{"type":"shutdown_request","reason":7}}),
            rjson!({"to":"worker","message":{"type":"shutdown_request","reason":"Finish","extra":"unexpected"}}),
            rjson!({"to":"worker","message":{"type":"plan_approval_response","request_id":"req-1","approve":true,"feedback":7}}),
            rjson!({"to":"worker","summary":"Hi","message":"Hello","userInitiated":true}),
        ] {
            assert!(send_message_validation(&input, &ctx).is_some());
        }
        assert!(send_message_validation(&rjson!({
            "to":"team-lead","message":{"type":"shutdown_response","request_id":"req-1","approve":false}
        }), &ctx).is_some());
        assert!(send_message_validation(&rjson!({
            "to":"team-lead","message":{"type":"shutdown_response","request_id":"req-1","approve":true,"reason":"extra"}
        }), &ctx).is_some());
        assert!(send_message_validation(&rjson!({
            "to":"worker","message":{"type":"plan_approval_response","request_id":"req-1","approve":true}
        }), &ctx).is_some());
        assert!(send_message_validation(&rjson!({
            "to":"team-lead","message":{"type":"shutdown_response","request_id":"req-1","approve":"false","reason":"Still working"}
        }), &ctx).is_none());
    }

    #[tokio::test]
    async fn wait_for_teammates_reports_unavailable() {
        let tool = create_wait_for_teammates_tool();
        let ctx = base_ctx();
        let res = tool.call(rjson!({}), &ctx, None).await.unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "Teammates are not available in this context."
        );
        assert_eq!(
            res.serializable.output,
            rjson!({ "ok": false, "message": "" })
        );
    }

    #[tokio::test]
    async fn wait_for_teammates_waits_and_reports_idle() {
        let tool = create_wait_for_teammates_tool();
        let waited = StdArc::new(AtomicBool::new(false));
        let mut ctx = base_ctx();
        ctx.background_tasks = Some(Arc::new(MockBackground {
            waited: waited.clone(),
        }));
        let res = tool.call(rjson!({}), &ctx, None).await.unwrap();
        assert!(waited.load(Ordering::SeqCst));
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(text_of(&res), "All teammates are idle.");
        assert_eq!(
            res.serializable.output,
            rjson!({ "ok": true, "message": "All teammates are idle." })
        );
    }
    struct PendingInputRegistry;
    #[async_trait]
    impl BackgroundTaskRegistry for PendingInputRegistry {
        fn get(&self, _: &RuntimeString) -> Option<BackgroundTaskHandle> {
            None
        }
        fn list(&self) -> Vec<BackgroundTaskHandle> {
            Vec::new()
        }
        fn stop(&self, _: &RuntimeString) -> bool {
            false
        }
        fn has_pending_input(&self) -> bool {
            true
        }
        async fn wait_for_idle(&self) {
            futures::future::pending::<()>().await;
        }
    }

    #[tokio::test]
    async fn teammate_wait_yields_to_pending_input_without_claiming_idle() {
        let mut ctx = base_ctx();
        ctx.background_tasks = Some(Arc::new(PendingInputRegistry));
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            create_wait_for_teammates_tool().call(rjson!({}), &ctx, None),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            result.serializable.output.get("reason"),
            Some(&RuntimeJson::from("input_pending"))
        );
        assert_eq!(text_of(&result), "New input is waiting.");
    }
}
