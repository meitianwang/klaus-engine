//! Complete conversation inspection. Prompt compaction must not erase inspection
//! history, and reading must not acquire a continuation writer or run the model.

use super::wire;
use klaus_agent_core::engine::session::Session;
use klaus_agent_core::runtime_values::{
    runtime_codec, RuntimeEncode, RuntimeJson, RuntimeObject, RuntimeString,
};
use klaus_agent_core::store::session_store::ThreadReadSnapshot;
use klaus_agent_core::store::thread_history::ThreadHistory;
use std::collections::{HashMap, HashSet};

/// Read a possibly absent inspection field without changing its runtime value.
fn field<'a>(value: &'a RuntimeJson, key: &str) -> &'a RuntimeJson {
    static NULL: RuntimeJson = RuntimeJson::Null;
    value.get(key).unwrap_or(&NULL)
}

/// Compatibility readers already hold the whole source projection. Select UI
/// dependencies without changing the raw page or its cursor accounting.
pub(super) fn presentation_records(
    source: &[RuntimeJson],
    selected: &[RuntimeJson],
) -> Vec<RuntimeJson> {
    let users: HashSet<_> = selected
        .iter()
        .filter(|row| is(field(row, "type"), "user"))
        .filter_map(|row| field(row, "uuid").as_string().cloned())
        .collect();
    let mut displays = HashMap::new();
    for (ordinal, row) in source.iter().enumerate() {
        if !is(field(row, "type"), "system") || !is(field(row, "subtype"), "user_display") {
            continue;
        }
        let Some(text) = field(row, "text").as_string() else {
            continue;
        };
        let Ok(envelope) = klaus_agent_core::runtime_values::runtime_json::parse(text) else {
            continue;
        };
        let Some(target) = field(&envelope, "messageUuid").as_string() else {
            continue;
        };
        if users.contains(target) && field(&envelope, "localRender").as_object().is_some() {
            displays.insert(target.clone(), (ordinal, row.clone()));
        }
    }
    let mut rows: Vec<_> = displays.into_values().collect();
    rows.sort_by_key(|(ordinal, _)| *ordinal);
    rows.into_iter().map(|(_, row)| row).collect()
}

fn is(value: &RuntimeJson, expected: &str) -> bool {
    value.as_string().is_some_and(|value| value == expected)
}

fn text(value: &RuntimeJson) -> RuntimeString {
    value.as_string().cloned().unwrap_or_default()
}

/// Routing and discriminator strings are scalar controls; content never uses
/// this accessor. A malformed control fails the read instead of losing a row.
fn scalar(value: &RuntimeJson) -> Result<Option<String>, String> {
    value
        .as_string()
        .map(|value| value.try_to_utf8().map_err(|error| error.to_string()))
        .transpose()
}

fn set(value: &mut RuntimeJson, key: &str, entry: RuntimeJson) {
    if value.is_null() {
        *value = RuntimeJson::Object(RuntimeObject::new());
    }
    value
        .as_object_mut()
        .expect("inspection projection is an object")
        .insert(key, entry);
}

macro_rules! object {
    ($($key:literal => $value:expr),* $(,)?) => {{
        let mut object = RuntimeObject::new();
        $(object.insert($key, exact_value(&$value)?);)*
        RuntimeJson::Object(object)
    }};
}

pub(super) fn exact_value<T: RuntimeEncode + ?Sized>(source: &T) -> Result<RuntimeJson, String> {
    runtime_codec::encode_value(source).map_err(|error| error.to_string())
}

pub(super) fn file_changes_value(
    changes: klaus_agent_core::engine::session::SessionFileChanges,
) -> Result<RuntimeJson, String> {
    use klaus_agent_protocol::types::{ProtocolSessionFileChange, ProtocolSessionTurnFileChanges};
    exact_value(
        &changes
            .turns
            .into_iter()
            .map(|turn| ProtocolSessionTurnFileChanges {
                turn_id: turn.turn_id,
                files: turn
                    .files
                    .into_iter()
                    .map(|file| ProtocolSessionFileChange {
                        path: file.path,
                        kind: file.kind,
                        added: file.added,
                        removed: file.removed,
                        turn_diff: file.turn_diff,
                    })
                    .collect(),
            })
            .collect::<Vec<_>>(),
    )
}

/// Retained only while a turn runs. This fills the gap between a streaming item
/// and its durable message, without making a read wait for the turn to finish.
#[derive(Default)]
pub(super) struct LiveItems {
    turns: HashMap<String, Vec<RuntimeJson>>,
    text_buffers: HashMap<String, HashMap<RuntimeString, Vec<u16>>>,
    pub submission_turns: HashMap<String, String>,
    pub ever_started: bool,
}

impl LiveItems {
    /// Materialize immutable inspection values only when a caller takes a snapshot.
    pub fn snapshot(&self) -> HashMap<String, Vec<RuntimeJson>> {
        let mut turns = self.turns.clone();
        for (turn_id, buffers) in &self.text_buffers {
            if let Some(items) = turns.get_mut(turn_id) {
                for item in items {
                    if let Some(text) = field(item, "id").as_string().and_then(|id| buffers.get(id))
                    {
                        set(item, "text", RuntimeString::from_utf16(text.clone()).into());
                    }
                }
            }
        }
        turns
    }

    pub fn observe(&mut self, submission_id: &str, event: &RuntimeJson) -> Result<(), String> {
        let Some(inner) = event.get("event") else {
            return Ok(());
        };
        let kind = scalar(field(inner, "type"))?.unwrap_or_default();
        if kind == "turn.started" {
            if let Some(core_id) = scalar(field(inner, "turnId"))? {
                self.submission_turns.insert(submission_id.into(), core_id);
                self.ever_started = true;
            }
            return Ok(());
        }
        if kind == "tool.event" {
            let Some(tool_id) = scalar(field(inner, "toolUseId"))? else {
                return Ok(());
            };
            let domain = field(inner, "event");
            let Some(domain_type) = scalar(field(domain, "type"))? else {
                return Ok(());
            };
            let turn_id = scalar(field(inner, "turnId"))?
                .or_else(|| self.submission_turns.get(submission_id).cloned())
                .unwrap_or_else(|| submission_id.to_owned());
            // One ordered tail preserves stdout, stderr and status interleaving.
            let is_output = matches!(domain_type.as_str(), "stdout" | "stderr" | "status");
            let suffix = if domain_type == "file_change" {
                field(field(domain, "change"), "path").as_string().cloned()
            } else {
                None
            };
            let id = if is_output {
                RuntimeString::from(format!("{tool_id}-progress-output"))
            } else if let Some(suffix) = suffix.filter(|suffix| suffix != &domain_type) {
                RuntimeString::from(format!("{tool_id}-progress-{domain_type}-")).concat(&suffix)
            } else {
                RuntimeString::from(format!("{tool_id}-progress-{domain_type}"))
            };
            let items = self.turns.entry(turn_id).or_default();
            let old = items
                .iter_mut()
                .find(|item| field(item, "id").as_string() == Some(&id));
            let mut retained = domain.clone();
            let mut truncated_bytes = old
                .as_ref()
                .and_then(|item| field(item, "truncatedBytes").as_u64())
                .unwrap_or(0);
            if is_output {
                let previous = old
                    .as_ref()
                    .map(|item| text(field(field(item, "event"), "chunk")))
                    .unwrap_or_default();
                let next = text(field(
                    domain,
                    if domain_type == "status" {
                        "message"
                    } else {
                        "chunk"
                    },
                ));
                let (tail, discarded) = bounded_tool_output(&previous, &next);
                retained = object! { "type" => "stdout", "chunk" => tail };
                truncated_bytes = truncated_bytes.saturating_add(discarded as u64);
            }
            let value = object! {
                "id" => id, "type" => "tool_progress", "toolUseId" => tool_id,
                "event" => retained, "truncatedBytes" => truncated_bytes,
                "truncated" => truncated_bytes > 0,
            };
            if let Some(old) = old {
                *old = value;
            } else {
                items.push(value);
            }
            return Ok(());
        }
        if !matches!(
            kind.as_str(),
            "item.started" | "item.updated" | "item.completed" | "text.delta" | "thinking.delta"
        ) {
            return Ok(());
        }
        let turn_id = self
            .submission_turns
            .get(submission_id)
            .map(String::as_str)
            .unwrap_or(submission_id);
        let items = self.turns.entry(turn_id.to_owned()).or_default();
        if let Some(item) = inner.get("item") {
            let id = scalar(field(item, "id"))?.unwrap_or_default();
            if let Some(buffers) = self.text_buffers.get_mut(turn_id) {
                buffers.remove(&RuntimeString::from(id.as_str()));
            }
            if let Some(old) = items.iter_mut().find(|old| is(field(old, "id"), &id)) {
                *old = item.clone();
            } else {
                items.push(item.clone());
            }
            if kind == "item.completed" {
                let delta_type = if is(field(item, "type"), "agent_message") {
                    Some("stream_text")
                } else if is(field(item, "type"), "reasoning") {
                    Some("stream_reasoning")
                } else {
                    None
                };
                if let Some(delta_type) = delta_type {
                    items.retain(|row| !is(field(row, "type"), delta_type));
                    if let Some(buffers) = self.text_buffers.get_mut(turn_id) {
                        buffers.retain(|id, _| {
                            items
                                .iter()
                                .any(|item| field(item, "id").as_string() == Some(id))
                        });
                    }
                }
            }
        } else if matches!(kind.as_str(), "text.delta" | "thinking.delta") {
            let delta_type = if kind == "text.delta" {
                "stream_text"
            } else {
                "stream_reasoning"
            };
            let delta = text(field(inner, "text"));
            if let Some(message_id) = scalar(field(inner, "messageId"))? {
                if let Some(item) = items
                    .iter_mut()
                    .find(|row| is(field(row, "id"), &message_id))
                {
                    append_live_text(&mut self.text_buffers, turn_id, item, &delta);
                    if let Some(phase) = inner.get("phase").filter(|value| value.is_string()) {
                        set(item, "phase", phase.clone());
                    }
                } else {
                    let item_type = if kind == "text.delta" {
                        "agent_message"
                    } else {
                        "reasoning"
                    };
                    let mut item =
                        object! { "id" => message_id, "type" => item_type, "text" => delta };
                    if let Some(phase) = inner.get("phase").filter(|value| value.is_string()) {
                        set(&mut item, "phase", phase.clone());
                    }
                    items.push(item);
                }
            } else if let Some(item) = items
                .iter_mut()
                .find(|row| is(field(row, "type"), delta_type))
            {
                append_live_text(&mut self.text_buffers, turn_id, item, &delta);
            } else {
                items.push(object! { "id" => format!("{turn_id}-{delta_type}"), "type" => delta_type, "text" => delta });
            }
        }
        Ok(())
    }

    pub fn finish(&mut self, submission_id: &str) {
        if let Some(turn_id) = self.submission_turns.remove(submission_id) {
            self.turns.remove(&turn_id);
            self.text_buffers.remove(&turn_id);
        }
        self.turns.remove(submission_id);
        self.text_buffers.remove(submission_id);
    }
}

fn append_live_text(
    buffers: &mut HashMap<String, HashMap<RuntimeString, Vec<u16>>>,
    turn_id: &str,
    item: &RuntimeJson,
    delta: &RuntimeString,
) {
    let id = text(field(item, "id"));
    buffers
        .entry(turn_id.to_owned())
        .or_default()
        .entry(id)
        .or_insert_with(|| text(field(item, "text")).as_utf16().to_vec())
        .extend_from_slice(delta.as_utf16());
}

/// Keep the last 20,000 runtime code points without normalizing surrogate
/// boundaries. The byte counter uses the explicit UTF-8 display projection,
/// retaining the existing accounting for scalar output and a finite tail.
fn bounded_tool_output(previous: &RuntimeString, next: &RuntimeString) -> (RuntimeString, usize) {
    const LIMIT_CHARACTERS: usize = 20_000;
    let previous = previous.as_utf16();
    let next = next.as_utf16();
    let total = previous.len() + next.len();
    let unit_at = |index: usize| {
        if index < previous.len() {
            previous[index]
        } else {
            next[index - previous.len()]
        }
    };
    let mut start = total;
    for _ in 0..LIMIT_CHARACTERS {
        if start == 0 {
            break;
        }
        start -= 1;
        if (0xdc00..=0xdfff).contains(&unit_at(start))
            && start > 0
            && (0xd800..=0xdbff).contains(&unit_at(start - 1))
        {
            start -= 1;
        }
    }
    // Copy only the bounded tail, including pairs spanning the two chunks.
    let mut retained = Vec::with_capacity(total - start);
    if start < previous.len() {
        retained.extend_from_slice(&previous[start..]);
        retained.extend_from_slice(next);
    } else {
        retained.extend_from_slice(&next[start - previous.len()..]);
    }
    // Count the display bytes of the discarded prefix in place. Never allocate
    // another buffer proportional to a possibly large stdout/stderr chunk.
    let mut discarded = 0usize;
    let mut index = 0;
    while index < start {
        let unit = unit_at(index);
        let paired = (0xd800..=0xdbff).contains(&unit)
            && index + 1 < start
            && (0xdc00..=0xdfff).contains(&unit_at(index + 1));
        let bytes = if paired {
            4
        } else if unit <= 0x7f {
            1
        } else if unit <= 0x7ff {
            2
        } else {
            3
        };
        discarded = discarded.saturating_add(bytes);
        index += if paired { 2 } else { 1 };
    }
    (RuntimeString::from_utf16(retained), discarded)
}

#[derive(Default)]
struct TurnBuilder {
    id: String,
    goal_continuation: Option<bool>,
    input_origin: Option<RuntimeString>,
    goal_id: Option<RuntimeString>,
    status: Option<String>,
    error: Option<RuntimeJson>,
    error_details: Option<RuntimeJson>,
    started_at: Option<i64>,
    completed_at: Option<i64>,
    duration_ms: Option<u64>,
    messages: Vec<RuntimeJson>,
    final_message_id: Option<String>,
    tools: HashMap<String, RuntimeJson>,
    has_user: bool,
    context_owned: bool,
}

impl TurnBuilder {
    fn new(id: String, context_owned: bool) -> Self {
        Self {
            id,
            context_owned,
            ..Default::default()
        }
    }
}

fn bind_goal_submission(turns: &mut Vec<TurnBuilder>, turn_id: &str, payload: &RuntimeJson) {
    let ids: Vec<_> = ["messageUuid", "displayUuid"]
        .into_iter()
        .filter_map(|key| field(payload, key).as_string())
        .collect();
    if ids.is_empty() {
        return;
    }
    let mut bound = Vec::new();
    for turn in turns.iter_mut() {
        turn.messages.retain(|message| {
            if field(message, "uuid")
                .as_string()
                .is_some_and(|id| ids.contains(&id))
            {
                bound.push(message.clone());
                false
            } else {
                true
            }
        });
        turn.has_user = turn.messages.iter().any(is_user_message);
    }
    let target = match turns.iter().position(|turn| turn.id == turn_id) {
        Some(target) => target,
        None => {
            turns.push(TurnBuilder::new(turn_id.to_owned(), true));
            turns.len() - 1
        }
    };
    bound.append(&mut turns[target].messages);
    turns[target].messages = bound;
    turns[target].has_user = turns[target].messages.iter().any(is_user_message);
    turns.retain(|turn| {
        turn.id == turn_id
            || turn.context_owned
            || !turn.messages.is_empty()
            || !turn.tools.is_empty()
            || turn.status.is_some()
    });
}

fn is_user_message(message: &RuntimeJson) -> bool {
    is(field(message, "type"), "user") && field(message, "isMeta").as_bool() != Some(true)
}

fn current_turn(turns: &mut Vec<TurnBuilder>, fallback: String) -> &mut TurnBuilder {
    if turns.is_empty() {
        turns.push(TurnBuilder::new(fallback, false));
    }
    turns.last_mut().unwrap()
}

/// Build the complete current branch oldest-first, then page turns newest-first.
/// Every content-bearing value remains in the exact runtime representation.
pub(super) fn project(
    snapshot: ThreadReadSnapshot,
    params: &wire::EngineReadThreadParams,
    live: Option<&Session>,
    active: &HashSet<String>,
    pending_active: bool,
    flags: Vec<String>,
    live_items: &HashMap<String, Vec<RuntimeJson>>,
) -> Result<RuntimeJson, String> {
    let limit = params.limit.unwrap_or(1);
    if !(1..=10).contains(&limit) || params.thread_id.trim().is_empty() {
        return Err("threadId must be nonempty and limit must be between 1 and 10".into());
    }
    let meta = exact_value(&snapshot.meta)?;
    let mut turns: Vec<TurnBuilder> = Vec::new();
    let mut seen = HashSet::new();
    let mut external_origin: Option<RuntimeJson> = None;
    for (position, raw) in snapshot.raw_items.iter().enumerate() {
        let item = exact_value(raw)?;
        match scalar(field(&item, "kind"))?.as_deref().unwrap_or_default() {
            "turn_context" => {
                let ctx = item.get("ctx").unwrap_or(&item);
                let id = scalar(field(ctx, "turnId"))?
                    .unwrap_or_else(|| format!("{}-turn-{position}", params.thread_id));
                if turns.last().is_none_or(|turn| turn.id != id) {
                    turns.push(TurnBuilder::new(id, true));
                }
                turns.last_mut().unwrap().started_at = field(ctx, "startedAtMs").as_i64();
            }
            "message" => {
                let mut message = item.get("message").unwrap_or(&item).clone();
                let id = scalar(field(&message, "uuid"))?.unwrap_or_default();
                if let Some(origin) = external_origin
                    .take()
                    .filter(|origin| is(field(origin, "messageId"), &id))
                {
                    set(&mut message, "_externalSource", origin);
                }
                let is_user = is_user_message(&message);
                if is_user
                    && turns
                        .last()
                        .is_none_or(|turn| !turn.context_owned && turn.has_user)
                {
                    turns.push(TurnBuilder::new(id.clone(), false));
                }
                let turn = current_turn(&mut turns, id.clone());
                if !turn.context_owned && message.get("_externalSource").is_some() {
                    turn.status = Some("imported".into());
                }
                turn.has_user |= is_user;
                seen.insert(id);
                turn.messages.push(message);
            }
            "compacted" => {
                let turn =
                    current_turn(&mut turns, format!("{}-turn-{position}", params.thread_id));
                let projected = object! {
                    "id" => format!("{}-compaction-{position}", params.thread_id),
                    "type" => "contextCompaction", "summary" => field(&item, "summary"), "source" => &item,
                };
                turn.messages
                    .push(object! { "type" => "_item", "item" => projected });
            }
            "rollback" => {
                let mut count = field(&item, "numTurns").as_u64().unwrap_or_default();
                while count > 0 {
                    let Some(removed) = turns.pop() else { break };
                    for message in &removed.messages {
                        if let Some(id) = scalar(field(message, "uuid"))? {
                            seen.remove(&id);
                        }
                    }
                    if removed.has_user {
                        count -= 1;
                    }
                }
            }
            "event" => {
                let event = scalar(field(&item, "event"))?.unwrap_or_default();
                let payload = field(&item, "payload").clone();
                if matches!(
                    event.as_str(),
                    klaus_agent_core::engine::approval_capture::APPROVAL_PARENT_HISTORY_EVENT
                        | klaus_agent_core::engine::approval_capture::APPROVAL_MESSAGE_ADMITTED_EVENT
                        | klaus_agent_core::engine::approval_capture::APPROVAL_MANUAL_ACTION_EVENT
                ) {
                    continue;
                }
                if event == klaus_agent_core::store::external_import::MESSAGE_EVENT {
                    external_origin = Some(payload);
                    continue;
                }
                let turn_id = match scalar(field(&item, "turnId"))? {
                    Some(id) => Some(id),
                    None => scalar(field(&payload, "turnId"))?,
                };
                if event == "goal_submission_bound" {
                    if let Some(turn_id) = turn_id {
                        bind_goal_submission(&mut turns, &turn_id, &payload);
                    }
                    continue;
                }
                let target = turn_id
                    .as_ref()
                    .and_then(|id| turns.iter_mut().find(|turn| &turn.id == id));
                if let Some(turn) = target {
                    if event == "turn_lifecycle" {
                        turn.goal_continuation = field(&payload, "goalContinuation").as_bool();
                        turn.input_origin = field(&payload, "inputOrigin").as_string().cloned();
                        turn.goal_id = field(&payload, "goalId").as_string().cloned();
                        let details = match raw {
                            klaus_agent_core::schemas::rollout::RolloutItem::Event(event) => event.payload.as_ref()
                                .and_then(klaus_agent_core::engine::session::turn_error_details_from_lifecycle),
                            _ => None,
                        };
                        turn.error_details = details.as_ref().map(exact_value).transpose()?;
                        turn.status = scalar(field(&payload, "status"))?;
                        turn.final_message_id = scalar(field(&payload, "finalMessageId"))?;
                        turn.started_at =
                            field(&payload, "startedAtMs").as_i64().or(turn.started_at);
                        turn.completed_at = field(&payload, "finishedAtMs").as_i64();
                        turn.duration_ms = field(&payload, "durationMs").as_u64();
                        let error = field(&payload, "error").as_string().cloned().or_else(|| {
                            turn.error_details
                                .as_ref()
                                .and_then(|details| field(details, "message").as_string())
                                .cloned()
                        });
                        if let Some(message) = error {
                            let additional =
                                field(&payload, "kind").as_string().cloned().or_else(|| {
                                    turn.error_details
                                        .as_ref()
                                        .and_then(|details| field(details, "kind").as_string())
                                        .cloned()
                                });
                            turn.error = Some(
                                object! { "message" => message, "additionalDetails" => additional },
                            );
                        }
                    } else if event == "tool_lifecycle" {
                        if let Some(id) = scalar(field(&payload, "toolUseId"))? {
                            turn.tools.insert(id, payload);
                        }
                    } else if event == "tool_result_output" {
                        if let Some(message) = turn.messages.iter_mut().rev().find(|message| {
                            is(field(message, "type"), "tool_result")
                                && field(message, "toolUseId") == field(&payload, "toolUseId")
                        }) {
                            set(
                                message,
                                "_inspectionOutput",
                                field(&payload, "output").clone(),
                            );
                        }
                    } else if event == "file_change" {
                        let projected = object! {
                            "id" => format!("{}-change-{position}", turn.id), "type" => "fileChange", "status" => "completed",
                            "changes" => vec![project_file_change(&payload)?],
                        };
                        turn.messages
                            .push(object! { "type" => "_item", "item" => projected });
                    } else {
                        let projected = object! { "type" => "klausItem", "id" => format!("{}-event-{position}", turn.id), "item" => item };
                        turn.messages
                            .push(object! { "type" => "_item", "item" => projected });
                    }
                }
            }
            "subagent_lifecycle" => {
                let owner = scalar(field(
                    field(field(&item, "record"), "context"),
                    "spawnTurnId",
                ))?;
                match owner
                    .as_ref()
                    .and_then(|id| turns.iter_mut().find(|turn| &turn.id == id))
                {
                    Some(turn) => {
                        let projected = object! { "type" => "klausItem", "id" => format!("{}-activity-{position}", turn.id), "item" => item };
                        turn.messages
                            .push(object! { "type" => "_item", "item" => projected });
                    }
                    None => eprintln!(
                        "[app-server] dropping turn-scoped lifecycle row for unknown turn `{}`",
                        owner.as_deref().unwrap_or("<unstamped>")
                    ),
                }
            }
            "background_process_lifecycle" => {
                if let Some(turn) = turns.last_mut() {
                    let projected = object! { "type" => "klausItem", "id" => format!("{}-activity-{position}", turn.id), "item" => item };
                    turn.messages
                        .push(object! { "type" => "_item", "item" => projected });
                }
            }
            _ => {}
        }
    }
    if let Some(session) = live {
        let index = session.turn_index_snapshot();
        for source in session.visible_history() {
            let mut message = exact_value(&source)?;
            if let klaus_agent_core::schemas::message::Message::ToolResult(result) = &source {
                if let Some(output) = &result.output {
                    set(&mut message, "_inspectionOutput", exact_value(output)?);
                }
            }
            let id = scalar(field(&message, "uuid"))?.unwrap_or_default();
            if seen.contains(&id) {
                if !field(&message, "_inspectionOutput").is_null() {
                    if let Some(persisted) = turns
                        .iter_mut()
                        .flat_map(|turn| &mut turn.messages)
                        .find(|row| is(field(row, "uuid"), &id))
                    {
                        set(
                            persisted,
                            "_inspectionOutput",
                            field(&message, "_inspectionOutput").clone(),
                        );
                    }
                }
                continue;
            }
            if is(field(&message, "type"), "compact_boundary") {
                continue;
            }
            let turn_id = index
                .message_turn
                .get(&RuntimeString::from(id.clone()))
                .map(|id| id.try_to_utf8().map_err(|error| error.to_string()))
                .transpose()?;
            let Some(turn_id) = turn_id else { continue };
            let position = turns.iter().position(|turn| turn.id == turn_id);
            let turn = if let Some(position) = position {
                &mut turns[position]
            } else {
                turns.push(TurnBuilder::new(turn_id, true));
                turns.last_mut().unwrap()
            };
            turn.has_user |= is_user_message(&message);
            turn.messages.push(message);
            seen.insert(id);
        }
        for turn in &mut turns {
            let id = RuntimeString::from(turn.id.clone());
            if let Some(metadata) = index.turn_goal_metadata.get(&id) {
                turn.goal_continuation = Some(metadata.goal_continuation);
                turn.input_origin = Some(metadata.input_origin.clone());
                turn.goal_id = metadata.goal_id.clone();
            }
            if let Some(status) = index.turn_status.get(&id) {
                turn.status = Some(status.try_to_utf8().map_err(|error| error.to_string())?);
            }
            turn.final_message_id = index
                .turn_final_message
                .get(&id)
                .map(|id| id.try_to_utf8().map_err(|error| error.to_string()))
                .transpose()?
                .or(turn.final_message_id.clone());
            turn.started_at = index
                .turn_started_at_ms
                .get(&id)
                .copied()
                .or(turn.started_at);
            turn.completed_at = index
                .turn_finished_at_ms
                .get(&id)
                .copied()
                .or(turn.completed_at);
            turn.duration_ms = index
                .turn_duration_ms
                .get(&id)
                .copied()
                .or(turn.duration_ms);
            if let Some(error) = index.turn_errors.get(&id) {
                turn.error = Some(
                    object! { "message" => error, "additionalDetails" => index.turn_error_kinds.get(&id) },
                );
            }
            if let Some(details) = index
                .turn_error_details
                .get(&id)
                .and_then(klaus_agent_core::engine::session::sanitize_turn_error_details)
            {
                turn.error_details = Some(exact_value(&details)?);
            } else if index
                .turn_status
                .get(&id)
                .is_some_and(|status| status != "failed")
            {
                turn.error_details = None;
            }
        }
    }
    for id in active {
        if !turns.iter().any(|turn| &turn.id == id) {
            turns.push(TurnBuilder::new(id.clone(), true));
        }
    }
    let preview = turns
        .iter()
        .flat_map(|turn| &turn.messages)
        .find(|message| is_user_message(message))
        .map(message_text)
        .unwrap_or_default();
    let cwd = text(field(&meta, "cwd"));
    let mut status = object! { "type" => if pending_active || !active.is_empty() { "active" } else if live.is_some() { "idle" } else { "notLoaded" } };
    if !flags.is_empty() {
        set(&mut status, "activeFlags", exact_value(&flags)?);
    }
    let info = object! {
        "id" => &params.thread_id, "title" => text(field(&meta, "title")), "preview" => preview,
        "cwd" => &cwd, "createdAt" => field(&meta, "createdAt").as_i64().unwrap_or_default(),
        "updatedAt" => snapshot.updated_at, "archived" => snapshot.archived, "status" => status,
    };
    let end = match params.cursor.as_deref() {
        Some(cursor) => turns
            .iter()
            .position(|turn| turn.id == cursor)
            .ok_or_else(|| format!("Unknown cursor for thread {}: {cursor}", params.thread_id))?,
        None => turns.len(),
    };
    let start = end.saturating_sub(limit);
    let has_more = start > 0;
    let next_cursor = has_more.then(|| turns[start].id.clone());
    let mut output = Vec::new();
    let mut error_details = std::collections::BTreeMap::new();
    for turn in turns.drain(start..end).rev() {
        let is_active = active.contains(&turn.id);
        let status = if is_active {
            "inProgress".to_owned()
        } else {
            turn.status.clone().unwrap_or_else(|| "interrupted".into())
        };
        let mut items = project_messages(
            &turn.messages,
            &turn.tools,
            &cwd,
            &status,
            turn.final_message_id.as_deref(),
        )?;
        if is_active {
            if let Some(streamed) = live_items.get(&turn.id) {
                for item in streamed {
                    let projected = project_live_item(item, &cwd)?;
                    let id = field(&projected, "id");
                    if let Some(persisted) = items.iter_mut().find(|item| field(item, "id") == id) {
                        if is(field(persisted, "type"), "commandExecution")
                            && is(field(persisted, "status"), "inProgress")
                        {
                            for key in ["aggregatedOutput", "exitCode", "durationMs", "status"] {
                                if !field(&projected, key).is_null() {
                                    set(persisted, key, field(&projected, key).clone());
                                }
                            }
                        }
                    } else {
                        items.push(projected);
                    }
                }
            }
        }
        if status == "failed" {
            if let Some(details) = turn.error_details {
                error_details.insert(turn.id.clone(), details);
            }
        }
        let mut projected = object! { "id" => &turn.id };
        if let Some(value) = turn.goal_continuation {
            set(&mut projected, "goalContinuation", value.into());
        }
        if let Some(value) = turn.input_origin {
            set(&mut projected, "inputOrigin", value.into());
        }
        if let Some(value) = turn.goal_id {
            set(&mut projected, "goalId", value.into());
        }
        set(&mut projected, "status", status.into());
        set(&mut projected, "error", exact_value(&turn.error)?);
        set(&mut projected, "startedAt", exact_value(&turn.started_at)?);
        set(
            &mut projected,
            "completedAt",
            exact_value(&turn.completed_at)?,
        );
        set(
            &mut projected,
            "durationMs",
            exact_value(&turn.duration_ms)?,
        );
        set(&mut projected, "items", items.into());
        output.push(projected);
    }
    let page = object! { "order" => "newest_first", "limit" => limit, "nextCursor" => next_cursor, "hasMore" => has_more };
    let mut reply = object! { "thread" => info, "page" => page, "turns" => output };
    if !error_details.is_empty() {
        let mut details = RuntimeObject::new();
        for (id, value) in error_details {
            details.insert(id, value);
        }
        set(&mut reply, "turnErrorDetails", details.into());
    }
    Ok(reply)
}

fn message_text(message: &RuntimeJson) -> RuntimeString {
    let mut joined = RuntimeString::default();
    let mut first = true;
    for block in field(message, "content").as_array().into_iter().flatten() {
        if is(field(block, "type"), "text") {
            if let Some(text) = field(block, "text").as_string() {
                if !first {
                    joined = joined.concat(&RuntimeString::from("\n"));
                }
                joined = joined.concat(text);
                first = false;
            }
        }
    }
    joined
}

fn project_messages(
    messages: &[RuntimeJson],
    tools: &HashMap<String, RuntimeJson>,
    cwd: &RuntimeString,
    turn_status: &str,
    final_message_id: Option<&str>,
) -> Result<Vec<RuntimeJson>, String> {
    let mut output = Vec::new();
    let mut calls = HashMap::new();
    let mut results = HashMap::new();
    for message in messages
        .iter()
        .filter(|message| is(field(message, "type"), "tool_result"))
    {
        if let Some(id) = scalar(field(message, "toolUseId"))? {
            results.insert(id, message);
        }
    }
    for message in messages {
        let start = output.len();
        let id = scalar(field(message, "uuid"))?.unwrap_or_default();
        match scalar(field(message, "type"))?.as_deref().unwrap_or_default() {
            "user" if is_user_message(message) => {
                let mut content = Vec::new();
                for block in field(message, "content").as_array().into_iter().flatten() {
                    if is(field(block, "type"), "image") {
                        if let (Some(mime), Some(base64)) = (field(block, "mimeType").as_string(), field(block, "base64").as_string()) {
                            let url = RuntimeString::from("data:").concat(mime).concat(&RuntimeString::from(";base64,")).concat(base64);
                            content.push(object! { "type" => "image", "url" => url });
                        } else { content.push(block.clone()); }
                    } else { content.push(block.clone()); }
                }
                let mut user = object! { "type" => "userMessage", "id" => &id, "content" => content };
                if let Some(answer) = message.get("questionAnswer").filter(|value| !value.is_null()) { set(&mut user, "questionAnswer", answer.clone()); }
                output.push(user);
            }
            "assistant" => {
                let mut first_text = true;
                for (position, block) in field(message, "content").as_array().into_iter().flatten().enumerate() {
                    let block_id = format!("{id}-{position}");
                    match scalar(field(block, "type"))?.as_deref().unwrap_or_default() {
                        "text" => {
                            let message_id = scalar(field(block, "messageId"))?.unwrap_or_else(|| if first_text { id.clone() } else { block_id.clone() });
                            let phase = field(block, "phase").as_string().cloned().unwrap_or_else(|| RuntimeString::from(
                                if final_message_id == Some(id.as_str()) || final_message_id == Some(message_id.as_str()) { "final_answer" } else { "commentary" }));
                            output.push(object! { "type" => "agentMessage", "id" => message_id, "text" => field(block, "text"), "phase" => phase });
                            first_text = false;
                        }
                        "thinking" => output.push(object! {
                            "type" => "reasoning", "id" => scalar(field(block, "messageId"))?.unwrap_or(block_id),
                            "summary" => Vec::<RuntimeJson>::new(), "content" => vec![field(block, "text").clone()],
                        }),
                        "tool_use" => {
                            let tool_id = scalar(field(block, "id"))?.unwrap_or_default();
                            let name = text(field(block, "name"));
                            calls.insert(tool_id.clone(), name.clone());
                            output.push(project_call(&tool_id, &name, field(block, "input"), results.get(&tool_id).copied(), tools.get(&tool_id), cwd, turn_status)?);
                        }
                        "web_search_call" => output.push(object! {
                            "type" => "webSearch", "id" => field(block, "id"), "query" => field(block, "query"),
                            "action" => field(block, "action"), "results" => field(block, "results"),
                        }),
                        _ => output.push(object! { "type" => "klausContentBlock", "id" => block_id, "block" => block }),
                    }
                }
            }
            "tool_result" => {
                let tool_id = scalar(field(message, "toolUseId"))?.unwrap_or_default();
                output.push(object! {
                    "type" => "functionCallOutput", "id" => id, "callId" => &tool_id, "name" => calls.get(&tool_id),
                    "namespace" => RuntimeJson::Null, "output" => result_output(message)?, "content" => field(message, "content"),
                    "extraUserBlocks" => field(message, "extraUserBlocks"), "isError" => field(message, "isError"),
                });
            }
            "_item" => output.push(field(message, "item").clone()),
            "compact_boundary" => output.push(object! { "type" => "contextCompaction", "id" => id, "summary" => field(message, "summary") }),
            _ => output.push(object! { "type" => "klausMessage", "id" => id, "message" => message }),
        }
        if let Some(origin) = message.get("_externalSource") {
            for item in &mut output[start..] {
                set(item, "externalSource", origin.clone());
            }
        }
    }
    Ok(output)
}

fn result_output(message: &RuntimeJson) -> Result<RuntimeJson, String> {
    if !field(message, "_inspectionOutput").is_null()
        || !field(message, "nativeToolOutput").is_null()
        || !field(message, "extraUserBlocks").is_null()
    {
        return Ok(object! {
            "structuredOutput" => field(message, "_inspectionOutput"), "nativeToolOutput" => field(message, "nativeToolOutput"),
            "content" => field(message, "content"), "extraUserBlocks" => field(message, "extraUserBlocks"),
        });
    }
    let blocks = field(message, "content").as_array();
    if blocks.is_some_and(|blocks| blocks.iter().all(|block| is(field(block, "type"), "text"))) {
        return Ok(message_text(message).into());
    }
    Ok(field(message, "content").clone())
}

fn project_call(
    id: &str,
    name: &RuntimeString,
    args: &RuntimeJson,
    result: Option<&RuntimeJson>,
    lifecycle: Option<&RuntimeJson>,
    cwd: &RuntimeString,
    turn_status: &str,
) -> Result<RuntimeJson, String> {
    let success = result.map(|result| field(result, "isError").as_bool() != Some(true));
    let phase = lifecycle.map(|row| field(row, "phase"));
    let status = if phase.is_some_and(|phase| is(phase, "cancelled")) {
        "interrupted"
    } else if success == Some(false) {
        "failed"
    } else if success == Some(true) {
        "completed"
    } else if turn_status == "inProgress" {
        "inProgress"
    } else {
        "interrupted"
    };
    let scalar_name = name.try_to_utf8().ok();
    let lower = scalar_name.as_ref().map(|name| name.to_lowercase());
    if matches!(
        lower.as_deref(),
        Some("bash" | "exec_command" | "shell" | "shell_command")
    ) {
        let result_value = match result {
            Some(message) if !field(message, "_inspectionOutput").is_null() => {
                field(message, "_inspectionOutput").clone()
            }
            Some(message) => result_output(message)?,
            None => RuntimeJson::Null,
        };
        let parsed = result_value
            .as_string()
            .and_then(|text| klaus_agent_core::runtime_values::runtime_json::parse(text).ok())
            .unwrap_or_else(|| result_value.clone());
        let command = args
            .get("command")
            .or_else(|| args.get("cmd"))
            .cloned()
            .unwrap_or_default();
        let text_output = result.map(message_text).unwrap_or_default();
        let output = if !text_output.is_empty() {
            text_output
        } else if let Some(text) = result_value.as_string() {
            text.clone()
        } else if result_value.is_null() {
            RuntimeString::default()
        } else {
            klaus_agent_core::runtime_values::runtime_json::stringify(&result_value)
        };
        Ok(object! {
            "type" => "commandExecution", "id" => id, "command" => command, "arguments" => args,
            "cwd" => args.get("workdir").or_else(|| args.get("cwd")).cloned().unwrap_or_else(|| cwd.clone().into()),
            "status" => status, "exitCode" => parsed.get("exit_code").or_else(|| parsed.get("exitCode")),
            "durationMs" => field(&parsed, "durationMs"), "aggregatedOutput" => output,
        })
    } else if let Some((server, tool)) = scalar_name
        .as_deref()
        .and_then(|name| name.strip_prefix("mcp__"))
        .and_then(|name| name.split_once("__"))
    {
        Ok(
            object! { "type" => "mcpToolCall", "id" => id, "server" => server, "tool" => tool, "arguments" => args, "status" => status, "durationMs" => RuntimeJson::Null },
        )
    } else {
        Ok(
            object! { "type" => "dynamicToolCall", "id" => id, "tool" => name, "arguments" => args, "status" => status, "success" => success, "durationMs" => RuntimeJson::Null },
        )
    }
}

fn project_file_change(change: &RuntimeJson) -> Result<RuntimeJson, String> {
    let mut change = change.clone();
    let kind = if is(field(&change, "kind"), "create") {
        object! { "type" => "add" }
    } else if is(field(&change, "kind"), "modify") {
        object! { "type" => "update", "move_path" => RuntimeJson::Null }
    } else if is(field(&change, "kind"), "delete") {
        object! { "type" => "delete" }
    } else {
        field(&change, "kind").clone()
    };
    set(&mut change, "kind", kind);
    Ok(change)
}

fn project_live_item(item: &RuntimeJson, cwd: &RuntimeString) -> Result<RuntimeJson, String> {
    let id = text(field(item, "id"));
    match scalar(field(item, "type"))?.as_deref().unwrap_or_default() {
        "agent_message" | "stream_text" => {
            let mut projected =
                object! { "type" => "agentMessage", "id" => id, "text" => field(item, "text") };
            if let Some(phase) = item.get("phase").filter(|phase| phase.is_string()) {
                set(&mut projected, "phase", phase.clone());
            }
            Ok(projected)
        }
        "reasoning" | "stream_reasoning" => Ok(
            object! { "type" => "reasoning", "id" => id, "summary" => Vec::<RuntimeJson>::new(), "content" => vec![field(item, "text").clone()] },
        ),
        "command_execution" => {
            let mut item = item.clone();
            set(&mut item, "type", "commandExecution".into());
            set(&mut item, "cwd", cwd.clone().into());
            if is(field(&item, "status"), "in_progress") {
                set(&mut item, "status", "inProgress".into());
            }
            Ok(item)
        }
        "file_change" => {
            let mut item = item.clone();
            let changes = field(&item, "changes")
                .as_array()
                .into_iter()
                .flatten()
                .map(project_file_change)
                .collect::<Result<Vec<_>, _>>()?;
            set(&mut item, "type", "fileChange".into());
            set(&mut item, "changes", changes.into());
            Ok(item)
        }
        "tool_call" => {
            let result = object! { "isError" => is(field(item, "status"), "failed"), "_inspectionOutput" => field(item, "result"), "content" => Vec::<RuntimeJson>::new() };
            let has_result =
                is(field(item, "status"), "completed") || is(field(item, "status"), "failed");
            let scalar_id = id.try_to_utf8().map_err(|error| error.to_string())?;
            let mut projected = project_call(
                &scalar_id,
                &text(field(item, "toolName")),
                field(item, "arguments"),
                has_result.then_some(&result),
                None,
                cwd,
                "inProgress",
            )?;
            set(&mut projected, "error", field(item, "error").clone());
            Ok(projected)
        }
        _ => Ok(object! { "type" => "klausItem", "id" => id, "item" => item }),
    }
}

/// Page the exact history encoding by durable turn membership before applying
/// item views or substituting display-only image references.
/// The paging window a history page request asks for.
pub(super) struct PageWindow<'a> {
    pub cursor: Option<&'a str>,
    pub limit: Option<usize>,
    pub items_view: Option<&'a str>,
    pub sort_direction: Option<&'a str>,
    pub whole: bool,
}

pub(super) fn project_history_page(
    rows: Vec<RuntimeJson>,
    history: &ThreadHistory,
    thread_id: &str,
    window: PageWindow<'_>,
) -> Result<(wire::ThreadHistoryPage, Vec<RuntimeJson>, Vec<RuntimeJson>), String> {
    let PageWindow {
        cursor,
        limit,
        items_view,
        sort_direction,
        whole,
    } = window;
    let limit = if whole {
        usize::MAX
    } else {
        limit.unwrap_or(25).clamp(1, 100)
    };
    let view = items_view.unwrap_or("full");
    if !matches!(view, "full" | "summary" | "notLoaded") {
        return Err(format!("unknown itemsView: {view}"));
    }
    let sort = sort_direction.unwrap_or("desc");
    if !matches!(sort, "desc" | "asc") {
        return Err(format!("unknown sortDirection: {sort}"));
    }
    let mut groups = group_turn_rows(rows, history, thread_id)?;
    let position = |cursor: &str| -> Result<usize, String> {
        groups
            .iter()
            .position(|(id, _)| id == cursor)
            .ok_or_else(|| format!("Unknown cursor for thread {thread_id}: {cursor}"))
    };
    // `desc` anchors the page at the newest end (the cursor names the turn the
    // page ENDS before), `asc` at the oldest end (the cursor names the turn the
    // page STARTS at); both refuse an unknown cursor instead of re-reading.
    let (start, end, order) = match (sort, cursor) {
        ("asc", Some(cursor)) => {
            let start = position(cursor)?;
            (
                start,
                start.saturating_add(limit).min(groups.len()),
                "oldest_first",
            )
        }
        ("asc", None) => (0, limit.min(groups.len()), "oldest_first"),
        (_, Some(cursor)) => {
            let end = position(cursor)?;
            (end.saturating_sub(limit), end, "newest_first")
        }
        (_, None) => {
            let end = groups.len();
            (end.saturating_sub(limit), end, "newest_first")
        }
    };
    let has_more = if sort == "asc" {
        end < groups.len()
    } else {
        start > 0
    };
    let next_cursor = has_more.then(|| groups[if sort == "asc" { end } else { start }].0.clone());
    let backwards_cursor = if sort == "asc" {
        (start > 0).then(|| groups[start - 1].0.clone())
    } else {
        (end < groups.len()).then(|| groups[end].0.clone())
    };
    // Turn shells travel with every view, `notLoaded` included: a client that
    // asked for turn membership only still needs each turn's identity and
    // lifecycle metadata to draw the turn before its messages arrive.
    let shells = groups[start..end]
        .iter()
        .map(|(id, _)| turn_shell(history, id))
        .collect::<Result<Vec<_>, String>>()?;
    let page = wire::ThreadHistoryPage {
        order: order.into(),
        limit,
        next_cursor,
        has_more,
        backwards_cursor,
    };
    let window: Vec<(String, Vec<RuntimeJson>)> = groups.drain(start..end).collect();
    let projected = apply_view_rows(window, view)?;
    Ok((page, projected, shells))
}

/// Group flat history rows into their turns, oldest-first, attributing each
/// row through the turn index and falling back to the preceding group (or the
/// synthetic preamble group) for rows the index does not carry.
fn group_turn_rows(
    rows: Vec<RuntimeJson>,
    history: &ThreadHistory,
    thread_id: &str,
) -> Result<Vec<(String, Vec<RuntimeJson>)>, String> {
    let mut groups: Vec<(String, Vec<RuntimeJson>)> = Vec::new();
    for row in rows {
        let uuid = scalar(field(&row, "uuid"))?;
        let attributed = uuid.as_ref().and_then(|uuid| {
            history
                .turn_index
                .message_turn
                .get(&RuntimeString::from(uuid.clone()))
        });
        let turn = attributed
            .map(|turn| turn.try_to_utf8().map_err(|error| error.to_string()))
            .transpose()?
            .or_else(|| groups.last().map(|(id, _)| id.clone()))
            .unwrap_or_else(|| format!("{thread_id}-preamble"));
        match groups.last_mut() {
            Some((last, rows)) if *last == turn => rows.push(row),
            _ => groups.push((turn, vec![row])),
        }
    }
    Ok(groups)
}

/// Apply an items view to whole turn groups and run the display-only image
/// substitution over every served row.
fn apply_view_rows(
    groups: Vec<(String, Vec<RuntimeJson>)>,
    view: &str,
) -> Result<Vec<RuntimeJson>, String> {
    let mut projected = Vec::new();
    for (_, turn_rows) in groups {
        let rows = match view {
            "notLoaded" => Vec::new(),
            "summary" => {
                let first_user = turn_rows.iter().position(is_user_message);
                let last_assistant = turn_rows
                    .iter()
                    .rposition(|row| is(field(row, "type"), "assistant"));
                match (first_user, last_assistant) {
                    (Some(user), Some(assistant)) if user != assistant => {
                        vec![turn_rows[user].clone(), turn_rows[assistant].clone()]
                    }
                    (Some(user), _) => vec![turn_rows[user].clone()],
                    (None, Some(assistant)) => vec![turn_rows[assistant].clone()],
                    (None, None) => Vec::new(),
                }
            }
            _ => turn_rows,
        };
        for mut row in rows {
            substitute_image_sources(&mut row)?;
            projected.push(row);
        }
    }
    Ok(projected)
}

/// Apply an items view to the rows of an index-served rollout slice: the slice
/// already covers exactly the served turns, so grouping and the view remain.
pub(super) fn project_indexed_rows(
    history: &ThreadHistory,
    thread_id: &str,
    rows: Vec<RuntimeJson>,
    view: &str,
) -> Result<Vec<RuntimeJson>, String> {
    let groups = group_turn_rows(rows, history, thread_id)?;
    apply_view_rows(groups, view)
}

/// Display-only substitution of an image block's inline bytes for the source
/// file it was saved from, when that file still exists on disk.
fn substitute_image_sources(row: &mut RuntimeJson) -> Result<(), String> {
    klaus_agent_core::store::thread_history::substitute_image_sources(row);
    Ok(())
}

/// One turn's identity and lifecycle metadata, without any of its messages.
///
/// A paged history read carries these shells beside its rows so a client that
/// asked for turn membership only (`itemsView: notLoaded`) can still draw every
/// turn of the page, and know each turn's status and timing before its items
/// arrive through a separate items read.
fn turn_shell(history: &ThreadHistory, turn_id: &str) -> Result<RuntimeJson, String> {
    let index = &history.turn_index;
    let key = RuntimeString::from(turn_id.to_string());
    let mut shell = object! { "id" => turn_id };
    if let Some(value) = index.turn_status.get(&key) {
        set(&mut shell, "status", exact_value(value)?);
    }
    if let Some(value) = index.turn_started_at_ms.get(&key) {
        set(&mut shell, "startedAtMs", exact_value(value)?);
    }
    if let Some(value) = index.turn_finished_at_ms.get(&key) {
        set(&mut shell, "finishedAtMs", exact_value(value)?);
    }
    if let Some(value) = index.turn_duration_ms.get(&key) {
        set(&mut shell, "durationMs", exact_value(value)?);
    }
    if let Some(value) = index.turn_errors.get(&key) {
        set(&mut shell, "error", exact_value(value)?);
    }
    if let Some(value) = index.turn_error_kinds.get(&key) {
        set(&mut shell, "errorKind", exact_value(value)?);
    }
    if let Some(value) = index.turn_error_details.get(&key) {
        set(&mut shell, "errorDetails", value.clone());
    }
    if let Some(value) = index.turn_cancelled_tools.get(&key) {
        let tools = value
            .iter()
            .map(exact_value)
            .collect::<Result<Vec<_>, String>>()?;
        set(&mut shell, "cancelledTools", exact_value(&tools)?);
    }
    if let Some(value) = index.turn_final_message.get(&key) {
        set(&mut shell, "finalMessageId", exact_value(value)?);
    }
    if let Some(value) = index.turn_goal_metadata.get(&key) {
        set(&mut shell, "goalMetadata", exact_value(value)?);
    }
    Ok(shell)
}

/// Upper bound on the decoded payload of one items page.
///
/// A page above it is refused with the error the client's halving retry
/// listens for, so an oversized turn degrades into smaller pages (or, for a
/// single row above the bound, into its turn shell) instead of pushing one
/// unbounded payload through the engine, the host bridge and the renderer.
/// Whole-read paths stay outside this guard on purpose: exports and mobile
/// keeps serving rows of any size.
pub(super) const MAX_DECODED_ITEMS_PAGE_BYTES: usize = 16 * 1024 * 1024;

pub(super) fn guard_presentation_page(
    rows: &[RuntimeJson],
    records: &[RuntimeJson],
) -> Result<(), String> {
    let decoded = projected_byte_len(rows).saturating_add(projected_byte_len(records));
    if decoded > MAX_DECODED_ITEMS_PAGE_BYTES {
        return Err(format!(
            "decoded message length too large: found {decoded} bytes"
        ));
    }
    Ok(())
}

/// The store already selected this window, so only display projection and the
/// decoded payload guard remain. Reapplying cursors here would page twice.
pub(super) fn project_indexed_items_rows(
    mut rows: Vec<RuntimeJson>,
) -> Result<Vec<RuntimeJson>, String> {
    for row in &mut rows {
        substitute_image_sources(row)?;
    }
    let decoded = projected_byte_len(&rows);
    if decoded > MAX_DECODED_ITEMS_PAGE_BYTES {
        return Err(format!(
            "decoded message length too large: found {decoded} bytes"
        ));
    }
    Ok(rows)
}

fn projected_byte_len(rows: &[RuntimeJson]) -> usize {
    rows.iter()
        .map(|row| {
            klaus_agent_core::runtime_values::runtime_json::stringify(row)
                .try_to_utf8()
                .ok()
                .map(|text| text.len())
                .unwrap_or(0)
        })
        .sum()
}

/// Page the messages of ONE turn by message cursor, carrying no other turn's
/// rows. `cursor` names the message uuid the page starts at (exclusive) in the
/// requested direction; an unknown cursor is refused.
pub(super) fn project_turn_items_page(
    rows: Vec<RuntimeJson>,
    history: &ThreadHistory,
    thread_id: &str,
    turn_id: &str,
    cursor: Option<&str>,
    limit: Option<usize>,
    sort_direction: Option<&str>,
) -> Result<(wire::ThreadHistoryPage, Vec<RuntimeJson>), String> {
    let limit = limit.unwrap_or(100).clamp(1, 100);
    let sort = sort_direction.unwrap_or("desc");
    if !matches!(sort, "desc" | "asc") {
        return Err(format!("unknown sortDirection: {sort}"));
    }
    let key = RuntimeString::from(turn_id.to_string());
    let uuid_of =
        |row: &RuntimeJson| -> Result<Option<String>, String> { scalar(field(row, "uuid")) };
    let mut turn_rows: Vec<RuntimeJson> = Vec::new();
    for row in rows {
        let uuid = uuid_of(&row)?;
        let attributed = uuid.as_ref().and_then(|uuid| {
            history
                .turn_index
                .message_turn
                .get(&RuntimeString::from(uuid.clone()))
        });
        if attributed.is_some_and(|turn| *turn == key) {
            turn_rows.push(row);
        }
    }
    let len = turn_rows.len();
    let position = |cursor: &str| -> Result<usize, String> {
        turn_rows
            .iter()
            .position(|row| uuid_of(row).ok().flatten().as_deref() == Some(cursor))
            .ok_or_else(|| format!("Unknown cursor for thread {thread_id}: {cursor}"))
    };
    let (start, end, order) = match (sort, cursor) {
        ("asc", Some(cursor)) => {
            let start = position(cursor)?;
            (start, start.saturating_add(limit).min(len), "oldest_first")
        }
        ("asc", None) => (0, limit.min(len), "oldest_first"),
        (_, Some(cursor)) => {
            let end = position(cursor)?;
            (end.saturating_sub(limit), end, "newest_first")
        }
        (_, None) => (len.saturating_sub(limit), len, "newest_first"),
    };
    let has_more = if sort == "asc" { end < len } else { start > 0 };
    let page_row = |index: usize| -> Result<String, String> {
        uuid_of(&turn_rows[index])?
            .ok_or_else(|| format!("paged row without uuid in thread {thread_id}"))
    };
    let next_cursor = if has_more {
        Some(page_row(if sort == "asc" { end } else { start })?)
    } else {
        None
    };
    let backwards_cursor = if sort == "asc" {
        (start > 0).then(|| page_row(start - 1)).transpose()?
    } else {
        (end < len).then(|| page_row(end)).transpose()?
    };
    let page = wire::ThreadHistoryPage {
        order: order.into(),
        limit,
        next_cursor,
        has_more,
        backwards_cursor,
    };
    let mut projected = Vec::new();
    for mut row in turn_rows.drain(start..end) {
        substitute_image_sources(&mut row)?;
        projected.push(row);
    }
    let decoded = projected_byte_len(&projected);
    if decoded > MAX_DECODED_ITEMS_PAGE_BYTES {
        return Err(format!(
            "decoded message length too large: found {decoded} bytes"
        ));
    }
    Ok((page, projected))
}

#[cfg(test)]
mod live_output_tests {
    use super::*;
    use serde_json::{json, Value};

    fn runtime(value: Value) -> RuntimeJson {
        let source = serde_json::to_string(&value).unwrap();
        klaus_agent_core::runtime_values::runtime_json::parse(&source.into()).unwrap()
    }

    #[test]
    fn presentation_records_keep_only_selected_users_and_the_last_valid_envelope() {
        let display = |id: &str, target: &str| {
            runtime(json!({
                "type":"system","uuid":id,"subtype":"user_display",
                "text":json!({"messageUuid":target,"localRender":{"displayText":id}}).to_string()
            }))
        };
        let selected = vec![runtime(json!({"type":"user","uuid":"user","content":[]}))];
        let source = vec![
            display("before", "user"),
            display("unrelated", "other"),
            display("after", "user"),
            runtime(json!({
                "type":"system","uuid":"invalid","subtype":"user_display","text":"invalid JSON"
            })),
        ];
        let dependencies = presentation_records(&source, &selected);
        assert_eq!(dependencies.len(), 1);
        assert!(is(field(&dependencies[0], "uuid"), "after"));
        assert_eq!(
            selected.len(),
            1,
            "sidecars never append a raw history item"
        );
    }

    #[test]
    fn items_decoded_guard_counts_presentation_records_together_with_raw_rows() {
        let rows = vec![runtime(
            json!({"type":"user","uuid":"u","text":"x".repeat(8*1024*1024)}),
        )];
        let records = vec![runtime(
            json!({"type":"system","uuid":"d","text":"x".repeat(8*1024*1024)}),
        )];
        assert!(projected_byte_len(&rows) < MAX_DECODED_ITEMS_PAGE_BYTES);
        assert!(projected_byte_len(&records) < MAX_DECODED_ITEMS_PAGE_BYTES);
        assert!(guard_presentation_page(&rows, &records)
            .unwrap_err()
            .contains("decoded message length too large"));
    }

    #[test]
    fn an_oversized_items_page_is_refused_so_the_client_can_halve() {
        let mut turn_index = klaus_agent_core::engine::session::TurnIndex::default();
        turn_index
            .message_turn
            .insert(RuntimeString::from("big"), RuntimeString::from("t1"));
        turn_index
            .message_turn
            .insert(RuntimeString::from("small"), RuntimeString::from("t1"));
        let history = klaus_agent_core::store::thread_history::ThreadHistory {
            history: Vec::new(),
            turn_index,
            tool_outputs: Default::default(),
            goal_completions: Vec::new(),
        };
        let big = runtime(json!({
            "uuid": "big",
            "type": "user",
            "content": [{ "type": "text", "text": "x".repeat(MAX_DECODED_ITEMS_PAGE_BYTES + 1024) }]
        }));
        let small = runtime(
            json!({ "uuid": "small", "type": "user", "content": [{ "type": "text", "text": "s" }] }),
        );
        let error = project_turn_items_page(
            vec![big, small.clone()],
            &history,
            "t",
            "t1",
            None,
            Some(100),
            None,
        )
        .unwrap_err();
        assert!(
            error.contains("decoded message length too large"),
            "{error}"
        );
        let (page, rows) =
            project_turn_items_page(vec![small], &history, "t", "t1", None, Some(100), None)
                .unwrap();
        assert_eq!(rows.len(), 1);
        assert!(!page.has_more);
    }

    #[test]
    fn live_output_preserves_stdout_stderr_status_interleaving() {
        let mut items = LiveItems::default();
        for domain in [
            json!({"type":"stdout","chunk":"A"}),
            json!({"type":"stderr","chunk":"B"}),
            json!({"type":"stdout","chunk":"C"}),
            json!({"type":"status","message":"D"}),
        ] {
            items
                .observe(
                    "submission",
                    &runtime(json!({"event":{
                        "type":"tool.event","turnId":"turn","toolUseId":"tool","event":domain
                    }})),
                )
                .unwrap();
        }
        assert_eq!(items.turns["turn"].len(), 1);
        assert_eq!(
            items.turns["turn"][0]["event"],
            runtime(json!({"type":"stdout","chunk":"ABCD"}))
        );
    }

    #[test]
    fn live_output_retains_bounded_utf8_tail_and_each_changed_file() {
        let mut items = LiveItems::default();
        let output = "完整输出🦀".repeat(6000);
        items
            .observe(
                "submission",
                &runtime(json!({"event":{
                    "type":"tool.event","turnId":"core-turn","toolUseId":"shell",
                    "event":{"type":"stdout","chunk":output}
                }})),
            )
            .unwrap();
        items
            .observe(
                "submission",
                &runtime(json!({"event":{
                    "type":"tool.event","turnId":"core-turn","toolUseId":"shell",
                    "event":{"type":"stdout","chunk":"tail"}
                }})),
            )
            .unwrap();
        for path in ["one.txt", "two.txt"] {
            items
                .observe(
                    "submission",
                    &runtime(json!({"event":{
                        "type":"tool.event","turnId":"core-turn","toolUseId":"shell",
                        "event":{"type":"file_change","change":{"path":path,"kind":"add"}}
                    }})),
                )
                .unwrap();
        }
        let rows = &items.turns["core-turn"];
        let stdout = rows
            .iter()
            .find(|row| is(field(field(row, "event"), "type"), "stdout"))
            .unwrap();
        let retained = stdout["event"]["chunk"]
            .as_string()
            .unwrap()
            .try_to_utf8()
            .unwrap();
        assert!(retained.chars().count() <= 20_000);
        assert!(retained.len() <= 80_000);
        assert_eq!(stdout["truncated"].as_bool(), Some(true));
        assert!(retained.ends_with("tail"));
        assert_eq!(
            stdout["truncatedBytes"].as_u64().unwrap() as usize,
            output.len() + 4 - retained.len()
        );
        assert_eq!(
            rows.iter()
                .filter(|row| is(field(field(row, "event"), "type"), "file_change"))
                .count(),
            2
        );
    }

    #[test]
    fn live_text_snapshots_preserve_exact_units_and_stable_previous_snapshots() {
        let mut items = LiveItems::default();
        items
            .observe(
                "submission",
                &runtime(json!({"event":{"type":"turn.started","turnId":"turn"}})),
            )
            .unwrap();
        items
            .observe(
                "submission",
                &runtime(json!({"event":{"type":"item.started","item":{
                    "id":"message","type":"agent_message","text":"prefix","phase":"commentary"
                }}})),
            )
            .unwrap();
        let mut expected = "prefix".encode_utf16().collect::<Vec<_>>();
        for units in [vec![0xd83e], vec![0xdded, 0xd800], vec![0x61, 0xdc00]] {
            expected.extend_from_slice(&units);
            let mut delta = runtime(
                json!({"event":{"type":"text.delta","messageId":"message","phase":"final_answer"}}),
            );
            set(
                delta.get_mut("event").unwrap(),
                "text",
                RuntimeString::from_utf16(units).into(),
            );
            items.observe("submission", &delta).unwrap();
        }
        let previous = items.snapshot();
        assert_eq!(
            previous["turn"][0]["text"].as_string().unwrap().as_utf16(),
            expected
        );
        assert_eq!(
            previous["turn"][0]["phase"].as_string().unwrap(),
            "final_answer"
        );
        for _ in 0..4096 {
            items
                .observe(
                    "submission",
                    &runtime(
                        json!({"event":{"type":"text.delta","messageId":"message","text":"tail"}}),
                    ),
                )
                .unwrap();
            expected.extend("tail".encode_utf16());
        }
        assert_eq!(
            items.snapshot()["turn"][0]["text"]
                .as_string()
                .unwrap()
                .as_utf16(),
            expected
        );
        assert_ne!(
            previous["turn"][0]["text"],
            items.snapshot()["turn"][0]["text"]
        );
        items.finish("submission");
        assert!(items.snapshot().is_empty());
        assert!(items.text_buffers.is_empty());
    }

    #[test]
    fn live_item_replacement_and_completion_override_buffered_deltas() {
        let mut items = LiveItems::default();
        let event = |value: serde_json::Value| runtime(json!({"event": value}));
        items.observe("s", &event(json!({"type":"item.started","item":{"id":"m","type":"agent_message","text":"old"}}))).unwrap();
        items
            .observe(
                "s",
                &event(json!({"type":"text.delta","messageId":"m","text":" appended"})),
            )
            .unwrap();
        let old_snapshot = items.snapshot();
        items.observe("s", &event(json!({"type":"item.updated","item":{"id":"m","type":"agent_message","text":"replacement"}}))).unwrap();
        items
            .observe(
                "s",
                &event(json!({"type":"text.delta","messageId":"m","text":" tail"})),
            )
            .unwrap();
        assert_eq!(
            items.snapshot()["s"][0]["text"].as_string().unwrap(),
            "replacement tail"
        );
        assert_eq!(
            old_snapshot["s"][0]["text"].as_string().unwrap(),
            "old appended"
        );
        items
            .observe("s", &event(json!({"type":"text.delta","text":"temporary"})))
            .unwrap();
        items
            .observe("s", &event(json!({"type":"text.delta","text":" text"})))
            .unwrap();
        items
            .observe(
                "s",
                &event(json!({"type":"thinking.delta","text":"reason"})),
            )
            .unwrap();
        items
            .observe("s", &event(json!({"type":"thinking.delta","text":"ing"})))
            .unwrap();
        items.observe("s", &event(json!({"type":"item.completed","item":{"id":"m","type":"agent_message","text":"final"}}))).unwrap();
        let snapshot = items.snapshot();
        assert_eq!(snapshot["s"].len(), 2);
        assert_eq!(snapshot["s"][0]["text"].as_string().unwrap(), "final");
        assert_eq!(snapshot["s"][1]["text"].as_string().unwrap(), "reasoning");
        items.observe("s", &event(json!({"type":"item.completed","item":{"id":"reason","type":"reasoning","text":"complete reasoning"}}))).unwrap();
        assert!(items.text_buffers["s"].is_empty());
        let snapshot = items.snapshot();
        assert_eq!(snapshot["s"].len(), 2);
        assert!(snapshot["s"].iter().any(|row| row["text"]
            .as_string()
            .is_some_and(|text| text == "complete reasoning")));
    }
}
