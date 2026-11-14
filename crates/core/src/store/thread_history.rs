//! Provider-free transcript replay. Prompt compaction must not erase saved UI history.

use crate::engine::session::TurnIndex;
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeEncode, RuntimeEncoder, RuntimeJson, RuntimeString,
};
use crate::schemas::message::{CompactBoundaryMessage, Message};
use crate::schemas::rollout::{RolloutItem, SessionRole};
use std::collections::HashMap;

/// Rollout event marking the end of a side task's inherited parent context prefix.
pub const SIDE_TASK_BOUNDARY_EVENT: &str = "side_task_boundary";

/// Return the rollout index where a side task's own visible history begins.
/// Ordinary and forked main threads return `0` so inherited branch history stays visible.
pub fn side_task_visible_start_index(items: &[RolloutItem]) -> usize {
    let is_side_task = items.iter().find_map(|item| match item {
        RolloutItem::SessionMeta { meta, .. } => Some(meta.role == Some(SessionRole::SideTask)),
        _ => None,
    });
    if is_side_task != Some(true) {
        return 0;
    }
    for (index, item) in items.iter().enumerate() {
        match item {
            RolloutItem::SessionMeta { .. } => continue,
            RolloutItem::Event(event)
                if event.event == SIDE_TASK_BOUNDARY_EVENT
                    || event.event == "approval_parent_history" =>
            {
                return index + 1;
            }
            RolloutItem::Checkpoint(_) | RolloutItem::TurnContext { .. } => return index,
            RolloutItem::Event(event)
                if event.event == "prediction_source" || event.event == "turn_goal_metadata" =>
            {
                return index;
            }
            _ => continue,
        }
    }
    items.len()
}

/// Collect message UUIDs belonging to a side task's hidden inherited parent prefix.
pub fn side_task_hidden_message_ids(items: &[RolloutItem]) -> Vec<RuntimeString> {
    let end = side_task_visible_start_index(items);
    items[..end]
        .iter()
        .filter_map(|item| match item {
            RolloutItem::Message { message } => Some(match message {
                Message::User(m) => m.uuid.clone(),
                Message::Assistant(m) => m.uuid.clone(),
                Message::ToolResult(m) => m.uuid.clone(),
                Message::System(m) => m.uuid.clone(),
                Message::CompactBoundary(m) => m.uuid.clone(),
            }),
            _ => None,
        })
        .collect()
}

#[derive(Debug)]
pub(crate) struct SavedPathReference {
    pub path: String,
    pub exact: bool,
}

/// Read the file declarations a capture tool writes beside its returned image
/// bytes. Older records did not stamp `sourcePath` on the image block itself,
/// but their `Saved:` line carries the same durable identity.
pub(crate) fn saved_path_references(text: &str) -> Vec<SavedPathReference> {
    let mut references = Vec::new();
    for line in text.lines() {
        let Some((_, declaration)) = line.split_once("Saved: ") else {
            continue;
        };
        let declaration = declaration.trim();
        if declaration.starts_with('"') || declaration.starts_with('[') {
            let Ok(value) = serde_json::from_str::<serde_json::Value>(declaration) else {
                continue;
            };
            let values = match value {
                serde_json::Value::Array(values) => values,
                value => vec![value],
            };
            for value in values {
                if let serde_json::Value::String(path) = value {
                    if !path.chars().any(|ch| ch.is_control()) {
                        references.push(SavedPathReference { path, exact: true });
                    }
                }
            }
        } else {
            references.extend(
                declaration
                    .split(',')
                    .map(str::trim)
                    .filter(|path| !path.is_empty())
                    .map(|path| SavedPathReference {
                        path: path.to_string(),
                        exact: false,
                    }),
            );
        }
    }
    references
}

/// Display-only image substitution shared by saved page readers and the facade.
/// A missing or non-scalar path keeps the complete inline image.
pub fn substitute_image_sources(row: &mut RuntimeJson) {
    let Some(content) = row.get_mut("content").and_then(RuntimeJson::as_array_mut) else {
        return;
    };
    let saved = content
        .iter()
        .filter(|block| {
            block
                .get("type")
                .and_then(RuntimeJson::as_string)
                .is_some_and(|kind| kind == "text")
        })
        .filter_map(|block| block.get("text").and_then(RuntimeJson::as_string))
        .filter_map(|text| text.try_to_utf8().ok())
        .flat_map(|text| saved_path_references(&text))
        .collect::<Vec<_>>();
    let mut saved_index = 0;
    for block in content {
        if block
            .get("type")
            .and_then(RuntimeJson::as_string)
            .is_none_or(|kind| kind != "image")
        {
            continue;
        }
        if block.get("sourcePath").is_none() {
            while saved_index < saved.len() {
                let candidate = &saved[saved_index];
                saved_index += 1;
                let exists = std::path::Path::new(&candidate.path).is_file();
                if exists || candidate.exact || saved.len() == 1 {
                    if let Some(fields) = block.as_object_mut() {
                        fields.insert(
                            "sourcePath",
                            RuntimeJson::String(candidate.path.clone().into()),
                        );
                    }
                    break;
                }
            }
        }
        let Some(path) = block
            .get("sourcePath")
            .and_then(RuntimeJson::as_string)
            .and_then(|path| path.try_to_utf8().ok())
        else {
            continue;
        };
        if !std::path::Path::new(&path).is_file() {
            continue;
        }
        let mut reference = crate::runtime_values::RuntimeObject::new();
        reference.insert("type", RuntimeJson::String("image".into()));
        reference.insert("path", RuntimeJson::String(path.into()));
        if let Some(mime) = block.get("mimeType").filter(|mime| !mime.is_null()) {
            reference.insert("mimeType", mime.clone());
        }
        *block = RuntimeJson::Object(reference);
    }
}

/// Original messages and the same durable turn metadata used by a live history reply.
#[derive(Debug, Clone, PartialEq)]
pub struct ThreadHistory {
    pub history: Vec<Message>,
    pub turn_index: TurnIndex,
    /// Result-message UUID -> structured output persisted in a separate event.
    pub tool_outputs: HashMap<RuntimeString, RuntimeJson>,
    pub goal_completions: Vec<crate::goal::retained_context::GoalCompletion>,
}

impl RuntimeEncode for ThreadHistory {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut history = Vec::with_capacity(self.history.len());
        for message in &self.history {
            let mut encoded = runtime_codec::encode_value(message)?;
            if let Message::ToolResult(result) = message {
                if let (Some(output), RuntimeJson::Object(fields)) =
                    (self.tool_outputs.get(&result.uuid), &mut encoded)
                {
                    fields.insert("output", output.clone());
                }
            }
            history.push(encoded);
        }
        let mut encoded = runtime_codec::encode_value(&self.turn_index)?;
        let RuntimeJson::Object(result) = &mut encoded else {
            unreachable!("TurnIndex always encodes as an object");
        };
        result.insert("history", RuntimeJson::Array(history));
        result.insert(
            "goalCompletions",
            runtime_codec::encode_value(&self.goal_completions)?,
        );
        result.insert(
            "agentNotifications",
            runtime_codec::encode_value(&self.turn_index.agent_notifications)?,
        );
        out.emit(encoded)
    }
}

/// Fold the complete saved conversation, applying only actual conversation rewinds.
/// The input is a fixed file snapshot; this performs no I/O or provider initialization.
pub fn project_thread_history(items: Vec<RolloutItem>) -> ThreadHistory {
    let visible_start = side_task_visible_start_index(&items);
    let visible_items = &items[visible_start..];
    let goal_completions = crate::goal::retained_context::completions_from_rollout(visible_items);
    let mut retained = Vec::new();
    let mut boundaries: Vec<usize> = Vec::new();
    let mut pending_context = None;
    for item in items.into_iter().skip(visible_start) {
        match &item {
            RolloutItem::Rollback(rollback) => {
                let count = (rollback.num_turns as usize).min(boundaries.len());
                if count > 0 {
                    let keep = boundaries.len() - count;
                    retained.truncate(boundaries[keep]);
                    boundaries.truncate(keep);
                    pending_context = None;
                }
            }
            RolloutItem::TurnContext { .. } => {
                pending_context = Some(retained.len());
                retained.push(item);
            }
            RolloutItem::Message {
                message: Message::User(user),
            } if user.is_meta != Some(true) => {
                boundaries.push(pending_context.take().unwrap_or(retained.len()));
                retained.push(item);
            }
            _ => retained.push(item),
        }
    }

    let mut turn_index = super::jsonl_store::rollout_metadata(&retained).2;
    let mut history = Vec::new();
    let mut tool_outputs = HashMap::new();
    let mut result_messages = HashMap::new();
    let mut current_turn = None;
    for (position, item) in retained.into_iter().enumerate() {
        match item {
            RolloutItem::TurnContext { ctx } => current_turn = ctx.turn_id,
            RolloutItem::Message { message } => {
                if let Message::ToolResult(result) = &message {
                    result_messages.insert(
                        (current_turn.clone(), result.tool_use_id.clone()),
                        result.uuid.clone(),
                    );
                }
                // Compaction summaries are provider prompt context kept
                // inspectable in the rollout. A transcript replay must not
                // serve them as ordinary assistant replies; the summary a
                // client shows rides the compact boundary record below.
                if crate::context::compact::full_compact::is_compaction_output(&message) {
                    continue;
                }
                history.push(message);
            }
            RolloutItem::Compacted(compacted) if !compacted.is_history_repair() => {
                let uuid: RuntimeString = format!("history-compaction-{position}").into();
                if let Some(turn_id) = &current_turn {
                    turn_index
                        .message_turn
                        .insert(uuid.clone(), turn_id.clone());
                }
                history.push(Message::CompactBoundary(CompactBoundaryMessage {
                    native_compaction: None,
                    uuid,
                    summary: compacted.summary,
                }));
            }
            RolloutItem::Event(event) if event.event == "tool_result_output" => {
                if let Some(payload) = event.payload.as_ref().and_then(RuntimeJson::as_object) {
                    if let (Some(id), Some(output)) = (
                        payload.get("toolUseId").and_then(RuntimeJson::as_str),
                        payload.get("output"),
                    ) {
                        let turn_id = event.turn_id.clone().or_else(|| current_turn.clone());
                        let message_id = result_messages
                            .get(&(turn_id, id.clone()))
                            .or_else(|| result_messages.get(&(None, id.clone())));
                        if let Some(message_id) = message_id {
                            tool_outputs.insert(message_id.clone(), output.clone());
                        }
                    }
                }
            }
            _ => {}
        }
    }
    ThreadHistory {
        goal_completions,
        history,
        turn_index,
        tool_outputs,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::content::{ContentBlock, HarnessEnvelope, TextBlock};
    use crate::schemas::message::{AssistantMessage, UserMessage};
    use crate::schemas::rollout::CompactedItem;

    fn user(id: &str) -> Message {
        Message::User(UserMessage {
            question_answer: None,
            uuid: id.into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: "x".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
        })
    }

    fn compaction_output_message(id: &str, text: &str) -> Message {
        Message::Assistant(AssistantMessage {
            uuid: id.into(),
            model: "m".into(),
            usage: None,
            content: vec![ContentBlock::Text(TextBlock {
                text: text.into(),
                citations: None,
                harness_envelope: Some(HarnessEnvelope::CompactionOutput),
                message_id: None,
                phase: None,
            })],
        })
    }

    fn item(message: Message) -> RolloutItem {
        RolloutItem::Message { message }
    }

    #[test]
    fn compaction_output_stays_out_of_the_replayed_transcript() {
        let history = project_thread_history(vec![
            item(user("u1")),
            item(compaction_output_message(
                "summary-1",
                "# Context checkpoint",
            )),
            RolloutItem::Compacted(CompactedItem {
                approval_parent_history: None,
                summary: "# Context checkpoint".into(),
                replaced_range: (0, 1),
                source_message_ids: None,
                replacement_history: None,
            }),
            item(user("u2")),
        ]);
        let uuids: Vec<_> = history
            .history
            .iter()
            .map(|message| match message {
                Message::User(user) => user.uuid.clone(),
                Message::Assistant(assistant) => assistant.uuid.clone(),
                Message::CompactBoundary(boundary) => boundary.uuid.clone(),
                other => panic!("unexpected replayed message {other:?}"),
            })
            .collect();
        assert_eq!(
            uuids,
            vec![
                RuntimeString::from("u1"),
                RuntimeString::from("history-compaction-2"),
                RuntimeString::from("u2"),
            ]
        );
        let Message::CompactBoundary(boundary) = &history.history[1] else {
            panic!("expected the boundary to carry the summary");
        };
        assert_eq!(boundary.summary, "# Context checkpoint");
    }

    #[test]
    fn saved_declaration_restores_a_legacy_inline_image_reference() {
        let dir = tempfile::tempdir().unwrap();
        let image = dir.path().join("capture.png");
        std::fs::write(&image, b"image bytes").unwrap();
        let source = serde_json::json!({
            "type":"tool_result",
            "uuid":"capture",
            "content":[
                {"type":"text","text":format!("Saved: {}", serde_json::json!(image.to_string_lossy()))},
                {"type":"image","mimeType":"image/png","base64":"aGVsbG8="}
            ]
        })
        .to_string();
        let mut row = crate::runtime_values::runtime_json::parse(&source.into()).unwrap();
        substitute_image_sources(&mut row);
        let content = row["content"].as_array().unwrap();
        assert_eq!(content[1]["type"].as_string().unwrap(), "image");
        assert_eq!(
            content[1]["path"].as_string().unwrap(),
            image.to_string_lossy().as_ref()
        );
        assert!(content[1].get("base64").is_none());
    }
}
