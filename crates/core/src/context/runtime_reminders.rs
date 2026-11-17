//! Source-scoped, append-only runtime reminder snapshots.
//!
//! The canonical meta messages are the journal: normal persistence, history
//! cuts and compaction own their lifetime. Request preparation only projects a
//! possible next snapshot; the turn owner admits it before calling the model.

use crate::provider::behavior::resolve_provider_behavior_profile;
use crate::runtime_values::RuntimeString;
use crate::schemas::config::ResolvedConfig;
use crate::schemas::content::{ContentBlock, TextBlock};
use crate::schemas::message::{Message, UserMessage};
use crate::types::provider::{ModelRequest, SystemPromptPart};
use sha2::{Digest, Sha256};

const PHASE: &str = "antigravity-runtime-reminders";
const ID_PREFIX: &str = "runtime-reminder-step:";
const SNAPSHOT_PREFIX: &str = "<system-reminder>\nRuntime reminder update. This snapshot supersedes all earlier runtime reminder snapshots; only the entries below remain active.\n";
const SNAPSHOT_SUFFIX: &str = "\n</system-reminder>";

pub(crate) fn enabled(config: &ResolvedConfig) -> bool {
    config
        .model_source
        .as_ref()
        .and_then(|source| source.source.as_deref())
        == Some("antigravity")
}

fn is_dynamic(part: &SystemPromptPart, include_all_non_cacheable: bool) -> bool {
    part.name == "task-plan-reminder"
        || part.name == "edited-files"
        || part.name == "loop-recovery"
        || part.name == "total-tokens"
        || (part.cacheable == Some(false)
            && (include_all_non_cacheable
                || part.name == "reminder"
                || part.name == "hook-context"))
}

pub(crate) fn is_step(message: &Message) -> bool {
    matches!(message, Message::User(user)
        if user.is_meta == Some(true)
        && user.uuid.starts_with(ID_PREFIX)
        && user.content.len() == 1
        && matches!(&user.content[0], ContentBlock::Text(text)
            if text.phase.as_ref().is_some_and(|phase| phase == PHASE)))
}

/// Runtime snapshots are not task content and must never enter a summary.
/// Keep the owning replacement's original source ids so discarded steps are
/// retired with the history they accompanied.
pub(crate) fn without_steps(messages: &[Message]) -> Vec<Message> {
    messages
        .iter()
        .filter(|message| !is_step(message))
        .cloned()
        .collect()
}

fn snapshot(parts: &[SystemPromptPart], include_all_non_cacheable: bool) -> RuntimeString {
    let mut parts: Vec<_> = parts
        .iter()
        .filter(|part| is_dynamic(part, include_all_non_cacheable))
        .collect();
    parts.sort_by(|a, b| a.name.cmp(&b.name));
    let mut text = RuntimeString::from(SNAPSHOT_PREFIX);
    if parts.is_empty() {
        text = text.concat(&RuntimeString::from(
            "No runtime reminder entries are currently active.",
        ));
    } else {
        for part in parts {
            text = text
                .concat(&RuntimeString::from("\n["))
                .concat(&part.name)
                .concat(&RuntimeString::from("]\n"))
                .concat(&part.text)
                .concat(&RuntimeString::from("\n"));
        }
    }
    text.concat(&RuntimeString::from(SNAPSHOT_SUFFIX))
}

/// Pure request projection. Repeated estimates and retries do not mutate the
/// session or mint new identities for the same transition.
pub(crate) fn project(request: &mut ModelRequest, active: bool) {
    if !active {
        request.messages.retain(|message| !is_step(message));
        return;
    }
    // Every model routed through this source concatenates `request.system` into
    // a single leading instruction field ahead of conversation history, so
    // non-cacheable per-turn context (`reminder`, `hook-context`) projects into
    // the append-only snapshot tail across all model families, while Gemini
    // models additionally project every `cacheable == Some(false)` slot
    // (`collaboration-mode`, `permissions`, `orchestration-mode`, dynamic
    // fragments) so `systemInstruction` stays byte-stable across mode/policy turns.
    let include_all_non_cacheable =
        resolve_provider_behavior_profile(None, &request.model).family == Some("gemini");
    let previous = request
        .messages
        .iter()
        .rev()
        .find(|message| is_step(message));
    let has_current = request
        .system
        .iter()
        .any(|part| is_dynamic(part, include_all_non_cacheable));
    if !has_current && previous.is_none() {
        return;
    }
    let text = snapshot(&request.system, include_all_non_cacheable);
    request
        .system
        .retain(|part| !is_dynamic(part, include_all_non_cacheable));
    if previous.is_some_and(|message| {
        matches!(message, Message::User(user)
            if matches!(&user.content[0], ContentBlock::Text(previous) if previous.text == text))
    }) {
        return;
    }
    let mut digest = Sha256::new();
    if let Some(last) = request.messages.last() {
        let id = match last {
            Message::User(message) => &message.uuid,
            Message::Assistant(message) => &message.uuid,
            Message::ToolResult(message) => &message.uuid,
            Message::System(message) => &message.uuid,
            Message::CompactBoundary(message) => &message.uuid,
        };
        for unit in id.as_utf16() {
            digest.update(unit.to_le_bytes());
        }
    }
    digest.update([0, 0]);
    for unit in text.as_utf16() {
        digest.update(unit.to_le_bytes());
    }
    let id = format!("{ID_PREFIX}{:x}", digest.finalize());
    request.messages.push(Message::User(UserMessage {
        question_answer: None,
        uuid: id.into(),
        content: vec![ContentBlock::Text(TextBlock {
            text,
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: Some(PHASE.into()),
        })],
        is_meta: Some(true),
    }));
}
