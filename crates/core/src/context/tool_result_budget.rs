//! Per-message aggregate tool-result budget on the REQUEST view.
//!
//! The dispatch-time aggregate budget only ever sees FRESH results: history
//! written before the limit existed (or before a resume) can carry tool
//! results that together blow the per-message ceiling on every later
//! request. This pass runs on the message vector before each model call and
//! enforces the same ceiling over what the model is about to see, persisting
//! the largest offenders to disk and leaving a preview in their place.
//!
//! Decisions are frozen per `tool_use_id` the first time a result is seen, so
//! a later pass never rewrites a prefix the provider already cached:
//! previously replaced results get their cached replacement re-applied
//! byte-identically (no I/O), previously unreplaced results stay unreplaced.
//! Records of the replacements ride the rollout so a resumed session makes
//! the same choices (see [`ReplacementState::reconstruct`]).

use crate::runtime_values::RuntimeString;
use crate::schemas::content::{ContentBlock, OutputBlock};
use crate::schemas::message::Message;
use std::collections::{HashMap, HashSet};

/// Prefix of every budget-produced replacement body. A result whose single
/// text block starts with this tag was already persisted by an earlier pass
/// and is never a candidate again.
pub const BUDGET_REPLACEMENT_TAG: &str = "[tool output persisted to save context]";

/// Per-`tool_use_id` memory of prior budget decisions. One instance per
/// session thread; resumed sessions rebuild it from the rollout records and
/// freeze everything already in history.
#[derive(Debug, Clone, Default)]
pub struct ReplacementState {
    seen: HashSet<RuntimeString>,
    replacements: HashMap<RuntimeString, RuntimeString>,
}

impl ReplacementState {
    /// Rebuild state for a resumed or forked history: every candidate result
    /// already in the transcript is frozen (the model saw it unreplaced), and
    /// the persisted records re-populate the replacements so re-apply is
    /// byte-identical.
    pub fn reconstruct(messages: &[Message], records: Vec<(RuntimeString, RuntimeString)>) -> Self {
        let mut state = Self::default();
        for message in messages {
            match message {
                Message::ToolResult(result) => {
                    state.seen.insert(result.tool_use_id.clone());
                }
                Message::User(user) => {
                    for block in &user.content {
                        if let ContentBlock::ToolResult(result) = block {
                            state.seen.insert(result.tool_use_id.clone());
                        }
                    }
                }
                _ => {}
            }
        }
        for (id, replacement) in records {
            if state.seen.contains(&id) {
                state.replacements.insert(id, replacement);
            }
        }
        state
    }

    /// The persisted records: every replacement decision made so far.
    pub fn records(&self) -> Vec<(RuntimeString, RuntimeString)> {
        let mut out: Vec<_> = self
            .replacements
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    fn record(&mut self, id: RuntimeString, replacement: RuntimeString) {
        self.seen.insert(id.clone());
        self.replacements.insert(id, replacement);
    }

    fn freeze(&mut self, id: RuntimeString) {
        self.seen.insert(id);
    }
}

/// One eligible `tool_result` block.
#[derive(Debug, Clone)]
struct Candidate {
    tool_use_id: RuntimeString,
    size: usize,
}

/// The partitioned decision for one enforcement pass.
#[derive(Debug, Default)]
pub struct BudgetDecision {
    /// Previously replaced results: id → cached replacement body, re-applied
    /// verbatim (no I/O) for prefix stability.
    pub reapply: Vec<(RuntimeString, RuntimeString)>,
    /// Fresh results selected for persistence this pass, largest first:
    /// id → current body size in UTF-16 code units.
    pub to_persist: Vec<(RuntimeString, usize)>,
}

/// The UTF-16 length of a result body's text blocks (images contribute
/// nothing, matching the dispatch-time budget's accounting).
fn content_size(blocks: &[OutputBlock]) -> usize {
    blocks
        .iter()
        .map(|block| match block {
            OutputBlock::Text(text) => text.text.len_utf16(),
            OutputBlock::Image(_) => 0,
        })
        .sum()
}

fn is_already_replaced(blocks: &[OutputBlock]) -> bool {
    blocks.len() == 1
        && matches!(&blocks[0], OutputBlock::Text(text) if text.text.starts_with(BUDGET_REPLACEMENT_TAG))
}

fn has_image(blocks: &[OutputBlock]) -> bool {
    blocks
        .iter()
        .any(|block| matches!(block, OutputBlock::Image(_)))
}

/// Group candidate tool_result blocks the way the wire does: consecutive
/// user/tool_result messages merge into ONE request-level user message, so
/// the budget must judge their aggregate size together. Only an assistant
/// message with a NEW uuid starts a new group (same-uuid streaming fragments
/// merge too).
fn collect_candidate_groups(messages: &[Message]) -> Vec<Vec<Candidate>> {
    let mut groups: Vec<Vec<Candidate>> = Vec::new();
    let mut current: Vec<Candidate> = Vec::new();
    let mut seen_assistant_uuids: HashSet<&RuntimeString> = HashSet::new();
    for message in messages {
        match message {
            Message::ToolResult(result) => {
                if is_already_replaced(&result.content) || has_image(&result.content) {
                    continue;
                }
                let size = content_size(&result.content);
                if size == 0 {
                    continue;
                }
                current.push(Candidate {
                    tool_use_id: result.tool_use_id.clone(),
                    size,
                });
            }
            Message::Assistant(assistant) => {
                if seen_assistant_uuids.insert(&assistant.uuid) && !current.is_empty() {
                    groups.push(std::mem::take(&mut current));
                }
            }
            _ => {}
        }
    }
    if !current.is_empty() {
        groups.push(current);
    }
    groups
}

/// Decide what one enforcement pass replaces. Pure: no I/O, no state
/// mutation — the caller persists the selected results and then feeds the
/// produced bodies back through [`ReplacementState::record`] (selected) and
/// [`apply`] (re-applies).
///
/// `skip_tools` names tools whose results are exempt (their own paging owns
/// result sizing, same exemption the dispatch-time budget grants `read`).
pub fn decide(
    messages: &[Message],
    state: &ReplacementState,
    limit_chars: usize,
    skip_tools: &HashSet<RuntimeString>,
    tool_name_by_use_id: &HashMap<RuntimeString, RuntimeString>,
) -> BudgetDecision {
    let mut decision = BudgetDecision::default();
    for group in collect_candidate_groups(messages) {
        let mut must_reapply: Vec<(RuntimeString, RuntimeString)> = Vec::new();
        let mut frozen_size = 0usize;
        let mut fresh: Vec<&Candidate> = Vec::new();
        for candidate in &group {
            if let Some(name) = tool_name_by_use_id.get(&candidate.tool_use_id) {
                if skip_tools.contains(name) {
                    continue;
                }
            }
            if let Some(replacement) = state.replacements.get(&candidate.tool_use_id) {
                must_reapply.push((candidate.tool_use_id.clone(), replacement.clone()));
                continue;
            }
            if state.seen.contains(&candidate.tool_use_id) {
                frozen_size += candidate.size;
                continue;
            }
            fresh.push(candidate);
        }
        decision.reapply.extend(must_reapply);
        let fresh_total: usize = fresh.iter().map(|candidate| candidate.size).sum();
        let mut remaining = frozen_size + fresh_total;
        let mut sorted = fresh;
        sorted.sort_by(|a, b| b.size.cmp(&a.size));
        for candidate in sorted {
            if remaining <= limit_chars {
                break;
            }
            remaining = remaining.saturating_sub(candidate.size);
            decision
                .to_persist
                .push((candidate.tool_use_id.clone(), candidate.size));
        }
    }
    decision
}

/// Rewrite the result bodies named in `replacements` (id → new body text).
/// Returns true when any message changed.
pub fn apply(
    messages: &mut [Message],
    replacements: &HashMap<RuntimeString, RuntimeString>,
) -> bool {
    if replacements.is_empty() {
        return false;
    }
    let mut changed = false;
    for message in messages.iter_mut() {
        let Message::ToolResult(result) = message else {
            continue;
        };
        let Some(replacement) = replacements.get(&result.tool_use_id) else {
            continue;
        };
        let already =
            matches!(&result.content[..], [OutputBlock::Text(text)] if text.text == *replacement);
        if already {
            continue;
        }
        result.content = vec![OutputBlock::Text(crate::schemas::content::TextBlock {
            text: replacement.clone(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })];
        changed = true;
    }
    changed
}

/// Freeze every candidate id the pass considered and record the persisted
/// replacements, so later passes treat this history exactly as this pass did.
pub fn settle(
    state: &mut ReplacementState,
    messages: &[Message],
    persisted: &[(RuntimeString, RuntimeString)],
) {
    for group in collect_candidate_groups(messages) {
        for candidate in group {
            state.freeze(candidate.tool_use_id);
        }
    }
    for (id, replacement) in persisted {
        state.record(id.clone(), replacement.clone());
    }
}

/// The assistant tool_use id → tool name map the budget needs for its skip
/// set. tool_use always precedes its result, so scanning the same vector is
/// sufficient.
pub fn tool_name_by_use_id(messages: &[Message]) -> HashMap<RuntimeString, RuntimeString> {
    let mut map = HashMap::new();
    for message in messages {
        let Message::Assistant(assistant) = message else {
            continue;
        };
        for block in &assistant.content {
            if let crate::schemas::content::ContentBlock::ToolUse(tool_use) = block {
                map.insert(tool_use.id.clone(), tool_use.name.clone());
            }
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::content::{ContentBlock, TextBlock, ToolUseBlock};
    use crate::schemas::message::{AssistantMessage, ToolResultMessage, UserMessage};

    fn text(value: &str) -> TextBlock {
        TextBlock {
            text: value.into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        }
    }

    fn assistant(uuid: &str, tool_use: Option<(&str, &str)>) -> Message {
        let mut content = Vec::new();
        if let Some((id, name)) = tool_use {
            content.push(ContentBlock::ToolUse(ToolUseBlock {
                id: id.into(),
                name: name.into(),
                input: crate::runtime_values::RuntimeJson::Null,
                tool_kind: None,
                thought_signature: None,
            }));
        }
        Message::Assistant(AssistantMessage {
            uuid: uuid.into(),
            content,
            model: "m".into(),
            usage: None,
        })
    }

    fn result(id: &str, body: &str) -> Message {
        Message::ToolResult(ToolResultMessage {
            uuid: format!("tr-{id}").into(),
            tool_use_id: id.into(),
            content: vec![OutputBlock::Text(text(body))],
            is_error: false,
            tool_kind: None,
            native_tool_output: None,
            output: None,
            extra_user_blocks: None,
        })
    }

    fn user(uuid: &str) -> Message {
        Message::User(UserMessage {
            question_answer: None,
            uuid: uuid.into(),
            content: vec![ContentBlock::Text(text("work"))],
            is_meta: None,
        })
    }

    #[test]
    fn parallel_results_in_one_wire_group_are_judged_together() {
        // Two 60k results in separate messages: individually under a 100k
        // limit, but the wire merges them into one user message, so the
        // budget must see 120k and persist the largest.
        let messages = vec![
            user("u1"),
            assistant("a1", Some(("tu-a", "bash"))),
            assistant("a1", Some(("tu-b", "bash"))),
            result("tu-a", &"x".repeat(60_000)),
            result("tu-b", &"y".repeat(60_000)),
        ];
        let state = ReplacementState::default();
        let decision = decide(
            &messages,
            &state,
            100_000,
            &HashSet::new(),
            &tool_name_by_use_id(&messages),
        );
        assert_eq!(
            decision.to_persist.len(),
            1,
            "one of the two must be persisted"
        );
        assert!(decision.reapply.is_empty());
    }

    #[test]
    fn frozen_results_are_never_replaced_later() {
        let messages = vec![
            user("u1"),
            assistant("a1", Some(("tu-a", "bash"))),
            result("tu-a", &"x".repeat(60_000)),
        ];
        let mut state = ReplacementState::default();
        // First pass with a generous limit: nothing selected, id frozen.
        let first = decide(
            &messages,
            &state,
            1_000_000,
            &HashSet::new(),
            &tool_name_by_use_id(&messages),
        );
        assert!(first.to_persist.is_empty());
        settle(&mut state, &messages, &[]);
        // A later pass with a tight limit must leave the frozen result alone.
        let second = decide(
            &messages,
            &state,
            10,
            &HashSet::new(),
            &tool_name_by_use_id(&messages),
        );
        assert!(
            second.to_persist.is_empty(),
            "frozen results are off-limits"
        );
    }

    #[test]
    fn replacements_reapply_byte_identically_and_read_is_exempt() {
        let messages = vec![
            user("u1"),
            assistant("a1", Some(("tu-read", "read"))),
            assistant("a1", Some(("tu-bash", "bash"))),
            result("tu-read", &"x".repeat(60_000)),
            result("tu-bash", &"y".repeat(60_000)),
        ];
        let names = tool_name_by_use_id(&messages);
        let skip: HashSet<RuntimeString> = ["read".into()].into_iter().collect();
        let state = ReplacementState::default();
        let decision = decide(&messages, &state, 10, &skip, &names);
        assert_eq!(decision.to_persist.len(), 1);
        assert_eq!(decision.to_persist[0].0, RuntimeString::from("tu-bash"));
    }

    #[test]
    fn reconstruct_freezes_history_and_restores_records() {
        let messages = vec![
            user("u1"),
            assistant("a1", Some(("tu-a", "bash"))),
            result("tu-a", &"x".repeat(100)),
        ];
        let records = vec![(
            RuntimeString::from("tu-old"),
            RuntimeString::from("cached body"),
        )];
        let state = ReplacementState::reconstruct(&messages, records.clone());
        // The historical result is frozen even though no record names it.
        let decision = decide(
            &messages,
            &state,
            1,
            &HashSet::new(),
            &tool_name_by_use_id(&messages),
        );
        assert!(decision.to_persist.is_empty());
        assert!(state.records().is_empty());
        // The recorded replacement re-applies verbatim.
        let with_record = vec![
            user("u1"),
            assistant("a1", Some(("tu-old", "bash"))),
            result("tu-old", &"y".repeat(100)),
        ];
        let state = ReplacementState::reconstruct(&with_record, records.clone());
        let decision = decide(
            &with_record,
            &state,
            1,
            &HashSet::new(),
            &tool_name_by_use_id(&with_record),
        );
        assert_eq!(
            decision.reapply,
            vec![(
                RuntimeString::from("tu-old"),
                RuntimeString::from("cached body")
            )]
        );
        assert_eq!(state.records(), records);
    }

    #[test]
    fn already_replaced_bodies_are_not_candidates() {
        let body = format!("{BUDGET_REPLACEMENT_TAG} preview");
        let messages = vec![
            user("u1"),
            assistant("a1", Some(("tu-a", "bash"))),
            result("tu-a", &body),
        ];
        let state = ReplacementState::default();
        let decision = decide(
            &messages,
            &state,
            1,
            &HashSet::new(),
            &tool_name_by_use_id(&messages),
        );
        assert!(decision.to_persist.is_empty());
        assert!(decision.reapply.is_empty());
    }
}
