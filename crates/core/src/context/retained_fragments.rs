//! Fragment liveness detection and on-demand re-injection across history rewrites.
//!
//! History rewrites (compaction, model migration) replace the model-visible
//! transcript and can silently drop instruction fragments that the harness had
//! injected as messages. Comparing configuration state alone cannot detect
//! that loss: unchanged state says nothing about what the model can still see.
//! Before a rewritten history goes back to the model, every registered section
//! resolves what the retained history actually shows:
//!
//! - `Known`: retained history still carries a fragment verified identical to
//!   the section's current value. Nothing is injected (zero bytes, idempotent).
//! - `Absent`: no fragment of the section survived the rewrite. The current
//!   value is rendered in full — including the case where the harness holds a
//!   precise current value but the fragment was lost from retained history.
//! - `Unknown`: retained history carries a fragment of the section, but it
//!   cannot be verified as identical to the current value. The stale copies
//!   are removed and the current value is re-rendered, so the model never
//!   reads two conflicting versions of the same list.
//!
//! Matching passes the role of the carrying message to the section, and only
//! harness-owned reminder blocks (system-reminder envelope) are considered, so
//! quoted echoes in ordinary conversation never count as live fragments.

use crate::context::manager::ContextFragment;
use crate::context::manager::ContextManager;
use crate::provider::behavior::ProviderBehaviorProfile;
use crate::runtime_values::RuntimeString;
use crate::schemas::content::ContentBlock;
use crate::schemas::content::HarnessEnvelope;
use crate::schemas::message::Message;
use crate::types::tool::BackgroundTaskHandle;
use crate::types::tool::BackgroundTaskRegistry;
use crate::types::tool::BackgroundTaskStatus;
use crate::types::tool::Tool;
use crate::types::tool::ToolExposure;
use std::collections::HashSet;
use std::sync::Arc;

/// What is known about a section's previously model-visible fragment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PreviousFragmentState {
    /// No matching fragment exists in retained history.
    Absent,
    /// Retained history contains the section's fragment, but the exact
    /// comparison state needed to verify it is unavailable.
    Unknown,
    /// The fragment in retained history is verified against the section's
    /// exact current value.
    Known,
}

/// One re-injectable fragment and its placement in the shared reminder message.
pub(crate) struct RetainedFragmentRender {
    pub(crate) id: &'static str,
    pub(crate) order: i32,
    pub(crate) text: RuntimeString,
}

/// A harness-managed message fragment that must stay visible to the model.
pub(crate) trait RetainedFragmentSection: Send + Sync {
    /// Recognizes this section's fragment in retained model history. The role
    /// of the carrying message participates in matching.
    fn matches_retained_fragment(&self, role: &str, text: &str) -> bool;

    /// Recognizes a retained fragment whose identity is verified against this
    /// section's current value; that identity depends on the current value.
    fn matches_current_retained_fragment(&self, role: &str, text: &str) -> bool;

    /// The current model-visible render; `None` when nothing should be visible.
    fn render_current(&self) -> Option<RetainedFragmentRender>;
}

/// Resolve what the retained history still shows for one section.
fn resolve_previous_state(
    history: &[Message],
    section: &dyn RetainedFragmentSection,
) -> PreviousFragmentState {
    let mut class_found = false;
    for message in history {
        let Some((role, content)) = role_and_content(message) else {
            continue;
        };
        for block in content {
            let ContentBlock::Text(text) = block else {
                continue;
            };
            if text.harness_envelope != Some(HarnessEnvelope::SystemReminder) {
                continue;
            }
            let Ok(rendered) = text.text.try_to_utf8() else {
                continue;
            };
            if !section.matches_retained_fragment(role, &rendered) {
                continue;
            }
            class_found = true;
            if section.matches_current_retained_fragment(role, &rendered) {
                return PreviousFragmentState::Known;
            }
        }
    }
    if class_found {
        PreviousFragmentState::Unknown
    } else {
        PreviousFragmentState::Absent
    }
}

fn role_and_content(message: &Message) -> Option<(&'static str, &Vec<ContentBlock>)> {
    match message {
        Message::User(user) => Some(("user", &user.content)),
        Message::Assistant(assistant) => Some(("assistant", &assistant.content)),
        _ => None,
    }
}

/// Re-inject the retained fragments a history rewrite dropped. Sections are
/// processed in registration order; a section whose fragment is still visible
/// contributes zero bytes. Returns whether the history changed.
pub(crate) fn reinject_retained_fragments(
    messages: &mut Vec<Message>,
    sections: &[&dyn RetainedFragmentSection],
    uuid: &dyn Fn() -> String,
    profile: &ProviderBehaviorProfile,
) -> bool {
    let mut changed = false;
    for section in sections {
        let state = resolve_previous_state(messages, *section);
        let Some(render) = section.render_current() else {
            // Nothing should currently be visible; surviving copies are left
            // as they are and nothing is injected.
            continue;
        };
        match state {
            // Still visible to the model: inject nothing.
            PreviousFragmentState::Known => {}
            // Did not survive the rewrite: render the current value in full.
            PreviousFragmentState::Absent => {
                inject(messages, &render, uuid, profile);
                changed = true;
            }
            // An unverifiable copy survives: replace it so the model never
            // reads two conflicting versions of the same list.
            PreviousFragmentState::Unknown => {
                remove_stale_fragments(messages, *section);
                inject(messages, &render, uuid, profile);
                changed = true;
            }
        }
    }
    changed
}

/// Remove harness-owned reminder blocks of this section and drop the meta
/// messages left empty. Real user content is never removed.
fn remove_stale_fragments(messages: &mut Vec<Message>, section: &dyn RetainedFragmentSection) {
    for message in messages.iter_mut() {
        let Message::User(user) = message else {
            continue;
        };
        user.content.retain(|block| {
            let ContentBlock::Text(text) = block else {
                return true;
            };
            if text.harness_envelope != Some(HarnessEnvelope::SystemReminder) {
                return true;
            }
            let Ok(rendered) = text.text.try_to_utf8() else {
                return true;
            };
            !section.matches_retained_fragment("user", &rendered)
        });
    }
    messages.retain(|message| match message {
        Message::User(user) => user.is_meta != Some(true) || !user.content.is_empty(),
        _ => true,
    });
}

fn inject(
    messages: &mut Vec<Message>,
    render: &RetainedFragmentRender,
    uuid: &dyn Fn() -> String,
    profile: &ProviderBehaviorProfile,
) {
    let mut manager = ContextManager::new();
    manager.upsert(ContextFragment::user_reinsertion(
        render.id,
        render.order,
        render.text.clone(),
    ));
    *messages = manager.render_messages(std::mem::take(messages), uuid, profile);
}

/// The section payload inside a rendered fragment: located by the class prefix
/// and stripped of the reminder-envelope suffix a provider style may add.
fn retained_payload<'a>(text: &'a str, class_prefix: &str) -> Option<&'a str> {
    let start = text.find(class_prefix)?;
    let payload = &text[start..];
    Some(payload.strip_suffix(ENVELOPE_SUFFIX).unwrap_or(payload))
}

const ENVELOPE_SUFFIX: &str = "\n</system-reminder>";

fn join_lines(values: &[RuntimeString]) -> RuntimeString {
    let Some(mut output) = values.first().cloned() else {
        return RuntimeString::default();
    };
    let separator = RuntimeString::from("\n");
    for value in &values[1..] {
        output = output.concat(&separator).concat(value);
    }
    output
}

// ---- Running background tasks ----

const RUNNING_TASKS_CLASS_PREFIX: &str = "These background tasks are still running";

const RUNNING_TASKS_PREAMBLE: &str = "These background tasks are still running after the context was compacted. Do NOT re-spawn them; read their results with task_output:\n";

/// Running background agents that the model must not re-spawn after a rewrite
/// dropped the tool call that started them.
pub(crate) struct RunningTasksSection {
    text: Option<RuntimeString>,
    payload: Option<String>,
}

impl RunningTasksSection {
    pub(crate) fn new(registry: Option<&Arc<dyn BackgroundTaskRegistry>>) -> Self {
        Self::from_handles(registry.map(|registry| registry.list()).unwrap_or_default())
    }

    pub(crate) fn from_handles(handles: Vec<BackgroundTaskHandle>) -> Self {
        let running: Vec<BackgroundTaskHandle> = handles
            .into_iter()
            .filter(|task| task.status == BackgroundTaskStatus::Running)
            .collect();
        if running.is_empty() {
            return Self {
                text: None,
                payload: None,
            };
        }
        let lines: Vec<RuntimeString> = running
            .iter()
            .map(|task| {
                let description = task
                    .description
                    .as_ref()
                    .map(|description| RuntimeString::from(" — ").concat(description))
                    .unwrap_or_default();
                RuntimeString::from("- ")
                    .concat(&task.agent_id)
                    .concat(&description)
                    .concat(&RuntimeString::from(" (output: "))
                    .concat(task.output_file.as_runtime_string())
                    .concat(&RuntimeString::from(")"))
            })
            .collect();
        let text = RuntimeString::from(RUNNING_TASKS_PREAMBLE).concat(&join_lines(&lines));
        let payload = text.try_to_utf8().ok();
        Self {
            text: Some(text),
            payload,
        }
    }
}

impl RetainedFragmentSection for RunningTasksSection {
    fn matches_retained_fragment(&self, role: &str, text: &str) -> bool {
        role == "user" && text.contains(RUNNING_TASKS_CLASS_PREFIX)
    }

    fn matches_current_retained_fragment(&self, role: &str, text: &str) -> bool {
        let Some(current) = self.payload.as_deref() else {
            return false;
        };
        role == "user"
            && retained_payload(text, RUNNING_TASKS_CLASS_PREFIX)
                .is_some_and(|found| found == current)
    }

    fn render_current(&self) -> Option<RetainedFragmentRender> {
        Some(RetainedFragmentRender {
            id: "running-background-tasks",
            order: 10,
            text: self.text.clone()?,
        })
    }
}

// ---- Surfaced deferred tools ----

const SURFACED_TOOLS_CLASS_PREFIX: &str =
    "These tools you previously found via tool_search remain available";

const SURFACED_TOOLS_PREAMBLE: &str = "These tools you previously found via tool_search remain available — call them directly without searching again:\n";

/// Deferred tools already surfaced via `tool_search`; after a rewrite dropped
/// the search call the model would otherwise search for them again.
pub(crate) struct SurfacedToolsSection {
    text: Option<RuntimeString>,
    payload: Option<String>,
}

impl SurfacedToolsSection {
    pub(crate) fn new(tools: &[Arc<dyn Tool>], surfaced: &HashSet<RuntimeString>) -> Self {
        let entries: Vec<(RuntimeString, RuntimeString)> = tools
            .iter()
            .filter(|tool| {
                tool.exposure() == ToolExposure::Deferred && surfaced.contains(&tool.name())
            })
            .map(|tool| (tool.name(), tool.description()))
            .collect();
        Self::from_entries(entries)
    }

    pub(crate) fn from_entries(entries: Vec<(RuntimeString, RuntimeString)>) -> Self {
        if entries.is_empty() {
            return Self {
                text: None,
                payload: None,
            };
        }
        let lines: Vec<RuntimeString> = entries
            .iter()
            .map(|(name, description)| {
                RuntimeString::from("- ")
                    .concat(name)
                    .concat(&RuntimeString::from(": "))
                    .concat(description)
            })
            .collect();
        let text = RuntimeString::from(SURFACED_TOOLS_PREAMBLE).concat(&join_lines(&lines));
        let payload = text.try_to_utf8().ok();
        Self {
            text: Some(text),
            payload,
        }
    }
}

impl RetainedFragmentSection for SurfacedToolsSection {
    fn matches_retained_fragment(&self, role: &str, text: &str) -> bool {
        role == "user" && text.contains(SURFACED_TOOLS_CLASS_PREFIX)
    }

    fn matches_current_retained_fragment(&self, role: &str, text: &str) -> bool {
        let Some(current) = self.payload.as_deref() else {
            return false;
        };
        role == "user"
            && retained_payload(text, SURFACED_TOOLS_CLASS_PREFIX)
                .is_some_and(|found| found == current)
    }

    fn render_current(&self) -> Option<RetainedFragmentRender> {
        Some(RetainedFragmentRender {
            id: "surfaced-tools",
            order: 20,
            text: self.text.clone()?,
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::content::TextBlock;
    use crate::schemas::message::AssistantMessage;
    use crate::schemas::message::CompactBoundaryMessage;
    use crate::schemas::message::UserMessage;

    fn test_profile(model: &str) -> ProviderBehaviorProfile {
        crate::provider::behavior::resolve_provider_behavior_profile(None, &model.into())
    }

    fn reminder_block(text: &str) -> ContentBlock {
        ContentBlock::Text(TextBlock {
            text: RuntimeString::from(text),
            citations: None,
            harness_envelope: Some(HarnessEnvelope::SystemReminder),
            message_id: None,
            phase: None,
        })
    }

    fn plain_block(text: &str) -> ContentBlock {
        ContentBlock::Text(TextBlock {
            text: RuntimeString::from(text),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })
    }

    fn meta_message(uuid: &str, blocks: Vec<ContentBlock>) -> Message {
        Message::User(UserMessage {
            question_answer: None,
            uuid: uuid.into(),
            content: blocks,
            is_meta: Some(true),
        })
    }

    fn real_user(uuid: &str, blocks: Vec<ContentBlock>) -> Message {
        Message::User(UserMessage {
            question_answer: None,
            uuid: uuid.into(),
            content: blocks,
            is_meta: None,
        })
    }

    fn assistant(uuid: &str, blocks: Vec<ContentBlock>) -> Message {
        Message::Assistant(AssistantMessage {
            uuid: uuid.into(),
            content: blocks,
            model: "test-model".into(),
            usage: None,
        })
    }

    fn boundary() -> Message {
        Message::CompactBoundary(CompactBoundaryMessage {
            uuid: "compact-1".into(),
            summary: "older context".into(),
            native_compaction: None,
        })
    }

    fn handle(id: &str, status: BackgroundTaskStatus) -> BackgroundTaskHandle {
        BackgroundTaskHandle {
            agent_id: id.into(),
            output_file: format!("/tmp/{id}.txt").as_str().into(),
            status,
            description: Some("inspect logs".into()),
            partial: None,
        }
    }

    fn running_section() -> RunningTasksSection {
        RunningTasksSection::from_handles(vec![handle("bg-1", BackgroundTaskStatus::Running)])
    }

    fn stale_running_section() -> RunningTasksSection {
        RunningTasksSection::from_handles(vec![handle("bg-0", BackgroundTaskStatus::Running)])
    }

    fn surfaced_section() -> SurfacedToolsSection {
        SurfacedToolsSection::from_entries(vec![(
            RuntimeString::from("extra_tool"),
            RuntimeString::from("Extra description"),
        )])
    }

    fn payload_of(section: &dyn RetainedFragmentSection) -> String {
        section
            .render_current()
            .expect("section has a current render")
            .text
            .try_to_utf8()
            .expect("payload is scalar text")
    }

    fn wrapped(payload: &str) -> String {
        format!("<system-reminder>\n{payload}\n</system-reminder>")
    }

    fn block_text(block: &ContentBlock) -> Option<String> {
        match block {
            ContentBlock::Text(text) => text.text.try_to_utf8().ok(),
            _ => None,
        }
    }

    fn class_block_count(messages: &[Message], section: &dyn RetainedFragmentSection) -> usize {
        messages
            .iter()
            .filter_map(|message| match message {
                Message::User(user) => Some(user),
                _ => None,
            })
            .flat_map(|user| user.content.iter())
            .filter(|block| match block {
                // Mirror the scanner: only harness-owned reminder blocks count.
                ContentBlock::Text(text) => {
                    text.harness_envelope == Some(HarnessEnvelope::SystemReminder)
                        && text.text.try_to_utf8().is_ok_and(|rendered| {
                            section.matches_retained_fragment("user", &rendered)
                        })
                }
                _ => false,
            })
            .count()
    }

    #[test]
    fn running_tasks_injection_uses_context_manager_reinsertion() {
        // A rewrite dropped every fragment: the section resolves Absent and the
        // current value is rendered as a fresh meta reminder message.
        let section = running_section();
        let mut messages = vec![boundary()];
        let changed = reinject_retained_fragments(
            &mut messages,
            &[&section],
            &|| "ctx-1".to_string(),
            &test_profile("claude-sonnet-4-6"),
        );
        assert!(changed);
        assert_eq!(messages.len(), 2);
        let Message::User(user) = &messages[1] else {
            panic!("expected inserted user message");
        };
        assert_eq!(user.uuid, "ctx-1");
        assert_eq!(user.is_meta, Some(true));
        let text = block_text(&user.content[0]).expect("text block");
        assert!(
            text.starts_with("<system-reminder>\nThese background tasks"),
            "{text}"
        );
        assert!(text.contains("bg-1"), "{text}");
        assert!(text.contains("task_output"), "{text}");
        assert_eq!(
            resolve_previous_state(&messages, &section),
            PreviousFragmentState::Known
        );
    }

    #[test]
    fn surfaced_tools_injection_merges_into_trailing_real_user_message() {
        let section = surfaced_section();
        let mut messages = vec![real_user("u-1", vec![plain_block("question")])];
        let changed = reinject_retained_fragments(
            &mut messages,
            &[&section],
            &|| "unused".to_string(),
            &test_profile("claude-sonnet-4-6"),
        );
        assert!(changed);
        assert_eq!(messages.len(), 1);
        let Message::User(user) = &messages[0] else {
            panic!("expected trailing user message");
        };
        assert_eq!(user.is_meta, None);
        assert_eq!(user.content.len(), 2);
        let text = block_text(&user.content[1]).expect("text block");
        assert!(text.starts_with("<system-reminder>\nThese tools"), "{text}");
        assert!(text.contains("extra_tool"), "{text}");
    }

    #[test]
    fn precise_current_state_with_fragment_lost_from_retained_history_forces_reinjection() {
        // The harness holds the exact current value (two running tasks) while
        // the rewritten history no longer contains any fragment of the section.
        // The precise state must not suppress re-injection: the section
        // resolves Absent and the full current value is rendered again.
        let section = RunningTasksSection::from_handles(vec![
            handle("bg-1", BackgroundTaskStatus::Running),
            handle("bg-2", BackgroundTaskStatus::Running),
        ]);
        assert!(section.payload.is_some());
        let mut messages = vec![
            boundary(),
            meta_message(
                "m-1",
                vec![reminder_block("An unrelated retained reminder.")],
            ),
        ];
        assert_eq!(
            resolve_previous_state(&messages, &section),
            PreviousFragmentState::Absent
        );
        let changed = reinject_retained_fragments(
            &mut messages,
            &[&section],
            &|| "gen-3".to_string(),
            &test_profile("claude-sonnet-4-6"),
        );
        assert!(changed);
        assert_eq!(class_block_count(&messages, &section), 1);
        let payload = payload_of(&section);
        assert!(payload.contains("bg-1") && payload.contains("bg-2"));
        assert_eq!(
            resolve_previous_state(&messages, &section),
            PreviousFragmentState::Known
        );
        // The unrelated retained reminder was not touched; the fresh fragment
        // merged into the trailing meta message as an additional block.
        let Message::User(first_meta) = &messages[1] else {
            panic!("unrelated meta message survives");
        };
        assert_eq!(first_meta.content.len(), 2);
        assert_eq!(
            block_text(&first_meta.content[0]).as_deref(),
            Some("An unrelated retained reminder.")
        );
        assert!(block_text(&first_meta.content[1])
            .is_some_and(|text| section.matches_retained_fragment("user", &text)));
    }

    #[test]
    fn live_current_fragment_injects_zero_bytes() {
        let running = running_section();
        let surfaced = surfaced_section();
        let mut messages = vec![boundary()];
        assert!(reinject_retained_fragments(
            &mut messages,
            &[&running, &surfaced],
            &|| "gen-1".to_string(),
            &test_profile("claude-sonnet-4-6"),
        ));
        let snapshot = messages.clone();
        assert!(!reinject_retained_fragments(
            &mut messages,
            &[&running, &surfaced],
            &|| "gen-2".to_string(),
            &test_profile("claude-sonnet-4-6"),
        ));
        assert_eq!(messages, snapshot);
    }

    #[test]
    fn stale_fragment_is_replaced_without_duplication() {
        // Unknown: a fragment of the section survives but cannot be verified
        // against the current value. The stale copy is removed and exactly one
        // current copy is injected.
        let stale_wrapped = wrapped(&payload_of(&stale_running_section()));
        let current = running_section();
        let mut messages = vec![
            meta_message(
                "m-stale",
                vec![
                    reminder_block(&stale_wrapped),
                    reminder_block("An unrelated reminder that must survive."),
                ],
            ),
            boundary(),
        ];
        let changed = reinject_retained_fragments(
            &mut messages,
            &[&current],
            &|| "gen-4".to_string(),
            &test_profile("claude-sonnet-4-6"),
        );
        assert!(changed);
        assert_eq!(class_block_count(&messages, &current), 1);
        let Message::User(stale_message) = &messages[0] else {
            panic!("stale message survives with its unrelated block");
        };
        assert_eq!(stale_message.content.len(), 1);
        assert_eq!(
            block_text(&stale_message.content[0]).as_deref(),
            Some("An unrelated reminder that must survive.")
        );
        // The replacement is idempotent.
        assert!(!reinject_retained_fragments(
            &mut messages,
            &[&current],
            &|| "gen-5".to_string(),
            &test_profile("claude-sonnet-4-6"),
        ));
    }

    #[test]
    fn stale_block_inside_a_real_user_message_is_removed_without_touching_user_text() {
        let stale_wrapped = wrapped(&payload_of(&stale_running_section()));
        let current = running_section();
        let mut messages = vec![
            real_user(
                "u-1",
                vec![plain_block("question"), reminder_block(&stale_wrapped)],
            ),
            boundary(),
        ];
        assert!(reinject_retained_fragments(
            &mut messages,
            &[&current],
            &|| "gen-6".to_string(),
            &test_profile("claude-sonnet-4-6"),
        ));
        let Message::User(user) = &messages[0] else {
            panic!("real user message is kept");
        };
        assert_eq!(user.is_meta, None);
        assert_eq!(user.content.len(), 1);
        assert_eq!(block_text(&user.content[0]).as_deref(), Some("question"));
        assert_eq!(class_block_count(&messages, &current), 1);
    }

    #[test]
    fn role_participates_in_fragment_matching() {
        let section = running_section();
        let echoed = wrapped(&payload_of(&section));
        let messages = vec![assistant("a-1", vec![reminder_block(&echoed)]), boundary()];
        // An assistant-role echo of the fragment is not a live copy.
        assert_eq!(
            resolve_previous_state(&messages, &section),
            PreviousFragmentState::Absent
        );
        assert!(section.matches_retained_fragment("user", &echoed));
        assert!(!section.matches_retained_fragment("assistant", &echoed));
    }

    #[test]
    fn quoted_fragment_text_without_reminder_envelope_is_not_a_live_copy() {
        let section = running_section();
        let payload = payload_of(&section);
        let mut messages = vec![real_user("u-1", vec![plain_block(&payload)]), boundary()];
        assert_eq!(
            resolve_previous_state(&messages, &section),
            PreviousFragmentState::Absent
        );
        assert!(reinject_retained_fragments(
            &mut messages,
            &[&section],
            &|| "gen-7".to_string(),
            &test_profile("claude-sonnet-4-6"),
        ));
        // The quoted copy is untouched; the fresh copy is the only class match
        // carrying the reminder envelope.
        assert_eq!(class_block_count(&messages, &section), 1);
    }

    #[test]
    fn sections_resolve_independently_in_registration_order() {
        let running = running_section();
        let surfaced = surfaced_section();
        let profile = test_profile("claude-sonnet-4-6");
        let mut messages = vec![boundary()];
        // First rewrite only brought the running fragment back.
        assert!(reinject_retained_fragments(
            &mut messages,
            &[&running],
            &|| "gen-8".to_string(),
            &profile,
        ));
        // Second pass: running is still visible (zero bytes), surfaced was
        // lost and is appended after it, preserving registration order.
        assert!(reinject_retained_fragments(
            &mut messages,
            &[&running, &surfaced],
            &|| "gen-9".to_string(),
            &profile,
        ));
        assert_eq!(messages.len(), 2);
        let Message::User(user) = &messages[1] else {
            panic!("expected reminder message");
        };
        assert_eq!(user.content.len(), 2);
        assert!(block_text(&user.content[0])
            .unwrap()
            .contains("These background tasks"));
        assert!(block_text(&user.content[1])
            .unwrap()
            .contains("These tools you previously found"));
    }

    #[test]
    fn empty_current_value_injects_nothing_and_leaves_surviving_copies_alone() {
        let done = RunningTasksSection::from_handles(vec![handle(
            "bg-9",
            BackgroundTaskStatus::Completed,
        )]);
        assert!(done.render_current().is_none());
        let stale_wrapped = wrapped(&payload_of(&stale_running_section()));
        let mut messages = vec![
            meta_message("m-old", vec![reminder_block(&stale_wrapped)]),
            boundary(),
        ];
        let before = messages.clone();
        assert!(!reinject_retained_fragments(
            &mut messages,
            &[&done],
            &|| "gen-10".to_string(),
            &test_profile("claude-sonnet-4-6"),
        ));
        assert_eq!(messages, before);
    }

    #[test]
    fn currency_check_is_envelope_shape_agnostic() {
        let section = running_section();
        let payload = payload_of(&section);
        // Generic style injects the payload bare.
        assert!(section.matches_current_retained_fragment("user", &payload));
        // Anthropic style wraps it.
        assert!(section.matches_current_retained_fragment("user", &wrapped(&payload)));
        // A leading envelope header also verifies.
        assert!(section
            .matches_current_retained_fragment("user", &format!("# System Reminder\n\n{payload}")));
        // A stale payload never verifies as current.
        assert!(!section.matches_current_retained_fragment(
            "user",
            &wrapped(&payload_of(&stale_running_section()))
        ));
        // End-to-end: a fragment injected under one profile stays verifiable.
        let mut messages = vec![boundary()];
        assert!(reinject_retained_fragments(
            &mut messages,
            &[&section],
            &|| "gen-11".to_string(),
            &test_profile("fake-model"),
        ));
        assert_eq!(
            resolve_previous_state(&messages, &section),
            PreviousFragmentState::Known
        );
    }
}
