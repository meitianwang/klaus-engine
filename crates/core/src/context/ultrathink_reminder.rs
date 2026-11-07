//! Reasoning-effort nudge when the latest user message says "ultrathink"
//! (ultrathink → high-effort system-reminder). A text-only effort bump; it
//! does NOT mutate the wire thinking budget.

use crate::runtime_values::RuntimeString;
use crate::schemas::content::ContentBlock;
use crate::schemas::message::Message;
use crate::types::reminder::{ReminderContext, ReminderProvider};
use async_trait::async_trait;

const ULTRATHINK_NUDGE: &str =
    "The user asked you to \"ultrathink\" — invest your maximum reasoning effort on this turn before you respond.";

/// Concatenate the text blocks of the most recent user message, or `None` when
/// there is no user message.
pub(crate) fn latest_user_text(messages: &[Message]) -> Option<RuntimeString> {
    messages.iter().rev().find_map(|m| match m {
        Message::User(u) => Some(runtime_join(
            u.content.iter().map(|block| match block {
                ContentBlock::Text(text) => text.text.clone(),
                _ => RuntimeString::default(),
            }),
            " ",
        )),
        _ => None,
    })
}

fn runtime_join(values: impl IntoIterator<Item = RuntimeString>, separator: &str) -> RuntimeString {
    let separator = RuntimeString::from(separator);
    let mut values = values.into_iter();
    let Some(mut joined) = values.next() else {
        return RuntimeString::default();
    };
    for value in values {
        joined = joined.concat(&separator).concat(&value);
    }
    joined
}

pub(crate) fn contains_ascii_word_ignore_case(text: &RuntimeString, needle: &[u8]) -> bool {
    let units = text.as_utf16();
    if units.len() < needle.len() {
        return false;
    }
    (0..=units.len() - needle.len()).any(|start| {
        let matches = units[start..start + needle.len()]
            .iter()
            .zip(needle)
            .all(|(unit, expected)| ascii_lower(*unit) == u16::from(*expected));
        matches
            && (start == 0 || !is_ascii_word(units[start - 1]))
            && (start + needle.len() == units.len() || !is_ascii_word(units[start + needle.len()]))
    })
}

fn ascii_lower(unit: u16) -> u16 {
    if (u16::from(b'A')..=u16::from(b'Z')).contains(&unit) {
        unit + u16::from(b'a' - b'A')
    } else {
        unit
    }
}

fn is_ascii_word(unit: u16) -> bool {
    (u16::from(b'a')..=u16::from(b'z')).contains(&ascii_lower(unit))
        || (u16::from(b'0')..=u16::from(b'9')).contains(&unit)
        || unit == u16::from(b'_')
}

pub struct UltrathinkReminderProvider;

impl UltrathinkReminderProvider {
    pub fn tick(&self, ctx: &ReminderContext) -> Option<RuntimeString> {
        let text = latest_user_text(&ctx.messages)?;
        if contains_ascii_word_ignore_case(&text, b"ultrathink") {
            Some(RuntimeString::from(ULTRATHINK_NUDGE))
        } else {
            None
        }
    }
}

#[async_trait]
impl ReminderProvider for UltrathinkReminderProvider {
    async fn reminder(&self, ctx: &ReminderContext) -> Option<RuntimeString> {
        self.tick(ctx)
    }
}

/// Build an ultrathink reminder provider.
pub fn create_ultrathink_reminder_provider() -> UltrathinkReminderProvider {
    UltrathinkReminderProvider
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::content::TextBlock;
    use crate::schemas::message::{AssistantMessage, UserMessage};

    fn user(text: &str) -> Message {
        Message::User(UserMessage {
            question_answer: None,
            uuid: "u".into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: text.into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
        })
    }

    fn assistant(text: &str) -> Message {
        Message::Assistant(AssistantMessage {
            uuid: "a".into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: text.into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            model: "m".into(),
            usage: None,
        })
    }

    fn ctx(messages: Vec<Message>) -> ReminderContext {
        ReminderContext {
            messages,
            cwd: "/tmp".into(),
            task_list_id: None,
            task_plan: None,
            signal: None,

            plan_file_path: None,
            plan_file_exists: None,
            has_task_tool: false,
        }
    }

    #[test]
    fn nudges_on_keyword() {
        let p = create_ultrathink_reminder_provider();
        let out = p
            .tick(&ctx(vec![user("please ultrathink this design")]))
            .unwrap();
        assert!(out.contains("maximum reasoning effort"));
    }

    #[test]
    fn case_insensitive() {
        let p = create_ultrathink_reminder_provider();
        assert!(p.tick(&ctx(vec![user("ULTRATHINK now")])).is_some());
    }

    #[test]
    fn null_without_keyword() {
        let p = create_ultrathink_reminder_provider();
        assert_eq!(p.tick(&ctx(vec![user("just think about it")])), None);
    }

    #[test]
    fn respects_word_boundaries() {
        let p = create_ultrathink_reminder_provider();
        assert_eq!(p.tick(&ctx(vec![user("keep ultrathinking")])), None);
    }

    #[test]
    fn reads_latest_user_message() {
        let p = create_ultrathink_reminder_provider();
        let msgs = vec![
            user("ultrathink the first task"),
            assistant("ok"),
            user("now do something ordinary"),
        ];
        assert_eq!(p.tick(&ctx(msgs)), None);
    }

    #[test]
    fn null_without_user_message() {
        let p = create_ultrathink_reminder_provider();
        assert_eq!(p.tick(&ctx(vec![assistant("hi")])), None);
    }
}
