//! Resolve the user's "ultracode" keyword into coordinated work guidance.
//! Delegation remains subject to the current collaboration mode and tool pool.

use crate::context::ultrathink_reminder::{contains_ascii_word_ignore_case, latest_user_text};
use crate::runtime_values::RuntimeString;
use crate::types::reminder::{ReminderContext, ReminderProvider};
use async_trait::async_trait;

const ULTRACODE_NUDGE: &str = "The user requested a coordinated multi-agent approach for this turn. Use the available collaboration tools within the current delegation mode; a workflow program is optional.";

pub struct UltracodeReminderProvider;

impl UltracodeReminderProvider {
    pub fn tick(&self, ctx: &ReminderContext) -> Option<RuntimeString> {
        let text = latest_user_text(&ctx.messages)?;
        if contains_ascii_word_ignore_case(&text, b"ultracode") {
            Some(RuntimeString::from(ULTRACODE_NUDGE))
        } else {
            None
        }
    }
}

#[async_trait]
impl ReminderProvider for UltracodeReminderProvider {
    async fn reminder(&self, ctx: &ReminderContext) -> Option<RuntimeString> {
        self.tick(ctx)
    }
}

/// Build an ultracode reminder provider.
pub fn create_ultracode_reminder_provider() -> UltracodeReminderProvider {
    UltracodeReminderProvider
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::content::{ContentBlock, TextBlock};
    use crate::schemas::message::{Message, UserMessage};

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
        let p = create_ultracode_reminder_provider();
        let out = p.tick(&ctx(vec![user("let's ultracode this")])).unwrap();
        assert!(out.contains("current delegation mode"));
        assert!(out.contains("workflow program is optional"));
    }

    #[test]
    fn case_insensitive() {
        let p = create_ultracode_reminder_provider();
        assert!(p.tick(&ctx(vec![user("ULTRACODE please")])).is_some());
    }

    #[test]
    fn null_without_keyword_or_boundary() {
        let p = create_ultracode_reminder_provider();
        assert_eq!(p.tick(&ctx(vec![user("just code it")])), None);
        assert_eq!(p.tick(&ctx(vec![user("ultracoders unite")])), None);
    }
}
