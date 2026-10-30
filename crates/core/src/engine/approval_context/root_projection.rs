// Copyright 2025 OpenAI. Licensed under the Apache License, Version 2.0.
// SPDX-License-Identifier: Apache-2.0
// Modified: root evidence is selected from the runtime's captured native history.

//! Selects bounded root evidence using retained identities and acceptance order.

use serde::Deserialize;
use std::collections::VecDeque;

use super::history_types::{
    ReconciledRetainedContext, RetainedContextEntry, RetainedContextOrder, RetainedUserMessage,
    UserInputOrigin,
};
use super::model::{ContentItem, MessagePhase, ResponseItem};
use super::{
    render_verified_answer, render_verified_answers, retained_assistant_message, truncate_text,
    MessageHistory, RootMessage,
};

const MAX_ROOT_MESSAGES: usize = 16;
const MAX_ROOT_MESSAGE_TOKENS: usize = 900;
const SUMMARY_PREFIX: &str = include_str!("assets/history_summary_prefix.md");

/// Omission of root instructions also invalidates authorization completeness.
pub struct RootConversationProjection {
    pub messages: Vec<RootMessage>,
    pub latest_user_turn_id: Option<String>,
    pub retained_context_complete: bool,
}

pub fn root_conversation_for_snapshot(history: &MessageHistory) -> Vec<RootMessage> {
    root_conversation_snapshot(history).messages
}

pub fn root_conversation_snapshot(history: &MessageHistory) -> RootConversationProjection {
    root_conversation_snapshot_with_legacy(history, &[])
}

/// Legacy inputs must come from the host's saved authorization backup, not an unsequenced live item.
pub fn root_conversation_snapshot_with_legacy(
    history: &MessageHistory,
    legacy_user_messages: &[RetainedUserMessage],
) -> RootConversationProjection {
    let retained = &history.retained;
    let reconciled = ReconciledRetainedContext::new(
        Some(retained),
        history.items.iter().filter_map(|envelope| {
            let text = user_text(&envelope.item)?;
            if !eligible_user_text(&text) || is_contextual_user_text(&text) {
                return None;
            }
            let order = envelope
                .metadata
                .as_ref()
                .filter(|metadata| !metadata.inherited_user_message)
                .and_then(|metadata| metadata.user_input_order);
            Some((
                order,
                RetainedUserMessage {
                    phase: None,
                    origin: UserInputOrigin::from_message(&envelope.item),
                    turn_id: envelope.item.turn_id().unwrap_or_default().to_owned(),
                    message_id: envelope.item.id().map(|id| id.as_str().to_owned()),
                    text,
                    complete: false,
                },
            ))
        }),
    );
    let mut missing_root = reconciled.missing_user_messages;
    let mut messages = reconciled
        .ordered_entries()
        .filter_map(|(order, entry)| match entry {
            RetainedContextEntry::UserMessage(message) => {
                let text = if message.text.is_empty() && !message.complete {
                    let original = message.message_id.as_deref().and_then(|id| {
                        history.items.iter().find(|envelope| {
                            envelope
                                .item
                                .id()
                                .is_some_and(|source| source.as_str() == id)
                        })
                    });
                    let recovered = match original {
                        Some(item) => user_text(&item.item),
                        None => message.message_id.as_deref().and_then(|id| {
                            legacy_user_messages
                                .iter()
                                .find(|source| source.message_id.as_deref() == Some(id))
                                .map(|source| source.text.clone())
                        }),
                    };
                    let Some(text) = recovered else {
                        missing_root = true;
                        return None;
                    };
                    text
                } else {
                    message.text.clone()
                };
                let goal = is_goal_update(&text);
                missing_root |= goal && !message.complete;
                if is_contextual_user_text(&text) && !goal {
                    return None;
                }
                eligible_user_text(&text).then(|| {
                    (
                        Some(order),
                        RootMessage::User(truncate_text(&text, MAX_ROOT_MESSAGE_TOKENS)),
                    )
                })
            }
            RetainedContextEntry::VerifiedAnswer(answer) => render_verified_answer(answer)
                .map(|text| (Some(order), RootMessage::UserInput(text))),
            RetainedContextEntry::AssistantMessage(_) => None,
        })
        .collect::<Vec<_>>();
    let mut latest_user_turn_id = reconciled.latest_user_turn_id.clone();
    let mut legacy_messages = VecDeque::new();
    let legacy_capacity = MAX_ROOT_MESSAGES.saturating_sub(messages.len());
    if missing_root && legacy_capacity > 0 {
        let mut latest_legacy_turn_id = None;
        for message in reconciled.unmatched_user_messages(legacy_user_messages.iter().cloned()) {
            latest_legacy_turn_id = (!message.turn_id.is_empty()).then_some(message.turn_id);
            legacy_messages.push_back(RootMessage::User(message.text));
            if legacy_messages.len() > legacy_capacity {
                legacy_messages.pop_front();
            }
        }
        if latest_user_turn_id.is_none() {
            latest_user_turn_id = latest_legacy_turn_id;
        }
    }
    let mut missing_assistant = retained.has_omitted_assistant_messages();
    let live_assistants = history
        .items
        .iter()
        .filter(|item| {
            !item
                .metadata
                .as_ref()
                .is_some_and(|metadata| metadata.compaction_output)
        })
        .filter_map(|envelope| {
            let text = assistant_text(&envelope.item)?;
            let id = envelope.item.id().map(|id| id.as_str());
            let original = reconciled.ordered_entries().find_map(|(order, entry)| {
                let RetainedContextEntry::AssistantMessage(message) = entry else {
                    return None;
                };
                id.filter(|id| message.message_id.as_deref() == Some(*id))
                    .map(|_| (order, message))
            });
            if let Some((order, message)) = original {
                if retained_assistant_message(message).is_some() {
                    return Some((
                        id,
                        Some(order),
                        RootMessage::Assistant(message.text.clone()),
                    ));
                }
            }
            let order = envelope
                .metadata
                .as_ref()
                .filter(|metadata| !metadata.inherited_user_message)
                .and_then(|metadata| metadata.user_input_order)
                .map(RetainedContextOrder::Local);
            let order = original.map(|(order, _)| order).or(order);
            let message = if order.is_some() {
                RootMessage::Assistant(text)
            } else {
                RootMessage::UnorderedAssistant(text)
            };
            Some((id, order, message))
        })
        .collect::<Vec<_>>();
    let mut assistants = reconciled
        .ordered_entries()
        .filter_map(|(order, entry)| {
            let RetainedContextEntry::AssistantMessage(message) = entry else {
                return None;
            };
            if message.phase == Some(MessagePhase::Commentary)
                || message.message_id.as_deref().is_some_and(|id| {
                    history.items.iter().any(|item| {
                        matches!(&item.item, ResponseItem::Message {
                            id: Some(source), phase: Some(MessagePhase::Commentary), ..
                        } if source.as_str() == id)
                    })
                })
            {
                return None;
            }
            if message.message_id.as_deref().is_some_and(|id| {
                live_assistants
                    .iter()
                    .any(|(live_id, _, _)| *live_id == Some(id))
            }) {
                return None;
            }
            let rendered = retained_assistant_message(message)
                .map(|_| RootMessage::Assistant(message.text.clone()));
            missing_assistant |= rendered.is_none();
            rendered.map(|message| (Some(order), message))
        })
        .collect::<Vec<_>>();
    assistants.extend(
        live_assistants
            .into_iter()
            .map(|(_, order, message)| (order, message)),
    );
    messages.extend(assistants);
    messages.sort_by_key(|(order, _)| *order);
    let removed = messages.len().saturating_sub(MAX_ROOT_MESSAGES);
    missing_root |= messages[..removed]
        .iter()
        .any(|(_, message)| matches!(message, RootMessage::User(_) | RootMessage::UserInput(_)));
    missing_assistant |= messages[..removed].iter().any(|(_, message)| {
        matches!(
            message,
            RootMessage::Assistant(_) | RootMessage::UnorderedAssistant(_)
        )
    });
    messages.drain(..removed);
    messages.sort_by_key(|(order, _)| (order.is_none(), *order));
    legacy_messages.drain(
        ..legacy_messages
            .len()
            .saturating_sub(MAX_ROOT_MESSAGES - messages.len()),
    );
    let mut messages = messages
        .into_iter()
        .map(|(_, message)| message)
        .collect::<Vec<_>>();
    if missing_assistant {
        messages.insert(0, RootMessage::IncompleteAssistantContext);
    }
    let verified_complete = render_verified_answers(retained).complete;
    if !verified_complete {
        messages.insert(0, RootMessage::IncompleteVerifiedAnswers);
    }
    if missing_root {
        messages.insert(0, RootMessage::IncompleteRootInstructions);
    }
    messages.insert(0, RootMessage::RetainedContextScope);
    if !legacy_messages.is_empty() {
        legacy_messages.push_front(RootMessage::LegacyContextScope);
        legacy_messages.extend(messages);
        messages = legacy_messages.into_iter().collect();
    }
    RootConversationProjection {
        messages,
        latest_user_turn_id,
        retained_context_complete: verified_complete && !missing_root,
    }
}

fn eligible_user_text(text: &str) -> bool {
    !text.starts_with(&format!("{SUMMARY_PREFIX}\n"))
        && !text.trim_start().starts_with("<user_action>")
}

fn user_text(item: &ResponseItem) -> Option<String> {
    let ResponseItem::Message { role, content, .. } = item else {
        return None;
    };
    if role != "user"
        || content.iter().any(
            |part| matches!(part, ContentItem::InputText { text } if is_contextual_user_text(text)),
        )
    {
        return None;
    }
    let text = content
        .iter()
        .enumerate()
        .filter_map(|(index, part)| {
            let ContentItem::InputText { text } = part else {
                return None;
            };
            let image_label = ((text == "<image>"
                || (text.starts_with("<image name=") && text.ends_with('>')))
                && matches!(content.get(index + 1), Some(ContentItem::InputImage { .. })))
                || (text == "</image>"
                    && index > 0
                    && matches!(content.get(index - 1), Some(ContentItem::InputImage { .. })));
            let audio_label = ((text == "<audio>"
                || (text.starts_with("<audio name=") && text.ends_with('>')))
                && matches!(content.get(index + 1), Some(ContentItem::InputAudio { .. })))
                || (text == "</audio>"
                    && index > 0
                    && matches!(content.get(index - 1), Some(ContentItem::InputAudio { .. })));
            (!image_label && !audio_label).then_some(text.as_str())
        })
        .collect::<String>();
    Some(text)
}

fn assistant_text(item: &ResponseItem) -> Option<String> {
    let ResponseItem::Message {
        role,
        content,
        phase,
        ..
    } = item
    else {
        return None;
    };
    if role != "assistant" || *phase == Some(MessagePhase::Commentary) {
        return None;
    }
    let text = content
        .iter()
        .filter_map(|part| match part {
            ContentItem::InputText { text } | ContentItem::OutputText { text, .. } => {
                Some(text.as_str())
            }
            _ => None,
        })
        .collect::<String>();
    Some(truncate_text(&text, MAX_ROOT_MESSAGE_TOKENS))
}

fn marked_text(text: &str, start: &str, end: &str) -> bool {
    if start.is_empty() || end.is_empty() {
        return false;
    }
    let text = text.trim_start();
    if !text
        .get(..start.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(start))
    {
        return false;
    }
    let text = text.trim_end();
    text.len()
        .checked_sub(end.len())
        .and_then(|start| text.get(start..))
        .is_some_and(|suffix| suffix.eq_ignore_ascii_case(end))
}

fn is_goal_update(text: &str) -> bool {
    marked_text(
        text,
        "<codex_internal_context source=\"user_goal\">",
        "</codex_internal_context>",
    )
}

#[derive(Deserialize)]
#[serde(rename = "hook_prompt")]
struct HookPromptXml {
    #[serde(rename = "@hook_run_id")]
    hook_run_id: String,
    #[serde(rename = "$text")]
    text: String,
}

fn hook_prompt_text(text: &str) -> Option<String> {
    let fragment: HookPromptXml = quick_xml::de::from_str(text.trim()).ok()?;
    (!fragment.hook_run_id.trim().is_empty()).then_some(fragment.text)
}

fn is_contextual_user_text(text: &str) -> bool {
    const MARKERS: &[(&str, &str)] = &[
        ("# AGENTS.md instructions", "</INSTRUCTIONS>"),
        ("<environment_context>", "</environment_context>"),
        (
            "<agent_message_board_notification>",
            "</agent_message_board_notification>",
        ),
        ("<skill>", "</skill>"),
        ("<user_shell_command>", "</user_shell_command>"),
        ("<turn_aborted>", "</turn_aborted>"),
        ("<subagent_notification>", "</subagent_notification>"),
        ("<recommended_plugins>", "</recommended_plugins>"),
    ];
    if MARKERS
        .iter()
        .any(|(start, end)| marked_text(text, start, end))
        || hook_prompt_text(text).is_some()
    {
        return true;
    }
    let text = text.trim();
    if let Some(rest) = text.strip_prefix("<external_") {
        if let Some((key, rest)) = rest.split_once('>') {
            if rest.ends_with(&format!("</external_{key}>")) {
                return true;
            }
        }
    }
    if text
        .starts_with("Warning: The maximum number of unified exec processes you can keep open is")
        || (text.starts_with("Warning: apply_patch was requested via ")
            && text.ends_with("Use the apply_patch tool instead of exec_command."))
        || text.starts_with(
            "Warning: Your account was flagged for potentially high-risk cyber activity",
        )
        || (text.starts_with("<goal_context>") && text.ends_with("</goal_context>"))
    {
        return true;
    }
    let Some(rest) = text.strip_prefix("<codex_internal_context source=\"") else {
        return false;
    };
    let Some((source, body)) = rest.split_once("\">") else {
        return false;
    };
    source
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_lowercase)
        && source
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        && body.ends_with("</codex_internal_context>")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::approval_context::history_types::{
        ResponseItemEnvelope, RetainedContextEvent, RetainedInputSource, ReviewMetadata,
        VerifiedAnswer, VerifiedQuestionAnswer,
    };
    use crate::engine::approval_context::model::MessageMetadata;

    fn original(id: &str, text: &str) -> RetainedUserMessage {
        RetainedUserMessage {
            turn_id: format!("turn-{id}"),
            message_id: Some(id.to_owned()),
            text: text.to_owned(),
            complete: true,
            origin: UserInputOrigin::User,
            phase: None,
        }
    }

    fn live(
        role: &str,
        id: &str,
        text: &str,
        phase: Option<MessagePhase>,
        order: Option<u64>,
    ) -> ResponseItemEnvelope {
        ResponseItemEnvelope {
            item: ResponseItem::Message {
                status: None,
                encrypted_content: None,
                id: Some(id.into()),
                role: role.to_owned(),
                content: vec![ContentItem::InputText {
                    text: text.to_owned(),
                }],
                phase,
                internal_chat_message_metadata_passthrough: Some(MessageMetadata {
                    turn_id: Some(format!("turn-{id}")),
                    ..Default::default()
                }),
            },
            metadata: Some(ReviewMetadata {
                user_input_order: order,
                ..Default::default()
            }),
        }
    }

    fn records(messages: &[RootMessage]) -> Vec<&RootMessage> {
        messages
            .iter()
            .filter(|message| {
                matches!(
                    message,
                    RootMessage::User(_)
                        | RootMessage::UserInput(_)
                        | RootMessage::Assistant(_)
                        | RootMessage::UnorderedAssistant(_)
                )
            })
            .collect()
    }

    #[test]
    fn root_limit_is_shared_across_users_verified_answers_and_assistants() {
        let mut history = MessageHistory::default();
        for index in 0..8 {
            history.retained.record_user_message(
                original(&format!("user-{index}"), "root restriction"),
                RetainedInputSource::Local(Some(index)),
            );
            history.retained.record_assistant_message(
                original(&format!("assistant-{index}"), "ordinary context"),
                RetainedInputSource::Local(Some(8 + index)),
            );
            history
                .retained
                .record(&RetainedContextEvent::VerifiedAnswer {
                    answer: VerifiedAnswer {
                        turn_id: "answer-turn".to_owned(),
                        call_id: format!("answer-{index}"),
                        questions: vec![VerifiedQuestionAnswer {
                            question: "Approve this exact action?".to_owned(),
                            answer: "No.".to_owned(),
                        }],
                    },
                    acceptance_order: Some(16 + index),
                });
        }
        let projected = root_conversation_snapshot(&history);
        assert_eq!(records(&projected.messages).len(), 16);
        assert_eq!(
            records(&projected.messages)
                .iter()
                .filter(|message| matches!(message, RootMessage::UserInput(_)))
                .count(),
            8
        );
        assert!(projected
            .messages
            .contains(&RootMessage::IncompleteRootInstructions));
        assert!(!projected.retained_context_complete);
        assert_eq!(
            projected.latest_user_turn_id.as_deref(),
            Some("turn-user-7")
        );
    }

    #[test]
    fn root_projection_prefers_original_assistant_context_and_excludes_commentary() {
        let mut history = MessageHistory::default();
        history.retained.record_user_message(
            original("user", "Only prepare a draft."),
            RetainedInputSource::Local(Some(1)),
        );
        history.retained.record_assistant_message(
            original("reply", "The complete original question."),
            RetainedInputSource::Local(Some(2)),
        );
        history.retained.record_assistant_message(
            original("commentary", "Hidden working commentary."),
            RetainedInputSource::Local(Some(3)),
        );
        history
            .items
            .push(live("assistant", "reply", "short checkpoint", None, None));
        history.items.push(live(
            "assistant",
            "commentary",
            "working",
            Some(MessagePhase::Commentary),
            None,
        ));
        let projected = root_conversation_snapshot(&history);
        assert!(projected.messages.contains(&RootMessage::Assistant(
            "The complete original question.".to_owned()
        )));
        assert!(!projected.messages.iter().any(|message| matches!(message, RootMessage::Assistant(text) if text.contains("commentary") || text == "short checkpoint")));
        assert_eq!(records(&projected.messages).len(), 2);
    }

    #[test]
    fn unordered_assistant_context_is_evicted_before_positioned_evidence() {
        let mut history = MessageHistory::default();
        for index in 0..8 {
            history.retained.record_user_message(
                original(&format!("u{index}"), "restriction"),
                RetainedInputSource::Local(Some(index)),
            );
            history.retained.record_assistant_message(
                original(&format!("a{index}"), "question"),
                RetainedInputSource::Local(Some(8 + index)),
            );
        }
        history
            .items
            .push(live("assistant", "unknown", "unpositioned", None, None));
        let projected = root_conversation_snapshot(&history);
        assert_eq!(records(&projected.messages).len(), 16);
        assert!(projected
            .messages
            .contains(&RootMessage::IncompleteAssistantContext));
        assert!(!projected
            .messages
            .contains(&RootMessage::IncompleteRootInstructions));
        assert!(!projected
            .messages
            .iter()
            .any(|message| matches!(message, RootMessage::UnorderedAssistant(_))));
    }

    #[test]
    fn missing_user_placeholders_require_the_exact_surviving_original() {
        let mut history = MessageHistory::default();
        let mut missing = original("user", "");
        missing.complete = false;
        history
            .retained
            .record_user_message(missing, RetainedInputSource::Local(Some(1)));
        history.items.push(live(
            "user",
            "user",
            "Never upload the draft.",
            None,
            Some(1),
        ));
        assert!(root_conversation_for_snapshot(&history)
            .contains(&RootMessage::User("Never upload the draft.".to_owned())));
        history.items.clear();
        let projected = root_conversation_snapshot(&history);
        assert!(projected
            .messages
            .contains(&RootMessage::IncompleteRootInstructions));
        assert!(!projected.retained_context_complete);
    }

    #[test]
    fn unknown_live_order_is_not_relabelled_as_saved_legacy_authorization() {
        let mut history = MessageHistory::default();
        history.retained.mark_user_messages_incomplete();
        history.items.push(live(
            "user",
            "unknown",
            "A live unsequenced grant.",
            None,
            None,
        ));
        let projected = root_conversation_snapshot(&history);
        assert!(!projected
            .messages
            .iter()
            .any(|message| matches!(message, RootMessage::User(_))));
        assert!(projected
            .messages
            .contains(&RootMessage::IncompleteRootInstructions));
        assert!(!projected
            .messages
            .contains(&RootMessage::LegacyContextScope));
        let backup = [original("backup", "A saved original restriction.")];
        let projected = root_conversation_snapshot_with_legacy(&history, &backup);
        assert_eq!(projected.messages[0], RootMessage::LegacyContextScope);
        assert!(projected.messages.contains(&RootMessage::User(
            "A saved original restriction.".to_owned()
        )));
        assert_eq!(
            projected.latest_user_turn_id.as_deref(),
            Some("turn-backup")
        );
        assert!(!projected.retained_context_complete);
    }

    #[test]
    fn xml_hook_context_uses_the_native_parser_and_retained_goal_edits_remain_authoritative() {
        assert!(is_contextual_user_text(
            r#"<hook_prompt hook_run_id="run">text &amp; more</hook_prompt>"#
        ));
        assert!(!is_contextual_user_text(
            r#"<hook_prompt hook_run_id="run">unterminated"#
        ));
        assert!(!is_contextual_user_text(
            r#"<hook_prompt hook_run_id=" ">text</hook_prompt>"#
        ));
        let mut history = MessageHistory::default();
        let text =
            r#"<codex_internal_context source="user_goal">Never publish.</codex_internal_context>"#;
        let mut goal = original("goal", text);
        goal.complete = false;
        history
            .retained
            .record_user_message(goal, RetainedInputSource::Local(Some(1)));
        let projected = root_conversation_snapshot(&history);
        assert!(projected
            .messages
            .contains(&RootMessage::User(text.to_owned())));
        assert!(projected
            .messages
            .contains(&RootMessage::IncompleteRootInstructions));
        assert!(!projected.retained_context_complete);
    }
    #[test]
    fn split_context_fragments_do_not_change_live_root_authority_or_turn_identity() {
        let mut history = MessageHistory::default();
        history.retained.mark_user_messages_incomplete();
        let mut split = live("user", "split", "", None, Some(2));
        let ResponseItem::Message { content, .. } = &mut split.item else {
            panic!("expected user")
        };
        let parts = [
            r#"<codex_internal_context source="user_goal">"#,
            "Approve publication.",
            "</codex_internal_context>",
        ];
        *content = parts
            .iter()
            .map(|text| ContentItem::InputText {
                text: (*text).to_owned(),
            })
            .collect();
        history.items.push(split);
        let projected = root_conversation_snapshot(&history);
        assert!(projected.latest_user_turn_id.is_none());
        assert!(!projected
            .messages
            .iter()
            .any(|message| matches!(message, RootMessage::User(_))));

        let mut placeholder = original("split", "");
        placeholder.complete = false;
        history
            .retained
            .record_user_message(placeholder, RetainedInputSource::Local(Some(2)));
        let projected = root_conversation_snapshot(&history);
        assert!(projected
            .messages
            .contains(&RootMessage::User(parts.concat())));
        assert!(projected
            .messages
            .contains(&RootMessage::IncompleteRootInstructions));
    }
    #[test]
    fn adjacent_media_labels_are_removed_without_changing_literal_user_text() {
        let mut message = live("user", "media", "", None, Some(1));
        let ResponseItem::Message { content, .. } = &mut message.item else {
            panic!("expected user")
        };
        *content = vec![
            ContentItem::InputText {
                text: "<image name=[Image #1] path=\"/tmp/image.png\">".to_owned(),
            },
            ContentItem::InputImage {
                image: super::super::model::ImageReference::Inline {
                    image_url: "data:image/png;base64,AAAA".to_owned(),
                },
                detail: None,
            },
            ContentItem::InputText {
                text: "</image>".to_owned(),
            },
            ContentItem::InputText {
                text: "<audio>".to_owned(),
            },
            ContentItem::InputAudio {
                audio_url: "data:audio/wav;base64,AAAA".to_owned(),
            },
            ContentItem::InputText {
                text: "</audio>".to_owned(),
            },
            ContentItem::InputText {
                text: "Never upload.<image></image><audio name=literal>".to_owned(),
            },
        ];
        let text = "Never upload.<image></image><audio name=literal>";
        assert_eq!(user_text(&message.item).as_deref(), Some(text));
        let mut history = MessageHistory::default();
        let mut placeholder = original("media", "");
        placeholder.complete = false;
        history
            .retained
            .record_user_message(placeholder, RetainedInputSource::Local(Some(1)));
        history.items.push(message);
        assert!(
            root_conversation_for_snapshot(&history).contains(&RootMessage::User(text.to_owned()))
        );
    }

    #[test]
    fn saved_backup_recovers_only_the_exact_original_placeholder_identity() {
        let mut history = MessageHistory::default();
        let mut placeholder = original("missing", "");
        placeholder.complete = false;
        history
            .retained
            .record_user_message(placeholder, RetainedInputSource::Local(Some(1)));
        let backup = [original("missing", "Never upload the draft.")];
        let projected = root_conversation_snapshot_with_legacy(&history, &backup);
        assert!(projected
            .messages
            .contains(&RootMessage::User("Never upload the draft.".to_owned())));
        assert!(!projected
            .messages
            .contains(&RootMessage::LegacyContextScope));
        let unrelated = [original("other", "A different original.")];
        let projected = root_conversation_snapshot_with_legacy(&history, &unrelated);
        assert!(!projected
            .messages
            .contains(&RootMessage::User("Never upload the draft.".to_owned())));
        assert!(projected
            .messages
            .contains(&RootMessage::IncompleteRootInstructions));
    }
}
