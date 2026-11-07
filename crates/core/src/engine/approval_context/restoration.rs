// Copyright 2025 OpenAI. Licensed under the Apache License, Version 2.0.
// SPDX-License-Identifier: Apache-2.0
// Modified: retained review evidence is restored through exact host message associations.

//! Restores bounded originals after compaction without treating model text as delivery proof.

use crate::schemas::message::Message;
use crate::types::provider::SystemPromptPart;

use super::bridge::{model_messages_with_delivery, ReviewMessageDelivery};
use super::model::{ContentItem, ContentItemKind, ResponseItem, TruncationPolicy};
use super::{
    ComposedContext, HistoryTruncation, RequestBudget, SectionError, TruncationObservation,
    BUDGET_OMISSION,
};

/// Missing records selected against the complete request's remaining input allowance.
/// The caller appends these records and persists them only after its final request check.
pub struct RestoredReviewMessages {
    pub messages: Vec<Message>,
    pub system: Vec<SystemPromptPart>,
    pub truncations: Vec<TruncationObservation>,
}

/// One review attempt's frozen originals and bounded host-only delivery associations.
/// Removed history records are discarded after successful preparation; candidate additions
/// count as delivered only when the next actual request contains their exact message.
#[derive(Clone)]
pub struct RetainedMessageRestorer {
    retained: ComposedContext,
    delivery: ReviewMessageDelivery,
}

impl RetainedMessageRestorer {
    pub fn new(retained: &ComposedContext, delivery: &ReviewMessageDelivery) -> Self {
        Self {
            retained: retained.retained_instructions(),
            delivery: delivery.clone(),
        }
    }

    /// Captures only delivery proof still present in the history saved for reuse.
    pub fn delivery_for_history(&self, current: &[Message]) -> ReviewMessageDelivery {
        self.delivery.for_history(current)
    }

    /// `budget.existing_context_tokens` includes the actual request's tools and output schema.
    /// The caller rechecks that complete request after these missing records are appended.
    pub fn prepare(
        &mut self,
        current: &[Message],
        budget: RequestBudget,
    ) -> Result<RestoredReviewMessages, SectionError> {
        let mut surviving = self.delivery.for_history(current);
        let history = surviving.annotated_history(current);
        let mut context = self.retained.clone();
        context.retain_new_instructions(&history);
        let context = context.enforce_budget(
            budget,
            BUDGET_OMISSION.to_owned(),
            HistoryTruncation::Preserve,
        )?;
        let truncations = context.truncations.clone();
        let mut envelopes = context.into_annotated_messages();
        for envelope in &mut envelopes {
            let ResponseItem::Message {
                role,
                content,
                internal_chat_message_metadata_passthrough,
                ..
            } = &mut envelope.item
            else {
                return Err(SectionError::UnsupportedDelivery {
                    section: "retained_user_instructions",
                });
            };
            if role != "user" {
                return Err(SectionError::UnsupportedDelivery {
                    section: "retained_user_instructions",
                });
            }
            let maximum =
                TruncationPolicy::Tokens(super::retained_instructions::MAX_INSTRUCTION_TOKENS)
                    .byte_budget();
            for part in content.iter() {
                let ContentItem::InputText { text } = part else {
                    return Err(SectionError::UnsupportedDelivery {
                        section: "retained_user_instructions",
                    });
                };
                if text.len() > maximum {
                    return Err(SectionError::EvidenceLimitExceeded {
                        section: "retained_user_instructions",
                    });
                }
            }
            internal_chat_message_metadata_passthrough
                .get_or_insert_with(Default::default)
                .content_item_kinds = Some(
                content
                    .iter()
                    .map(|_| ContentItemKind("guardian.retained_instructions".to_owned()))
                    .collect(),
            );
        }
        let mut system = Vec::new();
        let (messages, additions) = model_messages_with_delivery(&envelopes, &mut system)?;
        surviving.extend(additions);
        self.delivery = surviving;
        Ok(RestoredReviewMessages {
            messages,
            system,
            truncations,
        })
    }
}
