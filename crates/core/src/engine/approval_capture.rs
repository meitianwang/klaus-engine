// Copyright 2025 OpenAI. Licensed under the Apache License, Version 2.0.
// SPDX-License-Identifier: Apache-2.0
// Modified: captures admitted runtime messages and typed question responses.

//! Parent-owned evidence captured before provider projection and compaction.
//! Authorization comes from admitted input provenance, never from answer wording.
//! The transcript and retained families apply their own independent storage budgets.

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::approval_context::history_types::{
    ResponseItemEnvelope, RetainedContext, RetainedContextEntry, RetainedContextEvent,
    RetainedInputSource, RetainedSource, RetainedUserMessage, ReviewHistoryCheckpoint,
    ReviewMetadata, UserInputOrigin, VerifiedAnswer, VerifiedQuestionAnswer,
};
use super::approval_context::model::{
    ContentItem, ContentItemKind, MessageMetadata, MessagePhase, ResponseItem,
};
use super::approval_context::{
    is_inherited_manual_approval, manual_approval_fragment, messages_for_review, truncate_text,
    SectionHistory, TranscriptHistory,
};
use super::questions::QuestionAnswer;
use crate::runtime_values::{runtime_codec, runtime_json, RuntimeJson, RuntimeString};
use crate::schemas::content::ContentBlock;
use crate::schemas::message::Message;
use crate::schemas::rollout::RolloutItem;

const MAX_RETAINED_MESSAGE_TOKENS: usize = 900;

pub const APPROVAL_PARENT_HISTORY_EVENT: &str = "approval_parent_history";
pub const APPROVAL_MESSAGE_ADMITTED_EVENT: &str = "approval_message_admitted";
pub const APPROVAL_MANUAL_ACTION_EVENT: &str = "approval_manual_action";
pub const APPROVAL_REVIEW_EVENT: &str = "approval_review";

/// Admission provenance references the durable original without copying its body.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewMessageAdmission {
    message_id: String,
    fingerprint: [u8; 32],
    turn_id: String,
    acceptance_order: u64,
    role: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retained_source: Option<RetainedSource>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    source_unavailable: bool,
}

impl ReviewMessageAdmission {
    pub fn to_runtime_value(&self) -> Result<RuntimeJson, RuntimeString> {
        encode_review_record(self)
    }

    fn from_runtime_value(value: &RuntimeJson) -> Result<Self, RuntimeString> {
        let admission: Self = decode_review_record(value)?;
        let valid_role = matches!(admission.role.as_str(), "user" | "assistant" | "tool");
        let valid_source = admission.retained_source.as_ref().is_none_or(|source| {
            source.id.message_id == admission.message_id
                && source.id.turn_id == admission.turn_id
                && match source.id.role {
                    super::approval_context::history_types::RetainedSourceRole::User => {
                        admission.role == "user"
                    }
                    super::approval_context::history_types::RetainedSourceRole::Assistant => {
                        admission.role == "assistant"
                    }
                }
        });
        if !valid_role || !valid_source {
            return Err("Invalid review admission provenance.".into());
        }
        Ok(admission)
    }

    fn key(&self) -> (String, [u8; 32]) {
        (self.message_id.clone(), self.fingerprint)
    }
}

fn encode_review_record(value: &impl Serialize) -> Result<RuntimeJson, RuntimeString> {
    let text = serde_json::to_string(value)
        .map_err(|error| RuntimeString::from(format!("Cannot encode review evidence: {error}")))?;
    runtime_json::parse(&text.into()).map_err(|_| "Cannot encode review evidence record.".into())
}

fn decode_review_record<T: serde::de::DeserializeOwned>(
    value: &RuntimeJson,
) -> Result<T, RuntimeString> {
    let text = runtime_json::stringify(value)
        .try_to_utf8()
        .map_err(|_| RuntimeString::from("Invalid review evidence record text."))?;
    serde_json::from_str(&text)
        .map_err(|error| RuntimeString::from(format!("Invalid review evidence: {error}")))
}

pub(crate) fn manual_action_payload(
    envelope: &ResponseItemEnvelope,
) -> Result<RuntimeJson, RuntimeString> {
    encode_review_record(envelope)
}

/// Original identity is separate from the model-facing items a message renders.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OriginalMessageRecord {
    pub message_id: String,
    pub turn_id: String,
    pub acceptance_order: u64,
    fingerprint: [u8; 32],
    #[serde(default)]
    pub inherited: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    projection_unavailable: bool,
    #[serde(default)]
    item_keys: Vec<[u8; 32]>,
}

impl OriginalMessageRecord {
    fn order_key(&self) -> (bool, u64) {
        (!self.inherited, self.acceptance_order)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct AnswerSlot {
    key_digest: [u8; 32],
    question: String,
    answer: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct AcceptedAnswers {
    turn_id: String,
    call_id: String,
    slots: Vec<AnswerSlot>,
    incomplete: bool,
}

/// Host-only checkpoint. Store it as an opaque rollout event, never model text.
/// Restoring re-applies the live storage rules before publishing any evidence.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReviewParentCheckpoint {
    pub transcript: ReviewHistoryCheckpoint,
    pub retained: RetainedContext,
    pub generation: u64,
    pub user_message_revision: u64,
    originals: Vec<OriginalMessageRecord>,
    accepted_answers: Vec<AcceptedAnswers>,
    last_observed: Option<String>,
    next_order: u64,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    incremental_admissions: bool,
}

impl ReviewParentCheckpoint {
    pub fn to_runtime_value(&self) -> Result<RuntimeJson, RuntimeString> {
        let text = serde_json::to_string(self).map_err(|error| {
            RuntimeString::from(format!("Cannot encode review history: {error}"))
        })?;
        runtime_json::parse(&RuntimeString::from(text))
            .map_err(|_| "Cannot encode review history checkpoint.".into())
    }

    pub fn from_runtime_value(value: &RuntimeJson) -> Result<Self, RuntimeString> {
        let text = runtime_json::stringify(value)
            .try_to_utf8()
            .map_err(|_| RuntimeString::from("Invalid review history checkpoint text."))?;
        serde_json::from_str(&text)
            .map_err(|error| RuntimeString::from(format!("Invalid review history: {error}")))
    }
}

/// Immutable, bounded evidence selected from one parent state.
#[derive(Clone, Debug)]
pub struct ReviewParentSnapshot {
    pub items: Vec<ResponseItemEnvelope>,
    pub retained: RetainedContext,
    pub generation: u64,
    pub user_message_revision: u64,
    pub originals: Vec<OriginalMessageRecord>,
}

impl ReviewParentSnapshot {
    /// Unavailable originals are storage markers, never substitute model evidence.
    pub fn ensure_representable(&self) -> Result<(), RuntimeString> {
        if self.items.iter().any(|item| {
            item.metadata
                .as_ref()
                .is_some_and(|metadata| metadata.review_projection_unavailable)
        }) || self
            .originals
            .iter()
            .any(|record| record.projection_unavailable)
        {
            return Err(
                "Original review evidence is not representable by the review model transport."
                    .into(),
            );
        }
        Ok(())
    }
}

impl SectionHistory for ReviewParentSnapshot {
    fn items(&self) -> Box<dyn Iterator<Item = &ResponseItem> + Send + '_> {
        Box::new(
            self.items
                .iter()
                .filter(|item| !is_inherited_manual_approval(item))
                .map(|item| &item.item),
        )
    }

    fn items_with_sources(
        &self,
    ) -> Box<dyn Iterator<Item = (&ResponseItem, Option<&RetainedSource>)> + Send + '_> {
        Box::new(
            self.items
                .iter()
                .filter(|item| !is_inherited_manual_approval(item))
                .map(|item| {
                    (
                        &item.item,
                        item.metadata
                            .as_ref()
                            .and_then(|metadata| metadata.retained_source.as_ref()),
                    )
                }),
        )
    }

    fn retained_context(&self) -> Option<&RetainedContext> {
        Some(&self.retained)
    }
}

/// Own this state with the parent session, not with any one model request.
#[derive(Clone, Debug, Default)]
pub struct ReviewParentHistory {
    transcript: TranscriptHistory,
    retained: RetainedContext,
    originals: Vec<OriginalMessageRecord>,
    accepted_answers: Vec<AcceptedAnswers>,
    last_observed: Option<String>,
    next_order: u64,
    user_message_revision: u64,
    incremental_admissions: bool,
}

impl ReviewParentHistory {
    /// Reserve at admission, before asynchronous persistence or stream rendering.
    /// An abandoned reservation contains no authorization evidence.
    pub fn reserve_order(&mut self) -> u64 {
        let order = self.next_order;
        self.next_order = self.next_order.saturating_add(1);
        self.retained.observe_acceptance_order(order);
        order
    }

    /// Record an admitted original. Call only after any required persistence barrier.
    pub fn accept(&mut self, message: &Message, turn_id: &str) -> Result<bool, RuntimeString> {
        self.accept_original(message, turn_id, None, true)
    }

    /// Delayed recording retains the original admission/stream-start position.
    pub fn accept_at_order(
        &mut self,
        message: &Message,
        turn_id: &str,
        acceptance_order: u64,
    ) -> Result<bool, RuntimeString> {
        self.accept_original(message, turn_id, Some(acceptance_order), true)
    }

    /// Capture and persist this reference under the same admission lock.
    /// Reservations keep delayed assistant output in its stream-start position.
    pub(crate) fn admit(
        &mut self,
        message: &Message,
        turn_id: &RuntimeString,
        acceptance_order: Option<u64>,
    ) -> Result<Option<ReviewMessageAdmission>, RuntimeString> {
        let Some(id) = message_id(message) else {
            return Ok(None);
        };
        let role = match message {
            Message::User(_) => "user",
            Message::Assistant(_) => "assistant",
            Message::ToolResult(_) => "tool",
            _ => return Ok(None),
        };
        let acceptance_order = acceptance_order
            .or_else(|| {
                self.originals
                    .iter()
                    .find(|record| record.message_id == id)
                    .map(|record| record.acceptance_order)
            })
            .unwrap_or(self.next_order);
        let mut admission = ReviewMessageAdmission {
            message_id: id,
            fingerprint: fingerprint(message)?,
            turn_id: review_identity(turn_id),
            acceptance_order,
            role: role.to_owned(),
            retained_source: None,
            source_unavailable: turn_id.try_to_utf8().is_err(),
        };
        let changed = if admission.source_unavailable {
            self.record_unavailable_original(
                message,
                &admission.turn_id,
                Some(acceptance_order),
                false,
                true,
            )?
        } else {
            self.accept_at_order(message, &admission.turn_id, acceptance_order)?
        };
        self.incremental_admissions = true;
        admission.retained_source = self
            .retained
            .ordered_entries()
            .filter_map(|(_, entry)| self.retained.source(entry))
            .find(|source| source.id.message_id == admission.message_id)
            .or_else(|| {
                self.transcript.envelopes().find_map(|envelope| {
                    envelope
                        .metadata
                        .as_ref()
                        .and_then(|metadata| metadata.retained_source.as_ref())
                        .filter(|source| source.id.message_id == admission.message_id)
                        .cloned()
                })
            });
        Ok(changed.then_some(admission))
    }

    /// Catch up the unprojected request suffix without treating compaction as reset.
    /// Legacy originals with no admission observation remain explicitly incomplete.
    /// The accepted-message hooks are required for original turn IDs and exact ordering.
    pub fn ingest(&mut self, messages: &[Message], turn_id: &str) -> Result<(), RuntimeString> {
        let anchor = self
            .last_observed
            .as_deref()
            .and_then(|last| {
                messages
                    .iter()
                    .rposition(|message| message_id(message).is_some_and(|id| id == last))
            })
            .or_else(|| {
                messages.iter().rposition(|message| {
                    message_id(message).is_some_and(|id| {
                        self.originals.iter().any(|record| record.message_id == id)
                    })
                })
            });
        let start = anchor.map_or(0, |index| index + 1);
        for message in &messages[start..] {
            if message_id(message)
                .is_some_and(|id| self.originals.iter().any(|record| record.message_id == id))
            {
                continue;
            }
            let source_turn = match message {
                Message::User(user) => user
                    .question_answer
                    .as_ref()
                    .map(|answer| review_identity(&answer.turn_id)),
                _ => None,
            };
            // The current request's turn cannot identify older source messages.
            // A typed question answer has its own durable original turn identity.
            let recorded_turn = message_id(message).and_then(|id| {
                self.originals
                    .iter()
                    .find(|record| record.message_id == id)
                    .map(|record| record.turn_id.clone())
            });
            let source_turn = source_turn
                .as_deref()
                .or(recorded_turn.as_deref())
                .unwrap_or(if anchor.is_some() { turn_id } else { "" });
            self.accept_original(message, source_turn, None, false)?;
        }
        if anchor.is_none() && messages.iter().any(is_original_user) {
            self.retained.mark_user_messages_incomplete();
        }
        Ok(())
    }

    /// Append only the trusted developer fragment produced by an accepted denial override.
    /// It remains review context and never becomes a direct execution grant.
    pub fn record_manual_approval(
        &mut self,
        action: &serde_json::Value,
        turn_id: &str,
    ) -> Result<ResponseItemEnvelope, RuntimeString> {
        let mut item = manual_approval_fragment(action).map_err(|error| {
            RuntimeString::from(format!("Cannot encode approved action: {error}"))
        })?;
        let ResponseItem::Message {
            id,
            internal_chat_message_metadata_passthrough,
            ..
        } = &mut item
        else {
            unreachable!("manual approval is a developer message");
        };
        *id = Some(super::approval_context::model::ResponseItemId::new(
            "approval",
        ));
        internal_chat_message_metadata_passthrough
            .get_or_insert_with(MessageMetadata::default)
            .turn_id = Some(turn_id.to_owned());
        let envelope = ResponseItemEnvelope {
            item,
            metadata: Some(ReviewMetadata {
                user_input_order: Some(self.reserve_order()),
                ..Default::default()
            }),
        };
        self.transcript.record(&envelope);
        self.prune_indices();
        Ok(envelope)
    }

    fn replay_manual_action(&mut self, payload: &RuntimeJson) -> Result<(), RuntimeString> {
        let envelope: ResponseItemEnvelope = decode_review_record(payload)?;
        let valid = matches!(&envelope.item,
            ResponseItem::Message { role, internal_chat_message_metadata_passthrough: Some(metadata), .. }
            if role == "developer" && metadata.content_item_kinds.as_ref().is_some_and(|kinds|
                kinds.iter().any(|kind| kind.0 == "guardian.approved_action")));
        let order = envelope
            .metadata
            .as_ref()
            .filter(|metadata| !metadata.inherited_user_message)
            .and_then(|metadata| metadata.user_input_order)
            .filter(|_| valid)
            .ok_or_else(|| RuntimeString::from("Invalid manual approval provenance."))?;
        self.next_order = self.next_order.max(order.saturating_add(1));
        self.retained.observe_acceptance_order(order);
        self.transcript.record(&envelope);
        self.prune_indices();
        Ok(())
    }

    pub fn snapshot(&self) -> ReviewParentSnapshot {
        ReviewParentSnapshot {
            items: self.transcript.checkpoint().0,
            retained: self.retained.clone(),
            generation: self.transcript.generation(),
            user_message_revision: self.user_message_revision,
            originals: self.originals.clone(),
        }
    }

    pub fn checkpoint(&self) -> ReviewParentCheckpoint {
        ReviewParentCheckpoint {
            transcript: self.transcript.checkpoint(),
            retained: self.retained.clone(),
            generation: self.transcript.generation(),
            user_message_revision: self.user_message_revision,
            originals: self.originals.clone(),
            accepted_answers: self.accepted_answers.clone(),
            last_observed: self.last_observed.clone(),
            next_order: self.next_order,
            incremental_admissions: self.incremental_admissions,
        }
    }

    pub fn restore(checkpoint: &ReviewParentCheckpoint) -> Self {
        let mut transcript = TranscriptHistory::new(checkpoint.generation);
        for item in &checkpoint.transcript.0 {
            transcript.record(item);
        }
        let bounded = transcript.checkpoint();
        let mut retained = RetainedContext::default();
        retained.restore(Some(&checkpoint.retained), &bounded.0);
        let mut history = Self {
            transcript,
            retained,
            user_message_revision: checkpoint.user_message_revision,
            originals: checkpoint.originals.clone(),
            accepted_answers: checkpoint.accepted_answers.clone(),
            last_observed: checkpoint.last_observed.clone(),
            next_order: checkpoint.next_order.max(
                bounded
                    .0
                    .iter()
                    .filter_map(|item| {
                        item.metadata
                            .as_ref()
                            .filter(|metadata| !metadata.inherited_user_message)
                            .and_then(|metadata| metadata.user_input_order)
                    })
                    .max()
                    .map_or(0, |order| order.saturating_add(1)),
            ),
            incremental_admissions: checkpoint.incremental_admissions,
        };
        history.prune_indices();
        history
    }

    /// Select the parent evidence no later than the captured model-history prefix.
    /// A missing boundary discards later facts instead of importing future authorization.
    pub fn prefix_for_messages(&self, messages: &[Message]) -> Self {
        let selected: HashSet<_> = messages.iter().filter_map(message_id).collect();
        let cutoff = self
            .originals
            .iter()
            .filter(|record| selected.contains(&record.message_id))
            .map(OriginalMessageRecord::order_key)
            .max();
        let mut history = self.clone();
        let Some(cutoff) = cutoff else {
            history.clear();
            history.retained.mark_user_messages_incomplete();
            return history;
        };
        if cutoff.0 {
            history.retained.rollback(
                &[],
                None,
                RetainedInputSource::Local(Some(cutoff.1.saturating_add(1))),
            );
        } else {
            let first_future = history
                .retained
                .ordered_entries()
                .find_map(|(order, entry)| {
                    let message = match entry {
                        RetainedContextEntry::UserMessage(message)
                        | RetainedContextEntry::AssistantMessage(message) => message,
                        RetainedContextEntry::VerifiedAnswer(_) => return Some(order),
                    };
                    let original = self.originals.iter().find(|record| {
                        message.message_id.as_deref() == Some(record.message_id.as_str())
                    });
                    original
                        .is_none_or(|record| record.order_key() > cutoff)
                        .then_some(order)
                });
            if let Some(boundary) = first_future {
                history.retained.retain_before_order(boundary);
            }
        }
        let allowed_keys: HashSet<_> = self
            .originals
            .iter()
            .filter(|record| record.order_key() <= cutoff)
            .flat_map(|record| record.item_keys.iter().copied())
            .collect();
        let items = history
            .transcript
            .envelopes()
            .filter(|item| {
                allowed_keys.contains(&item_key(&item.item))
                    || cutoff.0
                        && item.metadata.as_ref().is_some_and(|metadata| {
                            !metadata.inherited_user_message
                                && metadata
                                    .user_input_order
                                    .is_some_and(|order| order <= cutoff.1)
                        })
            })
            .cloned()
            .collect::<Vec<_>>();
        history.transcript.reset(&items);
        history
            .originals
            .retain(|record| record.order_key() <= cutoff);
        history.last_observed = history
            .originals
            .last()
            .map(|record| record.message_id.clone());
        history.user_message_revision = history.user_message_revision.saturating_add(1);
        history.prune_indices();
        history
    }

    fn record_inherited_seed(
        &mut self,
        message: &Message,
        retain_instructions: bool,
    ) -> Result<(), RuntimeString> {
        let mut next = self.clone();
        match next.record_inherited_seed_inner(message, retain_instructions) {
            Ok(()) => {}
            Err(_) => {
                next = self.clone();
                next.record_unavailable_original(message, "", None, true, retain_instructions)?;
            }
        }
        *self = next;
        Ok(())
    }

    fn record_inherited_seed_inner(
        &mut self,
        message: &Message,
        retain_instructions: bool,
    ) -> Result<(), RuntimeString> {
        let items = messages_for_review(std::slice::from_ref(message), "")?;
        if items.is_empty()
            || items.iter().all(|candidate| {
                self.transcript.envelopes().any(|existing| {
                    candidate
                        .id()
                        .zip(existing.id())
                        .is_some_and(|(left, right)| left == right)
                        || candidate.item == existing.item
                })
            })
        {
            return Ok(());
        }
        if retain_instructions {
            self.retain_message(message, "", RetainedInputSource::Inherited, false)?;
        }
        if let Some(id) = message_id(message) {
            let order = self
                .originals
                .iter()
                .filter(|record| record.inherited)
                .map(|record| record.acceptance_order)
                .max()
                .map_or(0, |order| order.saturating_add(1));
            self.originals.push(OriginalMessageRecord {
                message_id: id,
                turn_id: String::new(),
                acceptance_order: order,
                fingerprint: fingerprint(message)?,
                inherited: true,
                projection_unavailable: false,
                item_keys: items.iter().map(|item| item_key(&item.item)).collect(),
            });
        }
        for mut item in items {
            let metadata = item.metadata.get_or_insert_with(ReviewMetadata::default);
            metadata.inherited_user_message = true;
            metadata.user_input_order = None;
            self.transcript.record(&item);
        }
        self.last_observed = message_id(message);
        self.prune_indices();
        Ok(())
    }

    /// Mark a copied parent prefix before it is admitted by a new session.
    /// Parent answers and manual approval records cannot become local authorization.
    /// A promoted root may retain the inherited instruction prefix explicitly.
    pub fn inherit_snapshot(
        snapshot: &ReviewParentSnapshot,
        retain_inherited_instructions: bool,
    ) -> Self {
        let mut history = Self::default();
        for envelope in &snapshot.items {
            let mut envelope = envelope.clone();
            let is_user =
                matches!(&envelope.item, ResponseItem::Message { role, .. } if role == "user");
            let metadata = envelope
                .metadata
                .get_or_insert_with(ReviewMetadata::default);
            metadata.inherited_user_message = true;
            if metadata.sender_user_messages.take().is_some() || !is_user {
                metadata.user_input_order = None;
            }
            history.transcript.record(&envelope);
        }
        if retain_inherited_instructions {
            for (_, entry) in snapshot.retained.ordered_entries() {
                match entry {
                    RetainedContextEntry::UserMessage(message) => {
                        history
                            .retained
                            .record_user_message(message.clone(), RetainedInputSource::Inherited);
                    }
                    RetainedContextEntry::AssistantMessage(message) => {
                        history.retained.record_assistant_message(
                            message.clone(),
                            RetainedInputSource::Inherited,
                        );
                    }
                    RetainedContextEntry::VerifiedAnswer(_) => {}
                }
            }
        }
        if retain_inherited_instructions {
            history
                .retained
                .inherit_instruction_omissions(&snapshot.retained);
        }
        let mut originals = snapshot.originals.clone();
        originals.sort_by_key(OriginalMessageRecord::order_key);
        history.originals = originals
            .into_iter()
            .enumerate()
            .map(|(order, mut record)| {
                record.acceptance_order = order as u64;
                record.inherited = true;
                record
            })
            .collect();
        history.last_observed = history
            .originals
            .last()
            .map(|record| record.message_id.clone());
        history.prune_indices();
        history
    }

    /// Rebuild from original rollout records, independently of the model projection.
    /// Checkpoints preserve trusted developer records and original source revisions;
    /// the raw suffix uses the same admission path as live messages.
    pub fn from_rollout(items: &[RolloutItem]) -> Result<Self, RuntimeString> {
        // A complete checkpoint supersedes earlier admissions, but references in
        // its suffix may still point to originals persisted before the checkpoint.
        let start = items
            .iter()
            .rposition(|item| match item {
                RolloutItem::Event(event) => event.event == APPROVAL_PARENT_HISTORY_EVENT,
                RolloutItem::Compacted(compacted) => compacted.approval_parent_history.is_some(),
                _ => false,
            })
            .unwrap_or(0);
        let mut admissions = HashMap::new();
        let mut admission_keys = HashSet::new();
        for (index, item) in items.iter().enumerate().skip(start) {
            if let RolloutItem::Event(event) = item {
                if event.event == APPROVAL_MESSAGE_ADMITTED_EVENT {
                    let payload = event.payload.as_ref().ok_or_else(|| {
                        RuntimeString::from("Review admission record has no payload.")
                    })?;
                    let admission = ReviewMessageAdmission::from_runtime_value(payload)?;
                    admission_keys.insert(admission.key());
                    admissions.insert(index, admission);
                }
            }
        }
        let requested_ids: HashSet<_> = admission_keys.iter().map(|(id, _)| id.as_str()).collect();
        let mut originals = HashMap::new();
        for item in items {
            let messages: &[Message] = match item {
                RolloutItem::Message { message } => std::slice::from_ref(message),
                RolloutItem::Compacted(compacted) => {
                    compacted.replacement_history.as_deref().unwrap_or_default()
                }
                _ => continue,
            };
            for message in messages {
                if let Some(id) =
                    message_id(message).filter(|id| requested_ids.contains(id.as_str()))
                {
                    let key = (id, fingerprint(message)?);
                    if admission_keys.contains(&key) {
                        originals.entry(key).or_insert(message);
                    }
                }
            }
        }
        // Human queue checkpoints can be the only durable original for input
        // admitted before delivery. Match the typed queue UUID and fingerprint;
        // displayed prose and harness-origin messages never establish provenance.
        let mut queued_originals = HashMap::new();
        for item in items {
            let RolloutItem::Event(event) = item else {
                continue;
            };
            if event.event != super::message_queue::QUEUE_STATE_ROLLOUT_EVENT {
                continue;
            }
            for value in event
                .payload
                .as_ref()
                .and_then(|payload| payload.get("items"))
                .and_then(RuntimeJson::as_array)
                .into_iter()
                .flatten()
            {
                let Some(value) = value.get("message") else {
                    continue;
                };
                let Some(id) = value.get("uuid").and_then(RuntimeJson::as_str) else {
                    continue;
                };
                if !requested_ids.contains(review_identity(id).as_str()) {
                    continue;
                }
                let queued: super::message_queue::QueuedMessage =
                    runtime_codec::decode_value(value).map_err(|_| {
                        RuntimeString::from("Invalid original queued review evidence.")
                    })?;
                if queued.is_meta == Some(true)
                    || queued.origin.is_some()
                    || queued.agent_id.is_some()
                {
                    continue;
                }
                let message = Message::User(crate::schemas::message::UserMessage {
                    uuid: queued.uuid.unwrap_or_default(),
                    content: super::message_queue::queued_value_to_content_blocks(&queued.value),
                    is_meta: None,
                    question_answer: None,
                });
                let key = (
                    message_id(&message).expect("human queue input"),
                    fingerprint(&message)?,
                );
                if admission_keys.contains(&key) && !originals.contains_key(&key) {
                    queued_originals.entry(key).or_insert(message);
                }
            }
        }
        let mut history = Self::default();
        let mut turn_id = items[..start]
            .iter()
            .rev()
            .find_map(|item| match item {
                RolloutItem::TurnContext { ctx } => Some(
                    ctx.turn_id
                        .as_ref()
                        .map(review_identity)
                        .unwrap_or_default(),
                ),
                _ => None,
            })
            .unwrap_or_default();
        let mut inherited_seed = false;
        let mut retain_inherited = false;
        let mut unavailable_turn_identity = items[..start]
            .iter()
            .rev()
            .find_map(|item| match item {
                RolloutItem::TurnContext { ctx } => Some(
                    ctx.turn_id
                        .as_ref()
                        .is_some_and(|id| id.try_to_utf8().is_err()),
                ),
                _ => None,
            })
            .unwrap_or(false);
        for (index, item) in items.iter().enumerate().skip(start) {
            match item {
                RolloutItem::SessionMeta { meta, .. } => {
                    inherited_seed =
                        meta.forked_from_id.is_some() || meta.parent_thread_id.is_some();
                    retain_inherited =
                        meta.role != Some(crate::schemas::rollout::SessionRole::SideTask);
                }
                RolloutItem::TurnContext { ctx } => {
                    inherited_seed = false;
                    unavailable_turn_identity = ctx
                        .turn_id
                        .as_ref()
                        .is_some_and(|id| id.try_to_utf8().is_err());
                    turn_id = ctx
                        .turn_id
                        .as_ref()
                        .map(review_identity)
                        .unwrap_or_default();
                }
                RolloutItem::Message { message } if inherited_seed => {
                    history.record_inherited_seed(message, retain_inherited)?;
                }
                RolloutItem::Message { message } => {
                    if let Some(id) = message_id(message) {
                        if requested_ids.contains(id.as_str())
                            && admission_keys.contains(&(id, fingerprint(message)?))
                        {
                            continue;
                        }
                    }
                    if history.incremental_admissions {
                        // Admission can precede the checkpoint while its raw
                        // message follows it. Never re-admit that body, or mint
                        // verified answers from an original with no admission.
                        if is_original_user(message)
                            && !message_id(message).is_some_and(|id| {
                                history.originals.iter().any(|record| {
                                    record.message_id == id
                                        && fingerprint(message).ok() == Some(record.fingerprint)
                                })
                            })
                        {
                            history.retained.mark_user_messages_incomplete();
                        }
                        continue;
                    }
                    let answer_turn = match message {
                        Message::User(user) => user
                            .question_answer
                            .as_ref()
                            .map(|answer| review_identity(&answer.turn_id)),
                        _ => None,
                    };
                    let source_turn = answer_turn.as_deref().unwrap_or(&turn_id);
                    if unavailable_turn_identity {
                        history.record_unavailable_original(
                            message,
                            source_turn,
                            None,
                            false,
                            true,
                        )?;
                    } else {
                        history.accept_original(
                            message,
                            source_turn,
                            None,
                            !source_turn.is_empty(),
                        )?;
                    }
                    if source_turn.is_empty() && is_original_user(message) {
                        history.retained.mark_user_messages_incomplete();
                    }
                }
                RolloutItem::Event(event) if event.event == APPROVAL_PARENT_HISTORY_EVENT => {
                    let value = event.payload.as_ref().ok_or_else(|| {
                        RuntimeString::from("Review history checkpoint has no payload.")
                    })?;
                    history = Self::restore(&ReviewParentCheckpoint::from_runtime_value(value)?);
                    inherited_seed = false;
                }
                RolloutItem::Event(event) if event.event == APPROVAL_MESSAGE_ADMITTED_EVENT => {
                    let admission = &admissions[&index];
                    let key = admission.key();
                    history.replay_admission(
                        admission,
                        originals
                            .get(&key)
                            .copied()
                            .or_else(|| queued_originals.get(&key)),
                    )?;
                    inherited_seed = false;
                }
                RolloutItem::Event(event) if event.event == APPROVAL_MANUAL_ACTION_EVENT => {
                    history.replay_manual_action(event.payload.as_ref().ok_or_else(|| {
                        RuntimeString::from("Manual approval record has no payload.")
                    })?)?;
                }
                RolloutItem::Rollback(rollback) if rollback.num_turns > 0 => {
                    if let Some((boundary_id, removed_turns)) =
                        rollback_boundary(&items[..index], rollback.num_turns)?
                    {
                        let removed_turns = removed_turns
                            .iter()
                            .map(review_identity)
                            .collect::<Vec<_>>();
                        let removed_turns =
                            removed_turns.iter().map(String::as_str).collect::<Vec<_>>();
                        history.rewind(&boundary_id, &removed_turns);
                    }
                }
                RolloutItem::Compacted(compacted)
                    if compacted.approval_parent_history.is_some() =>
                {
                    history = Self::restore(&ReviewParentCheckpoint::from_runtime_value(
                        compacted
                            .approval_parent_history
                            .as_ref()
                            .expect("checked checkpoint"),
                    )?);
                    inherited_seed = false;
                }
                RolloutItem::Compacted(compacted) if history.originals.is_empty() => {
                    // Imported checkpoints can lack original raw records. Keep their
                    // surviving window visible while preserving the missing-evidence marker.
                    if let Some(messages) = &compacted.replacement_history {
                        history.ingest(messages, &turn_id)?;
                    }
                    history.retained.mark_user_messages_incomplete();
                }
                _ => {}
            }
        }
        Ok(history)
    }

    fn replay_admission(
        &mut self,
        admission: &ReviewMessageAdmission,
        message: Option<&Message>,
    ) -> Result<(), RuntimeString> {
        self.incremental_admissions = true;
        if let Some(message) = message {
            if admission.source_unavailable {
                self.record_unavailable_original(
                    message,
                    &admission.turn_id,
                    Some(admission.acceptance_order),
                    false,
                    true,
                )?;
            } else {
                self.accept_at_order(message, &admission.turn_id, admission.acceptance_order)?;
            }
            self.restore_admission_source(admission);
            return Ok(());
        }
        if self.originals.iter().any(|record| {
            record.message_id == admission.message_id && record.fingerprint == admission.fingerprint
        }) {
            self.bind_original_turn(&admission.message_id, &admission.turn_id);
            self.restore_admission_source(admission);
            return Ok(());
        }
        // A torn write can leave admission ahead of its original. Preserve the
        // unavailable marker instead of turning an absent restriction into a grant.
        self.next_order = self
            .next_order
            .max(admission.acceptance_order.saturating_add(1));
        self.retained
            .observe_acceptance_order(admission.acceptance_order);
        self.retained.mark_user_messages_incomplete();
        if admission.role == "user" {
            self.user_message_revision = self.user_message_revision.saturating_add(1);
        }
        let envelope = ResponseItemEnvelope {
            item: ResponseItem::Message {
                id: Some(admission.message_id.clone().into()),
                status: None,
                encrypted_content: None,
                role: admission.role.clone(),
                content: Vec::new(),
                phase: None,
                internal_chat_message_metadata_passthrough: Some(MessageMetadata {
                    turn_id: Some(admission.turn_id.clone()),
                    ..Default::default()
                }),
            },
            metadata: Some(ReviewMetadata {
                review_projection_unavailable: true,
                user_input_order: Some(admission.acceptance_order),
                ..Default::default()
            }),
        };
        self.transcript.record(&envelope);
        self.originals
            .retain(|record| record.message_id != admission.message_id);
        self.originals.push(OriginalMessageRecord {
            message_id: admission.message_id.clone(),
            turn_id: admission.turn_id.clone(),
            acceptance_order: admission.acceptance_order,
            fingerprint: admission.fingerprint,
            inherited: false,
            projection_unavailable: true,
            item_keys: vec![item_key(&envelope.item)],
        });
        self.originals.sort_by_key(OriginalMessageRecord::order_key);
        self.last_observed = Some(admission.message_id.clone());
        self.prune_indices();
        Ok(())
    }

    fn restore_admission_source(&mut self, admission: &ReviewMessageAdmission) {
        let Some(source) = &admission.retained_source else {
            return;
        };
        self.retained.restore_source_revision(source);
        let mut transcript = TranscriptHistory::new(self.transcript.generation());
        for mut envelope in self.transcript.envelopes().cloned() {
            if let Some(existing) = envelope
                .metadata
                .as_mut()
                .and_then(|metadata| metadata.retained_source.as_mut())
                .filter(|existing| existing.id == source.id)
            {
                existing.revision = source.revision.clone();
            }
            transcript.record(&envelope);
        }
        self.transcript = transcript;
    }

    /// A real clear/reset invalidates review cursors. Ordinary compaction does not.
    pub fn clear(&mut self) {
        let generation = self.transcript.generation().saturating_add(1);
        let user_message_revision = self.user_message_revision.saturating_add(1);
        *self = Self {
            transcript: TranscriptHistory::new(generation),
            user_message_revision,
            ..Default::default()
        };
    }

    /// Remove the instruction boundary and every later accepted fact, including
    /// delayed replies that were delivered before an earlier queued input.
    pub fn rewind(&mut self, first_removed_message_id: &str, removed_turn_ids: &[&str]) {
        self.user_message_revision = self.user_message_revision.saturating_add(1);
        let order = self
            .originals
            .iter()
            .find(|record| record.message_id == first_removed_message_id)
            .map(|record| record.order_key());
        let checkpoint = self.transcript.checkpoint();
        let boundary = checkpoint.0.iter().find(|item| {
            item.id()
                .is_some_and(|id| id.as_str() == first_removed_message_id)
                || order.is_some_and(|order| {
                    item.metadata.as_ref().is_some_and(|metadata| {
                        !metadata.inherited_user_message
                            && order.0
                            && metadata.user_input_order == Some(order.1)
                    })
                })
        });
        self.retained.rollback(
            removed_turn_ids,
            Some(first_removed_message_id),
            RetainedInputSource::Local(order.filter(|order| order.0).map(|order| order.1)),
        );
        if let Some(boundary) = boundary {
            self.transcript.truncate_before(boundary);
        } else {
            self.transcript.reset(std::iter::empty());
        }
        self.originals.retain(|record| match order {
            Some(order) => record.order_key() < order,
            None => !removed_turn_ids.contains(&record.turn_id.as_str()),
        });
        self.last_observed = self
            .originals
            .last()
            .map(|record| record.message_id.clone());
        self.prune_indices();
    }

    fn accept_original(
        &mut self,
        message: &Message,
        turn_id: &str,
        acceptance_order: Option<u64>,
        original: bool,
    ) -> Result<bool, RuntimeString> {
        let mut next = self.clone();
        let changed = match next.accept_original_inner(message, turn_id, acceptance_order, original)
        {
            Ok(changed) => changed,
            Err(_) => {
                next = self.clone();
                next.record_unavailable_original(message, turn_id, acceptance_order, false, true)?
            }
        };
        if changed {
            *self = next;
        }
        Ok(changed)
    }

    fn accept_original_inner(
        &mut self,
        message: &Message,
        turn_id: &str,
        acceptance_order: Option<u64>,
        original: bool,
    ) -> Result<bool, RuntimeString> {
        let id = match message {
            Message::User(user) if user.is_meta != Some(true) => review_identity(&user.uuid),
            Message::Assistant(assistant) => review_identity(&assistant.uuid),
            Message::ToolResult(result) => review_identity(&result.uuid),
            Message::User(_) | Message::System(_) | Message::CompactBoundary(_) => {
                return Ok(false);
            }
        };
        let fingerprint = fingerprint(message)?;
        if self
            .originals
            .iter()
            .any(|record| record.message_id == id && record.fingerprint == fingerprint)
        {
            return Ok(self.bind_original_turn(&id, turn_id));
        }
        let mut items = messages_for_review(std::slice::from_ref(message), turn_id)?;
        if items.is_empty() {
            return Ok(false);
        }
        let order = acceptance_order
            .or_else(|| {
                self.originals
                    .iter()
                    .find(|record| record.message_id == id)
                    .map(|record| record.acceptance_order)
            })
            .unwrap_or_else(|| self.reserve_order());
        self.next_order = self.next_order.max(order.saturating_add(1));
        self.retained.observe_acceptance_order(order);
        let source = RetainedInputSource::Local(Some(order));
        let retained_source = self.retain_message(message, turn_id, source, original)?;
        if is_original_user(message) {
            self.user_message_revision = self.user_message_revision.saturating_add(1);
        }
        if let Message::User(user) = message {
            if let Some(answer) = &user.question_answer {
                self.record_answer(answer, order)?;
            }
        }
        for item in &mut items {
            let metadata = item.metadata.get_or_insert_with(ReviewMetadata::default);
            metadata.user_input_order = Some(order);
            if let ResponseItem::Message {
                role,
                content,
                internal_chat_message_metadata_passthrough,
                ..
            } = &mut item.item
            {
                let message_metadata = internal_chat_message_metadata_passthrough
                    .get_or_insert_with(MessageMetadata::default);
                message_metadata.turn_id = Some(turn_id.to_owned());
                if original && role == "user" && is_original_user(message) {
                    message_metadata.content_item_kinds = Some(
                        content
                            .iter()
                            .map(|item| {
                                ContentItemKind(
                                    match item {
                                        ContentItem::InputText { .. } => "user.text",
                                        ContentItem::InputImage { .. } => "user.image",
                                        _ => "user.other",
                                    }
                                    .to_owned(),
                                )
                            })
                            .collect(),
                    );
                }
                if let Some(source) = &retained_source {
                    let mut source = source.clone();
                    // A split text item cannot prove delivery of the whole original message.
                    let text = content
                        .iter()
                        .filter_map(|item| match item {
                            ContentItem::InputText { text }
                            | ContentItem::OutputText { text, .. } => Some(text.as_str()),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                    source.complete &= self.retained.ordered_entries().any(|(_, entry)| {
                        let retained = match entry {
                            RetainedContextEntry::UserMessage(message)
                            | RetainedContextEntry::AssistantMessage(message) => message,
                            _ => return false,
                        };
                        retained.message_id.as_deref() == Some(id.as_str()) && retained.text == text
                    });
                    metadata.retained_source = Some(source);
                }
            }
            self.transcript.record(item);
        }
        self.originals.retain(|record| record.message_id != id);
        self.originals.push(OriginalMessageRecord {
            message_id: id.clone(),
            turn_id: turn_id.to_owned(),
            acceptance_order: order,
            fingerprint,
            inherited: false,
            projection_unavailable: false,
            item_keys: items.iter().map(|item| item_key(&item.item)).collect(),
        });
        self.originals.sort_by_key(OriginalMessageRecord::order_key);
        self.last_observed = Some(id);
        self.prune_indices();
        Ok(true)
    }

    fn bind_original_turn(&mut self, id: &str, turn_id: &str) -> bool {
        let Some(record) = self
            .originals
            .iter_mut()
            .find(|record| record.message_id == id)
        else {
            return false;
        };
        if record.inherited || !record.turn_id.is_empty() || turn_id.is_empty() {
            return false;
        }
        record.turn_id = turn_id.to_owned();
        self.retained.bind_message_turn(id, turn_id);
        let mut transcript = TranscriptHistory::new(self.transcript.generation());
        for mut item in self.transcript.envelopes().cloned() {
            let belongs = item.id().is_some_and(|item_id| item_id.as_str() == id)
                || item
                    .metadata
                    .as_ref()
                    .and_then(|metadata| metadata.retained_source.as_ref())
                    .is_some_and(|source| source.id.message_id == id);
            if belongs {
                if let ResponseItem::Message {
                    internal_chat_message_metadata_passthrough,
                    ..
                } = &mut item.item
                {
                    let metadata = internal_chat_message_metadata_passthrough
                        .get_or_insert_with(MessageMetadata::default);
                    if metadata.turn_id.as_deref().is_none_or(str::is_empty) {
                        metadata.turn_id = Some(turn_id.to_owned());
                    }
                }
                if let Some(source) = item
                    .metadata
                    .as_mut()
                    .and_then(|metadata| metadata.retained_source.as_mut())
                {
                    if source.id.turn_id.is_empty() {
                        source.id.turn_id = turn_id.to_owned();
                    }
                }
            }
            transcript.record(&item);
        }
        self.transcript = transcript;
        true
    }

    /// Preserve admission and invalidation without inventing replacement evidence.
    /// The empty envelope exists only to apply the transcript's existing storage bounds.
    /// The flag remains on its original while either transcript or retained evidence survives.
    fn record_unavailable_original(
        &mut self,
        message: &Message,
        turn_id: &str,
        acceptance_order: Option<u64>,
        inherited: bool,
        retain_instructions: bool,
    ) -> Result<bool, RuntimeString> {
        let (id, role) = match message {
            Message::User(user) if user.is_meta != Some(true) => (&user.uuid, "user"),
            Message::Assistant(assistant) => (&assistant.uuid, "assistant"),
            Message::ToolResult(result) => (&result.uuid, "tool"),
            Message::User(_) | Message::System(_) | Message::CompactBoundary(_) => {
                return Ok(false)
            }
        };
        let id = review_identity(id);
        let fingerprint = fingerprint(message)?;
        if self
            .originals
            .iter()
            .any(|record| record.message_id == id && record.fingerprint == fingerprint)
        {
            return Ok(false);
        }
        let order = if inherited {
            self.originals
                .iter()
                .filter(|record| record.inherited)
                .map(|record| record.acceptance_order)
                .max()
                .map_or(0, |order| order.saturating_add(1))
        } else {
            let order = acceptance_order
                .or_else(|| {
                    self.originals
                        .iter()
                        .find(|record| record.message_id == id)
                        .map(|record| record.acceptance_order)
                })
                .unwrap_or_else(|| self.reserve_order());
            self.next_order = self.next_order.max(order.saturating_add(1));
            self.retained.observe_acceptance_order(order);
            if is_original_user(message) {
                self.user_message_revision = self.user_message_revision.saturating_add(1);
            }
            order
        };
        if retain_instructions && (is_original_user(message) || role == "assistant") {
            let retained = RetainedUserMessage {
                turn_id: turn_id.to_owned(),
                message_id: Some(id.clone()),
                text: String::new(),
                complete: false,
                origin: UserInputOrigin::User,
                phase: None,
            };
            let source = if inherited {
                RetainedInputSource::Inherited
            } else {
                RetainedInputSource::Local(Some(order))
            };
            if role == "assistant" {
                self.retained.record_assistant_message(retained, source);
            } else {
                self.retained.record_user_message(retained, source);
            }
        }
        let envelope = ResponseItemEnvelope {
            item: ResponseItem::Message {
                id: Some(id.clone().into()),
                status: None,
                encrypted_content: None,
                role: role.to_owned(),
                content: Vec::new(),
                phase: None,
                internal_chat_message_metadata_passthrough: Some(MessageMetadata {
                    turn_id: Some(turn_id.to_owned()),
                    ..Default::default()
                }),
            },
            metadata: Some(ReviewMetadata {
                inherited_user_message: inherited,
                review_projection_unavailable: true,
                user_input_order: (!inherited).then_some(order),
                ..Default::default()
            }),
        };
        self.transcript.record(&envelope);
        self.originals.retain(|record| record.message_id != id);
        self.originals.push(OriginalMessageRecord {
            message_id: id.clone(),
            turn_id: turn_id.to_owned(),
            acceptance_order: order,
            fingerprint,
            inherited,
            projection_unavailable: true,
            item_keys: vec![item_key(&envelope.item)],
        });
        self.originals.sort_by_key(OriginalMessageRecord::order_key);
        self.last_observed = Some(id);
        self.prune_indices();
        Ok(true)
    }

    fn retain_message(
        &mut self,
        message: &Message,
        turn_id: &str,
        source: RetainedInputSource,
        original: bool,
    ) -> Result<Option<RetainedSource>, RuntimeString> {
        let (id, blocks, assistant) = match message {
            Message::User(user) if is_original_user(message) => (&user.uuid, &user.content, false),
            Message::Assistant(assistant) => (&assistant.uuid, &assistant.content, true),
            _ => return Ok(None),
        };
        let mut complete = original;
        let mut text = Vec::new();
        let mut phase = None;
        for block in blocks {
            match block {
                ContentBlock::Text(block) if block.harness_envelope.is_none() => {
                    text.push(utf8(&block.text)?);
                    phase =
                        block
                            .phase
                            .as_ref()
                            .map(utf8)
                            .transpose()?
                            .and_then(|value| match value.as_str() {
                                "commentary" => Some(MessagePhase::Commentary),
                                "partial_answer" => Some(MessagePhase::PartialAnswer),
                                "final_answer" => Some(MessagePhase::FinalAnswer),
                                _ => None,
                            });
                }
                // Separate tool and reasoning items are captured by the transcript.
                ContentBlock::ToolUse(_)
                | ContentBlock::Thinking(_)
                | ContentBlock::WebSearchCall(_)
                    if assistant => {}
                _ => complete = false,
            }
        }
        let text = text.join("\n");
        let bounded = truncate_text(&text, MAX_RETAINED_MESSAGE_TOKENS);
        complete &= bounded == text;
        let message = RetainedUserMessage {
            turn_id: turn_id.to_owned(),
            message_id: Some(utf8(id)?),
            text: bounded,
            complete,
            origin: UserInputOrigin::User,
            phase,
        };
        Ok(if assistant {
            self.retained.record_assistant_message(message, source)
        } else {
            self.retained.record_user_message(message, source)
        })
    }

    fn record_answer(&mut self, answer: &QuestionAnswer, order: u64) -> Result<(), RuntimeString> {
        let turn_id = utf8(&answer.turn_id)?;
        let call_id = utf8(&answer.tool_call_id)?;
        let previous = self
            .accepted_answers
            .iter()
            .find(|record| record.turn_id == turn_id && record.call_id == call_id);
        let mut slots = Vec::new();
        for question in &answer.questions {
            let key = super::super::tools::builtins::ask_user::question_answer_key(question)
                .ok_or_else(|| RuntimeString::from("Accepted answer has no question identity."))?;
            let key_digest = digest_runtime_string(key);
            let text = question
                .get("question")
                .and_then(RuntimeJson::as_str)
                .ok_or_else(|| RuntimeString::from("Accepted answer has no original question."))?;
            let text = utf8(text)?;
            if let Some(value) = answer.answers.get(key) {
                slots.push(AnswerSlot {
                    key_digest,
                    question: text.clone(),
                    answer: utf8(value)?,
                });
            } else if let Some(slot) = previous.and_then(|record| {
                record
                    .slots
                    .iter()
                    .find(|slot| slot.key_digest == key_digest && slot.question == text)
            }) {
                slots.push(slot.clone());
            }
        }
        let mut event = RetainedContextEvent::VerifiedAnswer {
            answer: VerifiedAnswer {
                turn_id: turn_id.clone(),
                call_id: call_id.clone(),
                questions: slots
                    .iter()
                    .map(|slot| VerifiedQuestionAnswer {
                        question: slot.question.clone(),
                        answer: slot.answer.clone(),
                    })
                    .collect(),
            },
            acceptance_order: Some(order),
        };
        event.bound();
        let inherited_gap = previous.is_some_and(|previous| previous.incomplete);
        if inherited_gap {
            let RetainedContextEvent::VerifiedAnswer { answer, .. } = &mut event else {
                unreachable!("verified answer constructed above");
            };
            answer.questions.clear();
        }
        let incomplete = matches!(&event, RetainedContextEvent::VerifiedAnswer { answer, .. } if answer.questions.is_empty());
        if incomplete {
            slots.clear();
        }
        if self.retained.record(&event) {
            self.user_message_revision = self.user_message_revision.saturating_add(1);
        }
        self.accepted_answers
            .retain(|record| record.turn_id != turn_id || record.call_id != call_id);
        self.accepted_answers.push(AcceptedAnswers {
            turn_id,
            call_id,
            slots,
            incomplete,
        });
        Ok(())
    }

    /// Index lifetime is exactly the lifetime of its bounded source records.
    /// No independent unbounded UUID ledger or question payload buffer is kept.
    fn prune_indices(&mut self) {
        let current_keys: HashSet<_> = self
            .transcript
            .envelopes()
            .map(|item| item_key(&item.item))
            .collect();
        let local_items: Vec<_> = self
            .transcript
            .envelopes()
            .filter_map(|item| {
                let metadata = item.metadata.as_ref()?;
                (!metadata.inherited_user_message)
                    .then_some((metadata.user_input_order?, item_key(&item.item)))
            })
            .collect();
        let retained_ids: HashSet<_> = self
            .retained
            .ordered_entries()
            .filter_map(|(_, entry)| match entry {
                RetainedContextEntry::UserMessage(message)
                | RetainedContextEntry::AssistantMessage(message) => message.message_id.as_deref(),
                _ => None,
            })
            .collect();
        let mut seen_orders = HashSet::new();
        let mut seen_ids = HashSet::new();
        self.originals.retain_mut(|record| {
            if record.item_keys.is_empty() && !record.inherited {
                record.item_keys.extend(
                    local_items
                        .iter()
                        .filter(|(order, _)| *order == record.acceptance_order)
                        .map(|(_, key)| *key),
                );
            }
            record.item_keys.retain(|key| current_keys.contains(key));
            record.item_keys.sort_unstable();
            record.item_keys.dedup();
            record.item_keys.shrink_to_fit();
            (!record.item_keys.is_empty() || retained_ids.contains(record.message_id.as_str()))
                && seen_orders.insert(record.order_key())
                && seen_ids.insert(record.message_id.clone())
        });
        self.accepted_answers.retain(|record| {
            self.retained.verified_answers().any(|answer| {
                answer.turn_id == record.turn_id
                    && answer.call_id == record.call_id
                    && (record.incomplete && answer.questions.is_empty()
                        || !record.incomplete && !answer.questions.is_empty())
                    && answer.questions.len() == record.slots.len()
                    && answer
                        .questions
                        .iter()
                        .zip(&record.slots)
                        .all(|(pair, slot)| {
                            pair.question == slot.question && pair.answer == slot.answer
                        })
            })
        });
    }
}

/// Reconstruct the latest state of each lifecycle record in first-seen order.
/// Existing rollback projection determines which owning turns survive.
pub fn approval_reviews_from_rollout(
    items: &[RolloutItem],
) -> Result<Vec<crate::types::approval_review::ApprovalReviewEvent>, RuntimeString> {
    let mut reviews: Vec<crate::types::approval_review::ApprovalReviewEvent> = Vec::new();
    for (index, item) in items.iter().enumerate() {
        match item {
            RolloutItem::Event(event) if event.event == APPROVAL_REVIEW_EVENT => {
                let value = event.payload.as_ref().ok_or_else(|| {
                    RuntimeString::from("Approval review lifecycle record has no payload.")
                })?;
                let review: crate::types::approval_review::ApprovalReviewEvent =
                    runtime_codec::decode_value(value).map_err(|_| {
                        RuntimeString::from("Invalid approval review lifecycle record.")
                    })?;
                if let Some(previous) = reviews.iter_mut().find(|previous| previous.id == review.id)
                {
                    *previous = review;
                } else {
                    reviews.push(review);
                }
            }
            RolloutItem::Rollback(rollback) if rollback.num_turns > 0 => {
                if let Some((_, removed_turns)) =
                    rollback_boundary(&items[..index], rollback.num_turns)?
                {
                    reviews.retain(|review| !removed_turns.contains(&review.turn_id));
                }
            }
            _ => {}
        }
    }
    Ok(reviews)
}

fn rollback_boundary(
    prefix: &[RolloutItem],
    num_turns: u32,
) -> Result<Option<(String, HashSet<RuntimeString>)>, RuntimeString> {
    // Share the model projection's existing user-turn boundary rules. This also
    // covers typed answers admitted outside a compactor's source watermark.
    let before = crate::store::projection::project_for_prompt(prefix);
    let after = crate::store::history_rewind::drop_last_n_user_turns(&before, f64::from(num_turns));
    let Some(Message::User(boundary)) = before.get(after.len()) else {
        return Ok(None);
    };
    let removed_ids: HashSet<_> = before[after.len()..]
        .iter()
        .filter_map(message_id)
        .collect();
    let (_, _, turn_index, _) = crate::store::jsonl_store::rollout_metadata(prefix);
    let removed_turns = turn_index
        .message_turn
        .iter()
        .filter(|(message_id, _)| removed_ids.contains(&review_identity(message_id)))
        .map(|(_, turn_id)| turn_id.clone())
        .collect();
    Ok(Some((review_identity(&boundary.uuid), removed_turns)))
}

fn utf8(value: &RuntimeString) -> Result<String, RuntimeString> {
    value.try_to_utf8().map_err(|_| {
        "Original review evidence is not representable by the review model transport.".into()
    })
}

fn message_id(message: &Message) -> Option<String> {
    match message {
        Message::User(message) if message.is_meta != Some(true) => {
            Some(review_identity(&message.uuid))
        }
        Message::Assistant(message) => Some(review_identity(&message.uuid)),
        Message::ToolResult(message) => Some(review_identity(&message.uuid)),
        Message::User(_) | Message::System(_) | Message::CompactBoundary(_) => None,
    }
}

/// Non-scalar identities name only unavailable storage markers, never authority.
fn review_identity(value: &RuntimeString) -> String {
    value.try_to_utf8().unwrap_or_else(|_| {
        let digest = digest_runtime_string(value);
        let digest = digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        format!("unavailable-runtime-identity:{digest}")
    })
}

fn is_original_user(message: &Message) -> bool {
    matches!(message, Message::User(user) if user.is_meta != Some(true)
    && user.content.iter().any(|block| match block {
        ContentBlock::Text(text) => text.harness_envelope.is_none(),
        ContentBlock::Image(_) | ContentBlock::Document(_) | ContentBlock::Media(_) => true,
        _ => false,
    }))
}

struct Fingerprint(Sha256);

impl Hasher for Fingerprint {
    fn finish(&self) -> u64 {
        let hash = self.0.clone().finalize();
        u64::from_le_bytes(hash[..8].try_into().expect("eight digest bytes"))
    }

    fn write(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }
}

impl std::io::Write for Fingerprint {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn item_key(item: &ResponseItem) -> [u8; 32] {
    let mut digest = Fingerprint(Sha256::new());
    if let Some(id) = item.id() {
        Hasher::write(&mut digest, b"id:");
        Hasher::write(&mut digest, id.as_str().as_bytes());
    } else {
        serde_json::to_writer(&mut digest, item).expect("bounded evidence item serializes");
    }
    digest.0.finalize().into()
}

fn fingerprint(message: &Message) -> Result<[u8; 32], RuntimeString> {
    let value = runtime_codec::encode_value(message)
        .map_err(|_| RuntimeString::from("Cannot encode original review evidence identity."))?;
    let mut fingerprint = Fingerprint(Sha256::new());
    value.hash(&mut fingerprint);
    Ok(fingerprint.0.finalize().into())
}

fn digest_runtime_string(value: &RuntimeString) -> [u8; 32] {
    let mut fingerprint = Fingerprint(Sha256::new());
    value.hash(&mut fingerprint);
    fingerprint.0.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::{RuntimeMap, RuntimeObject};
    use crate::schemas::content::{HarnessEnvelope, OutputBlock, TextBlock};
    use crate::schemas::message::{
        AssistantMessage, CompactBoundaryMessage, ToolResultMessage, UserMessage,
    };

    fn text(value: &str) -> ContentBlock {
        ContentBlock::Text(TextBlock {
            text: value.into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })
    }

    fn user(id: &str, value: &str) -> Message {
        Message::User(UserMessage {
            uuid: id.into(),
            content: vec![text(value)],
            is_meta: None,
            question_answer: None,
        })
    }

    fn assistant(id: &str, value: &str) -> Message {
        Message::Assistant(AssistantMessage {
            uuid: id.into(),
            content: vec![text(value)],
            model: "model".into(),
            usage: None,
        })
    }

    fn tool(id: &str, value: &str) -> Message {
        let ContentBlock::Text(block) = text(value) else {
            unreachable!()
        };
        Message::ToolResult(ToolResultMessage {
            uuid: id.into(),
            tool_use_id: format!("call-{id}").into(),
            content: vec![OutputBlock::Text(block)],
            is_error: false,
            tool_kind: None,
            native_tool_output: None,
            output: None,
            extra_user_blocks: None,
        })
    }

    fn question(id: &str, value: &str) -> RuntimeObject {
        let mut object = RuntimeObject::new();
        object.insert("id", RuntimeJson::String(id.into()));
        object.insert("question", RuntimeJson::String(value.into()));
        object
    }

    fn answer(id: &str, response: &str, values: &[(&str, &str)]) -> Message {
        let mut answers = RuntimeMap::new();
        for (key, value) in values {
            answers.insert((*key).into(), RuntimeString::from(*value));
        }
        let Message::User(mut user) = user(id, "Accepted response") else {
            unreachable!()
        };
        user.question_answer = Some(Box::new(QuestionAnswer {
            request_id: "request".into(),
            turn_id: "origin-turn".into(),
            tool_call_id: "question-call".into(),
            client_response_id: response.into(),
            questions: vec![
                question("first", "Same question"),
                question("second", "Same question"),
            ],
            answers,
        }));
        Message::User(user)
    }

    #[test]
    fn original_roles_identity_and_source_revision_remain_attached() {
        let mut history = ReviewParentHistory::default();
        history
            .accept(&user("user-1", "Allow only reading this file."), "turn-1")
            .unwrap();
        history
            .accept(&assistant("assistant-1", "I will inspect it."), "turn-1")
            .unwrap();
        history
            .accept(&tool("tool-1", "File content"), "turn-1")
            .unwrap();
        let snapshot = history.snapshot();
        assert!(snapshot.retained.user_messages_complete());
        assert_eq!(snapshot.items.len(), 3);
        let sourced = snapshot
            .items_with_sources()
            .filter_map(|(item, source)| source.map(|source| (item, source)))
            .collect::<Vec<_>>();
        assert_eq!(sourced.len(), 2);
        assert_eq!(sourced[0].1.id.message_id, "user-1");
        assert_eq!(sourced[0].1.id.turn_id, "turn-1");
        assert!(sourced[0].1.complete);
        assert_eq!(snapshot.originals[2].message_id, "tool-1");
        assert_eq!(snapshot.originals[2].acceptance_order, 2);
    }

    #[test]
    fn repeated_full_request_does_not_reappend_evicted_originals() {
        let mut history = ReviewParentHistory::default();
        let messages = (0..200)
            .map(|index| user(&format!("u-{index}"), "Original request"))
            .collect::<Vec<_>>();
        for message in &messages {
            history.accept(message, "turn").unwrap();
        }
        let before = history.checkpoint();
        history.ingest(&messages, "later-turn").unwrap();
        history.ingest(&messages, "later-turn").unwrap();
        assert_eq!(history.checkpoint(), before);
        assert_eq!(history.snapshot().items.len(), 128);
    }

    #[test]
    fn ordinary_compaction_preserves_evidence_and_generation() {
        let mut history = ReviewParentHistory::default();
        let first = user("first", "Do not change the deployment.");
        let next = user("next", "Inspect the configuration.");
        history.accept(&first, "turn-1").unwrap();
        history.accept(&next, "turn-2").unwrap();
        let before = history.checkpoint();
        let compacted = vec![
            Message::CompactBoundary(CompactBoundaryMessage {
                native_compaction: None,
                uuid: "summary".into(),
                summary: "Shortened history".into(),
            }),
            next,
        ];
        history.ingest(&compacted, "turn-2").unwrap();
        assert_eq!(history.checkpoint(), before);
        assert!(history
            .snapshot()
            .items
            .iter()
            .any(|item| item.id().is_some_and(|id| id.as_str() == "first")));
    }

    #[test]
    fn accepted_order_survives_delayed_recording() {
        let mut history = ReviewParentHistory::default();
        let input = history.reserve_order();
        let response = history.reserve_order();
        let steer = history.reserve_order();
        history
            .accept_at_order(&user("steer", "Only list files."), "turn", steer)
            .unwrap();
        history
            .accept_at_order(&assistant("response", "I will write it."), "turn", response)
            .unwrap();
        history
            .accept_at_order(&user("input", "Inspect the workspace."), "turn", input)
            .unwrap();
        let snapshot = history.snapshot();
        let ids = snapshot
            .originals
            .iter()
            .map(|record| record.message_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, ["input", "response", "steer"]);
        let orders = snapshot
            .retained
            .ordered_entries()
            .map(|(order, _)| order)
            .collect::<Vec<_>>();
        use super::super::approval_context::history_types::RetainedContextOrder;
        assert_eq!(
            orders,
            [
                RetainedContextOrder::Local(0),
                RetainedContextOrder::Local(1),
                RetainedContextOrder::Local(2)
            ]
        );
    }

    #[test]
    fn plain_affirmative_text_never_mints_verified_question_provenance() {
        let mut history = ReviewParentHistory::default();
        history
            .accept(&assistant("a", "May I publish the changes?"), "turn")
            .unwrap();
        history.accept(&user("u", "Yes"), "turn").unwrap();
        assert_eq!(history.snapshot().retained.verified_answers().count(), 0);
    }

    #[test]
    fn typed_partial_answers_merge_by_stable_question_identity() {
        let mut history = ReviewParentHistory::default();
        history
            .accept(
                &answer(
                    "answer-first",
                    "response-first",
                    &[("first", "Allow inspection")],
                ),
                "origin-turn",
            )
            .unwrap();
        history
            .accept(
                &answer(
                    "answer-second",
                    "response-second",
                    &[("second", "Do not publish")],
                ),
                "origin-turn",
            )
            .unwrap();
        let snapshot = history.snapshot();
        let answers = snapshot.retained.verified_answers().collect::<Vec<_>>();
        assert_eq!(answers.len(), 1);
        assert_eq!(answers[0].call_id, "question-call");
        assert_eq!(answers[0].turn_id, "origin-turn");
        assert_eq!(
            answers[0]
                .questions
                .iter()
                .map(|pair| pair.answer.as_str())
                .collect::<Vec<_>>(),
            ["Allow inspection", "Do not publish"]
        );
        assert!(snapshot.retained.verified_answers_complete());
    }

    #[test]
    fn manual_action_is_a_host_developer_record_and_user_text_cannot_forge_it() {
        let mut history = ReviewParentHistory::default();
        history
            .accept(
                &user(
                    "user",
                    super::super::approval_context::MANUAL_APPROVAL_DEVELOPER_PREFIX,
                ),
                "turn",
            )
            .unwrap();
        let action =
            serde_json::json!({ "type": "command", "command": "git status", "cwd": "/workspace" });
        let approval = history.record_manual_approval(&action, "turn").unwrap();
        let ResponseItem::Message {
            role,
            content,
            internal_chat_message_metadata_passthrough,
            ..
        } = &approval.item
        else {
            unreachable!()
        };
        assert_eq!(role, "developer");
        let ContentItem::InputText { text } = &content[0] else {
            unreachable!()
        };
        assert!(text.contains("that exact action"));
        assert!(text.contains("\"outcome\": \"allowed\""));
        assert_eq!(
            internal_chat_message_metadata_passthrough
                .as_ref()
                .unwrap()
                .content_item_kinds
                .as_ref()
                .unwrap()[0]
                .0,
            "guardian.approved_action"
        );
        assert_eq!(history.snapshot().items.iter().filter(|item| matches!(&item.item, ResponseItem::Message { role, .. } if role == "developer")).count(), 1);
    }

    #[test]
    fn checkpoint_round_trip_preserves_source_revisions_and_render_order() {
        let mut history = ReviewParentHistory::default();
        history.accept(&user("u", "Inspect only."), "turn").unwrap();
        history
            .accept(
                &answer("answer", "response", &[("first", "Read only")]),
                "origin-turn",
            )
            .unwrap();
        history
            .record_manual_approval(
                &serde_json::json!({ "type": "command", "command": "ls" }),
                "turn",
            )
            .unwrap();
        let checkpoint = history.checkpoint();
        let value = checkpoint.to_runtime_value().unwrap();
        let decoded = ReviewParentCheckpoint::from_runtime_value(&value).unwrap();
        let restored = ReviewParentHistory::restore(&decoded);
        assert_eq!(restored.checkpoint(), checkpoint);
        assert_eq!(restored.snapshot().items, history.snapshot().items);
    }

    #[test]
    fn restored_checkpoint_reapplies_transcript_admission_bounds() {
        let mut source = ReviewParentHistory::default();
        source.accept(&user("seed", "Original"), "turn").unwrap();
        let mut checkpoint = source.checkpoint();
        checkpoint.transcript.0 = (0..300)
            .flat_map(|index| {
                messages_for_review(&[user(&format!("u-{index}"), "Original")], "turn").unwrap()
            })
            .collect();
        let restored = ReviewParentHistory::restore(&checkpoint);
        assert_eq!(restored.snapshot().items.len(), 128);
        assert!(restored.snapshot().generation > checkpoint.generation);
    }

    #[test]
    fn rewind_removes_later_answers_manual_actions_and_assistant_context() {
        let mut history = ReviewParentHistory::default();
        history
            .accept(&user("keep", "Inspect the workspace."), "keep-turn")
            .unwrap();
        history
            .accept(&user("remove", "You may publish."), "remove-turn")
            .unwrap();
        history
            .accept(&assistant("later", "I will publish."), "remove-turn")
            .unwrap();
        history
            .accept(
                &answer("answer", "response", &[("first", "Yes")]),
                "remove-turn",
            )
            .unwrap();
        history
            .record_manual_approval(
                &serde_json::json!({ "type": "command", "command": "publish" }),
                "remove-turn",
            )
            .unwrap();
        let generation = history.snapshot().generation;
        history.rewind("remove", &["remove-turn"]);
        let snapshot = history.snapshot();
        assert!(snapshot.generation > generation);
        assert_eq!(snapshot.items.len(), 1);
        assert_eq!(snapshot.originals.len(), 1);
        assert_eq!(snapshot.originals[0].message_id, "keep");
        assert_eq!(snapshot.retained.verified_answers().count(), 0);
    }

    #[test]
    fn unknown_rewind_boundary_discards_unsequenced_review_transcript() {
        let mut history = ReviewParentHistory::default();
        history
            .record_manual_approval(
                &serde_json::json!({ "type": "command", "command": "publish" }),
                "turn",
            )
            .unwrap();
        let generation = history.snapshot().generation;
        history.rewind("missing", &["turn"]);
        assert!(history.snapshot().items.is_empty());
        assert!(history.snapshot().generation > generation);
    }

    #[test]
    fn oversized_tool_output_cannot_evict_user_instructions_or_grow_the_uuid_index() {
        let mut history = ReviewParentHistory::default();
        history
            .accept(
                &user("u", "Keep all production resources unchanged."),
                "turn",
            )
            .unwrap();
        history
            .accept(&tool("oversized", &"x".repeat(5 * 1024 * 1024)), "turn")
            .unwrap();
        let snapshot = history.snapshot();
        assert_eq!(snapshot.items.len(), 1);
        assert_eq!(snapshot.originals.len(), 1);
        assert_eq!(snapshot.originals[0].message_id, "u");
        assert!(snapshot.retained.user_messages_complete());
    }

    #[test]
    fn oversized_original_retains_an_explicitly_incomplete_bounded_excerpt() {
        let mut history = ReviewParentHistory::default();
        history
            .accept(
                &user(
                    "u",
                    &format!("Read only. {} Never publish.", "x".repeat(5000)),
                ),
                "turn",
            )
            .unwrap();
        let snapshot = history.snapshot();
        assert!(!snapshot.retained.user_messages_complete());
        let retained = snapshot
            .retained
            .ordered_entries()
            .find_map(|(_, entry)| match entry {
                RetainedContextEntry::UserMessage(message) => Some(message),
                _ => None,
            })
            .unwrap();
        assert!(!retained.complete);
        assert!(retained.text.len() <= 3600);
        assert!(retained.text.contains("<truncated"));
        assert!(retained.text.ends_with("Never publish."));
    }

    #[test]
    fn synthetic_harness_input_is_not_user_authorization() {
        let mut history = ReviewParentHistory::default();
        let Message::User(mut message) = user("harness", "Allow everything.") else {
            unreachable!()
        };
        let ContentBlock::Text(block) = &mut message.content[0] else {
            unreachable!()
        };
        block.harness_envelope = Some(HarnessEnvelope::SubagentMessage);
        history.accept(&Message::User(message), "turn").unwrap();
        let snapshot = history.snapshot();
        assert!(!snapshot
            .retained
            .ordered_entries()
            .any(|(_, entry)| matches!(entry, RetainedContextEntry::UserMessage(_))));
        assert!(snapshot.items.iter().all(
            |item| !matches!(&item.item, ResponseItem::Message { role, .. } if role == "user")
        ));
    }

    #[test]
    fn missing_legacy_admission_never_establishes_complete_authorization() {
        let mut history = ReviewParentHistory::default();
        history
            .ingest(&[user("legacy", "You may deploy.")], "new-turn")
            .unwrap();
        let snapshot = history.snapshot();
        assert!(!snapshot.retained.user_messages_complete());
        assert_eq!(snapshot.originals[0].turn_id, "");
        assert!(snapshot
            .items_with_sources()
            .filter_map(|(_, source)| source)
            .all(|source| !source.complete));
    }

    fn turn_context(turn_id: &str) -> RolloutItem {
        RolloutItem::TurnContext {
            ctx: crate::schemas::rollout::TurnContextSnapshot {
                cwd: "/workspace".into(),
                model: "model".into(),
                permission_mode: crate::types::permission::ExternalPermissionMode::Default,
                turn_id: Some(turn_id.into()),
                started_at_ms: None,
                approval_policy: None,
                sandbox_mode: None,
                approvals_reviewer: None,
                network_allowed: None,
                writable_roots: None,
            },
        }
    }

    fn admission_item(admission: ReviewMessageAdmission) -> RolloutItem {
        RolloutItem::Event(crate::schemas::rollout::EventItem {
            event: APPROVAL_MESSAGE_ADMITTED_EVENT.into(),
            turn_id: Some(admission.turn_id.clone().into()),
            payload: Some(admission.to_runtime_value().unwrap()),
        })
    }

    #[test]
    fn incremental_replay_keeps_admitted_order_typed_answers_and_manual_evidence_exact() {
        let input = user("input", "Inspect only.");
        let output = assistant("output", "Inspection started.");
        let steer = user("steer", "Never publish.");
        let tool = tool("result", "Read-only tool output.");
        let first_answer = answer("answer-1", "reply-1", &[("first", "Inspect")]);
        let second_answer = answer("answer-2", "reply-2", &[("second", "Do not publish")]);
        let mut live = ReviewParentHistory::default();
        let mut items = vec![turn_context("turn")];
        let input_admission = live.admit(&input, &"turn".into(), None).unwrap().unwrap();
        items.push(admission_item(input_admission));
        items.push(RolloutItem::Message { message: input });
        let output_order = live.reserve_order();
        let steer_admission = live.admit(&steer, &"turn".into(), None).unwrap().unwrap();
        // Queue and accepted answer writers may persist before capture.
        items.push(RolloutItem::Message { message: steer });
        items.push(admission_item(steer_admission));
        let output_admission = live
            .admit(&output, &"turn".into(), Some(output_order))
            .unwrap()
            .unwrap();
        items.push(admission_item(output_admission));
        items.push(RolloutItem::Message { message: output });
        for message in [tool, first_answer, second_answer] {
            let admission = live
                .admit(&message, &"origin-turn".into(), None)
                .unwrap()
                .unwrap();
            items.push(RolloutItem::Message { message });
            items.push(admission_item(admission));
        }
        let manual = live
            .record_manual_approval(
                &serde_json::json!({"type":"command","command":"git status"}),
                "turn",
            )
            .unwrap();
        items.push(RolloutItem::Event(crate::schemas::rollout::EventItem {
            event: APPROVAL_MANUAL_ACTION_EVENT.into(),
            turn_id: Some("turn".into()),
            payload: Some(manual_action_payload(&manual).unwrap()),
        }));
        let replayed = ReviewParentHistory::from_rollout(&items).unwrap();
        assert_eq!(replayed.checkpoint(), live.checkpoint());
        let snapshot = replayed.snapshot();
        assert_eq!(
            snapshot
                .originals
                .iter()
                .map(|record| (record.message_id.as_str(), record.acceptance_order))
                .collect::<Vec<_>>(),
            [
                ("input", 0),
                ("output", 1),
                ("steer", 2),
                ("result", 3),
                ("answer-1", 4),
                ("answer-2", 5)
            ]
        );
        let answers = snapshot.retained.verified_answers().collect::<Vec<_>>();
        assert_eq!(answers.len(), 1);
        assert_eq!(answers[0].questions[0].answer, "Inspect");
        assert_eq!(answers[0].questions[1].answer, "Do not publish");
    }

    #[test]
    fn complete_compaction_replays_only_admitted_suffix_and_preserves_captured_raw_tail() {
        let input = user("input", "Inspect only.");
        let output = assistant("output", "I will inspect.");
        let answer = answer("answer", "reply", &[("first", "Read only")]);
        let mut live = ReviewParentHistory::default();
        live.admit(&input, &"turn".into(), None).unwrap();
        live.admit(&output, &"turn".into(), None).unwrap();
        let checkpoint = live.checkpoint();
        // The typed answer writer already flushed the original, while capture
        // races after the compaction lock. Its exact body survives in replacement.
        let compacted = RolloutItem::Compacted(crate::schemas::rollout::CompactedItem {
            summary: "Summary".into(),
            replaced_range: (0, 2),
            source_message_ids: Some(vec!["input".into(), "output".into()]),
            replacement_history: Some(vec![answer.clone()]),
            approval_parent_history: Some(checkpoint.to_runtime_value().unwrap()),
        });
        let answer_admission = live
            .admit(&answer, &"origin-turn".into(), None)
            .unwrap()
            .unwrap();
        let items = vec![
            compacted,
            RolloutItem::Message { message: output },
            admission_item(answer_admission),
            // An unaccepted typed answer may never manufacture authorization.
            RolloutItem::Message {
                message: super::tests::answer(
                    "unaccepted",
                    "unaccepted-reply",
                    &[("second", "Publish")],
                ),
            },
        ];
        let mut expected = live.checkpoint();
        let replayed = ReviewParentHistory::from_rollout(&items[..3]).unwrap();
        assert_eq!(replayed.checkpoint(), expected);
        assert!(replayed.snapshot().retained.user_messages_complete());
        expected.retained.mark_user_messages_incomplete();
        let replayed = ReviewParentHistory::from_rollout(&items).unwrap();
        assert_eq!(replayed.checkpoint(), expected);
        assert!(replayed
            .snapshot()
            .retained
            .verified_answers()
            .all(|answer| answer.questions.iter().all(|pair| pair.answer != "Publish")));
    }

    #[test]
    fn torn_admission_without_original_preserves_unavailable_evidence_marker() {
        let mut live = ReviewParentHistory::default();
        let admission = live
            .admit(&user("missing", "Do not publish."), &"turn".into(), None)
            .unwrap()
            .unwrap();
        let restored = ReviewParentHistory::from_rollout(&[admission_item(admission)]).unwrap();
        let snapshot = restored.snapshot();
        assert!(snapshot.ensure_representable().is_err());
        assert!(!snapshot.retained.user_messages_complete());
        assert_eq!(snapshot.originals[0].message_id, "missing");
        assert_eq!(snapshot.originals[0].acceptance_order, 0);
        let restored_again = ReviewParentHistory::restore(&restored.checkpoint());
        assert_eq!(restored_again.checkpoint(), restored.checkpoint());
    }

    #[test]
    fn raw_rollout_reconstruction_keeps_originals_across_model_compaction() {
        let items = vec![
            turn_context("old-turn"),
            RolloutItem::Message {
                message: user("original", "Do not modify production."),
            },
            RolloutItem::Message {
                message: assistant("assistant", "I will inspect the workspace."),
            },
            RolloutItem::Compacted(crate::schemas::rollout::CompactedItem {
                approval_parent_history: None,
                summary: "Summary".into(),
                replaced_range: (0, 2),
                source_message_ids: Some(vec!["original".into(), "assistant".into()]),
                replacement_history: None,
            }),
            turn_context("new-turn"),
            RolloutItem::Message {
                message: user("new", "Check the current status."),
            },
        ];
        let history = ReviewParentHistory::from_rollout(&items).unwrap();
        let snapshot = history.snapshot();
        assert_eq!(snapshot.items.len(), 3);
        assert!(snapshot.retained.user_messages_complete());
        assert_eq!(snapshot.originals[0].turn_id, "old-turn");
        assert_eq!(snapshot.originals[2].turn_id, "new-turn");
        assert_eq!(snapshot.generation, 0);
        assert!(!crate::store::projection::project_for_prompt(&items)
            .iter()
            .any(|message| { message_id(message).as_deref() == Some("original") }));
    }

    #[test]
    fn rollout_checkpoint_raw_suffix_and_rollback_share_live_source_rules() {
        let mut initial = ReviewParentHistory::default();
        initial
            .accept(&user("keep", "Inspect only."), "old-turn")
            .unwrap();
        let checkpoint = initial.checkpoint();
        let items = vec![
            turn_context("old-turn"),
            RolloutItem::Message {
                message: user("keep", "Inspect only."),
            },
            RolloutItem::Event(crate::schemas::rollout::EventItem {
                event: APPROVAL_PARENT_HISTORY_EVENT.into(),
                turn_id: None,
                payload: Some(checkpoint.to_runtime_value().unwrap()),
            }),
            RolloutItem::Compacted(crate::schemas::rollout::CompactedItem {
                approval_parent_history: None,
                summary: "Summary".into(),
                replaced_range: (0, 1),
                source_message_ids: Some(vec!["keep".into()]),
                replacement_history: None,
            }),
            turn_context("new-turn"),
            RolloutItem::Message {
                message: user("remove", "Publish now."),
            },
            RolloutItem::Message {
                message: assistant("later", "I will publish."),
            },
            RolloutItem::Rollback(crate::schemas::rollout::RollbackItem { num_turns: 1 }),
        ];
        let history = ReviewParentHistory::from_rollout(&items).unwrap();
        let snapshot = history.snapshot();
        assert_eq!(snapshot.items, checkpoint.transcript.0);
        assert_eq!(snapshot.originals.len(), 1);
        assert_eq!(snapshot.originals[0].message_id, "keep");
        assert!(snapshot.generation > checkpoint.generation);
        assert!(snapshot.retained.user_messages_complete());
    }

    fn lifecycle(
        id: &str,
        turn_id: &str,
        status: crate::types::approval_review::ApprovalReviewStatus,
    ) -> crate::types::approval_review::ApprovalReviewEvent {
        use crate::types::approval_review::{ApprovalReviewAction, ApprovalReviewCommandSource};
        crate::types::approval_review::ApprovalReviewEvent {
            id: id.into(),
            turn_id: turn_id.into(),
            target_item_id: Some("call".into()),
            plugin_id: None,
            script_path: None,
            started_at_ms: 100,
            completed_at_ms: (status
                != crate::types::approval_review::ApprovalReviewStatus::InProgress)
                .then_some(200),
            status,
            risk_level: None,
            user_authorization: None,
            rationale: Some("Recorded decision".into()),
            decision_source: None,
            model_context: None,
            review_reason: None,
            action: ApprovalReviewAction::Command {
                source: ApprovalReviewCommandSource::UnifiedExec,
                command: "ls".into(),
                cwd: "/workspace".into(),
            },
        }
    }

    fn lifecycle_item(review: &crate::types::approval_review::ApprovalReviewEvent) -> RolloutItem {
        RolloutItem::Event(crate::schemas::rollout::EventItem {
            event: APPROVAL_REVIEW_EVENT.into(),
            turn_id: Some(review.turn_id.clone()),
            payload: Some(runtime_codec::encode_value(review).unwrap()),
        })
    }

    #[test]
    fn lifecycle_replay_replaces_latest_state_in_first_seen_order_without_wire_changes() {
        use crate::types::approval_review::ApprovalReviewStatus;
        let started = lifecycle("first", "turn", ApprovalReviewStatus::InProgress);
        let second = lifecycle("second", "turn", ApprovalReviewStatus::Denied);
        let completed = lifecycle("first", "turn", ApprovalReviewStatus::Approved);
        let reviews = approval_reviews_from_rollout(&[
            lifecycle_item(&started),
            lifecycle_item(&second),
            lifecycle_item(&completed),
        ])
        .unwrap();
        assert_eq!(reviews, [completed.clone(), second]);
        assert_eq!(
            runtime_codec::encode_value(&reviews[0]).unwrap(),
            runtime_codec::encode_value(&completed).unwrap()
        );
    }

    #[test]
    fn lifecycle_replay_uses_the_existing_rollback_turn_projection() {
        use crate::types::approval_review::ApprovalReviewStatus;
        let keep = lifecycle("keep", "old-turn", ApprovalReviewStatus::Denied);
        let remove = lifecycle("remove", "new-turn", ApprovalReviewStatus::TimedOut);
        let items = vec![
            turn_context("old-turn"),
            RolloutItem::Message {
                message: user("old-input", "Inspect."),
            },
            lifecycle_item(&keep),
            turn_context("new-turn"),
            RolloutItem::Message {
                message: user("new-input", "Publish."),
            },
            lifecycle_item(&remove),
            RolloutItem::Rollback(crate::schemas::rollout::RollbackItem { num_turns: 1 }),
        ];
        assert_eq!(approval_reviews_from_rollout(&items).unwrap(), [keep]);
    }

    #[test]
    fn inherited_prefix_does_not_adopt_parent_answers_or_manual_approval_authority() {
        let mut parent = ReviewParentHistory::default();
        parent
            .accept(
                &user("parent-input", "Inspect the workspace."),
                "parent-turn",
            )
            .unwrap();
        parent
            .accept(
                &answer("answer", "response", &[("first", "Yes")]),
                "origin-turn",
            )
            .unwrap();
        parent
            .record_manual_approval(
                &serde_json::json!({ "type": "command", "command": "publish" }),
                "parent-turn",
            )
            .unwrap();
        let mut child = ReviewParentHistory::inherit_snapshot(&parent.snapshot(), false);
        let snapshot = child.snapshot();
        assert_eq!(snapshot.retained.verified_answers().count(), 0);
        assert!(!snapshot
            .items()
            .any(|item| matches!(item, ResponseItem::Message { role, .. } if role == "developer")));
        assert!(snapshot.items.iter().any(is_inherited_manual_approval));
        assert!(snapshot.originals.iter().all(|record| record.inherited));
        assert_eq!(snapshot.originals.len(), 2);
        assert_eq!(child.reserve_order(), 0);
        child
            .accept(
                &user("child-input", "Only read the listed file."),
                "child-turn",
            )
            .unwrap();
        assert_eq!(
            child.snapshot().originals.last().unwrap().acceptance_order,
            1
        );
        assert!(!child.snapshot().originals.last().unwrap().inherited);
    }

    #[test]
    fn promoted_root_adopts_inherited_instructions_with_an_explicit_answer_gap() {
        let mut parent = ReviewParentHistory::default();
        parent
            .accept(&user("parent-input", "Do not publish."), "parent-turn")
            .unwrap();
        parent
            .accept(
                &answer("answer", "response", &[("first", "Inspect only")]),
                "origin-turn",
            )
            .unwrap();
        let root = ReviewParentHistory::inherit_snapshot(&parent.snapshot(), true);
        let snapshot = root.snapshot();
        assert!(snapshot.retained.has_inherited_user_messages());
        assert!(!snapshot.retained.verified_answers_complete());
        assert_eq!(snapshot.retained.verified_answers().count(), 0);
        assert!(snapshot.retained.ordered_entries().all(|(order, _)| {
            matches!(
                order,
                super::super::approval_context::history_types::RetainedContextOrder::Inherited(_)
            )
        }));
    }

    #[test]
    fn a_later_partial_answer_cannot_erase_an_earlier_storage_omission() {
        let mut history = ReviewParentHistory::default();
        let restriction = "Do not publish any production changes. ".repeat(1000);
        history
            .accept(
                &answer(
                    "large-answer",
                    "first-response",
                    &[("first", restriction.as_str())],
                ),
                "origin-turn",
            )
            .unwrap();
        assert!(!history.snapshot().retained.verified_answers_complete());
        let checkpoint = history.checkpoint();
        let mut history = ReviewParentHistory::restore(&checkpoint);
        history
            .accept(
                &answer(
                    "small-answer",
                    "second-response",
                    &[("second", "Inspect only")],
                ),
                "origin-turn",
            )
            .unwrap();
        assert!(!history.snapshot().retained.verified_answers_complete());
        assert!(history
            .snapshot()
            .retained
            .verified_answers()
            .all(|answer| answer.questions.is_empty()));
    }

    #[test]
    fn projected_same_uuid_text_cannot_replace_an_admitted_original() {
        let mut history = ReviewParentHistory::default();
        history
            .accept(&user("input", "Do not publish."), "turn")
            .unwrap();
        let before = history.checkpoint();
        history
            .ingest(&[user("input", "Publishing is allowed.")], "turn")
            .unwrap();
        assert_eq!(history.checkpoint(), before);
    }

    #[test]
    fn a_completed_prefix_excludes_later_user_answers_and_manual_actions() {
        let mut history = ReviewParentHistory::default();
        let first = user("input", "Inspect only.");
        let completed = assistant("complete", "Inspection finished.");
        history.accept(&first, "completed-turn").unwrap();
        history.accept(&completed, "completed-turn").unwrap();
        history
            .accept(&user("future", "Publish now."), "future-turn")
            .unwrap();
        history
            .accept(
                &answer("future-answer", "response", &[("first", "Yes")]),
                "origin-turn",
            )
            .unwrap();
        history
            .record_manual_approval(
                &serde_json::json!({ "type": "command", "command": "publish" }),
                "future-turn",
            )
            .unwrap();
        let selected = history.prefix_for_messages(&[first, completed]);
        let snapshot = selected.snapshot();
        assert_eq!(snapshot.originals.len(), 2);
        assert_eq!(snapshot.retained.verified_answers().count(), 0);
        assert!(!snapshot.items.iter().any(
            |item| matches!(&item.item, ResponseItem::Message { role, .. } if role == "developer")
        ));
        assert!(snapshot
            .retained
            .ordered_entries()
            .all(|(_, entry)| match entry {
                RetainedContextEntry::UserMessage(message) => !message.text.contains("Publish now"),
                _ => true,
            }));
    }

    #[test]
    fn idle_admission_binds_real_turn_without_revising_original_evidence() {
        let mut history = ReviewParentHistory::default();
        let input = user("queued", "Inspect only.");
        history.accept_at_order(&input, "", 7).unwrap();
        let before = history.checkpoint();
        let original_source = history.snapshot().items[0]
            .metadata
            .as_ref()
            .unwrap()
            .retained_source
            .as_ref()
            .unwrap()
            .clone();
        assert!(history.accept_at_order(&input, "actual-turn", 99).unwrap());
        let after = history.checkpoint();
        assert_eq!(after.generation, before.generation);
        assert_eq!(after.next_order, before.next_order);
        assert_eq!(after.user_message_revision, before.user_message_revision);
        assert_eq!(after.originals[0].acceptance_order, 7);
        assert_eq!(after.originals[0].turn_id, "actual-turn");
        let snapshot = history.snapshot();
        assert_eq!(snapshot.items[0].turn_id(), Some("actual-turn"));
        let source = snapshot.items[0]
            .metadata
            .as_ref()
            .unwrap()
            .retained_source
            .as_ref()
            .unwrap();
        assert_eq!(source.id.turn_id, "actual-turn");
        assert_eq!(source.revision, original_source.revision);
        assert_eq!(source.complete, original_source.complete);
        let (_, entry) = snapshot.retained.ordered_entries().next().unwrap();
        let retained_source = snapshot.retained.source(entry).unwrap();
        assert_eq!(retained_source, *source);
        assert!(!history.accept(&input, "conflicting-turn").unwrap());
        assert_eq!(history.checkpoint(), after);
    }

    #[test]
    fn nested_inherited_prefix_keeps_available_restrictions_before_its_cut() {
        let first = user("first", "Inspect the first file only.");
        let middle = assistant("middle", "The first file was inspected.");
        let future = user("future", "Publish the later result.");
        let mut parent = ReviewParentHistory::default();
        parent.accept(&first, "first-turn").unwrap();
        parent.accept(&middle, "first-turn").unwrap();
        parent.accept(&future, "future-turn").unwrap();
        let mut child = ReviewParentHistory::inherit_snapshot(&parent.snapshot(), true);
        child
            .accept(
                &answer("answer", "response", &[("first", "Yes")]),
                "origin-turn",
            )
            .unwrap();
        child
            .record_manual_approval(
                &serde_json::json!({"type":"command","command":"publish"}),
                "origin-turn",
            )
            .unwrap();
        let nested = ReviewParentHistory::inherit_snapshot(&child.snapshot(), true);
        let restored = ReviewParentHistory::restore(&nested.checkpoint());
        let selected = restored.prefix_for_messages(&[first, middle]);
        let snapshot = selected.snapshot();
        assert_eq!(snapshot.originals.len(), 2);
        assert!(snapshot.originals.iter().all(|record| record.inherited));
        let texts = snapshot
            .retained
            .ordered_entries()
            .filter_map(|(_, entry)| match entry {
                RetainedContextEntry::UserMessage(message)
                | RetainedContextEntry::AssistantMessage(message) => Some(message.text.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            texts,
            [
                "Inspect the first file only.",
                "The first file was inspected."
            ]
        );
        assert_eq!(snapshot.retained.verified_answers().count(), 0);
        assert!(snapshot.retained.has_inherited_user_messages());
        assert!(!snapshot.retained.verified_answers_complete());
        assert_eq!(snapshot.items.len(), 2);
        assert!(!snapshot.items.iter().any(is_inherited_manual_approval));
    }

    #[test]
    fn fork_checkpoint_ends_inherited_seed_before_a_local_answer_without_turn_context() {
        let meta = crate::schemas::rollout::SessionMeta {
            id: "child".into(),
            created_at: 1,
            cwd: "/workspace".into(),
            model: "model".into(),
            title: None,
            originator: None,
            cli_version: None,
            source: None,
            role: Some(crate::schemas::rollout::SessionRole::Main),
            parent_thread_id: None,
            forked_from_id: Some("parent".into()),
            git: None,
            host_metadata: None,
        };
        let checkpoint = ReviewParentHistory::default()
            .checkpoint()
            .to_runtime_value()
            .unwrap();
        let items = vec![
            RolloutItem::SessionMeta {
                meta,
                schema_version: crate::schemas::rollout::ROLLOUT_SCHEMA_VERSION,
            },
            RolloutItem::Event(crate::schemas::rollout::EventItem {
                event: APPROVAL_PARENT_HISTORY_EVENT.into(),
                turn_id: None,
                payload: Some(checkpoint),
            }),
            RolloutItem::Message {
                message: answer("local-answer", "reply", &[("first", "Inspect only")]),
            },
        ];
        let history = ReviewParentHistory::from_rollout(&items).unwrap();
        assert_eq!(history.snapshot().retained.verified_answers().count(), 1);
        assert!(!history.snapshot().originals[0].inherited);
    }

    #[test]
    fn inherited_root_preserves_observed_retained_family_omissions() {
        let mut parent = ReviewParentHistory::default();
        parent
            .accept(&user("restriction", "Inspect only."), "parent-turn")
            .unwrap();
        for index in 0..9 {
            parent
                .accept(
                    &assistant(&format!("assistant-{index}"), "Inspection detail."),
                    "parent-turn",
                )
                .unwrap();
        }
        assert!(parent.snapshot().retained.has_omitted_assistant_messages());
        parent.retained.mark_user_messages_incomplete();
        let child = ReviewParentHistory::inherit_snapshot(&parent.snapshot(), true);
        let restored = ReviewParentHistory::restore(&child.checkpoint());
        assert!(restored
            .snapshot()
            .retained
            .has_omitted_assistant_messages());
        assert!(restored.snapshot().retained.has_missing_user_messages());
        assert!(!restored.snapshot().retained.verified_answers_complete());
    }

    #[test]
    fn inherited_checkpoint_does_not_adopt_the_parent_local_allocator() {
        let mut parent = ReviewParentHistory::default();
        parent
            .accept_at_order(&user("parent", "Inspect only."), "parent-turn", 42)
            .unwrap();
        let child = ReviewParentHistory::inherit_snapshot(&parent.snapshot(), true);
        let mut restored = ReviewParentHistory::restore(&child.checkpoint());
        assert_eq!(restored.reserve_order(), 0);
        restored
            .accept(&user("child", "Only inspect the next file."), "child-turn")
            .unwrap();
        let records = restored.snapshot().originals;
        assert!(records[0].inherited);
        assert_eq!(records[0].acceptance_order, 0);
        assert!(!records[1].inherited);
        assert_eq!(records[1].acceptance_order, 1);
    }

    #[test]
    fn inherited_tool_items_keep_prefix_identity_without_retained_text() {
        let first = tool("first", "First command output.");
        let later = tool("later", "Later command output.");
        let mut parent = ReviewParentHistory::default();
        parent.accept(&first, "first-turn").unwrap();
        parent.accept(&later, "later-turn").unwrap();
        let nested = ReviewParentHistory::inherit_snapshot(
            &ReviewParentHistory::inherit_snapshot(&parent.snapshot(), false).snapshot(),
            false,
        );
        let selected = nested.prefix_for_messages(&[first]);
        assert_eq!(selected.snapshot().items.len(), 1);
        assert_eq!(selected.snapshot().originals[0].message_id, "first");
        assert!(selected.snapshot().originals[0].inherited);
    }

    #[test]
    fn unavailable_projection_preserves_admission_checkpoint_and_rewind() {
        let mut original = user("raw", "placeholder");
        let Message::User(message) = &mut original else {
            unreachable!()
        };
        let ContentBlock::Text(block) = &mut message.content[0] else {
            unreachable!()
        };
        block.text = RuntimeString::from_utf16(vec![0x61, 0xd800, 0x62]);
        let original_value = runtime_codec::encode_value(&original).unwrap();
        let items = vec![
            turn_context("raw-turn"),
            RolloutItem::Message {
                message: original.clone(),
            },
        ];
        let mut history = ReviewParentHistory::from_rollout(&items).unwrap();
        let snapshot = history.snapshot();
        assert!(snapshot.ensure_representable().is_err());
        assert_eq!(snapshot.user_message_revision, 1);
        assert_eq!(snapshot.originals[0].acceptance_order, 0);
        assert!(!snapshot.retained.user_messages_complete());
        assert_eq!(history.reserve_order(), 1);
        assert!(!history.accept(&original, "raw-turn").unwrap());
        assert_eq!(history.snapshot().user_message_revision, 1);
        assert_eq!(
            runtime_codec::encode_value(&original).unwrap(),
            original_value
        );

        let encoded = history.checkpoint().to_runtime_value().unwrap();
        let checkpoint = ReviewParentCheckpoint::from_runtime_value(&encoded).unwrap();
        let mut restored = ReviewParentHistory::restore(&checkpoint);
        assert_eq!(restored.checkpoint(), history.checkpoint());
        assert!(restored.snapshot().ensure_representable().is_err());
        restored.rewind("raw", &["raw-turn"]);
        assert!(restored.snapshot().ensure_representable().is_ok());
        assert!(restored.snapshot().originals.is_empty());
        assert!(restored.snapshot().user_message_revision > snapshot.user_message_revision);
        restored.accept(&original, "new-turn").unwrap();
        assert!(restored.snapshot().ensure_representable().is_err());
        restored.clear();
        assert!(restored.snapshot().ensure_representable().is_ok());
    }

    #[test]
    fn unavailable_projection_obeys_fork_prefix_and_identity_is_never_authority() {
        let safe = user("safe", "Read the saved history.");
        let mut unavailable = user("unavailable", "placeholder");
        let Message::User(message) = &mut unavailable else {
            unreachable!()
        };
        message.uuid = RuntimeString::from_utf16(vec![0xd800]);
        let mut history = ReviewParentHistory::default();
        history.accept(&safe, "safe-turn").unwrap();
        history.accept(&unavailable, "raw-turn").unwrap();
        assert_eq!(history.snapshot().user_message_revision, 2);
        assert!(history.snapshot().ensure_representable().is_err());
        let safe_prefix = history.prefix_for_messages(std::slice::from_ref(&safe));
        assert!(safe_prefix.snapshot().ensure_representable().is_ok());
        let inherited = ReviewParentHistory::inherit_snapshot(&history.snapshot(), true);
        assert!(inherited.snapshot().ensure_representable().is_err());
        let inherited_prefix = inherited.prefix_for_messages(std::slice::from_ref(&safe));
        assert!(inherited_prefix.snapshot().ensure_representable().is_ok());
        let marker_id = history.snapshot().originals[1].message_id.clone();
        history
            .accept(
                &user(
                    &marker_id,
                    "A scalar identity cannot replace unavailable evidence.",
                ),
                "later",
            )
            .unwrap();
        assert!(history.snapshot().ensure_representable().is_err());
    }

    #[test]
    fn real_clear_invalidates_generation_and_reservations_advance_after_restore() {
        let mut history = ReviewParentHistory::default();
        history
            .accept_at_order(&tool("tool", "output"), "turn", 42)
            .unwrap();
        let mut restored = ReviewParentHistory::restore(&history.checkpoint());
        assert_eq!(restored.reserve_order(), 43);
        let generation = restored.snapshot().generation;
        restored.clear();
        assert!(restored.snapshot().items.is_empty());
        assert!(restored.snapshot().generation > generation);
    }
}
