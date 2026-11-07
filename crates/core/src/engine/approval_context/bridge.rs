//! Captures original roles and typed host provenance before provider projection.

use super::history_types::{ResponseItemEnvelope, RetainedContext};
use super::model::{
    ContentItem, FunctionCallOutputContentItem, ImageReference, MessageMetadata, MessagePhase,
    ResponseItem, ToolOutput, ToolOutputBody,
};
use super::{
    default_registry, ContextPresentation, ContextProfile, HistoryTruncation, PermissionContext,
    PlannedAction, PreviousReviews, RequestBudget, RootMessage, SectionError, SectionHistory,
    SectionInput, TranscriptCursor, TranscriptImageInput, TranscriptMode, TranscriptSelection,
    TruncationObservation, TrustedTool, BUDGET_OMISSION,
};
use crate::runtime_values::{runtime_codec, RuntimeString};
use crate::schemas::content::{ContentBlock, HarnessEnvelope, OutputBlock, TextBlock};
use crate::schemas::message::{AssistantMessage, Message, ToolResultMessage, UserMessage};
use crate::types::provider::{ProviderToolCallKind, SystemPromptPart};

#[derive(Clone, Default)]
pub struct MessageHistory {
    pub items: Vec<ResponseItemEnvelope>,
    pub retained: RetainedContext,
}

impl SectionHistory for MessageHistory {
    fn items(&self) -> Box<dyn Iterator<Item = &ResponseItem> + Send + '_> {
        Box::new(
            self.items
                .iter()
                .filter(|item| !super::is_inherited_manual_approval(item))
                .map(|item| &item.item),
        )
    }

    fn items_with_sources(
        &self,
    ) -> Box<
        dyn Iterator<Item = (&ResponseItem, Option<&super::history_types::RetainedSource>)>
            + Send
            + '_,
    > {
        Box::new(
            self.items
                .iter()
                .filter(|item| !super::is_inherited_manual_approval(item))
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

fn utf8(text: &RuntimeString) -> Result<String, RuntimeString> {
    text.try_to_utf8().map_err(|_| {
        "Review evidence contains text which cannot be represented by this model transport.".into()
    })
}

fn content_image(
    image: &crate::schemas::content::ImageBlock,
) -> Result<ContentItem, RuntimeString> {
    Ok(ContentItem::InputImage {
        image: ImageReference::Inline {
            image_url: format!(
                "data:{};base64,{}",
                utf8(&image.mime_type)?,
                utf8(&image.base64)?
            ),
        },
        detail: image.detail.map(|detail| match detail {
            crate::schemas::content::ImageDetail::Auto => super::model::ImageDetail::Auto,
            crate::schemas::content::ImageDetail::Low => super::model::ImageDetail::Low,
            crate::schemas::content::ImageDetail::High => super::model::ImageDetail::High,
            crate::schemas::content::ImageDetail::Original => super::model::ImageDetail::Original,
        }),
    })
}

fn output_items(
    items: &[OutputBlock],
) -> Result<Vec<FunctionCallOutputContentItem>, RuntimeString> {
    items
        .iter()
        .map(|item| match item {
            OutputBlock::Text(text) => Ok(FunctionCallOutputContentItem::InputText {
                text: utf8(&text.text)?,
            }),
            OutputBlock::Image(image) => {
                let ContentItem::InputImage { image, detail } = content_image(image)? else {
                    unreachable!()
                };
                Ok(FunctionCallOutputContentItem::InputImage { image, detail })
            }
        })
        .collect()
}

fn role_message(
    id: &RuntimeString,
    role: &str,
    content: Vec<ContentItem>,
    phase: Option<MessagePhase>,
    turn_id: &str,
) -> Result<ResponseItemEnvelope, RuntimeString> {
    Ok(ResponseItemEnvelope::new(ResponseItem::Message {
        status: None,
        encrypted_content: None,
        id: Some(utf8(id)?.into()),
        role: role.to_owned(),
        content,
        phase,
        internal_chat_message_metadata_passthrough: Some(MessageMetadata {
            turn_id: Some(turn_id.to_owned()),
            ..Default::default()
        }),
    }))
}

fn json_evidence<T: crate::runtime_values::RuntimeEncode>(
    value: &T,
) -> Result<String, RuntimeString> {
    utf8(
        &runtime_codec::encode_text(value)
            .map_err(|_| RuntimeString::from("Cannot encode original evidence for review."))?,
    )
}

fn native_json(
    value: &crate::runtime_values::RuntimeJson,
) -> Result<serde_json::Value, RuntimeString> {
    serde_json::from_str(&json_evidence(value)?).map_err(|_| {
        "Original native evidence cannot be represented by this review transport.".into()
    })
}

fn tool_result_for_review(message: &ToolResultMessage) -> Result<Vec<ResponseItem>, RuntimeString> {
    let mut content = output_items(&message.content)?;
    let mut extra_images = Vec::new();
    for block in message.extra_user_blocks.iter().flatten() {
        match block {
            ContentBlock::Image(image) => {
                let image = content_image(image)?;
                extra_images.push(image.clone());
                let ContentItem::InputImage { image, detail } = image else {
                    unreachable!()
                };
                content.push(FunctionCallOutputContentItem::InputImage { image, detail });
            }
            ContentBlock::Text(text) => content.push(FunctionCallOutputContentItem::InputText {
                text: utf8(&text.text)?,
            }),
            _ => return Err(
                "Original tool attachment evidence requires an available reviewer media transport."
                    .into(),
            ),
        }
    }
    if message.tool_kind == Some(ProviderToolCallKind::ToolSearch) {
        let mut items = vec![ResponseItem::ToolSearchOutput {
            call_id: utf8(&message.tool_use_id)?,
            output: message
                .native_tool_output
                .as_ref()
                .map(native_json)
                .transpose()?
                .unwrap_or(serde_json::Value::Null),
        }];
        if !extra_images.is_empty() {
            items.push(ResponseItem::Message {
                status: None,
                encrypted_content: None,
                id: None,
                role: "user".to_owned(),
                content: extra_images,
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            });
        }
        return Ok(items);
    }
    let mut body = ToolOutputBody::Content(content);
    if let Some(native_output) = &message.native_tool_output {
        body = ToolOutputBody::Native {
            body: Box::new(body),
            native_output: native_json(native_output)?,
        };
    }
    Ok(vec![ResponseItem::FunctionCallOutput {
        call_id: Some(utf8(&message.tool_use_id)?),
        name: None,
        namespace: None,
        output: ToolOutput { body },
    }])
}

/// Converts original items. Synthetic harness text cannot acquire a direct user's authority.
/// Host-generated developer records must be appended separately with their metadata intact.
pub fn messages_for_review(
    messages: &[Message],
    turn_id: &str,
) -> Result<Vec<ResponseItemEnvelope>, RuntimeString> {
    let mut items = Vec::new();
    for message in messages {
        match message {
            Message::User(message) => {
                let mut content = Vec::new();
                for block in &message.content {
                    match block {
                        ContentBlock::Text(block)
                            if block.harness_envelope == Some(HarnessEnvelope::SubagentMessage) =>
                        {
                            if !content.is_empty() {
                                items.push(role_message(
                                    &message.uuid,
                                    "user",
                                    std::mem::take(&mut content),
                                    None,
                                    turn_id,
                                )?);
                            }
                            items.push(role_message(
                                &message.uuid,
                                "assistant",
                                vec![ContentItem::InputText {
                                    text: utf8(&block.text)?,
                                }],
                                Some(MessagePhase::Commentary),
                                turn_id,
                            )?);
                        }
                        ContentBlock::Text(block)
                            if message.is_meta != Some(true)
                                && block.harness_envelope.is_none() =>
                        {
                            content.push(ContentItem::InputText {
                                text: utf8(&block.text)?,
                            });
                        }
                        ContentBlock::Image(image) if message.is_meta != Some(true) => {
                            content.push(content_image(image)?)
                        }
                        ContentBlock::ToolResult(result) => {
                            if !content.is_empty() {
                                items.push(role_message(
                                    &message.uuid,
                                    "user",
                                    std::mem::take(&mut content),
                                    None,
                                    turn_id,
                                )?);
                            }
                            items.push(ResponseItemEnvelope::new(
                                ResponseItem::FunctionCallOutput {
                                    call_id: Some(utf8(&result.tool_use_id)?),
                                    name: None,
                                    namespace: None,
                                    output: ToolOutput {
                                        body: ToolOutputBody::Content(output_items(
                                            &result.content,
                                        )?),
                                    },
                                },
                            ));
                        }
                        ContentBlock::Document(_) if message.is_meta != Some(true) => {
                            return Err("Original document evidence requires an available reviewer document transport.".into());
                        }
                        ContentBlock::Media(_) if message.is_meta != Some(true) => {
                            return Err("Original audio/video evidence requires an available reviewer media transport.".into());
                        }
                        _ => {}
                    }
                }
                if !content.is_empty() {
                    items.push(ResponseItemEnvelope::new(ResponseItem::Message {
                        status: None,
                        encrypted_content: None,
                        id: Some(utf8(&message.uuid)?.into()),
                        role: "user".to_owned(),
                        content,
                        phase: None,
                        internal_chat_message_metadata_passthrough: Some(MessageMetadata {
                            turn_id: Some(turn_id.to_owned()),
                            ..Default::default()
                        }),
                    }));
                }
            }
            Message::Assistant(message) => {
                let mut content = Vec::new();
                let mut phase = None;
                for block in &message.content {
                    match block {
                        ContentBlock::Text(block) => {
                            phase = block
                                .phase
                                .as_ref()
                                .map(utf8)
                                .transpose()?
                                .and_then(|phase| match phase.as_str() {
                                    "commentary" => Some(MessagePhase::Commentary),
                                    "partial_answer" => Some(MessagePhase::PartialAnswer),
                                    "final_answer" => Some(MessagePhase::FinalAnswer),
                                    _ => None,
                                });
                            content.push(ContentItem::OutputText {
                                text: utf8(&block.text)?,
                                annotations: Vec::new(),
                            });
                        }
                        ContentBlock::Image(image) => content.push(content_image(image)?),
                        ContentBlock::ToolUse(tool) => {
                            if !content.is_empty() {
                                items.push(role_message(
                                    &message.uuid,
                                    "assistant",
                                    std::mem::take(&mut content),
                                    phase,
                                    turn_id,
                                )?);
                            }
                            if tool.tool_kind == Some(ProviderToolCallKind::ToolSearch) {
                                items.push(ResponseItemEnvelope::new(
                                    ResponseItem::ToolSearchCall {
                                        id: Some(utf8(&tool.id)?.into()),
                                        call_id: utf8(&tool.id)?,
                                        arguments: native_json(&tool.input)?,
                                    },
                                ));
                                continue;
                            }
                            items.push(ResponseItemEnvelope::new(ResponseItem::FunctionCall {
                                name: utf8(&tool.name)?,
                                namespace: None,
                                arguments: utf8(
                                    &runtime_codec::encode_text(&tool.input).map_err(|_| {
                                        RuntimeString::from(
                                            "Cannot encode original tool arguments for review.",
                                        )
                                    })?,
                                )?,
                                call_id: utf8(&tool.id)?,
                            }))
                        }
                        ContentBlock::Thinking(thinking) => {
                            if !content.is_empty() {
                                items.push(role_message(
                                    &message.uuid,
                                    "assistant",
                                    std::mem::take(&mut content),
                                    phase,
                                    turn_id,
                                )?);
                            }
                            items.push(ResponseItemEnvelope::new(ResponseItem::Reasoning {
                                id: thinking
                                    .message_id
                                    .as_ref()
                                    .map(utf8)
                                    .transpose()?
                                    .map(Into::into),
                                summary: vec![
                                    super::model::ReasoningItemReasoningSummary::SummaryText {
                                        text: utf8(&thinking.text)?,
                                    },
                                ],
                                content: None,
                                encrypted_content: thinking
                                    .encrypted_content
                                    .as_ref()
                                    .map(utf8)
                                    .transpose()?,
                                signature: thinking.signature.as_ref().map(utf8).transpose()?,
                            }))
                        }
                        ContentBlock::RedactedThinking(thinking) => {
                            if !content.is_empty() {
                                items.push(role_message(
                                    &message.uuid,
                                    "assistant",
                                    std::mem::take(&mut content),
                                    phase,
                                    turn_id,
                                )?);
                            }
                            items.push(ResponseItemEnvelope::new(ResponseItem::Reasoning {
                                id: None,
                                summary: Vec::new(),
                                content: None,
                                encrypted_content: Some(utf8(&thinking.data)?),
                                signature: None,
                            }));
                        }
                        ContentBlock::WebSearchCall(search) => {
                            if !content.is_empty() {
                                items.push(role_message(
                                    &message.uuid,
                                    "assistant",
                                    std::mem::take(&mut content),
                                    phase,
                                    turn_id,
                                )?);
                            }
                            items.push(ResponseItemEnvelope::new(ResponseItem::WebSearchCall {
                                action: search.action.as_ref().map(|action| {
                                    serde_json::from_str(&json_evidence(action)?).map_err(|_| RuntimeString::from("Cannot encode original search evidence for review."))
                                }).transpose()?,
                            }));
                            items.push(ResponseItemEnvelope::new(
                                ResponseItem::FunctionCallOutput {
                                    call_id: None,
                                    name: Some("web_search".to_owned()),
                                    namespace: None,
                                    output: ToolOutput {
                                        body: ToolOutputBody::Text(json_evidence(search)?),
                                    },
                                },
                            ));
                        }
                        _ => {}
                    }
                }
                if !content.is_empty() {
                    items.push(ResponseItemEnvelope::new(ResponseItem::Message {
                        status: None,
                        encrypted_content: None,
                        id: Some(utf8(&message.uuid)?.into()),
                        role: "assistant".to_owned(),
                        content,
                        phase,
                        internal_chat_message_metadata_passthrough: Some(MessageMetadata {
                            turn_id: Some(turn_id.to_owned()),
                            ..Default::default()
                        }),
                    }));
                }
            }
            Message::ToolResult(message) => items.extend(
                tool_result_for_review(message)?
                    .into_iter()
                    .map(ResponseItemEnvelope::new),
            ),
            Message::System(_) | Message::CompactBoundary(_) => {}
        }
    }
    Ok(items)
}

pub struct PrepareReviewInput<'a> {
    /// Captured canonical system, developer, user, project and applied instruction parts.
    pub canonical_system: &'a [SystemPromptPart],
    pub history: &'a dyn SectionHistory,
    pub root_conversation: &'a [RootMessage],
    pub trusted_user_answers: &'a [String],
    pub permissions: Option<&'a PermissionContext>,
    pub action: &'a PlannedAction,
    pub previous_reviews: Option<&'a PreviousReviews>,
    pub trusted_tool: Option<&'a TrustedTool>,
    pub trusted_skill_paths: &'a [String],
    pub node_repl: Option<&'a super::NodeReplContext<'a>>,
    pub profile: ContextProfile,
    pub mode: TranscriptMode,
    pub parent_history_version: u64,
    pub session_id: &'a str,
    pub budget: RequestBudget,
    pub history_truncation: HistoryTruncation,
    pub independent_snapshot: bool,
}

/// Host-only association between generated messages and their admitted evidence metadata.
#[derive(Clone, Default)]
pub struct ReviewMessageDelivery {
    pub(crate) records: Vec<ReviewMessageRecord>,
}

impl ReviewMessageDelivery {
    /// Keeps only proof associated with exact messages in the actual history order.
    pub fn retain_history(&mut self, current: &[Message]) {
        self.records = self.records_for_history(current).cloned().collect();
    }

    /// Adds newly generated associations without duplicating an existing exact message.
    pub fn extend(&mut self, other: Self) {
        for record in other.records {
            if !self
                .records
                .iter()
                .any(|existing| existing.message == record.message)
            {
                self.records.push(record);
            }
        }
    }

    /// Reattaches host metadata only to exact surviving generated messages.
    pub fn annotated_history(&self, current: &[Message]) -> Vec<ResponseItemEnvelope> {
        self.records_for_history(current)
            .map(|record| record.envelope.clone())
            .collect()
    }

    pub(crate) fn for_history(&self, current: &[Message]) -> Self {
        Self {
            records: self.records_for_history(current).cloned().collect(),
        }
    }

    fn records_for_history<'a>(
        &'a self,
        current: &'a [Message],
    ) -> impl Iterator<Item = &'a ReviewMessageRecord> + 'a {
        current.iter().filter_map(|message| {
            self.records
                .iter()
                .find(|record| message == &record.message)
        })
    }
}

#[derive(Clone)]
pub(crate) struct ReviewMessageRecord {
    pub(crate) message: Message,
    pub(crate) envelope: ResponseItemEnvelope,
}

pub struct PreparedReviewInput {
    pub system: Vec<SystemPromptPart>,
    pub messages: Vec<Message>,
    pub annotated_messages: Vec<ResponseItemEnvelope>,
    pub delivery: ReviewMessageDelivery,
    pub transcript_cursor: TranscriptCursor,
    pub truncations: Vec<TruncationObservation>,
    pub estimated_tokens: usize,
    pub admitted_context: super::ComposedContext,
    pub retained_context: super::ComposedContext,
}

/// Composes and admits the same evidence used by the reviewer and asynchronous scorer.
/// The caller commits the proposed cursor only after the corresponding review is admitted.
pub fn prepare_for_messages(
    input: PrepareReviewInput<'_>,
) -> Result<PreparedReviewInput, SectionError> {
    prepare_for_messages_with_history(input, &[], &ReviewMessageDelivery::default())
}

/// Selects newly required originals against the exact history of a reused reviewer.
pub fn prepare_for_messages_with_history(
    input: PrepareReviewInput<'_>,
    reviewer_history: &[Message],
    delivery: &ReviewMessageDelivery,
) -> Result<PreparedReviewInput, SectionError> {
    let collected = default_registry().prepare(&SectionInput {
        target: input.profile.target,
        history: input.history,
        transcript: &input.profile.transcript,
        root_conversation: input.root_conversation,
        trusted_user_answers: input.trusted_user_answers,
        planned_action: Some(input.action),
        permissions: input.permissions,
        previous_reviews: input.previous_reviews,
        trusted_tool: input.trusted_tool,
        trusted_skill_paths: input.trusted_skill_paths,
        images: input
            .profile
            .include_images
            .then_some(TranscriptImageInput {
                enabled: true,
                include_tool_outputs: input.profile.transcript.options.include_tool_outputs,
                node_repl_images: &[],
            }),
        node_repl: input.node_repl,
    })?;
    let (selection, transcript_cursor) = input
        .mode
        .select(collected.transcript_entries(), input.parent_history_version);
    let (entries, offset, presentation, placeholder) = match (input.profile.target, selection) {
        (super::ContextTarget::Sync, TranscriptSelection::Full(entries)) => (
            entries,
            0,
            ContextPresentation::SyncFull {
                session_id: input.session_id,
            },
            "<no retained transcript entries>",
        ),
        (super::ContextTarget::Sync, TranscriptSelection::Delta { entries, offset }) => (
            entries,
            offset,
            ContextPresentation::SyncDelta {
                session_id: input.session_id,
            },
            "<no retained transcript delta entries>",
        ),
        (super::ContextTarget::Async, TranscriptSelection::Full(entries)) => (
            entries,
            0,
            ContextPresentation::Async,
            "<no retained transcript entries>",
        ),
        (super::ContextTarget::Async, TranscriptSelection::Delta { entries, offset }) => (
            entries,
            offset,
            ContextPresentation::AsyncDelta,
            "<no retained transcript delta entries>",
        ),
    };
    let mut transcript = input.profile.prepare_transcript(entries, offset);
    if entries.is_empty() {
        transcript
            .items
            .push(super::Budgeted::required(super::TranscriptContent::Text(
                placeholder.to_owned(),
            )));
    }
    let mut composed = collected.compose(presentation, transcript)?;
    let retained_context = composed.retained_instructions();
    composed.retain_new_instructions(&delivery.annotated_history(reviewer_history));
    if input.independent_snapshot {
        composed.deduplicate_transcript_instructions();
    }
    let composed = composed.enforce_budget(
        input.budget,
        BUDGET_OMISSION.to_owned(),
        input.history_truncation,
    )?;
    let estimated_tokens = composed.estimated_tokens();
    let truncations = composed.truncations.clone();
    let annotated_messages = composed.clone().into_annotated_messages();
    let mut system = input.canonical_system.to_vec();
    let (messages, delivery) = model_messages_with_delivery(&annotated_messages, &mut system)?;
    Ok(PreparedReviewInput {
        system,
        messages,
        annotated_messages,
        delivery,
        transcript_cursor,
        truncations,
        estimated_tokens,
        admitted_context: composed,
        retained_context,
    })
}

/// Maps developer evidence into the transport's system channel without changing its text.
/// Native opaque messages are rejected when the transport cannot preserve their identity.
pub fn model_messages(
    items: &[ResponseItemEnvelope],
    system: &mut Vec<SystemPromptPart>,
) -> Result<Vec<Message>, SectionError> {
    model_messages_with_delivery(items, system).map(|(messages, _)| messages)
}

/// Records delivery proof against the exact generated message, including its unique identity.
pub fn model_messages_with_delivery(
    items: &[ResponseItemEnvelope],
    system: &mut Vec<SystemPromptPart>,
) -> Result<(Vec<Message>, ReviewMessageDelivery), SectionError> {
    let mut messages = Vec::new();
    let mut delivery = ReviewMessageDelivery::default();
    for (index, envelope) in items.iter().enumerate() {
        let ResponseItem::Message { role, content, .. } = &envelope.item else {
            return Err(SectionError::UnsupportedDelivery {
                section: "conversation_transcript",
            });
        };
        if role == "developer" || role == "system" {
            for (part, content) in content.iter().enumerate() {
                let ContentItem::InputText { text } = content else {
                    return Err(SectionError::UnsupportedDelivery {
                        section: "developer_context",
                    });
                };
                system.push(SystemPromptPart {
                    name: format!("review-evidence-{index}-{part}").into(),
                    text: text.clone().into(),
                    cacheable: Some(true),
                    cache_scope: None,
                });
            }
            continue;
        }
        let content = content
            .iter()
            .map(|item| match item {
                ContentItem::InputText { text } | ContentItem::OutputText { text, .. } => {
                    Ok(ContentBlock::Text(TextBlock {
                        text: text.clone().into(),
                        citations: None,
                        harness_envelope: None,
                        message_id: None,
                        phase: None,
                    }))
                }
                ContentItem::InputImage {
                    image: ImageReference::Inline { image_url },
                    detail,
                } => {
                    let Some((mime, base64)) = image_url
                        .strip_prefix("data:")
                        .and_then(|url| url.split_once(";base64,"))
                    else {
                        return Err(SectionError::UnsupportedDelivery {
                            section: "transcript_images",
                        });
                    };
                    Ok(ContentBlock::Image(crate::schemas::content::ImageBlock {
                        mime_type: mime.into(),
                        base64: base64.into(),
                        detail: detail.map(|detail| match detail {
                            super::model::ImageDetail::Auto => {
                                crate::schemas::content::ImageDetail::Auto
                            }
                            super::model::ImageDetail::Low => {
                                crate::schemas::content::ImageDetail::Low
                            }
                            super::model::ImageDetail::High => {
                                crate::schemas::content::ImageDetail::High
                            }
                            super::model::ImageDetail::Original => {
                                crate::schemas::content::ImageDetail::Original
                            }
                        }),
                        source_path: None,
                    }))
                }
                ContentItem::InputImage {
                    image: ImageReference::File { .. },
                    ..
                }
                | ContentItem::InputAudio { .. } => Err(SectionError::UnsupportedDelivery {
                    section: "transcript_images",
                }),
            })
            .collect::<Result<Vec<_>, _>>()?;
        match role.as_str() {
            "user" => messages.push(Message::User(UserMessage {
                question_answer: None,
                uuid: format!("review-input-{}", uuid::Uuid::new_v4()).into(),
                content,
                is_meta: None,
            })),
            "assistant" => messages.push(Message::Assistant(AssistantMessage {
                uuid: format!("review-assistant-{}", uuid::Uuid::new_v4()).into(),
                content,
                model: RuntimeString::default(),
                usage: None,
            })),
            _ => {
                return Err(SectionError::UnsupportedDelivery {
                    section: "conversation_transcript",
                });
            }
        }
        if envelope.metadata.as_ref().is_some_and(|metadata| {
            metadata
                .guardian_sources
                .iter()
                .any(|source| source.complete)
                || metadata.guardian_source_order_guidance
                || metadata.guardian_retained_omissions.is_some()
        }) {
            delivery.records.push(ReviewMessageRecord {
                message: messages.last().expect("converted review message").clone(),
                envelope: envelope.clone(),
            });
        }
    }
    Ok((messages, delivery))
}
