// Copyright 2025 OpenAI. Licensed under the Apache License, Version 2.0.
// SPDX-License-Identifier: Apache-2.0
// Modified: isolated evidence representations and transport adaptation for this runtime.

//! Host-owned review evidence. None of these records can be minted by model text.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ResponseItemId(pub String);

impl ResponseItemId {
    pub fn new(prefix: &str) -> Self {
        Self(format!("{prefix}_{}", uuid::Uuid::new_v4()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for ResponseItemId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for ResponseItemId {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptFormat {
    Line,
    Json,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TruncationPolicy {
    Bytes(usize),
    Tokens(usize),
}

impl TruncationPolicy {
    pub fn token_budget(self) -> usize {
        match self {
            Self::Bytes(bytes) => bytes.div_ceil(4),
            Self::Tokens(tokens) => tokens,
        }
    }

    pub fn byte_budget(self) -> usize {
        match self {
            Self::Bytes(bytes) => bytes,
            Self::Tokens(tokens) => tokens.saturating_mul(4),
        }
    }
}

pub fn estimate_compaction_content_length(encoded_len: usize) -> usize {
    encoded_len.div_ceil(3)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageDetail {
    Auto,
    Low,
    High,
    Original,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ImageReference {
    Inline { image_url: String },
    File { file_id: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentItem {
    InputText {
        text: String,
    },
    OutputText {
        text: String,
        #[serde(default)]
        annotations: Vec<serde_json::Value>,
    },
    InputImage {
        #[serde(flatten)]
        image: ImageReference,
        #[serde(skip_serializing_if = "Option::is_none")]
        detail: Option<ImageDetail>,
    },
    InputAudio {
        audio_url: String,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FunctionCallOutputContentItem {
    InputText {
        text: String,
    },
    InputImage {
        #[serde(flatten)]
        image: ImageReference,
        #[serde(skip_serializing_if = "Option::is_none")]
        detail: Option<ImageDetail>,
    },
    InputAudio {
        audio_url: String,
    },
    EncryptedContent {
        encrypted_content: String,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ToolOutputBody {
    Text(String),
    Content(Vec<FunctionCallOutputContentItem>),
    Native {
        body: Box<ToolOutputBody>,
        native_output: serde_json::Value,
    },
}

impl ToolOutputBody {
    pub fn to_text(&self) -> Option<String> {
        match self {
            Self::Text(text) => Some(text.clone()),
            Self::Content(items) => {
                let text = items
                    .iter()
                    .filter_map(|item| match item {
                        FunctionCallOutputContentItem::InputText { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                (!text.is_empty()).then(|| text.join("\n"))
            }
            Self::Native { body, .. } => body.to_text(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolOutput {
    pub body: ToolOutputBody,
}

impl ToolOutput {
    pub fn content_items(&self) -> Option<&[FunctionCallOutputContentItem]> {
        match &self.body {
            ToolOutputBody::Content(items) => Some(items),
            ToolOutputBody::Text(_) => None,
            ToolOutputBody::Native { body, .. } => match body.as_ref() {
                ToolOutputBody::Content(items) => Some(items),
                ToolOutputBody::Text(_) | ToolOutputBody::Native { .. } => None,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessagePhase {
    Commentary,
    PartialAnswer,
    FinalAnswer,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ContentItemKind(pub String);

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MessageMetadata {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_kinds"
    )]
    pub content_item_kinds: Option<Vec<ContentItemKind>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_time: Option<serde_json::Number>,
    #[serde(default, skip_deserializing, skip_serializing_if = "Option::is_none")]
    pub cell_id: Option<String>,
    #[serde(default, skip_deserializing, skip_serializing_if = "Option::is_none")]
    pub executed_tool_calls: Option<Vec<serde_json::Value>>,
    #[serde(default, skip_deserializing, skip_serializing_if = "Option::is_none")]
    pub tool_calls_complete: Option<bool>,
}

fn deserialize_kinds<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Vec<ContentItemKind>>, D::Error> {
    let value = serde_json::Value::deserialize(deserializer)?;
    Ok(serde_json::from_value(value).unwrap_or_default())
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentMessageInputContent {
    InputText { text: String },
    EncryptedContent { encrypted_content: String },
}

pub fn plaintext_agent_message_content(content: &[AgentMessageInputContent]) -> Option<String> {
    let text = content
        .iter()
        .filter_map(|part| match part {
            AgentMessageInputContent::InputText { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    (!text.is_empty()).then(|| text.join("\n"))
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReasoningItemContent {
    ReasoningText { text: String },
    Text { text: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReasoningItemReasoningSummary {
    SummaryText { text: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponseItem {
    Message {
        #[serde(skip_serializing_if = "Option::is_none")]
        status: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        encrypted_content: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<ResponseItemId>,
        role: String,
        content: Vec<ContentItem>,
        #[serde(skip_serializing_if = "Option::is_none")]
        phase: Option<MessagePhase>,
        #[serde(skip_serializing_if = "Option::is_none")]
        internal_chat_message_metadata_passthrough: Option<MessageMetadata>,
    },
    AgentMessage {
        id: Option<ResponseItemId>,
        author: String,
        recipient: String,
        content: Vec<AgentMessageInputContent>,
    },
    FunctionCall {
        name: String,
        namespace: Option<String>,
        arguments: String,
        call_id: String,
    },
    CustomToolCall {
        name: String,
        namespace: Option<String>,
        input: String,
        call_id: String,
    },
    FunctionCallOutput {
        call_id: Option<String>,
        name: Option<String>,
        namespace: Option<String>,
        output: ToolOutput,
    },
    CustomToolCallOutput {
        call_id: String,
        output: ToolOutput,
    },
    Reasoning {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<ResponseItemId>,
        summary: Vec<ReasoningItemReasoningSummary>,
        content: Option<Vec<ReasoningItemContent>>,
        encrypted_content: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
    },
    LocalShellCall {
        action: serde_json::Value,
    },
    WebSearchCall {
        action: Option<serde_json::Value>,
    },
    Compaction {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<ResponseItemId>,
        encrypted_content: String,
    },
    ContextCompaction {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<ResponseItemId>,
        encrypted_content: Option<String>,
    },
    AdditionalTools {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<ResponseItemId>,
        role: String,
        tools: Vec<serde_json::Value>,
    },
    ImageGenerationCall {},
    ToolSearchCall {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<ResponseItemId>,
        call_id: String,
        arguments: serde_json::Value,
    },
    ToolSearchOutput {
        call_id: String,
        output: serde_json::Value,
    },
    ConfigurationUpdate {},
    CompactionTrigger {},
    Other,
}

impl ResponseItem {
    pub fn id(&self) -> Option<&ResponseItemId> {
        match self {
            Self::AdditionalTools { id, .. }
            | Self::Message { id, .. }
            | Self::AgentMessage { id, .. }
            | Self::Reasoning { id, .. }
            | Self::Compaction { id, .. }
            | Self::ContextCompaction { id, .. }
            | Self::ToolSearchCall { id, .. } => id.as_ref(),
            _ => None,
        }
    }

    pub fn turn_id(&self) -> Option<&str> {
        match self {
            Self::Message {
                internal_chat_message_metadata_passthrough: Some(metadata),
                ..
            } => metadata.turn_id.as_deref(),
            _ => None,
        }
    }

    pub fn is_user_message(&self) -> bool {
        matches!(self, Self::Message { role, .. } if role == "user")
    }
}

#[derive(Deserialize)]
pub struct InterAgentCommunication {
    #[serde(default)]
    pub id: Option<ResponseItemId>,
    pub author: super::agent_path::AgentPath,
    pub recipient: super::agent_path::AgentPath,
    #[serde(default)]
    pub other_recipients: Vec<super::agent_path::AgentPath>,
    pub content: String,
    #[serde(default)]
    pub encrypted_content: Option<String>,
    #[serde(default)]
    pub internal_chat_message_metadata_passthrough: Option<MessageMetadata>,
    pub trigger_turn: bool,
}

impl InterAgentCommunication {
    pub fn is_message_content(content: &[ContentItem]) -> bool {
        match content {
            [ContentItem::InputText { text }] | [ContentItem::OutputText { text, .. }] => {
                serde_json::from_str::<Self>(text).is_ok()
            }
            _ => false,
        }
    }
}

pub fn is_node_repl_backed_tool(name: &str, namespace: Option<&str>) -> bool {
    if let Some(namespace) = namespace {
        let namespace = namespace.strip_prefix("mcp__").unwrap_or(namespace);
        return matches!(
            namespace.strip_suffix("__").unwrap_or(namespace),
            "node_repl" | "cua_repl"
        );
    }
    let name = name.strip_prefix("mcp__").unwrap_or(name);
    name.split_once("__")
        .is_some_and(|(server, _)| matches!(server, "node_repl" | "cua_repl"))
}

#[derive(Clone, Debug, PartialEq)]
pub enum UserInput {
    Text {
        text: String,
        text_elements: Vec<serde_json::Value>,
    },
    Image {
        image: ImageReference,
        detail: Option<ImageDetail>,
    },
    Audio {
        audio_url: String,
    },
}

pub trait ContextualUserFragment {
    fn role(&self) -> &'static str;
    fn content_kind(&self) -> ContentItemKind;
    fn markers(&self) -> (&'static str, &'static str);
    fn type_markers() -> (&'static str, &'static str)
    where
        Self: Sized;
    fn body(&self) -> String;

    fn render(&self) -> String {
        let (start, end) = self.markers();
        format!("{start}{}{end}", self.body())
    }

    fn into(self) -> ResponseItem
    where
        Self: Sized,
    {
        ResponseItem::Message {
            status: None,
            encrypted_content: None,
            id: None,
            role: self.role().to_owned(),
            content: vec![ContentItem::InputText {
                text: self.render(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: Some(MessageMetadata {
                content_item_kinds: Some(vec![self.content_kind()]),
                ..Default::default()
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ModelInfo {
    pub context_window: Option<i64>,
    pub used_fallback_model_metadata: bool,
    pub effective_context_window_percent: i64,
}

impl ModelInfo {
    pub fn resolved_context_window(&self) -> Option<i64> {
        self.context_window
    }
}
