// Copyright 2025 OpenAI. Licensed under the Apache License, Version 2.0.
// SPDX-License-Identifier: Apache-2.0
// Modified: retained evidence metadata is isolated from the model-facing conversation.

use super::model::{ResponseItem, ResponseItemId};
use serde::{Deserialize, Serialize};

mod retained;
pub use retained::*;
mod heartbeat;
pub use heartbeat::{Heartbeat, UserInputOrigin};
mod sender_user_messages;
pub use sender_user_messages::SenderUserMessages;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RetainedOmissions {
    pub user_instructions: bool,
    pub assistant_context: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReviewMetadata {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub guardian_sources: Vec<RetainedSource>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub guardian_review_ids: Vec<ResponseItemId>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub guardian_source_order_guidance: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guardian_retained_omissions: Option<RetainedOmissions>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retained_source: Option<RetainedSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_input_order: Option<u64>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub compaction_output: bool,
    /// Host-only marker: no replacement evidence may be rendered for this item.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub review_projection_unavailable: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub inherited_user_message: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender_user_messages: Option<Box<SenderUserMessages>>,
}

impl ReviewMetadata {
    pub fn mark_retained_sources_incomplete(&mut self) {
        self.guardian_review_ids.clear();
        self.guardian_source_order_guidance = false;
        self.guardian_retained_omissions = None;
        if let Some(source) = &mut self.retained_source {
            source.complete = false;
        }
        for source in &mut self.guardian_sources {
            source.complete = false;
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResponseItemEnvelope {
    pub item: ResponseItem,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ReviewMetadata>,
}

impl ResponseItemEnvelope {
    pub fn new(item: ResponseItem) -> Self {
        Self {
            item,
            metadata: None,
        }
    }

    pub fn into_item(self) -> ResponseItem {
        self.item
    }
}

impl std::ops::Deref for ResponseItemEnvelope {
    type Target = ResponseItem;

    fn deref(&self) -> &Self::Target {
        &self.item
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReviewHistoryCheckpoint(pub Vec<ResponseItemEnvelope>);

mod reconciled;
pub use reconciled::ReconciledRetainedContext;
