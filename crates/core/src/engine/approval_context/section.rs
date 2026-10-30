// Copyright 2025 OpenAI. Licensed under the Apache License, Version 2.0.
// SPDX-License-Identifier: Apache-2.0
// Modified: integrated host-owned evidence and model adapters for this runtime.

//! Section identities survive consumer-specific transcript selection and rendering.

use crate::engine::approval_context::ConversationTranscriptEntry;
use crate::engine::approval_context::PlannedAction;
use crate::engine::approval_context::PreviousReviews;
use crate::engine::approval_context::RenderedNodeReplEvidence;
use crate::engine::approval_context::TranscriptImages;
use crate::engine::approval_context::TrustedSkills;
use crate::engine::approval_context::TrustedTool;

/// Ordered evidence with a stable section identity and source-specific content.
///
/// Variants preserve provenance: transcript entries carry their original roles,
/// root messages remain line-role-labeled, and answers are host-verified fragments.
/// Conversation, authorization and action evidence retain user-role delivery.
/// Host-attested reviews and tool identities use separate developer messages.
/// Review actions/rationales and remote tool descriptions remain untrusted.
#[derive(Clone, Debug, PartialEq)]
pub enum ContextSection<T = ConversationTranscriptEntry> {
    ConversationTranscript {
        items: Vec<T>,
    },
    RootConversation {
        items: Vec<String>,
    },
    TrustedUserAnswers {
        items: Vec<String>,
    },
    RetainedUserInstructions {
        items: Vec<crate::engine::approval_context::Budgeted<String>>,
    },
    SenderUserMessages {
        items: Vec<String>,
    },
    PlannedAction(PlannedAction),
    PreviousReviews(PreviousReviews),
    TrustedTool(TrustedTool),
    TrustedSkills(TrustedSkills),
    TranscriptImages(TranscriptImages),
    NodeReplEvidence(RenderedNodeReplEvidence),
    PermissionContext {
        items: Vec<String>,
    },
}
