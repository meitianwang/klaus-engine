// Copyright 2025 OpenAI. Licensed under the Apache License, Version 2.0.
// SPDX-License-Identifier: Apache-2.0
// Modified: integrated host-owned evidence and model adapters for this runtime.

//! Both reviewers consume the same host-rendered, delivery-bound sender evidence.

use crate::engine::approval_context::history_types::RetainedContext;

use crate::engine::approval_context::ContextSection;
use crate::engine::approval_context::SectionContributor;
use crate::engine::approval_context::SectionError;
use crate::engine::approval_context::SectionInput;
use crate::engine::approval_context::SectionScope;

pub(crate) struct SenderUserMessagesSection;

impl SectionContributor for SenderUserMessagesSection {
    fn scope(&self) -> SectionScope {
        SectionScope::Shared
    }

    fn contribute(&self, input: &SectionInput<'_>) -> Result<Option<ContextSection>, SectionError> {
        Ok(input
            .history
            .retained_context()
            .and_then(RetainedContext::sender_user_messages)
            .map(|snapshot| ContextSection::SenderUserMessages {
                items: vec![snapshot.text.clone()],
            }))
    }
}
