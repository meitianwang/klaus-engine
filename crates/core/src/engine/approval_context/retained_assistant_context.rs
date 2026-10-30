// Copyright 2025 OpenAI. Licensed under the Apache License, Version 2.0.
// SPDX-License-Identifier: Apache-2.0
// Modified: integrated host-owned evidence and model adapters for this runtime.

//! Typed framing for retained assistant evidence in independent Approval snapshots.
//! Evidence stays in separate content parts to preserve its budgeting and source metadata.

use crate::engine::approval_context::model::ContentItemKind;
use crate::engine::approval_context::model::ContextualUserFragment;

/// Owns only the section delimiters; separately budgeted assistant entries supply the body.
pub(crate) struct RetainedAssistantContext;

impl ContextualUserFragment for RetainedAssistantContext {
    fn role(&self) -> &'static str {
        "user"
    }

    fn content_kind(&self) -> ContentItemKind {
        ContentItemKind("guardian.retained_assistant_context".to_owned())
    }

    fn markers(&self) -> (&'static str, &'static str) {
        Self::type_markers()
    }

    fn type_markers() -> (&'static str, &'static str) {
        (
            ">>> RETAINED ASSISTANT CONTEXT START\n\n",
            ">>> RETAINED ASSISTANT CONTEXT END\n\n",
        )
    }

    fn body(&self) -> String {
        String::new()
    }
}
