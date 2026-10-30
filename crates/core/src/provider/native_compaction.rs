//! Refuse opaque context at transports that cannot interpret it.

use crate::schemas::message::Message;
use crate::types::errors::{ProviderError, ProviderErrorKind};

pub fn contains_native_checkpoint(messages: &[Message]) -> bool {
    messages.iter().any(|message| matches!(message, Message::CompactBoundary(boundary) if boundary.native_compaction.is_some()))
}

pub fn unsupported_checkpoint() -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::InvalidRequest,
        message: "The conversation contains an encrypted checkpoint from another model source. Convert it with its original source before continuing.".into(),
        status: None,
        code: Some("native_compaction_incompatible".into()),
        error_type: None,
        request_id: None,
        media_origins: None,
        media_repair_complete: false,
        retry_after_ms: None,
    }
}

pub fn rejected_checkpoint_stream() -> crate::types::provider::ModelStream {
    Box::pin(futures::stream::once(async {
        crate::types::provider::ModelStreamEvent::Error {
            error: unsupported_checkpoint(),
        }
    }))
}
