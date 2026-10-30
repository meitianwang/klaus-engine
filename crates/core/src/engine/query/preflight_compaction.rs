//! Dispatch one foreground compaction operation through the captured provider.

use super::Shared;
use crate::engine::deps::CompactionResult;
use crate::runtime_values::RuntimeString;
use crate::schemas::message::Message;
use std::sync::Arc;

pub(super) async fn compact(
    shared: &Arc<Shared>,
    messages: &[Message],
    instructions: Option<&RuntimeString>,
    estimated_tokens: u64,
) -> CompactionResult {
    let config = shared.config();
    if shared.deps().compactor.reactive_enabled() {
        shared
            .deps()
            .compactor
            .threshold_compact(messages, &config, instructions)
            .await
    } else {
        shared
            .deps()
            .compactor
            .maybe_compact(messages, estimated_tokens, &config, instructions)
            .await
    }
}
