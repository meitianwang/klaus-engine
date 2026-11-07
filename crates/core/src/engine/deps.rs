//! Foundational engine dependencies: the injectable `EngineDeps` seam plus the
//! context-compaction contract (`Compactor`) the turn loop calls into.

use crate::context::compact::outcome::{CompactionDiagnostic, CompactionNoopReason};
use crate::provider::anthropic::estimate_tokens_fallback;
use crate::runtime_values::RuntimeString;
use crate::schemas::config::ResolvedConfig;
use crate::schemas::message::Message;
use crate::types::provider::{CallModel, ModelProvider, ModelRequest};
use rand::RngCore;
use std::sync::Arc;

/// The outcome of a compaction attempt: the (possibly rewritten) message list,
/// how many estimated tokens the pass freed, and whether it actually compacted.
#[derive(Debug, Clone, PartialEq)]
pub struct CompactionResult {
    pub messages: Vec<Message>,
    pub freed_tokens: u64,
    pub compacted: bool,
    pub reason: Option<CompactionNoopReason>,
    pub diagnostics: Vec<CompactionDiagnostic>,
}

impl CompactionResult {
    pub(crate) fn has_new_checkpoint(&self, source: &[Message]) -> bool {
        self.messages.iter().any(|message| {
            let Message::CompactBoundary(boundary) = message else {
                return false;
            };
            !source.iter().any(|previous| {
                matches!(previous, Message::CompactBoundary(previous) if previous.uuid == boundary.uuid)
            })
        })
    }
}

const UNSUPPORTED_MODEL_RECONFIGURATION: &str =
    "the configured compactor does not support model reconfiguration";

/// Context-compaction seam. The no-op keeps the whole history; the real compactor
/// swaps in summary-based trimming.
#[async_trait::async_trait]
pub trait Compactor: Send + Sync {
    /// Summarize all supplied input without dropping messages on overflow.
    async fn local_compact_exact(
        &self,
        messages: &[Message],
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.local_compact(messages, config, instructions).await
    }

    async fn local_compact(
        &self,
        messages: &[Message],
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.manual_compact(messages, config, instructions).await
    }

    async fn post_turn_compact(
        &self,
        messages: &[Message],
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.manual_compact(messages, config, instructions).await
    }

    /// Create independent state for an isolated query runtime.
    fn fork_isolated(
        &self,
        _call_model: CallModel,
        _config: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        Err("the configured compactor does not support isolated sessions".to_owned())
    }

    /// Model-backed compactors bind their summary transport to each captured
    /// request. Transport-independent custom compactors keep their existing
    /// instance; ordinary capture must not require model reconfiguration.
    fn rebind_summary(
        &self,
        provider: Arc<dyn ModelProvider>,
        config: &ResolvedConfig,
    ) -> Result<Option<Arc<dyn Compactor>>, String> {
        match self.rebind_model(provider, config) {
            Ok(compactor) => Ok(Some(compactor)),
            Err(error) if error == UNSUPPORTED_MODEL_RECONFIGURATION => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Bind an independent summary transport for the selected model.
    fn rebind_model(
        &self,
        _provider: Arc<dyn ModelProvider>,
        _config: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        Err(UNSUPPORTED_MODEL_RECONFIGURATION.to_string())
    }

    /// User-requested compaction is a fresh operation, independent of a prior turn.
    async fn manual_compact(
        &self,
        messages: &[Message],
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.reactive_compact(messages, config, instructions).await
    }

    /// Summarize one selected side while preserving the complementary history.
    async fn partial_compact(
        &self,
        messages: &[Message],
        _selected_index: usize,
        _direction: crate::context::compact::full_compact::PartialCompactionDirection,
        _config: &ResolvedConfig,
        _instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        CompactionResult {
            messages: messages.to_vec(),
            freed_tokens: 0,
            compacted: false,
            reason: Some(CompactionNoopReason::NoProgress),
            diagnostics: Vec::new(),
        }
    }

    /// Summarize a conversation that reached its automatic window limit.
    async fn threshold_compact(
        &self,
        messages: &[Message],
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.reactive_compact(messages, config, instructions).await
    }

    fn bind_output_observer(
        &self,
        _observer: Option<crate::context::compact::full_compact::CompactionOutputSink>,
    ) {
    }

    /// Supply the live request prefix for a cache-preserving summary fork.
    fn prepare_request(
        &self,
        _request: &crate::types::provider::ModelRequest,
        _signal: &crate::types::abort::AbortSignal,
    ) {
    }

    /// Compact when the pre-turn estimate crosses the trigger. `custom_instructions`
    /// carries a PreCompact hook's additional context, folded into the summary
    /// prompt.
    async fn maybe_compact(
        &self,
        messages: &[Message],
        estimated_tokens: u64,
        config: &ResolvedConfig,
        custom_instructions: Option<&RuntimeString>,
    ) -> CompactionResult;

    fn reactive_enabled(&self) -> bool {
        true
    }

    /// Compact in response to an over-budget error mid-turn.
    async fn reactive_compact(
        &self,
        messages: &[Message],
        config: &ResolvedConfig,
        custom_instructions: Option<&RuntimeString>,
    ) -> CompactionResult;
}

/// The no-op compactor: keeps the full history, freeing nothing.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopCompactor;

#[async_trait::async_trait]
impl Compactor for NoopCompactor {
    fn fork_isolated(
        &self,
        _call_model: CallModel,
        _config: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        Ok(Arc::new(Self))
    }

    fn reactive_enabled(&self) -> bool {
        false
    }

    fn rebind_model(
        &self,
        _provider: Arc<dyn ModelProvider>,
        _config: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        Ok(Arc::new(Self))
    }

    async fn maybe_compact(
        &self,
        messages: &[Message],
        _estimated_tokens: u64,
        _config: &ResolvedConfig,
        _custom_instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        CompactionResult {
            messages: messages.to_vec(),
            freed_tokens: 0,
            compacted: false,
            reason: Some(CompactionNoopReason::CompactorDisabled),
            diagnostics: Vec::new(),
        }
    }

    async fn reactive_compact(
        &self,
        messages: &[Message],
        _config: &ResolvedConfig,
        _custom_instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        CompactionResult {
            messages: messages.to_vec(),
            freed_tokens: 0,
            compacted: false,
            reason: Some(CompactionNoopReason::CompactorDisabled),
            diagnostics: Vec::new(),
        }
    }
}

/// The shared no-op compactor.
pub fn noop_compactor() -> Arc<dyn Compactor> {
    Arc::new(NoopCompactor)
}

/// Estimate the pre-turn token count for a request.
pub type EstimateTokensFn = Arc<dyn Fn(&ModelRequest) -> u64 + Send + Sync>;
/// Mint a fresh identifier. Defined in `types::tool` so lower layers (MCP
/// elicitation, compaction) can name it without depending on the engine.
pub use crate::types::tool::UuidFn;
/// Wall-clock milliseconds since the Unix epoch.
pub type NowFn = Arc<dyn Fn() -> i64 + Send + Sync>;

/// Narrow, injectable dependency set for the engine — the test seam. Held behind
/// an `Arc` when shared across a session's tasks (the model-call seam is a
/// non-cloneable boxed closure, so `EngineDeps` is not itself `Clone`).
pub struct EngineDeps {
    pub provider_id: RuntimeString,
    pub call_model: CallModel,
    pub estimate_tokens: EstimateTokensFn,
    pub compactor: Arc<dyn Compactor>,
    pub uuid: UuidFn,
    pub now: NowFn,
}

impl std::fmt::Debug for EngineDeps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineDeps")
            .field("provider_id", &self.provider_id)
            .field("call_model", &"<fn>")
            .field("estimate_tokens", &"<fn>")
            .field("compactor", &"<dyn Compactor>")
            .field("uuid", &"<fn>")
            .field("now", &"<fn>")
            .finish()
    }
}

/// Per-field overrides for [`production_deps`] (all default to absent).
#[derive(Default)]
pub struct EngineDepsOverrides {
    pub call_model: Option<CallModel>,
    pub estimate_tokens: Option<EstimateTokensFn>,
    pub compactor: Option<Arc<dyn Compactor>>,
    pub uuid: Option<UuidFn>,
    pub now: Option<NowFn>,
}

impl std::fmt::Debug for EngineDepsOverrides {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineDepsOverrides")
            .field("call_model", &self.call_model.is_some())
            .field("estimate_tokens", &self.estimate_tokens.is_some())
            .field("compactor", &self.compactor.is_some())
            .field("uuid", &self.uuid.is_some())
            .field("now", &self.now.is_some())
            .finish()
    }
}

/// Build the production dependency set from a provider: the call seam streams
/// through the provider, token estimation prefers the provider's own estimator and
/// falls back to the chars/4 heuristic, compaction is the no-op, and uuid/now use a
/// random UUID v4 and wall-clock time. Any field may be overridden.
pub fn production_deps(
    provider: Arc<dyn ModelProvider>,
    overrides: EngineDepsOverrides,
) -> EngineDeps {
    let call_provider = Arc::clone(&provider);
    let provider_id = provider.info().name.clone();
    let default_call_model: CallModel =
        Box::new(move |req, signal| call_provider.stream(req, signal));
    let est_provider = Arc::clone(&provider);
    let default_estimate: EstimateTokensFn = Arc::new(move |req: &ModelRequest| {
        est_provider
            .estimate_tokens(req)
            .unwrap_or_else(|| estimate_tokens_fallback(req))
    });
    EngineDeps {
        provider_id,
        call_model: overrides.call_model.unwrap_or(default_call_model),
        estimate_tokens: overrides.estimate_tokens.unwrap_or(default_estimate),
        compactor: overrides.compactor.unwrap_or_else(noop_compactor),
        uuid: overrides
            .uuid
            .unwrap_or_else(|| Arc::new(random_uuid) as UuidFn),
        now: overrides
            .now
            .unwrap_or_else(|| Arc::new(|| chrono::Utc::now().timestamp_millis()) as NowFn),
    }
}

/// A random version-4 UUID.
fn random_uuid() -> String {
    let mut b = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut b);
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::fake_provider;
    use crate::types::provider::ModelRequest;

    fn req() -> ModelRequest {
        ModelRequest {
            supports_image_detail_original: false,
            model_verbosity: None,
            reasoning_summary: Default::default(),
            model: "m".into(),
            image_limits: None,
            max_media_blocks: None,
            system: vec![],
            messages: vec![],
            tools: vec![],
            deferred_tools: Vec::new(),
            tool_choice: None,
            max_output_tokens: 100,
            task_budget: None,
            output_schema: None,
            web_search: None,
            thinking: None,
            effort: None,
            skip_cache_write: None,
            cache_ttl: None,
            cache_domain: None,
            cache_key: None,
            context_management: None,
            context_hints: None,
            thinking_context: None,
        }
    }

    #[tokio::test]
    async fn noop_compactor_keeps_messages() {
        let c = NoopCompactor;
        let msgs: Vec<Message> = vec![];
        let cfg = crate::schemas::config::default_config();
        let r = c.maybe_compact(&msgs, 123, &cfg, None).await;
        assert_eq!(r.messages, msgs);
        assert_eq!(r.freed_tokens, 0);
        assert!(!r.compacted);
        let r2 = c.reactive_compact(&msgs, &cfg, None).await;
        assert!(!r2.compacted);
    }

    #[test]
    fn production_deps_wires_estimate_uuid_now() {
        let provider: Arc<dyn ModelProvider> = Arc::new(fake_provider());
        let deps = production_deps(provider, EngineDepsOverrides::default());
        // FakeProvider.estimate_tokens returns Some(100), so the provider path wins
        // over the chars/4 fallback.
        assert_eq!((deps.estimate_tokens)(&req()), 100);
        let id = (deps.uuid)();
        assert_eq!(id.len(), 36);
        assert!((deps.now)() > 0);
    }

    #[test]
    fn overrides_take_precedence() {
        let provider: Arc<dyn ModelProvider> = Arc::new(fake_provider());
        let deps = production_deps(
            provider,
            EngineDepsOverrides {
                estimate_tokens: Some(Arc::new(|_r: &ModelRequest| 7u64)),
                now: Some(Arc::new(|| 42i64)),
                ..Default::default()
            },
        );
        assert_eq!((deps.estimate_tokens)(&req()), 7);
        assert_eq!((deps.now)(), 42);
    }
}
