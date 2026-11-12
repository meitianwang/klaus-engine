//! One compaction operation replaces history only after a completed model response.

use super::full_compact::{
    create_full_compactor, CompactionResult, FullCompactor, FullCompactorOptions,
    PartialCompactionDirection, UuidFn,
};
use super::outcome::CompactionNoopReason;
use crate::runtime_values::RuntimeString;
use crate::schemas::config::ResolvedConfig;
use crate::schemas::message::Message;
use crate::types::abort::AbortSignal;
use crate::types::provider::{CallModel, ModelProvider};
use async_trait::async_trait;
use std::sync::Arc;

/// Construction options for [`create_compactor`].
pub struct CompactorOptions {
    pub call_model: CallModel,
    pub model: RuntimeString,
    pub max_output_tokens: Option<u64>,
    pub uuid: UuidFn,
}

impl std::fmt::Debug for CompactorOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactorOptions")
            .field("model", &self.model)
            .field("max_output_tokens", &self.max_output_tokens)
            .finish_non_exhaustive()
    }
}

/// The context-compaction seam.
#[async_trait]
pub trait Compactor: Send + Sync {
    /// Preserve every input message when the provider rejects an oversized summary.
    async fn local_compact_exact(
        &self,
        messages: Vec<Message>,
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.local_compact(messages, config, instructions).await
    }

    fn bind_native_provider(&self, _provider: Arc<dyn ModelProvider>) {}

    /// True when a summary would run through the provider's native remote
    /// compaction channel instead of a model request.
    fn native_summary_active(&self) -> bool {
        false
    }
    fn native_precompute_context(&self) -> Option<RuntimeString> {
        None
    }
    fn fork_precompute(
        &self,
        call_model: CallModel,
        config: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        self.fork_isolated(call_model, config)
    }

    async fn local_compact(
        &self,
        messages: Vec<Message>,
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.manual_compact(messages, config, instructions).await
    }

    async fn post_turn_compact(
        &self,
        messages: Vec<Message>,
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

    fn rebind_summary(
        &self,
        call_model: CallModel,
        config: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        self.rebind_model(call_model, config)
    }

    fn rebind_model(
        &self,
        _call_model: CallModel,
        _config: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        Err("the configured compactor does not support model reconfiguration".to_string())
    }

    /// User-requested compaction is a fresh operation, independent of a prior turn.
    async fn manual_compact(
        &self,
        messages: Vec<Message>,
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.reactive_compact(messages, config, instructions).await
    }

    /// Compact a selected conversation side while retaining the opposite side.
    async fn partial_compact(
        &self,
        messages: Vec<Message>,
        _selected_index: usize,
        _direction: PartialCompactionDirection,
        _config: &ResolvedConfig,
        _instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        CompactionResult {
            messages,
            freed_tokens: 0,
            compacted: false,
            reason: Some(CompactionNoopReason::NoProgress),
            diagnostics: Vec::new(),
        }
    }

    fn bind_output_observer(
        &self,
        _observer: Option<crate::context::compact::full_compact::CompactionOutputSink>,
    ) {
    }

    fn bind_session_memory(
        &self,
        _provider: Option<
            Arc<dyn crate::context::compact::full_compact::SessionMemoryCompactProvider>,
        >,
    ) {
    }

    /// Supply the live request prefix for a cache-preserving summary fork.
    fn prepare_request(
        &self,
        _request: &crate::types::provider::ModelRequest,
        _signal: &crate::types::abort::AbortSignal,
    ) {
    }

    /// Proactive compaction. `estimated_tokens` is the caller's estimate of the
    /// full request context, including fixed prefix and protected messages that
    /// are not present in `messages`.
    async fn maybe_compact(
        &self,
        messages: Vec<Message>,
        estimated_tokens: u64,
        config: &ResolvedConfig,
        custom_instructions: Option<&RuntimeString>,
    ) -> CompactionResult;

    /// Whether this seam permits an automatic reactive summary request.
    fn reactive_enabled(&self) -> bool {
        true
    }

    /// Proactive grouped recovery obeys automatic-failure protection. A real
    /// provider overflow can still use the independent reactive entry point.
    async fn threshold_compact(
        &self,
        messages: Vec<Message>,
        config: &ResolvedConfig,
        custom_instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.reactive_compact(messages, config, custom_instructions)
            .await
    }

    /// Reactive compaction goes straight to a fresh full summarization.
    async fn reactive_compact(
        &self,
        messages: Vec<Message>,
        config: &ResolvedConfig,
        custom_instructions: Option<&RuntimeString>,
    ) -> CompactionResult;
}

pub struct RealCompactor {
    full: Arc<FullCompactor>,
}

impl std::fmt::Debug for RealCompactor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RealCompactor")
            .field("full", &self.full)
            .finish()
    }
}

pub fn create_compactor(options: CompactorOptions) -> RealCompactor {
    RealCompactor {
        full: Arc::new(create_full_compactor(FullCompactorOptions {
            call_model: options.call_model,
            model: options.model,
            max_output_tokens: options.max_output_tokens,
            uuid: options.uuid,
        })),
    }
}

#[async_trait]
impl Compactor for RealCompactor {
    fn native_precompute_context(&self) -> Option<RuntimeString> {
        self.full.native_precompute_context()
    }
    fn fork_precompute(
        &self,
        call_model: CallModel,
        config: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        let full = self.full.rebind_model(call_model, config);
        full.retain_native_provider_from(&self.full)?;
        Ok(Arc::new(Self {
            full: Arc::new(full),
        }))
    }
    fn bind_native_provider(&self, provider: Arc<dyn ModelProvider>) {
        self.full.bind_native_provider(provider);
    }

    fn native_summary_active(&self) -> bool {
        self.full.native_summary_active()
    }

    fn fork_isolated(
        &self,
        call_model: CallModel,
        config: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        Ok(Arc::new(Self {
            full: Arc::new(self.full.rebind_model(call_model, config)),
        }))
    }

    fn rebind_summary(
        &self,
        call_model: CallModel,
        config: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        let mut full = self.full.rebind_model(call_model, config);
        full.pin_summary_target(config);
        full.retain_session_memory_from(&self.full);
        Ok(Arc::new(Self {
            full: Arc::new(full),
        }))
    }

    fn rebind_model(
        &self,
        call_model: CallModel,
        config: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        self.rebind_summary(call_model, config)
    }

    fn bind_output_observer(&self, observer: Option<super::full_compact::CompactionOutputSink>) {
        self.full.bind_output_observer(observer);
    }

    fn bind_session_memory(
        &self,
        provider: Option<Arc<dyn super::full_compact::SessionMemoryCompactProvider>>,
    ) {
        self.full.bind_session_memory(provider);
    }

    fn prepare_request(
        &self,
        request: &crate::types::provider::ModelRequest,
        signal: &AbortSignal,
    ) {
        self.full.prepare_request(request, signal);
    }

    async fn maybe_compact(
        &self,
        messages: Vec<Message>,
        estimated_tokens: u64,
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        if config.compaction_disabled {
            return CompactionResult {
                messages,
                freed_tokens: 0,
                compacted: false,
                reason: Some(CompactionNoopReason::CompactorDisabled),
                diagnostics: Vec::new(),
            };
        }
        if !config.auto_compaction
            || estimated_tokens < super::policy::preflight_threshold(config) as u64
        {
            return CompactionResult {
                messages,
                freed_tokens: 0,
                compacted: false,
                reason: Some(CompactionNoopReason::NoProgress),
                diagnostics: Vec::new(),
            };
        }
        self.full
            .threshold_compact(messages, config, instructions)
            .await
    }

    async fn manual_compact(
        &self,
        messages: Vec<Message>,
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.full
            .manual_compact(messages, config, instructions)
            .await
    }

    async fn local_compact(
        &self,
        messages: Vec<Message>,
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.full
            .local_compact(messages, config, instructions)
            .await
    }

    async fn local_compact_exact(
        &self,
        messages: Vec<Message>,
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.full
            .local_compact_exact(messages, config, instructions)
            .await
    }

    async fn post_turn_compact(
        &self,
        messages: Vec<Message>,
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.full
            .post_turn_compact(messages, config, instructions)
            .await
    }

    async fn partial_compact(
        &self,
        messages: Vec<Message>,
        index: usize,
        direction: PartialCompactionDirection,
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.full
            .partial_compact(messages, index, direction, config, instructions)
            .await
    }

    async fn threshold_compact(
        &self,
        messages: Vec<Message>,
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.full
            .threshold_compact(messages, config, instructions)
            .await
    }

    async fn reactive_compact(
        &self,
        messages: Vec<Message>,
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.full
            .reactive_compact(messages, config, instructions)
            .await
    }
}
