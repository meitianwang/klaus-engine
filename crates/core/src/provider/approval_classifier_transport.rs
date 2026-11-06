//! Dedicated classifier transport with bounded, reusable Responses connections.

use std::pin::Pin;
use std::sync::Arc;

use futures::{Stream, StreamExt};

use crate::runtime_values::{RuntimeJson, RuntimeString};
use crate::types::abort::AbortSignal;
use crate::types::errors::ProviderError;
use crate::types::provider::{ModelCapabilities, ModelProvider, ModelRequest, ModelStreamEvent};

/// Attribution remains separate from the parent conversation's model input.
#[derive(Clone, Debug, Default)]
pub struct ClassifierAttribution {
    pub session_id: String,
    pub parent_thread_id: String,
    pub parent_turn_id: String,
    /// Stable across retries of the same classification.
    pub turn_id: String,
    pub root_turn_id: Option<String>,
    pub parent_response_id: Option<String>,
    pub originator: Option<String>,
    pub service_tier: Option<String>,
}

#[derive(Clone)]
pub struct ClassifierRequest {
    pub request: ModelRequest,
    pub attribution: ClassifierAttribution,
    pub retain_history: bool,
    /// Complete native items, including unchanged committed item identities.
    pub native_input: Option<Vec<RuntimeJson>>,
}

#[derive(Clone)]
pub struct ClassifierTarget {
    pub model: RuntimeString,
    pub metadata: ModelCapabilities,
    pub reasoning_effort: Option<RuntimeString>,
}

#[derive(Debug, Clone)]
pub enum ClassifierTransportEvent {
    Model(ModelStreamEvent),
    /// The complete provider-native item; no ID or opaque content is reconstructed.
    OutputItem(RuntimeJson),
}

pub type ClassifierStream = Pin<Box<dyn Stream<Item = ClassifierTransportEvent> + Send>>;

/// Rebuild a client with the same trust and proxy configuration for a route.
/// The caller must honor the requested redirect policy.
pub type ClassifierHttpClientFactory =
    Arc<dyn Fn(bool) -> Result<reqwest::Client, ProviderError> + Send + Sync>;

/// One classification owns its finite credential recovery sequence.
#[async_trait::async_trait]
pub trait ClassifierAuthRecovery: Send {
    /// A successful step permits another request; failure permanently ends the episode.
    async fn next(&mut self) -> bool;
}

#[async_trait::async_trait]
pub trait ApprovalClassifierTransport: Send + Sync {
    fn request_retries_managed(&self) -> bool {
        false
    }
    async fn prewarm(&self);
    fn stream(&self, request: ClassifierRequest, signal: AbortSignal) -> ClassifierStream;
    /// Capture recovery ownership before the classification's first request.
    async fn auth_recovery(
        &self,
        _signal: &AbortSignal,
    ) -> Option<Box<dyn ClassifierAuthRecovery>> {
        None
    }
    /// Stops admission, cancels opens and requests, and joins bounded cleanup.
    async fn shutdown(&self);
}

/// Each owning runtime receives an independent transport generation.
pub fn create_classifier_transport(
    provider: Arc<dyn ModelProvider>,
    attribution: ClassifierAttribution,
) -> Arc<dyn ApprovalClassifierTransport> {
    provider
        .create_approval_classifier_transport(attribution, None)
        .unwrap_or_else(|| Arc::new(ProviderClassifierTransport(provider)))
}

struct ProviderClassifierTransport(Arc<dyn ModelProvider>);

#[async_trait::async_trait]
impl ApprovalClassifierTransport for ProviderClassifierTransport {
    fn request_retries_managed(&self) -> bool {
        true
    }
    async fn prewarm(&self) {}

    fn stream(&self, request: ClassifierRequest, signal: AbortSignal) -> ClassifierStream {
        if request.native_input.as_ref().is_some_and(|items| {
            items.iter().any(|item| {
                item.get("type")
                    .and_then(RuntimeJson::as_str)
                    .is_some_and(|kind| kind == "compaction" || kind == "encrypted_agent_message")
            })
        }) {
            let error = ProviderError {
                kind: crate::types::errors::ProviderErrorKind::InvalidRequest,
                message: "This provider cannot consume native opaque classifier context.".into(),
                status: None,
                code: Some("unsupported_classifier_context".into()),
                error_type: None,
                media_origins: None,
                media_repair_complete: false,
                credential_recovery: None,
                retry_after_ms: None,
                request_id: None,
            };
            return Box::pin(futures::stream::once(async move {
                ClassifierTransportEvent::Model(ModelStreamEvent::Error { error })
            }));
        }
        Box::pin(
            self.0
                .stream(request.request, signal)
                .map(ClassifierTransportEvent::Model),
        )
    }

    async fn shutdown(&self) {}
}

mod responses;
pub(crate) use responses::{
    ClassifierAttempt, ClassifierConnector, ClassifierEndpoint, RawClassifierStream,
    ResponsesClassifierTransport,
};

#[cfg(test)]
pub(crate) fn test_request() -> ClassifierRequest {
    use crate::schemas::effort::{EffortLevel, EffortValue};
    use crate::types::provider::{ReasoningSummary, ThinkingContextPolicy, ToolChoice};
    ClassifierRequest {
        request: ModelRequest {
            model: "gpt-5.6-luna".into(),
            image_limits: None,
            max_media_blocks: None,
            supports_image_detail_original: false,
            system: Vec::new(),
            messages: Vec::new(),
            tools: Vec::new(),
            deferred_tools: Vec::new(),
            tool_choice: Some(ToolChoice::None),
            max_output_tokens: 16_384,
            task_budget: None,
            output_schema: None,
            web_search: None,
            thinking: None,
            effort: Some(EffortValue::Level(EffortLevel::Low)),
            model_verbosity: None,
            reasoning_summary: ReasoningSummary::None,
            skip_cache_write: None,
            cache_ttl: None,
            cache_key: Some("guardian-v2:parent-thread".into()),
            context_management: None,
            context_hints: None,
            thinking_context: Some(ThinkingContextPolicy::KeepAll),
        },
        attribution: ClassifierAttribution {
            session_id: "owning-session".into(),
            parent_thread_id: "parent-thread".into(),
            parent_turn_id: "parent-turn".into(),
            turn_id: "classification-turn".into(),
            root_turn_id: Some("root-turn".into()),
            parent_response_id: Some("parent-response".into()),
            originator: Some("klaus".into()),
            service_tier: None,
        },
        retain_history: true,
        native_input: Some(Vec::new()),
    }
}
