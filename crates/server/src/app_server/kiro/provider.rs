//! Source-local terminal and refusal semantics on the normal Responses codec.
use futures::StreamExt;
use klaus_agent_core::{
    runtime_values::RuntimeString,
    schemas::message::Usage,
    types::{
        abort::AbortSignal,
        errors::ProviderErrorKind,
        provider::{
            ModelCapabilities, ModelProvider, ModelRequest, ModelStopReason, ModelStream,
            ModelStreamEvent, ProviderCapabilities, ProviderInfo,
        },
    },
};
use std::sync::Arc;
pub(super) fn decorate(inner: Arc<dyn ModelProvider>) -> Arc<dyn ModelProvider> {
    Arc::new(Provider { inner })
}
struct Provider {
    inner: Arc<dyn ModelProvider>,
}
#[async_trait::async_trait]
impl ModelProvider for Provider {
    fn info(&self) -> &ProviderInfo {
        self.inner.info()
    }
    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        self.inner.capabilities(model)
    }
    fn model_metadata(&self, model: &RuntimeString) -> ModelCapabilities {
        self.inner.model_metadata(model)
    }
    fn offline_model_catalog(&self) -> Vec<(RuntimeString, ModelCapabilities)> {
        self.inner.offline_model_catalog()
    }
    fn has_model_capabilities(&self, model: &RuntimeString) -> bool {
        self.inner.has_model_capabilities(model)
    }
    fn estimate_tokens(&self, request: &ModelRequest) -> Option<u64> {
        self.inner.estimate_tokens(request)
    }
    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        let mut upstream = self.inner.stream(request, signal);
        Box::pin(async_stream::stream! {
            let mut usage=Usage{input_tokens:0,output_tokens:0,cache_read_input_tokens:0,cache_creation_input_tokens:0,total_tokens:None};
            while let Some(event)=upstream.next().await{
                let event=match event{
                    ModelStreamEvent::UsageSnapshot{usage:ref value}=>{usage=value.clone();event},
                    ModelStreamEvent::Error{mut error}=>{
                        let code=error.code.as_ref().and_then(|v|v.try_to_utf8().ok()).unwrap_or_default();
                        let reason=error.error_type.as_ref().and_then(|v|v.try_to_utf8().ok()).unwrap_or_default();
                        if code=="response_incomplete"{
                            match reason.as_str(){
                                "max_output_tokens"=>{yield ModelStreamEvent::MessageStop{stop_reason:ModelStopReason::MaxTokens,usage};return;},
                                "content_filter"=>error.kind=ProviderErrorKind::Withheld,
                                "compaction_required_for_model_handoff"=>{error.kind=ProviderErrorKind::PromptTooLong;error.code=Some("context_length_exceeded".into());error.message="kiro_context_length_exceeded".into();},
                                _=>{},
                            }
                        }else{
                            match code.as_str(){
                                "kiro_model_access_denied"|"kiro_model_consent_required"|"kiro_model_not_found"=>error.kind=ProviderErrorKind::InvalidRequest,
                                "kiro_quota_exhausted"=>error.kind=ProviderErrorKind::Quota,
                                "kiro_rate_limited"=>error.kind=ProviderErrorKind::RateLimit,
                                "kiro_context_length_exceeded"=>error.kind=ProviderErrorKind::PromptTooLong,
                                "kiro_login_expired"=>error.kind=ProviderErrorKind::Auth,
                                _=>{},
                            }
                        }
                        ModelStreamEvent::Error{error}
                    },
                    other=>other,
                };
                yield event;
            }
        })
    }
}
