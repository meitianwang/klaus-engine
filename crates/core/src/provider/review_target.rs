//! Separate, explicitly admitted model targets for approval review.

use crate::runtime_values::RuntimeString;
use crate::schemas::effort::{EffortLevel, EffortValue};
use crate::types::abort::AbortSignal;
use crate::types::errors::{ProviderError, ProviderErrorKind};
use crate::types::provider::{
    AuthHeaders, ModelCapabilities, ModelProvider, ModelRequest, ModelStream, ProviderCapabilities,
    ProviderInfo,
};
use std::sync::Arc;

pub type OfflineModelCatalog =
    Arc<dyn Fn() -> Vec<(RuntimeString, ModelCapabilities)> + Send + Sync>;

/// Attach an account-bound local catalog to the transport. The callback performs
/// no network I/O and is invoked separately for every model-resolution attempt.
pub fn with_review_model_catalog(
    inner: Arc<dyn ModelProvider>,
    catalog: OfflineModelCatalog,
) -> Arc<dyn ModelProvider> {
    Arc::new(ReviewCatalogProvider { inner, catalog })
}

struct ReviewCatalogProvider {
    inner: Arc<dyn ModelProvider>,
    catalog: OfflineModelCatalog,
}

#[async_trait::async_trait]
impl ModelProvider for ReviewCatalogProvider {
    async fn native_checkpoint_compatibility(
        &self,
        checkpoint: &crate::schemas::message::NativeCompactionCheckpoint,
        model: &RuntimeString,
        signal: &crate::types::abort::AbortSignal,
    ) -> Result<bool, crate::types::errors::ProviderError> {
        self.inner
            .native_checkpoint_compatibility(checkpoint, model, signal)
            .await
    }

    fn native_compaction_context(
        &self,
        model: &RuntimeString,
    ) -> Option<crate::types::provider::NativeCompactionContext> {
        if self.remote_compaction_support(model)
            == crate::types::provider::RemoteCompactionSupport::Unsupported
        {
            return None;
        }
        self.inner
            .native_compaction_context(model)
            .map(|mut context| {
                context.identity = format!(
                    "{:?}:{:?}",
                    context.identity,
                    self.compaction_compatibility_hash(model)
                )
                .into();
                context
            })
    }

    fn remote_compaction_support(
        &self,
        model: &RuntimeString,
    ) -> crate::types::provider::RemoteCompactionSupport {
        self.inner.remote_compaction_support(model)
    }
    fn compaction_stream_max_retries(&self) -> Option<u32> {
        self.inner.compaction_stream_max_retries()
    }
    fn supports_compaction_model_fallback(&self) -> bool {
        self.inner.supports_compaction_model_fallback()
    }
    async fn compact_remote(
        &self,
        request: ModelRequest,
        signal: AbortSignal,
    ) -> Result<crate::types::provider::RemoteCompactionResult, ProviderError> {
        let model = request.model.clone();
        let mut result = self.inner.compact_remote(request, signal).await?;
        result.checkpoint.comp_hash = self.compaction_compatibility_hash(&model);
        Ok(result)
    }

    async fn compact_remote_with_usage(
        &self,
        request: crate::types::provider::ModelRequest,
        signal: crate::types::abort::AbortSignal,
        idle_ms: u64,
        observer: crate::types::provider::NativeCompactionUsageObserver,
        expected_scope: Option<RuntimeString>,
    ) -> Result<crate::types::provider::RemoteCompactionResult, crate::types::errors::ProviderError>
    {
        let model = request.model.clone();
        let mut result = self
            .inner
            .compact_remote_with_usage(request, signal, idle_ms, observer, expected_scope)
            .await?;
        result.checkpoint.comp_hash = self.compaction_compatibility_hash(&model);
        Ok(result)
    }

    async fn compact_remote_with_idle_timeout(
        &self,
        request: ModelRequest,
        signal: AbortSignal,
        idle_ms: u64,
    ) -> Result<crate::types::provider::RemoteCompactionResult, ProviderError> {
        let model = request.model.clone();
        let mut result = self
            .inner
            .compact_remote_with_idle_timeout(request, signal, idle_ms)
            .await?;
        result.checkpoint.comp_hash = self.compaction_compatibility_hash(&model);
        Ok(result)
    }
    fn info(&self) -> &ProviderInfo {
        self.inner.info()
    }
    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        self.inner.capabilities(model)
    }
    fn model_metadata(&self, model: &RuntimeString) -> ModelCapabilities {
        (self.catalog)()
            .into_iter()
            .find(|(id, _)| id == model)
            .map(|(_, metadata)| metadata)
            .unwrap_or_else(|| self.inner.model_metadata(model))
    }
    fn offline_model_catalog(&self) -> Vec<(RuntimeString, ModelCapabilities)> {
        (self.catalog)()
    }
    fn approval_review_preferred_model(&self) -> RuntimeString {
        self.inner.approval_review_preferred_model()
    }
    fn uses_official_api_catalog(&self) -> bool {
        self.inner.uses_official_api_catalog()
    }
    fn managed_review_cloud_auth_source(
        &self,
    ) -> Option<Arc<dyn crate::engine::approval_requirements_cloud::ManagedReviewCloudAuthSource>>
    {
        self.inner.managed_review_cloud_auth_source()
    }
    fn classifier_auth_changes(
        &self,
    ) -> Option<tokio::sync::watch::Receiver<crate::provider::auth::CredentialChangeState>> {
        self.inner.classifier_auth_changes()
    }
    fn approval_request_tokens(&self, request: &ModelRequest) -> Result<usize, RuntimeString> {
        self.inner.approval_request_tokens(request)
    }
    fn approval_request_tokens_with_metadata(
        &self,
        request: &ModelRequest,
        metadata: ModelCapabilities,
        raw_effort: Option<RuntimeString>,
    ) -> Result<usize, RuntimeString> {
        self.inner
            .approval_request_tokens_with_metadata(request, metadata, raw_effort)
    }

    fn create_approval_classifier_transport(
        &self,
        attribution: crate::provider::approval_classifier_transport::ClassifierAttribution,
        target: Option<crate::provider::approval_classifier_transport::ClassifierTarget>,
    ) -> Option<Arc<dyn crate::provider::approval_classifier_transport::ApprovalClassifierTransport>>
    {
        self.inner
            .create_approval_classifier_transport(attribution, target)
    }

    fn review_target_provider(&self) -> Option<Arc<dyn ModelProvider>> {
        Some(
            self.inner
                .review_target_provider()
                .unwrap_or_else(|| self.inner.clone()),
        )
    }
    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        self.inner.stream(request, signal)
    }
    fn stream_with_model_metadata(
        &self,
        request: ModelRequest,
        metadata: ModelCapabilities,
        raw_effort: Option<RuntimeString>,
        total_attempts: u32,
        signal: AbortSignal,
    ) -> ModelStream {
        self.inner
            .stream_with_model_metadata(request, metadata, raw_effort, total_attempts, signal)
    }
    fn estimate_tokens(&self, request: &ModelRequest) -> Option<u64> {
        self.inner.estimate_tokens(request)
    }
    async fn resolve_auth(&self) -> Option<Result<AuthHeaders, ProviderError>> {
        self.inner.resolve_auth().await
    }
    fn credential_account_id(&self) -> Option<String> {
        self.inner.credential_account_id()
    }
    async fn rotate_credential(
        &self,
        failed_id: &str,
    ) -> Option<crate::provider::rotation::RotateOutcome> {
        self.inner.rotate_credential(failed_id).await
    }
}

/// The target and attribution captured for one review attempt.
pub struct ResolvedReviewTarget {
    pub provider: Arc<dyn ModelProvider>,
    pub model: RuntimeString,
    pub metadata: ModelCapabilities,
    pub capabilities: ProviderCapabilities,
    pub effort: Option<EffortValue>,
    pub raw_effort: Option<RuntimeString>,
    pub default_review_model: RuntimeString,
    pub catalog_contains_preferred: bool,
    pub model_overridden: bool,
    pub model_override: Option<RuntimeString>,
}

fn typed_effort(value: &RuntimeString) -> Option<EffortValue> {
    let level = match value.try_to_utf8().ok()?.as_str() {
        "low" => EffortLevel::Low,
        "medium" => EffortLevel::Medium,
        "high" => EffortLevel::High,
        "xhigh" => EffortLevel::Xhigh,
        "max" => EffortLevel::Max,
        "ultra" => EffortLevel::Xhigh,
        _ => return None,
    };
    Some(EffortValue::Level(level))
}

fn raw_typed_effort(value: Option<EffortValue>) -> Option<RuntimeString> {
    value.and_then(|value| match value {
        EffortValue::Level(level) => Some(
            match level {
                EffortLevel::None => "none",
                EffortLevel::Minimal => "minimal",
                EffortLevel::Persistent => "persistent",
                EffortLevel::Low => "low",
                EffortLevel::Medium => "medium",
                EffortLevel::High => "high",
                EffortLevel::Xhigh => "xhigh",
                EffortLevel::Max => "max",
                EffortLevel::Ultra => "xhigh",
            }
            .into(),
        ),
        _ => None,
    })
}

fn low_or(metadata: &ModelCapabilities, fallback: Option<RuntimeString>) -> Option<RuntimeString> {
    if metadata
        .supported_reasoning_efforts
        .iter()
        .any(|level| level == "low")
        || metadata
            .effort
            .as_ref()
            .is_some_and(|effort| effort.levels.contains(&EffortLevel::Low))
    {
        Some("low".into())
    } else {
        fallback
    }
}

pub(crate) fn unsupported_review_effort() -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::InvalidRequest,
        message: "The selected provider transport cannot represent this review reasoning effort"
            .into(),
        status: Some(400),
        code: Some("unsupported_review_effort".into()),
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        credential_recovery: None,
        retry_after_ms: None,
        request_id: None,
    }
}

/// Preserve existing typed adapters and reject source effort strings they cannot encode.
pub(crate) fn apply_typed_review_effort(
    request: &mut ModelRequest,
    raw: Option<&RuntimeString>,
) -> Result<(), ProviderError> {
    if let Some(raw) = raw {
        request.effort = if raw == "none" {
            None
        } else {
            Some(typed_effort(raw).ok_or_else(unsupported_review_effort)?)
        };
    }
    Ok(())
}

/// Resolve against the full local catalog while preserving the captured action's
/// metadata when the provider's preferred reviewer is unavailable.
pub fn resolve_approval_review_target(
    parent_provider: Arc<dyn ModelProvider>,
    parent_model: &RuntimeString,
    parent_effort: Option<EffortValue>,
) -> ResolvedReviewTarget {
    let parent_metadata = parent_provider.model_metadata(parent_model);
    let preferred = parent_provider.approval_review_preferred_model();
    let catalog = parent_provider.offline_model_catalog();
    let model_override = parent_metadata.auto_review_model_override.clone();
    let wanted = model_override.as_ref().unwrap_or(&preferred);
    let catalog_contains_preferred = catalog.iter().any(|(id, _)| id == &preferred);
    let selected = catalog.iter().find(|(id, _)| id == wanted);
    let (model, metadata, raw_effort) = if let Some((id, metadata)) = selected {
        let fallback = metadata.default_reasoning_effort.clone().or_else(|| {
            raw_typed_effort(
                metadata
                    .effort
                    .as_ref()
                    .and_then(|effort| effort.default)
                    .map(EffortValue::Level),
            )
        });
        (id.clone(), metadata.clone(), low_or(metadata, fallback))
    } else {
        let fallback = raw_typed_effort(parent_effort.clone())
            .or_else(|| parent_metadata.default_reasoning_effort.clone())
            .or_else(|| {
                raw_typed_effort(
                    parent_metadata
                        .effort
                        .as_ref()
                        .and_then(|effort| effort.default)
                        .map(EffortValue::Level),
                )
            });
        (
            model_override
                .clone()
                .unwrap_or_else(|| parent_model.clone()),
            parent_metadata.clone(),
            low_or(&parent_metadata, fallback),
        )
    };
    let raw_effort = raw_effort.map(|effort| {
        if effort == "ultra" {
            "xhigh".into()
        } else {
            effort
        }
    });
    let effort = raw_effort.as_ref().and_then(typed_effort).or_else(|| {
        if selected.is_none() {
            parent_effort.filter(|effort| matches!(effort, EffortValue::Number(_)))
        } else {
            None
        }
    });
    let provider = capture_review_model_target_with_effort(
        parent_provider,
        model.clone(),
        metadata.clone(),
        raw_effort.clone(),
        2,
    );
    let capabilities = provider.capabilities(&model);
    ResolvedReviewTarget {
        provider,
        model,
        metadata,
        capabilities,
        effort,
        raw_effort,
        default_review_model: preferred,
        catalog_contains_preferred,
        model_overridden: model_override.is_some(),
        model_override,
    }
}

/// Admit one explicit model using the same transport and credential source.
/// This creates a new capture; it never relaxes an existing capture's allowlist.
pub fn capture_review_model_target(
    provider: Arc<dyn ModelProvider>,
    model: RuntimeString,
    metadata: ModelCapabilities,
) -> Arc<dyn ModelProvider> {
    capture_review_model_target_with_retries(provider, model, metadata, 2)
}

pub fn capture_review_model_target_with_retries(
    provider: Arc<dyn ModelProvider>,
    model: RuntimeString,
    metadata: ModelCapabilities,
    total_attempts: u32,
) -> Arc<dyn ModelProvider> {
    capture_review_model_target_with_effort(provider, model, metadata, None, total_attempts)
}

pub fn capture_review_model_target_with_effort(
    provider: Arc<dyn ModelProvider>,
    model: RuntimeString,
    metadata: ModelCapabilities,
    raw_effort: Option<RuntimeString>,
    total_attempts: u32,
) -> Arc<dyn ModelProvider> {
    let raw_effort = raw_effort.map(|effort| {
        if effort == "ultra" {
            "xhigh".into()
        } else {
            effort
        }
    });
    let root = provider.review_target_provider().unwrap_or(provider);
    let target: Arc<dyn ModelProvider> = Arc::new(DeclaredReviewTarget {
        root,
        model: model.clone(),
        metadata,
        raw_effort,
        total_attempts: total_attempts.max(1),
    });
    super::model_catalog::capture_model_provider(target, &[model])
}

struct DeclaredReviewTarget {
    root: Arc<dyn ModelProvider>,
    model: RuntimeString,
    metadata: ModelCapabilities,
    raw_effort: Option<RuntimeString>,
    total_attempts: u32,
}

#[async_trait::async_trait]
impl ModelProvider for DeclaredReviewTarget {
    fn info(&self) -> &ProviderInfo {
        self.root.info()
    }
    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        let mut caps = self.root.capabilities(model);
        if model == &self.model {
            if let Some(window) = self.metadata.context_window {
                caps.context_window = window;
            }
            if let Some(output) = self.metadata.max_output_tokens {
                caps.max_output_tokens = output;
            }
            if let Some(tools) = self.metadata.tools {
                caps.tools = tools;
            }
            caps.supports_image_detail_original = self.metadata.supports_image_detail_original;
            caps.thinking = self.metadata.thinking.is_some() || self.metadata.effort.is_some();
            caps.structured_output = self.metadata.structured_output;
            caps.web_search = self.metadata.web_search;
            caps.native_pdf = self.metadata.native_pdf;
            caps.vision = self.metadata.vision;
            caps.image_limits = self.metadata.image_limits.or(caps.image_limits);
            caps.max_media_blocks = self.metadata.max_media_blocks.or(caps.max_media_blocks);
        }
        caps
    }
    fn model_metadata(&self, model: &RuntimeString) -> ModelCapabilities {
        if model == &self.model {
            self.metadata.clone()
        } else {
            ModelCapabilities::default()
        }
    }
    fn offline_model_catalog(&self) -> Vec<(RuntimeString, ModelCapabilities)> {
        self.root.offline_model_catalog()
    }
    fn approval_review_preferred_model(&self) -> RuntimeString {
        self.root.approval_review_preferred_model()
    }
    fn uses_official_api_catalog(&self) -> bool {
        self.root.uses_official_api_catalog()
    }
    fn managed_review_cloud_auth_source(
        &self,
    ) -> Option<Arc<dyn crate::engine::approval_requirements_cloud::ManagedReviewCloudAuthSource>>
    {
        self.root.managed_review_cloud_auth_source()
    }
    fn classifier_auth_changes(
        &self,
    ) -> Option<tokio::sync::watch::Receiver<crate::provider::auth::CredentialChangeState>> {
        self.root.classifier_auth_changes()
    }
    fn approval_request_tokens(&self, request: &ModelRequest) -> Result<usize, RuntimeString> {
        self.root.approval_request_tokens_with_metadata(
            request,
            self.metadata.clone(),
            self.raw_effort.clone(),
        )
    }
    fn create_approval_classifier_transport(
        &self,
        attribution: crate::provider::approval_classifier_transport::ClassifierAttribution,
        target: Option<crate::provider::approval_classifier_transport::ClassifierTarget>,
    ) -> Option<Arc<dyn crate::provider::approval_classifier_transport::ApprovalClassifierTransport>>
    {
        self.root.create_approval_classifier_transport(
            attribution,
            Some(target.unwrap_or_else(|| {
                crate::provider::approval_classifier_transport::ClassifierTarget {
                    model: self.model.clone(),
                    metadata: self.metadata.clone(),
                    reasoning_effort: self.raw_effort.clone(),
                }
            })),
        )
    }

    fn review_target_provider(&self) -> Option<Arc<dyn ModelProvider>> {
        Some(self.root.clone())
    }
    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        if request.model != self.model {
            return super::model_catalog::uncaptured_model_stream(&request.model);
        }
        let mut request = request;
        if self.raw_effort.is_none()
            && self.metadata.effort.is_none()
            && self.metadata.supported_reasoning_efforts.is_empty()
        {
            request.effort = None;
        }
        self.root.stream_with_model_metadata(
            request,
            self.metadata.clone(),
            self.raw_effort.clone(),
            self.total_attempts,
            signal,
        )
    }
    fn estimate_tokens(&self, request: &ModelRequest) -> Option<u64> {
        self.root.estimate_tokens(request)
    }
    async fn resolve_auth(&self) -> Option<Result<AuthHeaders, ProviderError>> {
        self.root.resolve_auth().await
    }
    fn credential_account_id(&self) -> Option<String> {
        self.root.credential_account_id()
    }
    async fn rotate_credential(
        &self,
        failed_id: &str,
    ) -> Option<crate::provider::rotation::RotateOutcome> {
        self.root.rotate_credential(failed_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
    use crate::types::provider::{EffortCapability, ModelStreamEvent};
    use std::sync::Mutex;

    struct TypedAdapter {
        inner: OpenAIProvider,
        requests: Mutex<Vec<ModelRequest>>,
    }

    impl ModelProvider for TypedAdapter {
        fn info(&self) -> &ProviderInfo {
            self.inner.info()
        }
        fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
            self.inner.capabilities(model)
        }
        fn approval_review_preferred_model(&self) -> RuntimeString {
            "review".into()
        }
        fn offline_model_catalog(&self) -> Vec<(RuntimeString, ModelCapabilities)> {
            vec![("review".into(), ModelCapabilities::default())]
        }
        fn stream(&self, request: ModelRequest, _signal: AbortSignal) -> ModelStream {
            self.requests.lock().unwrap().push(request);
            Box::pin(futures::stream::iter([ModelStreamEvent::TextDelta {
                text: "{\"outcome\":\"allow\"}".into(),
            }]))
        }
    }

    #[tokio::test]
    async fn non_reasoning_review_reaches_typed_adapter_without_inherited_effort() {
        use crate::runtime_values::runtime_codec;
        use futures::StreamExt;
        let source = Arc::new(TypedAdapter {
            inner: OpenAIProvider::new(OpenAIProviderOptions::default()),
            requests: Mutex::new(Vec::new()),
        });
        let selected = resolve_approval_review_target(source.clone(), &"parent".into(), None);
        assert_eq!(selected.model, "review");
        assert_eq!(selected.raw_effort, None);
        let request: ModelRequest = runtime_codec::decode_text(&RuntimeString::from(
            r#"{"model":"review","system":[],"messages":[],"tools":[],"maxOutputTokens":128,"effort":"high"}"#,
        )).unwrap();
        let events = selected
            .provider
            .stream(request.clone(), AbortSignal::new())
            .collect::<Vec<_>>()
            .await;
        assert!(
            matches!(&events[..], [ModelStreamEvent::TextDelta { text }] if text.contains("allow"))
        );
        assert_eq!(source.requests.lock().unwrap()[0].effort, None);
        let low = capture_review_model_target_with_effort(
            source.clone(),
            "review".into(),
            ModelCapabilities::default(),
            Some("low".into()),
            2,
        );
        let events = low
            .stream(request.clone(), AbortSignal::new())
            .collect::<Vec<_>>()
            .await;
        assert!(matches!(&events[..], [ModelStreamEvent::TextDelta { .. }]));
        assert_eq!(
            source.requests.lock().unwrap()[1].effort,
            Some(EffortValue::Level(EffortLevel::Low))
        );
        for unsupported in ["persistent", "custom-effort", "None"] {
            let target = capture_review_model_target_with_effort(
                source.clone(),
                "review".into(),
                ModelCapabilities::default(),
                Some(unsupported.into()),
                2,
            );
            let events = target
                .stream(request.clone(), AbortSignal::new())
                .collect::<Vec<_>>()
                .await;
            assert!(
                matches!(&events[..], [ModelStreamEvent::Error { error }] if error.code.as_ref().is_some_and(|code| code == "unsupported_review_effort"))
            );
        }
        assert_eq!(source.requests.lock().unwrap().len(), 2);
    }

    #[test]
    fn parent_fallback_retains_captured_metadata_while_each_review_reads_current_catalog() {
        let old = ModelCapabilities {
            effort: Some(EffortCapability {
                levels: vec![EffortLevel::High],
                default: Some(EffortLevel::High),
            }),
            context_window: Some(100000),
            ..Default::default()
        };
        let live = Arc::new(Mutex::new(vec![(
            RuntimeString::from("parent"),
            old.clone(),
        )]));
        let source = live.clone();
        let inner: Arc<dyn ModelProvider> = Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
            api: Some(OpenAiApi::Responses),
            default_model: Some("parent".into()),
            capabilities: Some(old.clone()),
            ..Default::default()
        }));
        let provider =
            with_review_model_catalog(inner, Arc::new(move || source.lock().unwrap().clone()));
        let captured =
            crate::provider::model_catalog::capture_model_provider(provider, &["parent".into()]);
        let mut newer = old.clone();
        newer.context_window = Some(300000);
        *live.lock().unwrap() = vec![("parent".into(), newer.clone())];
        let fallback = resolve_approval_review_target(captured.clone(), &"parent".into(), None);
        assert_eq!(fallback.metadata.context_window, Some(100000));
        assert_eq!(fallback.model, "parent");
        newer.effort = Some(EffortCapability {
            levels: vec![EffortLevel::Low, EffortLevel::High],
            default: Some(EffortLevel::High),
        });
        live.lock()
            .unwrap()
            .push(("codex-auto-review".into(), newer));
        let distinct = resolve_approval_review_target(captured.clone(), &"parent".into(), None);
        assert_eq!(distinct.model, "codex-auto-review");
        assert_eq!(distinct.capabilities.context_window, 300000);
        assert_eq!(distinct.effort, Some(EffortValue::Level(EffortLevel::Low)));
        assert!(!captured.has_model_capabilities(&distinct.model));
        assert!(distinct.provider.has_model_capabilities(&distinct.model));
    }
    #[test]
    fn legacy_local_mode_values_are_normalized_before_native_review_requests() {
        assert_eq!(
            typed_effort(&"ultra".into()),
            Some(EffortValue::Level(EffortLevel::Xhigh))
        );
        assert_eq!(
            raw_typed_effort(Some(EffortValue::Level(EffortLevel::Ultra))),
            Some("xhigh".into())
        );
    }
}
