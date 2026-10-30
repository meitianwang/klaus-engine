//! Defect: cloud refusals lost their source code before retry and turn failure.
//! Root cause: a custom Gemini transport was reduced to HTTP status and text.
//! Work item: 20261019 preserve source refusal details at the public stream.

use futures::StreamExt;
use klaus_agent_core::provider::classify::RawProviderError;
use klaus_agent_core::provider::gemini::{
    GeminiApiError, GeminiChunkStream, GeminiClient, GeminiProvider, GeminiProviderOptions,
    GenerateContentParameters,
};
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{EngineError, ProviderErrorKind};
use klaus_agent_core::types::provider::{
    AuthHeaders, ModelProvider, ModelRequest, ModelStreamEvent,
};
use std::sync::Arc;

struct Refusal {
    midstream: bool,
}

#[async_trait::async_trait]
impl GeminiClient for Refusal {
    fn error_details(&self, error: &GeminiApiError) -> RawProviderError {
        RawProviderError {
            status: error.status,
            message: error.message.clone(),
            code: Some("antigravity_model_access_denied".into()),
            error_type: Some("PERMISSION_DENIED".into()),
            headers: error.headers.clone().unwrap_or_default(),
            ..Default::default()
        }
    }
    async fn generate_content_stream(
        &self,
        params: GenerateContentParameters,
        headers: Option<AuthHeaders>,
        extra_headers: Vec<(String, String)>,
        signal: AbortSignal,
    ) -> Result<GeminiChunkStream, GeminiApiError> {
        assert_eq!(params.model, "declared-model");
        assert!(headers.is_none());
        assert!(extra_headers.is_empty());
        assert!(!signal.is_cancelled());
        let error = GeminiApiError {
            status: Some(403),
            message: "antigravity_model_access_denied: The service denied this model.".into(),
            headers: Some(std::collections::HashMap::from([(
                "x-request-id".into(),
                "fixture-123".into(),
            )])),
        };
        if self.midstream {
            Ok(Box::pin(futures::stream::iter([Err(error)])))
        } else {
            Err(error)
        }
    }
}

#[tokio::test]
async fn custom_source_refusals_keep_their_kind_and_details_in_turn_failure_payloads() {
    for midstream in [false, true] {
        let provider = GeminiProvider::new(GeminiProviderOptions {
            default_model: Some("declared-model".into()),
            client: Some(Arc::new(Refusal { midstream })),
            ..Default::default()
        });
        let request:ModelRequest=runtime_codec::decode_text(&RuntimeString::from(r#"{"model":"declared-model","system":[],"messages":[],"tools":[],"maxOutputTokens":100}"#)).unwrap();
        let events: Vec<_> = provider.stream(request, AbortSignal::new()).collect().await;
        let error = events
            .into_iter()
            .find_map(|event| match event {
                ModelStreamEvent::Error { error } => Some(error),
                _ => None,
            })
            .unwrap();
        assert_eq!(error.kind, ProviderErrorKind::InvalidRequest);
        assert_eq!(error.status, Some(403));
        assert_eq!(
            error.code.as_ref().unwrap(),
            "antigravity_model_access_denied"
        );
        assert_eq!(error.error_type.as_ref().unwrap(), "PERMISSION_DENIED");
        assert_eq!(error.request_id.as_ref().unwrap(), "fixture-123");
        let failure = EngineError::from_provider(error, "");
        let public = runtime_codec::encode_text(&failure)
            .unwrap()
            .try_to_utf8()
            .unwrap();
        for expected in [
            "providerError",
            "antigravity_model_access_denied",
            "invalid_request",
            "PERMISSION_DENIED",
            "fixture-123",
            "403",
        ] {
            assert!(public.contains(expected), "{public}");
        }
    }
}
