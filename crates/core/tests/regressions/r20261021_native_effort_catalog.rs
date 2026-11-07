//! Defect: native Gemini reasoning selections never reached the request transport.
//! Root cause: request construction ignored the selected effort despite a declared ladder.
//! Work item: 20261021 native reasoning recovery.

use futures::StreamExt;
use klaus_agent_core::provider::gemini::{
    GeminiApiError, GeminiChunkStream, GeminiClient, GeminiProvider, GeminiProviderOptions,
    GeminiThinkingDialect, GenerateContentParameters,
};
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::schemas::effort::{EffortLevel, EffortValue};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    AuthHeaders, EffortCapability, ModelCapabilities, ModelProvider, ModelRequest,
    ModelStreamEvent, ThinkingCapability,
};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct CaptureClient(Mutex<Vec<GenerateContentParameters>>);

#[async_trait::async_trait]
impl GeminiClient for CaptureClient {
    async fn generate_content_stream(
        &self,
        params: GenerateContentParameters,
        _headers: Option<AuthHeaders>,
        _extra_headers: Vec<(String, String)>,
        _signal: AbortSignal,
    ) -> Result<GeminiChunkStream, GeminiApiError> {
        self.0.lock().unwrap().push(params);
        let response = runtime_codec::decode_text(&RuntimeString::from(
            r#"{"candidates":[{"finishReason":"STOP"}]}"#,
        ))
        .unwrap();
        Ok(Box::pin(futures::stream::iter([Ok(response)])))
    }
}

#[tokio::test]
async fn declared_native_effort_reaches_the_public_gemini_provider() {
    for (dialect, levels, expected_level, expected_budget) in [
        (
            GeminiThinkingDialect::Level,
            vec![EffortLevel::Low],
            Some("LOW"),
            None,
        ),
        (
            GeminiThinkingDialect::Budget {
                min: 128,
                max: 32768,
                can_disable: false,
            },
            vec![EffortLevel::Low, EffortLevel::Max],
            None,
            Some(32768),
        ),
    ] {
        let client = Arc::new(CaptureClient::default());
        let provider = GeminiProvider::new(GeminiProviderOptions {
            client: Some(client.clone()),
            thinking_dialect: Some(dialect),
            capabilities: Some(ModelCapabilities {
                thinking: Some(ThinkingCapability::default()),
                effort: Some(EffortCapability {
                    levels,
                    default: None,
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
        let mut request: ModelRequest = runtime_codec::decode_text(&RuntimeString::from(
            r#"{"model":"native-effort-fixture","system":[],"messages":[],"tools":[],"maxOutputTokens":128}"#,
        )).unwrap();
        request.effort = Some(EffortValue::Level(EffortLevel::Max));
        let events = provider
            .stream(request, AbortSignal::new())
            .collect::<Vec<_>>()
            .await;
        assert!(!events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::Error { .. })));
        let calls = client.0.lock().unwrap();
        assert_eq!(calls.len(), 1);
        let thinking = calls[0]
            .config
            .thinking_config
            .as_ref()
            .expect("declared native effort");
        assert_eq!(
            thinking.thinking_level,
            expected_level.map(RuntimeString::from)
        );
        assert_eq!(thinking.thinking_budget, expected_budget);
    }
}
