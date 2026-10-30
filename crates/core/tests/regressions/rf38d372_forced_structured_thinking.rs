//! Defect: Anthropic-compatible gateways rejected forced structured title output.
//! Root cause: omitted thinking let gateways enable it beside a forced tool.
//! Original fix: f38d372; forced gateway requests must explicitly disable thinking.

use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
use klaus_agent_core::types::provider::{
    ModelCapabilities, ModelRequest, ThinkingCapability, ThinkingConfig,
};
use serde_json::{json, Value};

use super::rbd0a6abc_empty_tool_choice::{
    capture_provider_request, record_engine_request, single_attempt,
};

async fn gateway_body_with_capability(
    request: ModelRequest,
    thinking: ThinkingCapability,
) -> Value {
    capture_provider_request(request, |base_url| {
        Box::new(AnthropicProvider::new(AnthropicProviderOptions {
            api_key: Some("test-key".into()),
            base_url: Some(base_url),
            capabilities: Some(ModelCapabilities {
                thinking: Some(thinking),
                structured_output: true,
                image_limits: None,
                max_media_blocks: None,
                ..Default::default()
            }),
            retry: Some(single_attempt()),
            ..Default::default()
        }))
    })
    .await
}

async fn gateway_body(request: ModelRequest) -> Value {
    gateway_body_with_capability(
        request,
        ThinkingCapability {
            adaptive: true,
            interleaved: false,
            can_disable_thinking: true,
        },
    )
    .await
}

#[tokio::test]
async fn gateway_forced_structured_titles_explicitly_disable_thinking() {
    let request = record_engine_request(true).await;
    assert!(request.output_schema.is_some());
    for thinking in [
        None,
        Some(ThinkingConfig::Disabled),
        Some(ThinkingConfig::Adaptive),
        Some(ThinkingConfig::Enabled {
            budget_tokens: Some(64),
        }),
    ] {
        let mut request = request.clone();
        request.thinking = thinking;
        let body = gateway_body(request).await;
        assert_eq!(body["tools"][0]["name"], json!("emit_result"));
        assert_eq!(
            body["tool_choice"],
            json!({"type": "tool", "name": "emit_result"})
        );
        assert_eq!(
            body["thinking"],
            json!({"type": "disabled"}),
            "{thinking:?}: {body}"
        );
    }
}

#[tokio::test]
async fn ordinary_gateway_requests_preserve_declared_thinking() {
    let request = record_engine_request(true).await;
    for (thinking, expected) in [
        (None, None),
        (Some(ThinkingConfig::Disabled), None),
        (
            Some(ThinkingConfig::Adaptive),
            Some(json!({"type": "adaptive"})),
        ),
        (
            Some(ThinkingConfig::Enabled {
                budget_tokens: Some(64),
            }),
            Some(json!({"type": "enabled", "budget_tokens": 64})),
        ),
    ] {
        let mut request = request.clone();
        request.output_schema = None;
        request.thinking = thinking;
        let body = gateway_body(request).await;
        assert_eq!(
            body.get("thinking"),
            expected.as_ref(),
            "{thinking:?}: {body}"
        );
        assert!(
            body.get("tools").is_none(),
            "ordinary requests do not force a schema tool: {body}"
        );
    }
}

#[tokio::test]
async fn gateway_forced_structured_titles_keep_thinking_when_model_cannot_disable() {
    let request = record_engine_request(true).await;
    let body = gateway_body_with_capability(
        request,
        ThinkingCapability {
            adaptive: true,
            interleaved: false,
            can_disable_thinking: false,
        },
    )
    .await;
    assert_eq!(body["tools"][0]["name"], json!("emit_result"));
    assert_eq!(
        body["thinking"],
        json!({"type": "adaptive"}),
        "a model that rejects disabled thinking must keep its own thinking mode"
    );
}

#[tokio::test]
async fn gateway_forced_structured_titles_omit_thinking_without_an_adaptive_mode() {
    let request = record_engine_request(true).await;
    let body = gateway_body_with_capability(
        request,
        ThinkingCapability {
            adaptive: false,
            interleaved: false,
            can_disable_thinking: false,
        },
    )
    .await;
    assert_eq!(body["tools"][0]["name"], json!("emit_result"));
    assert!(
        body.get("thinking").is_none(),
        "without an adaptive mode the gateway default must be left untouched"
    );
}
