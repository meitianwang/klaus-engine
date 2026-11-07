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
    forced_tool_choice_with_thinking: Option<bool>,
) -> Value {
    capture_provider_request(request, |base_url| {
        Box::new(AnthropicProvider::new(AnthropicProviderOptions {
            api_key: Some("test-key".into()),
            base_url: Some(base_url),
            capabilities: Some(ModelCapabilities {
                thinking: Some(thinking),
                forced_tool_choice_with_thinking,
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
        None,
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
async fn gateway_structured_titles_use_auto_tool_when_thinking_cannot_disable() {
    let mut request = record_engine_request(true).await;
    request.thinking = Some(ThinkingConfig::Adaptive);
    let body = gateway_body_with_capability(
        request,
        ThinkingCapability {
            adaptive: true,
            interleaved: false,
            can_disable_thinking: false,
        },
        Some(false),
    )
    .await;
    assert_eq!(body["tools"][0]["name"], json!("emit_result"));
    assert_eq!(body["tool_choice"], json!({"type": "auto"}));
    assert_eq!(
        body["thinking"],
        json!({"type": "adaptive"}),
        "a model that rejects disabled thinking must keep its own thinking mode"
    );
}

#[tokio::test]
async fn gateway_structured_titles_use_auto_tool_without_an_adaptive_mode() {
    let request = record_engine_request(true).await;
    let body = gateway_body_with_capability(
        request,
        ThinkingCapability {
            adaptive: false,
            interleaved: false,
            can_disable_thinking: false,
        },
        Some(false),
    )
    .await;
    assert_eq!(body["tools"][0]["name"], json!("emit_result"));
    assert_eq!(body["tool_choice"], json!({"type": "auto"}));
    assert!(
        body.get("thinking").is_none(),
        "without an adaptive mode the gateway default must be left untouched"
    );
}

#[tokio::test]
async fn required_thinking_without_a_declared_tool_restriction_preserves_forced_output() {
    let mut request = record_engine_request(true).await;
    request.thinking = Some(ThinkingConfig::Adaptive);
    let expected_system: Vec<String> = request
        .system
        .iter()
        .map(|part| part.text.try_to_utf8().unwrap())
        .collect();
    for declaration in [None, Some(true)] {
        let body = gateway_body_with_capability(
            request.clone(),
            ThinkingCapability {
                adaptive: true,
                interleaved: false,
                can_disable_thinking: false,
            },
            declaration,
        )
        .await;
        assert_eq!(
            body["tool_choice"],
            json!({"type":"tool","name":"emit_result"})
        );
        assert_eq!(body["thinking"], json!({"type":"adaptive"}));
        let system: Vec<&str> = body["system"]
            .as_array()
            .unwrap()
            .iter()
            .map(|part| part["text"].as_str().unwrap())
            .collect();
        assert_eq!(system, expected_system);
    }
}

#[tokio::test]
async fn declared_tool_restriction_does_not_change_ordinary_agent_requests() {
    let mut request = record_engine_request(false).await;
    request.thinking = Some(ThinkingConfig::Adaptive);
    let body = gateway_body_with_capability(
        request,
        ThinkingCapability {
            adaptive: true,
            interleaved: false,
            can_disable_thinking: false,
        },
        Some(false),
    )
    .await;
    assert_eq!(body["tool_choice"], json!({"type":"auto"}));
    assert_eq!(body["thinking"], json!({"type":"adaptive"}));
    assert_eq!(body["tools"][0]["name"], json!("probe"));
    assert!(!body["tools"].to_string().contains("emit_result"));
    assert!(!body["system"].to_string().contains("emit_result"));
}
