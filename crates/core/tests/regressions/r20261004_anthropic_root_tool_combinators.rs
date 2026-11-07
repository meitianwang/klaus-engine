//! Defect: Claude subscription turns failed with a 400 before generating text.
//! Root cause: the Anthropic adapter forwarded unsupported root tool combinators.
//! Fixed by: 2026-10-04 Claude tool-schema compatibility work item.

use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
use klaus_agent_core::provider::claude_subscription::{
    create_claude_subscription_provider, ClaudeSubscriptionOptions,
};
use klaus_agent_core::provider::oauth::{OAuthConfig, OAuthTokens};
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::types::provider::{ModelRequest, ToolSpec, ToolSpecKind};
use serde_json::{json, Value};

use super::rbd0a6abc_empty_tool_choice::{
    capture_provider_request, record_engine_request, single_attempt,
};

fn exact(value: &Value) -> RuntimeJson {
    runtime_json::parse(&RuntimeString::from(value.to_string())).unwrap()
}

fn tool(schema: &Value) -> ToolSpec {
    ToolSpec {
        name: "browser_eval".into(),
        description: "Evaluate JavaScript in the selected browser.".into(),
        input_schema: exact(schema),
        kind: ToolSpecKind::Function,
        format: None,
    }
}

async fn body(request: ModelRequest, subscription: bool) -> Value {
    capture_provider_request(request, |base_url| {
        if subscription {
            let (provider, _) = create_claude_subscription_provider(ClaudeSubscriptionOptions {
                oauth: OAuthConfig {
                    client_id: "test-client".into(),
                    authorize_url: "https://auth.example.com/authorize".into(),
                    token_url: "https://auth.example.com/token".into(),
                    redirect_uri: "http://localhost/callback".into(),
                    ..Default::default()
                },
                store: None,
                tokens: Some(OAuthTokens {
                    access_token: "test-token".into(),
                    ..Default::default()
                }),
                base_url: Some(base_url),
                beta_header: None,
                default_model: None,
                access_policy: None,
                identity: None,
            });
            Box::new(provider)
        } else {
            Box::new(AnthropicProvider::new(AnthropicProviderOptions {
                api_key: Some("test-key".into()),
                base_url: Some(base_url),
                retry: Some(single_attempt()),
                ..Default::default()
            }))
        }
    })
    .await
}

#[tokio::test]
async fn browser_alias_constraints_do_not_reject_claude_subscription_requests() {
    let schema = json!({
        "type": "object",
        "description": "Choose a script or expression.",
        "properties": {
            "script": {"type": "string"},
            "expression": {"type": "string"},
            "args": {"oneOf": [{"type": "object"}, {"type": "array"}]},
            "anyOf": {"type": "string"}
        },
        "required": [],
        "additionalProperties": false,
        "anyOf": [{"required": ["script"]}, {"required": ["expression"]}]
    });
    let mut request = record_engine_request(false).await;
    request.tools = vec![tool(&schema)];
    let original = request.tools[0].clone();
    let body = body(request.clone(), true).await;
    let projected = &body["tools"][0]["input_schema"];
    assert!(projected.get("anyOf").is_none(), "{projected}");
    for key in ["type", "properties", "required", "additionalProperties"] {
        assert_eq!(projected[key], schema[key], "{key}");
    }
    let description = projected["description"].as_str().unwrap();
    assert!(description.starts_with("Choose a script or expression."));
    assert!(description.contains(&json!({"anyOf": schema["anyOf"]}).to_string()));
    assert_eq!(
        request.tools[0], original,
        "the source tool schema stays intact"
    );
}

#[tokio::test]
async fn all_root_combinators_retain_their_constraints_as_guidance() {
    let schema = json!({
        "type": "object",
        "properties": {"value": {"type": "string"}},
        "anyOf": [{"required": ["value"]}, {"required": ["alias"]}],
        "oneOf": [{"properties": {"value": {"const": "a"}}}, {"properties": {"value": {"const": "b"}}}],
        "allOf": [{"required": ["value"]}]
    });
    let mut request = record_engine_request(false).await;
    request.tools = vec![tool(&schema)];
    let body = body(request, false).await;
    let projected = &body["tools"][0]["input_schema"];
    for keyword in ["anyOf", "oneOf", "allOf"] {
        assert!(projected.get(keyword).is_none(), "{projected}");
    }
    let constraints =
        json!({"anyOf": schema["anyOf"], "oneOf": schema["oneOf"], "allOf": schema["allOf"]});
    let description = projected["description"].as_str().unwrap();
    let (_, encoded) = description.split_once("Input constraints: ").unwrap();
    assert_eq!(serde_json::from_str::<Value>(encoded).unwrap(), constraints);
    assert_eq!(projected["properties"], schema["properties"]);
}

#[tokio::test]
async fn ordinary_and_nested_composition_schemas_are_preserved() {
    let schema = json!({
        "type": "object",
        "properties": {"target": {"anyOf": [{"type": "string"}, {"type": "number"}]}},
        "required": ["target"]
    });
    let mut request = record_engine_request(false).await;
    request.tools = vec![tool(&schema)];
    let body = body(request, false).await;
    assert_eq!(body["tools"][0]["input_schema"], schema);
}

#[tokio::test]
async fn branch_only_unions_keep_object_inputs_and_all_branch_guidance() {
    let schema = json!({
        "anyOf": [{"properties": {"x": {"type": "string"}}}, {}]
    });
    let mut request = record_engine_request(false).await;
    request.tools = vec![tool(&schema)];
    let body = body(request, false).await;
    let projected = &body["tools"][0]["input_schema"];
    assert_eq!(projected["type"], "object");
    assert!(projected.get("anyOf").is_none());
    assert!(
        projected.get("properties").is_none(),
        "do not restrict x to string"
    );
    let (_, encoded) = projected["description"]
        .as_str()
        .unwrap()
        .split_once("Input constraints: ")
        .unwrap();
    assert_eq!(serde_json::from_str::<Value>(encoded).unwrap(), schema);
}

#[tokio::test]
async fn forced_output_tools_use_the_same_anthropic_schema_projection() {
    let schema = json!({
        "type": "object",
        "properties": {"title": {"type": "string"}},
        "anyOf": [{"required": ["title"]}]
    });
    let mut request = record_engine_request(true).await;
    request.output_schema = Some(exact(&schema));
    let body = body(request, false).await;
    assert_eq!(body["tools"][0]["name"], "emit_result");
    assert!(body["tools"][0]["input_schema"].get("anyOf").is_none());
    assert_eq!(
        body["tools"][0]["input_schema"]["properties"],
        schema["properties"]
    );
}
