//! Defect: a stopped browser batch was reported as a successful complete operation.
//! Root cause: error-only host parsing discarded completed steps while the builtin always marked typed responses successful.
//! Work item: 2026-10-15 partial browser batch delivery.

use super::r20261015_browser_diagnostics_and_targets::make_ctx;
use klaus_agent_core::browser::{BrowserHostToolMapping, BrowserRuntimeResponse};
use klaus_agent_core::mcp::types::McpCallResult;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::content::OutputBlock;
use klaus_agent_core::tools::builtins::browser::create_browser_tools;
use klaus_agent_core::types::tool::InvokeToolResult;
use std::sync::Arc;

fn json(source: &str) -> RuntimeJson {
    runtime_json::parse(&RuntimeString::from(source)).unwrap()
}

#[tokio::test]
async fn browser_batch_partial_failure_keeps_completed_results_and_error_status() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let partial = json(include_str!(
        "../fixtures/browser-partial-batch-vector.json"
    ));
    let typed: BrowserRuntimeResponse = runtime_codec::decode_value(&partial).unwrap();
    assert!(typed.partial);
    assert_eq!(runtime_codec::encode_value(&typed).unwrap(), partial);
    let input = json(
        r#"{"actions":[{"type":"navigate","url":"https://app.test/form"},{"type":"click","ref":"missing-control"},{"type":"type","text":"should not run"}]}"#,
    );
    for wrapped in [false, true] {
        let mut ctx = make_ctx("/tmp", process.clone());
        ctx.browser_tool_mapping = Some(BrowserHostToolMapping::for_mcp_server("fixture"));
        let response = partial.clone();
        ctx.invoke_tool = Some(Arc::new(move |name, _| {
            assert_eq!(name, "mcp__fixture__batch");
            let response = response.clone();
            Box::pin(async move {
                let output = if wrapped {
                    runtime_codec::encode_value(&McpCallResult {
                        content: vec![],
                        is_error: Some(true),
                        meta: None,
                        structured_content: Some({
                            let mut content =
                                klaus_agent_core::runtime_values::RuntimeObject::new();
                            content.insert("response", response);
                            RuntimeJson::Object(content)
                        }),
                    })
                    .unwrap()
                } else {
                    response
                };
                Ok(InvokeToolResult {
                    is_error: true,
                    output: Some(output),
                    content: vec![],
                })
            })
        }));
        let tool = create_browser_tools()
            .into_iter()
            .find(|tool| tool.static_name() == "browser_batch")
            .unwrap();
        let result = tool
            .call(tool.parse_input(&input).unwrap(), &ctx, None)
            .await
            .unwrap();
        assert_eq!(result.serializable.is_error, Some(true));
        assert_eq!(
            result.serializable.output.get("ok"),
            Some(&RuntimeJson::Bool(false))
        );
        assert_eq!(result.serializable.output.get("response"), Some(&partial));
        let text = result
            .serializable
            .model_content
            .iter()
            .find_map(|block| match block {
                OutputBlock::Text(block) => Some(&block.text),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            runtime_json::parse(text).unwrap(),
            partial,
            "completed side effects and the precise failure stay visible to the model"
        );
    }
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn browser_batch_unstructured_or_incomplete_error_keeps_the_existing_error_path() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    for output in [
        r#"{"error":{"code":"host_error","message":"Browser disconnected."}}"#,
        r#"{"partial":true,"value":{"completed":1,"stopped":true}}"#,
        r#"{"partial":true,"error":{"code":"invalid-code","message":"Malformed host error."}}"#,
    ] {
        let mut ctx = make_ctx("/tmp", process.clone());
        ctx.browser_tool_mapping = Some(BrowserHostToolMapping::for_mcp_server("fixture"));
        let output = runtime_codec::encode_value(&McpCallResult {
            content: vec![],
            is_error: Some(true),
            meta: None,
            structured_content: Some(json(output)),
        })
        .unwrap();
        let ordinary = output
            .get("structuredContent")
            .and_then(|value| value.get("partial"))
            .is_none();
        ctx.invoke_tool = Some(Arc::new(move |_, _| {
            let output = output.clone();
            Box::pin(async move {
                Ok(InvokeToolResult {
                    is_error: true,
                    output: Some(output),
                    content: vec![],
                })
            })
        }));
        let tool = create_browser_tools()
            .into_iter()
            .find(|tool| tool.static_name() == "browser_batch")
            .unwrap();
        let result = tool
            .call(
                tool.parse_input(&json(r#"{"actions":[]}"#)).unwrap(),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(result.serializable.is_error, Some(true));
        assert!(
            result.serializable.output.get("response").is_none(),
            "a typed partial result requires a valid structured error"
        );
        if ordinary {
            assert_eq!(
                result
                    .serializable
                    .output
                    .get("error")
                    .unwrap()
                    .get("message"),
                Some(&RuntimeJson::from("Browser disconnected."))
            );
        }
    }
    let encoded = runtime_codec::encode_value(&BrowserRuntimeResponse::default()).unwrap();
    assert!(encoded.get("partial").is_none());
    assert!(encoded.get("error").is_none());
    process.shutdown().await.unwrap();
}
