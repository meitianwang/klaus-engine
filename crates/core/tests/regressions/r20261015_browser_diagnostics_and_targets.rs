//! Defect: browser diagnostics and semantic element targets were lost at the engine boundary.
//! Root cause: typed entries omitted payload fields and named operations exposed only CSS targets.
//! Work item: 2026-10-15 browser diagnostic and element-target contract.

use klaus_agent_core::browser::{
    BrowserHostToolMapping, BrowserRuntimeRequest, BrowserRuntimeResponse,
};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{
    runtime_codec, runtime_json, RuntimeJson, RuntimePath, RuntimeString,
};
use klaus_agent_core::sandbox::process::ProcessSandbox;
use klaus_agent_core::schemas::content::OutputBlock;
use klaus_agent_core::tools::builtins::browser::create_browser_tools;
use klaus_agent_core::tools::file_state::InMemoryFileStateCache;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::permission::PermissionContext;
use klaus_agent_core::types::tool::{InvokeToolResult, ToolContext};
use std::sync::Arc;

fn vectors() -> RuntimeJson {
    runtime_json::parse(&RuntimeString::from(include_str!(
        "../fixtures/browser-diagnostic-target-vectors.json"
    )))
    .unwrap()
}

#[tokio::test]
async fn browser_diagnostics_preserve_host_payloads_in_typed_and_model_results() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let fixture = vectors();
    let response = fixture.get("diagnostics").unwrap().clone();
    let decoded: BrowserRuntimeResponse = runtime_codec::decode_value(&response).unwrap();
    let encoded = runtime_codec::encode_value(&decoded).unwrap();
    for field in [
        "console",
        "network",
        "count",
        "total",
        "truncated",
        "evicted",
    ] {
        assert_eq!(encoded.get(field), response.get(field));
    }
    for field in ["console", "network"] {
        let mut ctx = make_ctx("/tmp", process.clone());
        ctx.browser_tool_mapping = Some(BrowserHostToolMapping::for_mcp_server("fixture"));
        let response = response.clone();
        let expected = response.get(field).unwrap().clone();
        let metadata = response.clone();
        ctx.invoke_tool = Some(Arc::new(move |name, request| {
            assert_eq!(name, RuntimeString::from(format!("mcp__fixture__{field}")));
            let typed: BrowserRuntimeRequest = runtime_codec::decode_value(&request).unwrap();
            assert_eq!(runtime_codec::encode_value(&typed).unwrap(), request);
            if field == "network" {
                assert_eq!(
                    request.get("operation").unwrap().get("includeBodies"),
                    Some(&RuntimeJson::Bool(true))
                );
            }
            let response = response.clone();
            Box::pin(async move {
                Ok(InvokeToolResult {
                    is_error: false,
                    output: Some(response),
                    content: vec![],
                })
            })
        }));
        let tool = create_browser_tools()
            .into_iter()
            .find(|tool| tool.static_name() == format!("browser_{field}"))
            .unwrap();
        let input = runtime_json::parse(&RuntimeString::from(if field == "network" {
            r#"{"includeBodies":true}"#
        } else {
            "{}"
        }))
        .unwrap();
        let result = tool
            .call(tool.parse_input(&input).unwrap(), &ctx, None)
            .await
            .unwrap();
        assert_eq!(result.serializable.is_error, Some(false));
        assert_eq!(
            result
                .serializable
                .output
                .get("response")
                .unwrap()
                .get(field),
            Some(&expected)
        );
        let text = result
            .serializable
            .model_content
            .iter()
            .filter_map(|block| match block {
                OutputBlock::Text(block) => Some(block.text.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(text.len(), 1);
        let model = runtime_json::parse(&text[0]).unwrap();
        assert_eq!(
            model.get(field),
            Some(&expected),
            "model results retain headers, bodies, timestamps and structured console arguments"
        );
        for key in ["count", "total", "truncated", "evicted"] {
            assert_eq!(
                model.get(key),
                metadata.get(key),
                "model results report bounded diagnostic coverage"
            );
            assert_eq!(
                result.serializable.output.get("response").unwrap().get(key),
                metadata.get(key)
            );
        }
    }
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn named_browser_tools_forward_typed_element_criteria_and_reference_aliases() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    for vector in vectors().get("targets").unwrap().as_array().unwrap() {
        let mut ctx = make_ctx("/tmp", process.clone());
        ctx.browser_tool_mapping = Some(BrowserHostToolMapping::for_mcp_server("fixture"));
        let expected = vector.get("expected").unwrap().clone();
        let kind = vector.get("type").unwrap().clone();
        let target_field = vector.get("targetField").cloned();
        ctx.invoke_tool = Some(Arc::new(move |_, request| {
            let typed: BrowserRuntimeRequest = runtime_codec::decode_value(&request).unwrap();
            assert_eq!(runtime_codec::encode_value(&typed).unwrap(), request);
            let operation = request.get("operation").unwrap();
            assert_eq!(operation.get("type"), Some(&kind));
            let target = target_field
                .as_ref()
                .map(|key| operation.get(key.as_string().unwrap()).unwrap())
                .unwrap_or(operation);
            for (key, value) in expected.as_object().unwrap() {
                assert_eq!(
                    target.get(key),
                    Some(value),
                    "target field must reach the host unchanged"
                );
            }
            let response = runtime_codec::encode_value(&BrowserRuntimeResponse::default()).unwrap();
            Box::pin(async move {
                Ok(InvokeToolResult {
                    is_error: false,
                    output: Some(response),
                    content: vec![],
                })
            })
        }));
        let name = vector
            .get("tool")
            .unwrap()
            .as_str()
            .unwrap()
            .try_to_utf8()
            .unwrap();
        let tool = create_browser_tools()
            .into_iter()
            .find(|tool| tool.static_name() == name)
            .unwrap();
        let schema = tool.input_schema();
        for key in [
            "ref",
            "refId",
            "role",
            "name",
            "label",
            "placeholder",
            "testId",
            "query",
            "exact",
            "nth",
        ] {
            assert!(
                schema.get("properties").unwrap().get(key).is_some(),
                "{name} is missing {key}"
            );
        }
        let input = vector.get("input").unwrap();
        let result = tool
            .call(tool.parse_input(input).unwrap(), &ctx, None)
            .await
            .unwrap();
        assert_eq!(
            result.serializable.is_error,
            Some(false),
            "{name}: {:?}",
            result.serializable.model_content
        );
    }
    process.shutdown().await.unwrap();
}

pub(super) fn make_ctx(cwd: &str, process: RuntimeProcessContext) -> ToolContext {
    ToolContext {
        async_question: None,
        background_processes: Arc::new(
            klaus_agent_core::command_runtime::background::BackgroundCommandRegistry::new(),
        ),
        process_context: process,
        cwd: RuntimePath::from(cwd),
        signal: AbortSignal::new(),
        file_state: Arc::new(InMemoryFileStateCache::new()),
        permission: PermissionContext {
            allow_rules: Vec::new(),
            deny_rules: Vec::new(),
            ask_rules: Vec::new(),
            additional_writable_roots: Vec::new(),
            internal_writable_roots: Vec::new(),
            pinned_memory_root: None,
            should_avoid_prompts: false,
        },
        sandbox: Arc::new(ProcessSandbox),
        planning_read_only: false,
        approval_policy: None,
        sandbox_mode: None,
        approvals_reviewer: None,
        web_search_access: None,
        network_allowed: None,
        emit: std::sync::Arc::new(|_| Box::pin(async {})),
        native_pdf: None,
        vision: None,
        shell_env: None,
        depth: None,
        forward_subagent_text: None,
        spawn_sub_agent: None,
        spawn_background_agent: None,
        spawn_workflow_agent: None,
        surface_tools: None,
        activate_skill_paths: None,
        tool_catalog: None,
        elicit: None,
        plan_file: None,
        apply_skill_context: None,
        run_bash: None,
        mcp_clients: None,
        background_tasks: None,
        task_list_id: None,
        task_plan: None,
        worktree: None,
        workflow_journal: None,
        swarm: None,
        cron: None,
        goal: None,
        goal_generation: None,
        goal_owned_resources: None,
        lsp: None,
        browser: None,
        browser_tool_mapping: None,
        run_hook: None,
        invoke_tool: None,
        advisor: None,
        sandbox_retry_on_denial: None,
        request_approval: None,
        request_network_access: None,
        enqueue_background_notice: None,
        record_rollout_event: None,
    }
}
