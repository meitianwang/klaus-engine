//! Defect: delegated work silently inherited the parent despite configured model tiers.
//! Root cause: child creation did not require an explicit model selection.
//! Work item: 2026-10-28 required child model selection.

use super::r20261005_ultra_orchestration::harness_with_child_options;
use klaus_agent_core::engine::agent_definitions::AgentDefinition;
use klaus_agent_core::engine::session::{
    InferenceTarget, InferenceTargetState, InferenceTargets, Input, TurnOptions,
};
use klaus_agent_core::provider::behavior::{
    render_tool_specs_for_behavior, resolve_provider_behavior_profile,
};
use klaus_agent_core::runtime_values::runtime_json;
use klaus_agent_core::tools::builtins::task::{create_task_tool, TaskToolOptions};
use klaus_agent_core::tools::pool::to_tool_spec;
use klaus_agent_core::types::provider::{ModelCapabilities, ModelStreamEvent, ProviderInfo};
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn required_child_model_recordings_replay_with_their_bound_models() {
    use klaus_agent_core::runtime_process::RuntimeProcessContext;
    use klaus_agent_core::testing::provider_behavior::{
        load_checked_in_manifest, run_scripted_task_tolerant, EvalProfile,
    };
    use klaus_agent_core::testing::{node_interpreter_present, shared_test_process_supervisor};
    if !node_interpreter_present() {
        eprintln!("skipping recorded model-selection calls: Node is unavailable");
        return;
    }
    let context = RuntimeProcessContext::initialize(shared_test_process_supervisor()).unwrap();
    let manifest = load_checked_in_manifest().unwrap();
    let mut replayed = 0;
    for task in &manifest.tasks {
        if !task.profiles.values().any(|profile| {
            profile.scripted_turns.iter().flatten().any(|event| {
                matches!(event, ModelStreamEvent::ToolUseStart { name, .. }
                if ["Task", "task", "spawn_agent"].iter().any(|expected| name == *expected))
            })
        }) {
            continue;
        }
        let name = task.name.try_to_utf8().unwrap();
        let traces = run_scripted_task_tolerant(
            &context,
            &manifest,
            &name,
            &[
                EvalProfile::Anthropic,
                EvalProfile::Openai,
                EvalProfile::Generic,
            ],
        )
        .await
        .unwrap();
        for trace in traces {
            assert!(
                trace.result.success,
                "{name}: {:?}",
                trace.result.behavior_failures
            );
            assert!(
                trace.result.score_success && trace.result.behavior_success,
                "{name}"
            );
            replayed += 1;
        }
    }
    assert_eq!(replayed, 15);
    println!("Replayed {replayed} model-selecting task/profile recordings.");
}

#[test]
fn new_child_model_is_required_in_every_provider_schema_and_runtime_parser() {
    let tool = create_task_tool(TaskToolOptions::default());
    for provider in ["anthropic", "openai", "unknown"] {
        let profile = resolve_provider_behavior_profile(
            Some(&ProviderInfo {
                name: provider.into(),
                base_url: None,
                env_key: None,
                default_model: "fixture".into(),
            }),
            &"fixture".into(),
        );
        let specs = render_tool_specs_for_behavior(&[to_tool_spec(tool.as_ref())], &profile);
        let schema = &specs[0].input_schema;
        assert!(schema["required"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value.as_str().is_some_and(|value| value == "model")));
        assert!(schema["properties"].get("resume").is_none());
        assert!(schema["properties"]["model"]["description"]
            .as_str()
            .unwrap()
            .contains("task complexity"));
    }
    for model in [
        None,
        Some(json!(null)),
        Some(json!("")),
        Some(json!("  ")),
        Some(json!("inherit")),
        Some(json!(7)),
    ] {
        let mut input = json!({"description":"worker", "prompt":"Inspect the assigned scope."});
        if let Some(model) = model {
            input["model"] = model;
        }
        let input = runtime_json::parse(&input.to_string().into()).unwrap();
        assert!(tool.parse_input(&input).is_err(), "{input:?}");
    }
    for model in ["low", "medium", "high"] {
        let input = runtime_json::parse(
            &json!({"description":"worker", "prompt":"Inspect", "model":model})
                .to_string()
                .into(),
        )
        .unwrap();
        assert!(
            tool.parse_input(&input).is_ok(),
            "selection availability is resolved during creation"
        );
    }
    let catalog_tool = create_task_tool(TaskToolOptions {
        model_catalog: true,
        model_options: vec![klaus_agent_core::schemas::config::SubagentModelOption {
            value: "custom:chosen-model".into(),
            label: "Chosen model".into(),
        }],
        agents: Vec::new(),
    });
    let schema = catalog_tool.input_schema();
    assert_eq!(
        schema["properties"]["model"]["enum"],
        runtime_json::parse(&r#"["custom:chosen-model"]"#.into()).unwrap()
    );
    for (selection, allowed) in [
        ("low", false),
        ("custom:chosen-model", true),
        ("custom:unknown", true),
    ] {
        let input = runtime_json::parse(
            &json!({"description":"worker", "prompt":"Inspect", "model":selection})
                .to_string()
                .into(),
        )
        .unwrap();
        assert_eq!(catalog_tool.parse_input(&input).is_ok(), allowed);
    }
    let empty = create_task_tool(TaskToolOptions {
        model_catalog: true,
        ..Default::default()
    });
    let input = runtime_json::parse(
        &json!({"description":"worker","prompt":"Inspect","model":"custom:unknown"})
            .to_string()
            .into(),
    )
    .unwrap();
    assert!(empty.parse_input(&input).is_err());
    assert!(empty.input_schema()["properties"]["model"]["description"]
        .as_str()
        .unwrap()
        .contains("no available models"));
}

#[tokio::test]
async fn explicit_model_wins_over_role_and_invalid_selection_falls_back_to_parent() {
    for (selection, expected) in [
        ("custom:available", "chosen-model"),
        ("high", "chosen-model"),
        ("custom:missing", "test-model"),
        ("custom:unavailable", "test-model"),
        ("low", "test-model"),
    ] {
        let config = json!({"modelTiers":{"high":"chosen-model"},
            "subagentModelCatalog":!["high","low"].contains(&selection),
            "subagentModelOptions":[{"value":"custom:available","label":"Chosen model"},{"value":"custom:unavailable","label":"Unavailable model"}]});
        let h = harness_with_child_options(None, Some("task-input"), ModelCapabilities::default(), config,
            Some(vec![AgentDefinition { name:"configured".into(), model:Some("role-model".into()), ..Default::default() }]),
            Some(json!({"description":"worker", "prompt":"ultra-worker-directive", "model":selection, "subagent_type":"configured", "fork_turns":"none", "run_in_background":false})),
        ).await;
        h.session
            .install_inference_targets(
                Some(InferenceTargets {
                    high: Some(InferenceTargetState::Ready(InferenceTarget {
                        source: "custom".into(),
                        model_id: "available".into(),
                        model: "chosen-model".into(),
                        provider: h.provider.clone(),
                        pricing: Vec::new(),
                        review_requirements: None,
                    })),
                    low: Some(InferenceTargetState::Unavailable {
                        source: "custom".into(),
                        model_id: "unavailable".into(),
                        code: "MODEL_NOT_FOUND".into(),
                        message: "Retired model".into(),
                    }),
                    ..Default::default()
                }),
                None,
                None,
            )
            .unwrap();
        let result = h
            .session
            .run(
                Input::Text("Dispatch the worker.".into()),
                TurnOptions::default(),
            )
            .await;
        assert!(result.error.is_none(), "{:?}", result.error);
        {
            let requests = h.provider.requests.lock().unwrap();
            let children = requests.iter().filter(|request| request.messages.iter().any(|message| {
                matches!(message, klaus_agent_core::schemas::message::Message::User(user) if user.content.iter().any(|block| {
                    matches!(block, klaus_agent_core::schemas::content::ContentBlock::Text(text) if text.text.contains("ultra-worker-directive"))
                }))
            })).collect::<Vec<_>>();
            assert_eq!(
                children.len(),
                1,
                "{selection}: the worker must run exactly once"
            );
            assert_eq!(children[0].model, expected, "{selection}");
        }
        h.teardown().await;
    }
}

#[tokio::test]
async fn preparation_errors_do_not_fall_back_or_start_a_child() {
    for response in ["failure", "wrong-ready", "wrong-unavailable"] {
        let h = harness_with_child_options(
            None, Some("task-input"), ModelCapabilities::default(), json!({"subagentModelCatalog":true,"subagentModelOptions":[{"value":"custom:requested","label":"Requested model"}]}), None,
            Some(json!({"description":"worker", "prompt":"ultra-worker-directive", "model":"custom:requested", "run_in_background":false})),
        ).await;
        h.session.install_subagent_target_resolver(Arc::new({
            let provider = h.provider.clone();
            move |_, _| {
                let provider = provider.clone();
                Box::pin(async move {
                    if response == "wrong-ready" {
                        Ok(InferenceTargetState::Ready(InferenceTarget {
                            source: "custom".into(),
                            model_id: "different".into(),
                            model: "test-model".into(),
                            provider,
                            pricing: Vec::new(),
                            review_requirements: None,
                        }))
                    } else if response == "wrong-unavailable" {
                        Ok(InferenceTargetState::Unavailable {
                            source: "custom".into(),
                            model_id: "different".into(),
                            code: "MODEL_NOT_FOUND".into(),
                            message: "Different model is unavailable".into(),
                        })
                    } else {
                        Err("Model preparation service failed".into())
                    }
                })
            }
        }));
        let result = h
            .session
            .run(
                Input::Text("Dispatch the worker.".into()),
                TurnOptions::default(),
            )
            .await;
        assert!(result.error.is_none(), "{:?}", result.error);
        assert!(h.session.subagent_lifecycle_snapshot().agents.is_empty());
        let failure = if response != "failure" {
            "different source or model"
        } else {
            "Model preparation service failed"
        };
        {
            let requests = h.provider.requests.lock().unwrap();
            assert!(requests.iter().flat_map(|request| &request.messages).any(|message| {
                matches!(message, klaus_agent_core::schemas::message::Message::ToolResult(result) if result.is_error && result.content.iter().any(|output| {
                    matches!(output, klaus_agent_core::schemas::content::OutputBlock::Text(text) if text.text.contains(failure))
                }))
            }));
        }
        h.teardown().await;
    }
}
