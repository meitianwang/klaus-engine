//! Child inference overrides lost raw effort and full-history model selections.
//! Layer requested defaults before explicit role fields and validate each selected model.
//! Model-facing task schemas must preserve the history and effort override fields.
//! Work item: 2026-10-24 child inference precedence.

use super::r20261005_ultra_orchestration::harness_with_child_options;
use klaus_agent_core::engine::agent_definitions::AgentDefinition;
use klaus_agent_core::engine::session::{
    InferencePublication, InferenceTarget, InferenceTargetState, Input, TurnOptions,
};
use klaus_agent_core::provider::behavior::{
    render_tool_specs_for_behavior, resolve_provider_behavior_profile,
};
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeDecode, RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::content::{ContentBlock, OutputBlock};
use klaus_agent_core::schemas::effort::{EffortLevel, EffortValue};
use klaus_agent_core::schemas::message::Message;
use klaus_agent_core::tools::builtins::task::{create_task_tool, TaskToolOptions};
use klaus_agent_core::tools::pool::to_tool_spec;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelCapabilities, ModelProvider, ModelRequest, ModelStream, ProviderCapabilities, ProviderInfo,
};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct LowEffortRoleProvider {
    transport: Arc<dyn ModelProvider>,
    calls: AtomicUsize,
}

impl ModelProvider for LowEffortRoleProvider {
    fn info(&self) -> &ProviderInfo {
        self.transport.info()
    }

    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        self.transport.capabilities(model)
    }

    fn model_metadata(&self, _: &RuntimeString) -> ModelCapabilities {
        ModelCapabilities {
            supported_reasoning_efforts: vec!["low".into()],
            default_reasoning_effort: Some("low".into()),
            ..Default::default()
        }
    }

    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.transport.stream(request, signal)
    }
}

#[tokio::test]
async fn selected_target_identity_validates_effort_across_equal_wire_model_ids() {
    for different_source in [false, true] {
        let mut facts = metadata();
        facts.supported_reasoning_efforts =
            vec![if different_source { "high" } else { "low" }.into()];
        facts.default_reasoning_effort = Some(if different_source { "high" } else { "low" }.into());
        let role = AgentDefinition {
            name: "configured".into(),
            model: Some(
                if different_source {
                    "custom:role-catalog"
                } else {
                    "test-model"
                }
                .into(),
            ),
            ..Default::default()
        };
        let h = harness_with_child_options(
            Some("high"),
            Some("task-input"),
            facts,
            json!({
                "modelSource":{"source":"custom", "modelId":"test-model"},
                "subagentModelCatalog":true,
                "subagentModelOptions":[{
                    "value":if different_source { "custom:role-catalog" } else { "custom:test-model" },
                    "label":"Selected fixture model"
                }]
            }),
            Some(vec![role]),
            Some(
                json!({"model": if different_source { "custom:role-catalog" } else { "custom:test-model" }, "reasoning_effort": if different_source { "high" } else { "low" }, "description":"worker", "prompt":"ultra-worker-directive",
                "fork_turns":"none", "run_in_background":false, "subagent_type":"configured"}),
            ),
        )
        .await;
        let target = Arc::new(LowEffortRoleProvider {
            transport: h.provider.clone(),
            calls: AtomicUsize::new(0),
        });
        h.session.install_subagent_target_resolver(Arc::new({
            let target = target.clone();
            let parent = h.provider.clone();
            move |source, model_id| {
                assert_eq!(source, "custom");
                assert_eq!(
                    model_id,
                    if different_source {
                        "role-catalog"
                    } else {
                        "test-model"
                    }
                );
                let provider: Arc<dyn ModelProvider> = if different_source {
                    target.clone()
                } else {
                    parent.clone()
                };
                Box::pin(async move {
                    Ok(InferenceTargetState::Ready(InferenceTarget {
                        source,
                        model_id,
                        model: "test-model".into(),
                        provider,
                        pricing: Vec::new(),
                        review_requirements: None,
                    }))
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
        {
            let requests = h.provider.requests.lock().unwrap();
            let children = requests.iter().filter(|request| request.messages.iter().any(|message| {
                matches!(message, Message::User(user) if user.content.iter().any(|block| {
                    matches!(block, ContentBlock::Text(text) if text.text.contains("ultra-worker-directive"))
                }))
            })).count();
            assert_eq!(children, usize::from(!different_source));
            if different_source {
                let rejected = requests.iter().flat_map(|request| &request.messages).any(|message| {
                    matches!(message, Message::ToolResult(result) if result.is_error
                            && result.content.iter().any(|output| {
                                matches!(output, OutputBlock::Text(text) if text.text.contains(
                                    "Reasoning effort `high` is not supported for model `test-model`. Supported reasoning efforts: low"
                                ))
                            }))
                });
                assert!(
                    rejected,
                    "the target provider's supported list must reject the inherited effort"
                );
                assert!(h.session.subagent_lifecycle_snapshot().agents.is_empty());
            }
        }
        assert_eq!(
            target.calls.load(Ordering::SeqCst),
            0,
            "rejected inference cannot reach the target provider"
        );
        h.teardown().await;
    }
}

fn metadata() -> ModelCapabilities {
    ModelCapabilities {
        supported_reasoning_efforts: [
            "none",
            "minimal",
            "low",
            "medium",
            "high",
            "xhigh",
            "max",
            "ultra",
            "persistent",
            "custom/exact",
        ]
        .into_iter()
        .map(Into::into)
        .collect(),
        default_reasoning_effort: Some("minimal".into()),
        multi_agent_reasoning_effort: Some("high".into()),
        ..Default::default()
    }
}

#[test]
fn task_reasoning_input_accepts_nullable_open_strings_and_rejects_empty_or_nonstrings() {
    let tool = create_task_tool(TaskToolOptions::default());
    let profile = resolve_provider_behavior_profile(None, &"claude-sonnet-4-6".into());
    let rendered = render_tool_specs_for_behavior(&[to_tool_spec(tool.as_ref())], &profile);
    assert_eq!(rendered.len(), 1);
    assert_eq!(rendered[0].name, "Task");
    for (field, description) in [
        (
            "fork_turns",
            "Conversation history to inherit: all (default) or none. Positive integer strings also inherit the full conversation.",
        ),
        (
            "reasoning_effort",
            "Reasoning effort override for the child. Omit to inherit the parent selection.",
        ),
    ] {
        let expected = klaus_agent_core::runtime_values::runtime_json::parse(
            &json!({"type":"string", "description":description})
                .to_string()
                .into(),
        )
        .unwrap();
        let actual = rendered[0]
            .input_schema
            .pointer(&format!("/properties/{field}"))
            .unwrap_or_else(|| panic!("Model-facing task schema must expose {field}"));
        assert!(
            actual.semantic_eq_unordered(&expected),
            "{field}: {actual:?}"
        );
    }
    for effort in [
        json!(null),
        json!("none"),
        json!("minimal"),
        json!("persistent"),
        json!("custom/exact"),
        json!("unknown"),
    ] {
        let input = klaus_agent_core::runtime_values::runtime_json::parse(
            &json!({"model": "low", "description":"worker", "prompt":"Inspect", "reasoning_effort":effort})
                .to_string()
                .into(),
        )
        .unwrap();
        assert!(tool.parse_input(&input).is_ok(), "{effort}");
    }
    for effort in [json!(""), json!(1), json!(false), json!({})] {
        let input = klaus_agent_core::runtime_values::runtime_json::parse(
            &json!({"model": "low", "description":"worker", "prompt":"Inspect", "reasoning_effort":effort})
                .to_string()
                .into(),
        )
        .unwrap();
        assert!(tool.parse_input(&input).is_err(), "{effort}");
    }
    for raw in ["none", "minimal", "persistent", "custom/exact", "55"] {
        let value = EffortValue::decode_runtime(&RuntimeJson::String(raw.into())).unwrap();
        assert_eq!(value.raw_name(), RuntimeString::from(raw));
        let encoded = runtime_codec::encode_text(&value).unwrap();
        assert_eq!(encoded, RuntimeString::from(json!(raw).to_string()));
        let serde = serde_json::to_string(&value).unwrap();
        assert_eq!(serde_json::from_str::<EffortValue>(&serde).unwrap(), value);
    }
}

#[test]
fn full_history_is_the_default_and_legacy_turn_counts_do_not_truncate() {
    let tool = create_task_tool(TaskToolOptions::default());
    for fork_turns in [
        json!(null),
        json!(""),
        json!("all"),
        json!(" ALL "),
        json!("3"),
        json!("none"),
    ] {
        let input = klaus_agent_core::runtime_values::runtime_json::parse(
            &json!({"model": "low", "description":"worker", "prompt":"Inspect", "fork_turns":fork_turns})
                .to_string()
                .into(),
        )
        .unwrap();
        assert!(tool.parse_input(&input).is_ok(), "{fork_turns}");
    }
    for fork_turns in [json!(0), json!(true), json!("0"), json!("1.5")] {
        let input = klaus_agent_core::runtime_values::runtime_json::parse(
            &json!({"model": "low", "description":"worker", "prompt":"Inspect", "fork_turns":fork_turns})
                .to_string()
                .into(),
        )
        .unwrap();
        assert!(tool.parse_input(&input).is_err(), "{fork_turns}");
    }
    for value in [true, false] {
        let input = klaus_agent_core::runtime_values::runtime_json::parse(
            &json!({"model": "low", "description":"worker", "prompt":"Inspect", "fork_context":value})
                .to_string()
                .into(),
        )
        .unwrap();
        assert!(tool.parse_input(&input).is_err());
    }
}

#[tokio::test]
async fn child_raw_effort_overrides_inherited_ultra_and_preserves_custom_values() {
    for (selection, expected) in [
        (json!(null), EffortValue::Level(EffortLevel::High)),
        (json!("low"), EffortValue::Level(EffortLevel::Low)),
        (json!("none"), EffortValue::Level(EffortLevel::None)),
        (
            json!("custom/exact"),
            EffortValue::Custom("custom/exact".into()),
        ),
        (json!("persistent"), EffortValue::Custom("disabled".into())),
    ] {
        let h = harness_with_child_options(Some("ultra"), Some("task-input"), metadata(), json!({}), None, Some(json!({"model": "low",
            "description":"worker", "prompt":"ultra-worker-directive", "fork_turns":"none", "run_in_background":false,
            "reasoning_effort":selection
        }))).await;
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
            let child = requests
                .iter()
                .find(|request| {
                    request
                        .messages
                        .iter()
                        .any(|message| format!("{message:?}").contains("ultra-worker-directive"))
                })
                .unwrap();
            assert_eq!(child.effort, Some(expected));
            let active = child.system.iter().any(|part| {
                part.text
                    .contains("Proactive multi-agent delegation is active")
            });
            assert_eq!(active, selection.is_null());
        }
        h.teardown().await;
    }
}

#[tokio::test]
async fn a_different_model_resets_effort_before_role_effort_fields_override() {
    for (mut config, call_model, role, expected_model, expected_effort) in [
        (json!({}), Some("test-model"), None, "test-model", "high"),
        (
            json!({"defaultSubagentModel":"test-model"}),
            None,
            None,
            "test-model",
            "high",
        ),
        (
            json!({"defaultSubagentReasoningEffort":"medium"}),
            None,
            None,
            "test-model",
            "medium",
        ),
        (
            json!({"defaultSubagentReasoningEffort":"medium"}),
            Some("test-model"),
            Some(("test-model", "low")),
            "test-model",
            "low",
        ),
        (
            json!({"modelTiers":{"high":"role-model","low":"call-model"}}),
            Some("low"),
            Some(("high", "low")),
            "call-model",
            "low",
        ),
    ] {
        let mut input = json!({"model": "low", "description":"worker", "prompt":"ultra-worker-directive", "fork_turns":"none", "run_in_background":false});
        if let Some(model) = call_model {
            if matches!(model, "low" | "medium" | "high") {
                input["model"] = json!(model);
            } else {
                let selection = format!("custom:{model}");
                config["modelSource"] = json!({"source":"custom", "modelId":"test-model"});
                config["subagentModelCatalog"] = json!(true);
                config["subagentModelOptions"] = json!([{
                    "value":selection, "label":"Selected fixture model"
                }]);
                input["model"] = json!(selection);
            }
        }
        let agents = role.map(|(model, effort)| {
            input["subagent_type"] = json!("configured");
            vec![AgentDefinition {
                name: "configured".into(),
                model: Some(model.into()),
                effort: Some(EffortValue::from_raw(&effort.into()).unwrap()),
                ..Default::default()
            }]
        });
        let h = harness_with_child_options(
            Some("ultra"),
            Some("task-input"),
            metadata(),
            config,
            agents,
            Some(input),
        )
        .await;
        if call_model.is_some_and(|model| !matches!(model, "low" | "medium" | "high")) {
            let (revision, config, _) = h.session.model_runtime();
            h.session
                .reconfigure_inference_with_targets(
                    h.provider.clone(),
                    config,
                    None,
                    None,
                    InferencePublication {
                        expected_revision: revision,
                        change_id: None,
                        targets: None,
                        advisor_target: None,
                        orchestrator_target: None,
                        orchestration_mode: false,
                        orchestrator_fallback_to_main: false,
                        feature_targets: Default::default(),
                        source: Some(("custom".into(), "test-model".into())),
                        rates: None,
                        publication_key: None,
                        review_requirements: None,
                    },
                )
                .unwrap();
        }
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
            let child = requests
                .iter()
                .find(|request| {
                    request
                        .messages
                        .iter()
                        .any(|message| format!("{message:?}").contains("ultra-worker-directive"))
                })
                .unwrap();
            assert_eq!(child.model, expected_model);
            assert_eq!(
                child.effort.as_ref().unwrap().raw_name(),
                RuntimeString::from(expected_effort)
            );
        }
        h.teardown().await;
    }
}

#[tokio::test]
async fn full_history_grandchild_retains_parent_role_and_actual_tool_pool() {
    for (scenario, expected, forbidden) in [
        (
            "task-inherited-context",
            "PARENT_ROLE_SENTINEL",
            "REPLACEMENT_ROLE_SENTINEL",
        ),
        (
            "task-inherited-context-replace",
            "REPLACEMENT_ROLE_SENTINEL",
            "PARENT_ROLE_SENTINEL",
        ),
    ] {
        let agents = vec![
            AgentDefinition {
                name: "bounded".into(),
                prompt: Some("PARENT_ROLE_SENTINEL".into()),
                tools: Some(vec!["read".into(), "task".into()]),
                ..Default::default()
            },
            AgentDefinition {
                name: "replacement".into(),
                prompt: Some("REPLACEMENT_ROLE_SENTINEL".into()),
                ..Default::default()
            },
        ];
        let h = harness_with_child_options(
            Some("ultra"), Some(scenario), metadata(), json!({}), Some(agents),
            Some(json!({"model": "low", "description":"bounded worker", "prompt":"ultra-worker-directive", "subagent_type":"bounded", "fork_turns":"none", "run_in_background":false})),
        ).await;
        let result = h
            .session
            .run(
                Input::Text("Dispatch the bounded worker.".into()),
                TurnOptions::default(),
            )
            .await;
        assert!(result.error.is_none(), "{:?}", result.error);
        {
            let requests = h.provider.requests.lock().unwrap();
            let root = requests.first().unwrap();
            assert!(root.tools.iter().any(|tool| tool.name == "write"));
            let grandchild = requests.iter().find(|request| request.messages.iter().any(|message| match message {
                klaus_agent_core::schemas::message::Message::User(user) => user.content.iter().any(|block| matches!(block,
                    klaus_agent_core::schemas::content::ContentBlock::Text(text) if text.text.contains("full-history-grandchild")
                )),
                _ => false,
            })).expect("the grandchild must reach the provider");
            assert!(grandchild
                .system
                .iter()
                .any(|part| part.text.contains(expected)));
            assert!(!grandchild
                .system
                .iter()
                .any(|part| part.text.contains(forbidden)));
            assert!(grandchild.tools.iter().any(|tool| tool.name == "read"));
            assert!(!grandchild.tools.iter().any(|tool| tool.name == "write"));
            assert!(!grandchild.tools.iter().any(|tool| tool.name == "edit"));
        }
        h.teardown().await;
    }
}
