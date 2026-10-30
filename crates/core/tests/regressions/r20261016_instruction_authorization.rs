//! Defect: question and child-model guidance omitted their authorization boundaries.
//! Root cause: native and provider-rendered tool descriptions had separate policies.
//! Work item: 20261016 instruction authorization policy.

use klaus_agent_core::context::system_prompt::{build_system_prompt, SystemPromptInput};
use klaus_agent_core::engine::BUILTIN_AGENTS;
use klaus_agent_core::provider::behavior::{
    normalize_tool_input_for_behavior, render_tool_specs_for_behavior,
    resolve_provider_behavior_profile,
};
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::config::ResolvedConfig;
use klaus_agent_core::tools::builtins::index::{default_builtins, BuiltinsOptions};
use klaus_agent_core::tools::builtins::task::{
    create_task_tool, AgentDefinition as TaskAgentDefinition, TaskToolOptions,
};
use klaus_agent_core::tools::pool::{find_tool, to_tool_spec};
use klaus_agent_core::types::provider::ProviderInfo;
use serde_json::Value;

fn contract() -> Value {
    serde_json::from_str(include_str!(
        "../fixtures/instruction-authorization-vectors.json"
    ))
    .unwrap()
}

#[test]
fn question_tools_keep_runtime_permission_approval_separate_in_every_profile() {
    let builtins = default_builtins(&ResolvedConfig::default(), BuiltinsOptions::default());
    let specs = ["ask_user", "ask_user_async"]
        .map(|name| to_tool_spec(find_tool(&builtins, name).unwrap().as_ref()));
    for provider in ["anthropic", "openai", "unknown"] {
        let profile = resolve_provider_behavior_profile(
            Some(&ProviderInfo {
                name: provider.into(),
                base_url: None,
                env_key: None,
                default_model: "policy-fixture".into(),
            }),
            &"policy-fixture".into(),
        );
        for tool in render_tool_specs_for_behavior(&specs, &profile) {
            for required in contract()["question_tool_fragments"].as_array().unwrap() {
                assert!(
                    tool.description.contains(required.as_str().unwrap()),
                    "{provider} question tool {:?} lost its permission boundary",
                    tool.name
                );
            }
        }
    }
}

#[test]
fn child_model_selection_requirement_survives_provider_schema_rendering() {
    let builtins = default_builtins(&ResolvedConfig::default(), BuiltinsOptions::default());
    let spec = to_tool_spec(find_tool(&builtins, "task").unwrap().as_ref());
    for provider in ["anthropic", "openai", "unknown"] {
        let profile = resolve_provider_behavior_profile(
            Some(&ProviderInfo {
                name: provider.into(),
                base_url: None,
                env_key: None,
                default_model: "policy-fixture".into(),
            }),
            &"policy-fixture".into(),
        );
        let rendered = render_tool_specs_for_behavior(std::slice::from_ref(&spec), &profile);
        let description = rendered[0]
            .input_schema
            .pointer("/properties/model/description")
            .and_then(|value| value.as_str())
            .unwrap();
        for required in [
            "Required for every new child task",
            "Choose by task complexity",
            "takes precedence over role and configured child defaults",
            "Do not omit it or use \"inherit\"",
            "An unavailable tier falls back",
            "use followup_task where available",
        ] {
            assert!(
                description.contains(required),
                "{provider} child model schema lost authorization: {description:?}"
            );
        }
        for forbidden in [
            "pass \"inherit\" to retain the parent's model even when the chosen role has a model preset",
            "Omitting this field uses the role's model preset when present",
            "Selecting a role authorizes changing models",
        ] {
            assert!(
                !description.contains(forbidden),
                "{provider} child model schema contains an obsolete authorization rule: {description:?}"
            );
        }
    }
}

#[test]
fn role_model_defaults_and_explicit_arguments_survive_provider_adapters() {
    let vector = contract();
    assert_eq!(vector["default_model_argument"], "required");
    let roles = [
        ("Explore", None),
        ("Plan", Some("high")),
        ("researcher", Some("medium")),
        ("general-purpose", None),
    ];
    let mut definitions = Vec::new();
    for (role, preset) in roles {
        let definition = BUILTIN_AGENTS
            .iter()
            .find(|definition| definition.name == role)
            .unwrap();
        assert_eq!(
            definition.model.clone(),
            preset.map(RuntimeString::from),
            "{role} model preset changed"
        );
        definitions.push(TaskAgentDefinition {
            name: definition.name.clone(),
            description: definition.description.clone(),
            isolation: None,
            background: None,
        });
    }
    let tool = create_task_tool(TaskToolOptions {
        model_catalog: false,
        model_options: Vec::new(),
        agents: definitions,
    });
    let spec = to_tool_spec(tool.as_ref());
    let exact =
        |value: Value| runtime_json::parse(&RuntimeString::from(value.to_string())).unwrap();
    for provider in ["anthropic", "openai", "unknown"] {
        let profile = resolve_provider_behavior_profile(
            Some(&ProviderInfo {
                name: provider.into(),
                base_url: None,
                env_key: None,
                default_model: "policy-fixture".into(),
            }),
            &"policy-fixture".into(),
        );
        let rendered = render_tool_specs_for_behavior(std::slice::from_ref(&spec), &profile);
        let visible = &rendered[0];
        assert_eq!(
            visible.input_schema.pointer("/properties/model/type"),
            Some(&RuntimeJson::from("string"))
        );
        for (role, _) in roles {
            for model in [Some("high"), Some("authorized-child-model")] {
                let mut expected = serde_json::json!({
                    "description": "Preserve authorized model input",
                    "prompt": "Inspect the provided input.",
                    "subagent_type": role
                });
                if let Some(model) = model {
                    expected["model"] = Value::from(model);
                }
                let input = if visible.name == "spawn_agent" {
                    expected["name"] = Value::from("Preserve authorized model input");
                    let mut input = serde_json::json!({
                        "task_name": "Preserve authorized model input",
                        "message": "Inspect the provided input.",
                        "agent_type": role
                    });
                    if let Some(model) = model {
                        input["model"] = Value::from(model);
                    }
                    input
                } else {
                    expected.clone()
                };
                let normalized =
                    normalize_tool_input_for_behavior(&visible.name, exact(input), &profile);
                assert!(
                    normalized.semantic_eq_unordered(&exact(expected)),
                    "{provider}/{role}/{model:?} must preserve the complete child task fields: {normalized:?}"
                );
            }
        }
    }
}

#[test]
fn shared_behavior_policy_does_not_treat_question_answers_as_runtime_permission() {
    let parts = build_system_prompt(&SystemPromptInput {
        config: ResolvedConfig::default(),
        ..Default::default()
    });
    let base = parts.iter().find(|part| part.name == "base").unwrap();
    assert!(base
        .text
        .contains(contract()["shared_question_boundary"].as_str().unwrap()));
    assert!(!base
        .text
        .contains("when progress requires the user's decision or approval"));
}
