//! Defect: local tool-budget estimates changed every provider's direct inventory.
//! Root cause: request preparation applied an adapter policy without an opt-in.
//! Work item: 20261021 provider-owned tool admission with recovery preserved.

use std::sync::atomic::Ordering;

use klaus_agent_core::context::tokens::estimate_prefix_tokens;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_values::RuntimeString;

use super::r20261019_small_context_tools::{
    assert_workspace_instructions_preserved, fixture_with_tool_budget,
    fixture_with_tool_budget_inventory,
};

#[tokio::test]
async fn only_opted_in_providers_defer_tools_and_keep_original_permission_checks() {
    for opted_in in [false, true] {
        for deny in [false, true] {
            let fixture =
                fixture_with_tool_budget_inventory(16_384, true, deny, false, false, opted_in, 140);
            let required_instruction =
                RuntimeString::from("Mandatory stable instruction. ".repeat(10));
            let result = fixture
                .session
                .run(
                    Input::Text("Run the probe.".into()),
                    TurnOptions {
                        system_prompt_append: Some(required_instruction.clone()),
                        ..Default::default()
                    },
                )
                .await;
            assert!(result.error.is_none(), "{:?}", result.error);
            assert_eq!(fixture.checks.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.calls.load(Ordering::SeqCst), usize::from(!deny));
            {
                let requests = fixture.requests.lock().unwrap();
                assert_eq!(requests.len(), if opted_in { 3 } else { 2 });
                let first = &requests[0];
                assert_eq!(
                    first
                        .system
                        .iter()
                        .find(|part| part.name == "append")
                        .expect("the required stable instruction must remain in the system prefix")
                        .text,
                    required_instruction
                );
                if opted_in {
                    assert_eq!(
                        first
                            .tools
                            .iter()
                            .map(|tool| tool.name.clone())
                            .collect::<Vec<_>>(),
                        vec![RuntimeString::from("tool_search")]
                    );
                    assert!(requests[1]
                        .tools
                        .iter()
                        .any(|tool| tool.name == "budget_probe"));
                    assert!(!requests[1]
                        .tools
                        .iter()
                        .any(|tool| tool.name.starts_with("large_tool_")));
                } else {
                    let prefix_tokens = estimate_prefix_tokens(&first.system, &first.tools);
                    let deferral_limit = 16_384 - first.max_output_tokens;
                    let auto_compact_limit = 16_384 * 90 / 100;
                    assert!(prefix_tokens > deferral_limit);
                    assert!(prefix_tokens < auto_compact_limit);
                    assert!(!first.tools.iter().any(|tool| tool.name == "tool_search"));
                    for index in 0..17 {
                        let name = if index == 0 {
                            "budget_probe".to_string()
                        } else {
                            format!("large_tool_{index}")
                        };
                        assert!(
                            first.tools.iter().any(|tool| tool.name == name.as_str()),
                            "{name}"
                        );
                    }
                }
            }
            fixture.session.shutdown_and_wait().await.unwrap();
        }
    }
}

#[tokio::test]
async fn estimated_instruction_pressure_keeps_actual_admission_reachable_for_both_policies() {
    for opted_in in [false, true] {
        let fixture = fixture_with_tool_budget(16_384, false, false, true, false, opted_in);
        let required_instruction =
            RuntimeString::from("Mandatory stable instruction. ".repeat(3000));
        let result = fixture
            .session
            .run(
                Input::Text("Reply briefly.".into()),
                TurnOptions {
                    system_prompt_append: Some(required_instruction.clone()),
                    ..Default::default()
                },
            )
            .await;
        assert!(result.error.is_none(), "{:?}", result.error);
        {
            let requests = fixture.requests.lock().unwrap();
            assert_eq!(
                requests.len(),
                1,
                "estimated pressure must not replace provider admission"
            );
            let request = &requests[0];
            assert_eq!(request.model, "window-model");
            assert_eq!(request.max_output_tokens, 4096);
            let required_part = request
                .system
                .iter()
                .find(|part| part.name == "append")
                .expect("the required stable instruction must remain in the system prefix");
            assert_eq!(required_part.text, required_instruction);
            assert_workspace_instructions_preserved(request);
            let input_limit = 16_384 - request.max_output_tokens;
            assert!(estimate_prefix_tokens(std::slice::from_ref(required_part), &[]) > input_limit);
            assert!(estimate_prefix_tokens(&request.system, &request.tools) > input_limit);
            if opted_in {
                assert_eq!(
                    request
                        .tools
                        .iter()
                        .map(|tool| tool.name.clone())
                        .collect::<Vec<_>>(),
                    vec![RuntimeString::from("tool_search")]
                );
            } else {
                assert!(!request.tools.iter().any(|tool| tool.name == "tool_search"));
                assert_eq!(request.tools.len(), 18);
                assert!(request.tools.iter().any(|tool| tool.name == "update_plan"));
                for index in 0..17 {
                    let name = if index == 0 {
                        "budget_probe".to_string()
                    } else {
                        format!("large_tool_{index}")
                    };
                    let tool = request
                        .tools
                        .iter()
                        .find(|tool| tool.name == name.as_str())
                        .expect("the provider's direct inventory must remain intact");
                    if index != 0 {
                        assert!(
                            tool.description == "Large declaration. ".repeat(10_000),
                            "declaration changed for {name}"
                        );
                    }
                }
            }
        }
        assert_eq!(fixture.checks.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
        fixture.session.shutdown_and_wait().await.unwrap();
    }
}
