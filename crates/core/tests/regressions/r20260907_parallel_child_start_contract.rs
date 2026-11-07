//! Defect: a parallel child turn start was rejected as non-contract interleaving.
//! Root cause: terminal normalization predated the child identity progress event.
//! Fixed for the 2026-09-07 parallel child-start contract work item.

use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{RuntimeJson, RuntimeString};
use klaus_agent_core::testing::provider_behavior::{
    load_checked_in_manifest, run_scripted_task_tolerant, EvalProfile,
};
use klaus_agent_core::testing::{node_interpreter_present, shared_test_process_supervisor};
use std::collections::BTreeSet;

fn assert_initial_skill_catalog(message: &RuntimeJson) {
    use klaus_agent_core::extensions::skills::builtin_skills::builtin_skills;
    use klaus_agent_core::extensions::skills::loader::model_invocable;
    use klaus_agent_core::extensions::skills::skill_listing_reminder::create_skill_listing_reminder_provider;

    assert!(text_is(message, "/type", "user"), "{message:?}");
    assert_eq!(message.get("isMeta"), Some(&RuntimeJson::Bool(true)));
    let blocks = message["content"].as_array().unwrap();
    assert_eq!(blocks.len(), 1);
    assert!(text_is(&blocks[0], "/type", "text"));
    assert!(text_is(&blocks[0], "/harnessEnvelope", "system-reminder"));
    let text = text_at(&blocks[0], "/text").try_to_utf8().unwrap();
    let (marker, listing) = text.split_once('\n').expect("retained catalog marker");
    let parts = marker
        .strip_prefix("<skill-catalog:")
        .and_then(|value| value.strip_suffix('>'))
        .expect("only a retained skill catalog may precede the directive")
        .split(':')
        .collect::<Vec<_>>();
    assert_eq!(parts.len(), 3);
    assert_eq!(parts[0].len(), 64);
    assert!(parts[0]
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
    assert_eq!(parts[1], "0");
    assert_eq!(
        parts[2].parse::<usize>().unwrap(),
        model_invocable(builtin_skills()).len()
    );
    let expected = create_skill_listing_reminder_provider(builtin_skills, None)
        .tick()
        .unwrap();
    assert_eq!(
        listing,
        expected.try_to_utf8().unwrap(),
        "unexpected retained context"
    );
}

fn text_at<'a>(value: &'a RuntimeJson, pointer: &str) -> &'a RuntimeString {
    value
        .pointer(pointer)
        .and_then(RuntimeJson::as_str)
        .unwrap_or_else(|| panic!("missing text at {pointer}: {value:?}"))
}

fn text_is(value: &RuntimeJson, pointer: &str, expected: &str) -> bool {
    value.pointer(pointer).and_then(RuntimeJson::as_str) == Some(&RuntimeString::from(expected))
}

fn is_skill_catalog_context(message: &RuntimeJson) -> bool {
    text_is(message, "/type", "user")
        && message.pointer("/isMeta").and_then(RuntimeJson::as_bool) == Some(true)
        && message["content"]
            .as_array()
            .is_some_and(|content| content.len() == 1)
        && text_is(message, "/content/0/type", "text")
        && text_is(message, "/content/0/harnessEnvelope", "system-reminder")
        && message
            .pointer("/content/0/text")
            .and_then(RuntimeJson::as_str)
            .is_some_and(|text| text.starts_with("<skill-catalog:"))
}

#[tokio::test]
async fn parallel_subagent_replay_keeps_child_starts_bound_to_real_requests() {
    if !node_interpreter_present() {
        eprintln!("skipping Node-reconstructed provider contract: no Node interpreter available");
        return;
    }
    let context = RuntimeProcessContext::initialize(shared_test_process_supervisor()).unwrap();
    let manifest = load_checked_in_manifest().unwrap();
    let traces = run_scripted_task_tolerant(
        &context,
        &manifest,
        "provider-parallel-subagents",
        &[
            EvalProfile::Anthropic,
            EvalProfile::Openai,
            EvalProfile::Generic,
        ],
    )
    .await
    .unwrap();
    assert_eq!(traces.len(), 3);
    for trace in &traces {
        // The public strict suite requires this same success predicate. Keep the
        // focused task replay, then independently bind its current child evidence.
        assert!(trace.result.success, "{:?}", trace.result.behavior_failures);
        assert!(trace.result.score_success);
        assert!(trace.result.behavior_success);
        let child_starts = trace
            .engine_events
            .iter()
            .enumerate()
            .filter(|(_, event)| {
                text_is(event, "/type", "tool.event")
                    && text_is(event, "/event/type", "agent_progress")
                    && text_is(event, "/event/event/type", "turn.started")
            })
            .collect::<Vec<_>>();
        assert_eq!(
            child_starts.len(),
            2,
            "each real child keeps its turn start"
        );
        assert_eq!(
            child_starts
                .iter()
                .map(|(_, event)| text_at(event, "/toolUseId").clone())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([RuntimeString::from("t1"), RuntimeString::from("t2")])
        );

        let parent_key = text_at(&trace.model_requests[0], "/cacheKey");
        let child_requests = trace
            .model_requests
            .iter()
            .filter(|request| {
                request["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|message| {
                        message
                            .pointer("/content/0/text")
                            .and_then(RuntimeJson::as_str)
                            .is_some_and(|text| text.starts_with("<fork-worker>"))
                    })
            })
            .collect::<Vec<_>>();
        assert_eq!(
            child_requests.len(),
            2,
            "both children must reach the provider"
        );
        let parent_uuids = trace
            .history
            .iter()
            .map(|message| text_at(message, "/uuid").clone())
            .collect::<BTreeSet<_>>();
        let parent_turn = text_at(
            trace
                .engine_events
                .iter()
                .find(|event| text_is(event, "/type", "turn.started"))
                .unwrap(),
            "/turnId",
        );
        let mut child_keys = BTreeSet::new();
        let mut child_roots = BTreeSet::new();
        let mut child_turns = BTreeSet::new();
        for (child_index, event) in child_starts {
            let owner = text_at(event, "/toolUseId");
            let agent = text_at(event, "/event/agentId");
            let turn = text_at(event, "/event/event/turnId");
            assert!(!agent.is_empty() && !turn.is_empty());
            assert!(
                child_keys.insert(agent.clone()),
                "children share an identity"
            );
            assert!(child_turns.insert(turn.clone()), "children share a turn");
            assert_ne!(turn, parent_turn, "child reuses the parent's turn identity");

            let parent_event_indices = |kind: &str| {
                trace
                    .engine_events
                    .iter()
                    .enumerate()
                    .filter(|(_, event)| {
                        text_is(event, "/type", kind)
                            && text_is(event, "/item/type", "tool_call")
                            && event.pointer("/item/id").and_then(RuntimeJson::as_str)
                                == Some(owner)
                    })
                    .map(|(index, _)| index)
                    .collect::<Vec<_>>()
            };
            let starts = parent_event_indices("item.started");
            let terminals = parent_event_indices("item.completed");
            assert_eq!(starts.len(), 1);
            assert_eq!(terminals.len(), 1);
            assert!(starts[0] < child_index && child_index < terminals[0]);
            assert!(text_is(
                &trace.engine_events[terminals[0]],
                "/item/status",
                "completed"
            ));

            let calls = trace
                .normalized_internal_calls
                .iter()
                .filter(|call| text_at(call, "/id") == owner)
                .collect::<Vec<_>>();
            assert_eq!(calls.len(), 1);
            assert!(text_is(calls[0], "/internalName", "task"));
            let directive = text_at(calls[0], "/internalInput/prompt")
                .try_to_utf8()
                .unwrap();
            assert!(
                calls[0]["internalInput"].get("fork_turns").is_none(),
                "this fixture must exercise default full inheritance"
            );
            let expected_directive = RuntimeString::from(
                klaus_agent_core::engine::fork::build_fork_boilerplate(&directive),
            );
            let requests = child_requests
                .iter()
                .copied()
                .filter(|request| {
                    request["messages"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|message| {
                            message
                                .pointer("/content/0/text")
                                .and_then(RuntimeJson::as_str)
                                == Some(&expected_directive)
                        })
                })
                .collect::<Vec<_>>();
            assert_eq!(
                requests.len(),
                1,
                "child progress must name its real request"
            );
            assert_eq!(
                text_at(requests[0], "/cacheKey"),
                parent_key,
                "full forks retain the parent cache key"
            );
            assert_eq!(
                requests[0].get("skipCacheWrite"),
                Some(&RuntimeJson::Bool(true))
            );
            assert!(
                requests[0]["tools"].as_array().unwrap().iter().any(|tool| {
                    ["Task", "spawn_agent", "task"]
                        .iter()
                        .any(|name| text_is(tool, "/name", name))
                }),
                "full forks retain the nested delegation surface"
            );
            let raw_messages = requests[0]["messages"].as_array().unwrap();
            let catalog_contexts = raw_messages
                .iter()
                .filter(|message| is_skill_catalog_context(message))
                .collect::<Vec<_>>();
            for catalog in &catalog_contexts {
                assert_initial_skill_catalog(catalog);
            }
            let parent_assistant = trace
                .history
                .iter()
                .position(|message| {
                    text_is(message, "/type", "assistant")
                        && message["content"].as_array().unwrap().iter().any(|block| {
                            block.get("id").and_then(RuntimeJson::as_str) == Some(owner)
                        })
                })
                .unwrap();
            let inherited_catalogs = trace.history[..=parent_assistant]
                .iter()
                .filter(|message| is_skill_catalog_context(message))
                .collect::<Vec<_>>();
            assert!(
                !inherited_catalogs.is_empty(),
                "the parent catalog must be observable"
            );
            for catalog in &inherited_catalogs {
                assert!(
                    raw_messages.contains(catalog),
                    "the inherited catalog retains its original UUID and content"
                );
            }
            assert!(
                catalog_contexts.len() >= inherited_catalogs.len()
                    && catalog_contexts.len() <= inherited_catalogs.len() + 1
            );
            let messages = raw_messages
                .iter()
                .filter(|message| !is_skill_catalog_context(message))
                .collect::<Vec<_>>();
            let inherited = trace.history[..=parent_assistant]
                .iter()
                .filter(|message| !is_skill_catalog_context(message))
                .collect::<Vec<_>>();
            assert_eq!(
                &messages[..inherited.len()],
                inherited.as_slice(),
                "the entire initiating conversation must be inherited"
            );
            let generated = &messages[inherited.len()..];
            assert_eq!(
                generated.len(),
                3,
                "both unresolved sibling calls need placeholders before the directive"
            );
            let placeholder_ids = generated[..2]
                .iter()
                .map(|message| {
                    assert!(text_is(message, "/type", "tool_result"));
                    assert!(text_is(
                        message,
                        "/content/0/text",
                        "Forked — the parent handled this call; continue with your directive."
                    ));
                    assert_eq!(message.get("isError"), Some(&RuntimeJson::Bool(false)));
                    text_at(message, "/toolUseId").clone()
                })
                .collect::<BTreeSet<_>>();
            assert_eq!(
                placeholder_ids,
                BTreeSet::from([RuntimeString::from("t1"), RuntimeString::from("t2")])
            );
            let directive_message = generated[2];
            assert!(text_is(directive_message, "/type", "user"));
            assert_eq!(directive_message["content"].as_array().unwrap().len(), 1);
            assert_eq!(
                text_at(directive_message, "/content/0/text"),
                &expected_directive
            );
            for message in generated {
                let uuid = text_at(message, "/uuid");
                assert!(
                    !uuid.is_empty() && !parent_uuids.contains(uuid),
                    "generated child UUID collides with inherited history"
                );
                assert!(
                    child_roots.insert(uuid.clone()),
                    "children share a generated UUID"
                );
                assert_ne!(uuid, turn, "child message and turn identities are distinct");
            }
        }
    }
}
