use super::*;
use crate::schemas::config::SubagentModelOption;

#[tokio::test]
async fn subagent_model_policy_refreshes_schema_and_prompt_on_the_next_captured_step() {
    let slot = Arc::new(Mutex::new(None::<std::sync::Weak<Session>>));
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let count = Arc::new(AtomicUsize::new(0));
    let mut deps = mk_deps(Vec::new());
    deps.call_model = Box::new({
        let slot = slot.clone();
        let requests = requests.clone();
        move |request, _| {
            requests.lock().unwrap().push(request);
            let first = count.fetch_add(1, Ordering::SeqCst) == 0;
            let events = if first {
                slot.lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .upgrade()
                    .unwrap()
                    .set_subagent_model_settings(
                        Some(true),
                        Some(vec![SubagentModelOption {
                            value: "custom:available".into(),
                            label: "Available model".into(),
                        }]),
                    )
                    .unwrap();
                let mut events = vec![ev::message_start(None)];
                events.extend(exact_tool_use_events(
                    "next-step",
                    "think",
                    &crate::runtime_values::runtime_json::parse(
                        &r#"{"thought":"Continue."}"#.into(),
                    )
                    .unwrap(),
                ));
                events.push(ev::stop(Some(ModelStopReason::ToolUse), None));
                events
            } else {
                vec![
                    ev::message_start(None),
                    ev::text("Done."),
                    ev::stop(Some(ModelStopReason::EndTurn), None),
                ]
            };
            Box::pin(futures::stream::iter(events))
        }
    });
    let session = mk_session_with_deps(
        deps,
        ResolvedConfig::default(),
        vec![
            crate::tools::builtins::task::create_task_tool(Default::default()),
            crate::tools::builtins::think::create_think_tool(),
        ],
    );
    *slot.lock().unwrap() = Some(Arc::downgrade(&session));
    let result = session
        .run(
            Input::Text("Check the next step.".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    let model_schema = |request: &ModelRequest| {
        request
            .tools
            .iter()
            .find(|tool| tool.name == "task" || tool.name == "spawn_agent" || tool.name == "Task")
            .unwrap()
            .input_schema["properties"]["model"]
            .clone()
    };
    assert_eq!(
        model_schema(&requests[0])["enum"],
        crate::runtime_values::runtime_json::parse(&r#"["low","medium","high"]"#.into()).unwrap()
    );
    assert_eq!(
        model_schema(&requests[1])["enum"],
        crate::runtime_values::runtime_json::parse(&r#"["custom:available"]"#.into()).unwrap()
    );
    assert!(requests[0]
        .system
        .iter()
        .any(|part| part.name == "subagent-model-selection"
            && part.text.contains("Concrete model IDs are disabled")));
    assert!(requests[1].system.iter().any(
        |part| part.name == "subagent-model-selection" && part.text.contains("Available model")
    ));
}

#[test]
fn main_model_reconfiguration_preserves_the_live_subagent_model_directory() {
    let session = mk_session_with(Vec::new(), ResolvedConfig::default(), Vec::new());
    let options = vec![SubagentModelOption {
        value: "custom:available".into(),
        label: "Available model".into(),
    }];
    session
        .set_subagent_model_settings(Some(true), Some(options.clone()))
        .unwrap();
    let revision = session.model_revision();
    session
        .reconfigure_inference(
            Arc::new(fake_provider()),
            ResolvedConfig::default(),
            None,
            None,
            revision,
        )
        .unwrap();
    let captured = session.inference_config();
    assert!(captured.subagent_model_catalog);
    assert_eq!(captured.subagent_model_options, options);
}
