//! Work item 01a15833: a later passive message cannot overtake a reserved direction.

use super::*;

fn direction(id: &str, trigger_turn: bool) -> QueuedMessage {
    QueuedMessage {
        trigger_turn: Some(trigger_turn),
        value: QueuedValue::Text(id.into()),
        mode: "subagent-message".into(),
        priority: Some(QueuePriority::Next),
        uuid: Some(id.into()),
        session_id: Some("mailbox-order-session".into()),
        is_meta: Some(true),
        origin: None,
        agent_id: Some("mailbox-order-agent".into()),
        correlation_id: None,
        delivery: Some(crate::engine::message_queue::SteerDelivery::User),
        display_metadata: None,
        pdf_bytes: 0,
        pdf_documents: 0,
    }
}

fn queued_ids(request: &ModelRequest) -> Vec<RuntimeString> {
    request
        .messages
        .iter()
        .filter_map(|message| match message {
            Message::User(user)
                if user.uuid == "older-direction" || user.uuid == "later-passive" =>
            {
                Some(user.uuid.clone())
            }
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn passive_arrival_during_resume_checkpoint_wait_cannot_overtake_reserved_input() {
    let queue = Arc::new(MessageQueue::for_session("mailbox-order-session".into()));
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let held = Arc::new(std::sync::atomic::AtomicBool::new(false));
    queue.set_persistence_sink(Arc::new({
        let entered = entered.clone();
        let release = release.clone();
        move |state| {
            let entered = entered.clone();
            let release = release.clone();
            let checkpoint = runtime_codec::encode_value(&state).unwrap();
            let hold = checkpoint
                .get("resumeReserved")
                .and_then(RuntimeJson::as_array)
                .is_some_and(|ids| ids.contains(&RuntimeJson::from("older-direction")))
                && !held.swap(true, Ordering::SeqCst);
            Box::pin(async move {
                if hold {
                    entered.add_permits(1);
                    release.acquire().await.unwrap().forget();
                }
                Ok(())
            })
        }
    }));
    let tools: Vec<Arc<dyn Tool>> = vec![Arc::new(ResultShapeTool {
        name: "mailbox_boundary",
        schema: JsonSchema::new(),
        text: "boundary complete".to_string(),
        max_chars: None,
        output: RuntimeJson::Null,
        extra_user_blocks: None,
    })];
    let tool_turn = |id: &str| {
        let mut events = vec![ev::message_start(Some("gpt-5"))];
        events.extend(exact_tool_use_events(
            id,
            "mailbox_boundary",
            &RuntimeJson::Object(RuntimeObject::new()),
        ));
        events.push(ev::stop(Some(ModelStopReason::ToolUse), None));
        events
    };
    let scripted = Arc::new(Mutex::new(vec![
        tool_turn("first-boundary"),
        tool_turn("second-boundary"),
        vec![
            ev::message_start(Some("gpt-5")),
            ev::text("finished"),
            ev::stop(None, None),
        ],
    ]));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let deps = deps_with(Box::new({
        let queue = queue.clone();
        let requests = requests.clone();
        move |request: ModelRequest, _: AbortSignal| -> ModelStream {
            let mut requests = requests.lock().unwrap();
            if requests.is_empty() {
                queue.enqueue(direction("older-direction", true)).unwrap();
            }
            requests.push(request);
            Box::pin(futures::stream::iter(scripted.lock().unwrap().remove(0)))
        }
    }));
    let mut params = mk_params(vec![user_msg("perform two bounded steps")], tools);
    params.model = "gpt-5".into();
    params.agent_id = Some("mailbox-order-agent".into());
    params.message_queue = Some(queue.clone());
    let running = tokio::spawn(async move { query(params, deps).collect::<Vec<_>>().await });
    tokio::time::timeout(Duration::from_secs(5), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    queue.enqueue(direction("later-passive", false)).unwrap();
    release.add_permits(1);
    let events = tokio::time::timeout(Duration::from_secs(5), running)
        .await
        .unwrap()
        .unwrap();
    assert!(!events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnFailed { .. })));
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(
        queued_ids(&requests[1]),
        vec![RuntimeString::from("older-direction")]
    );
    assert_eq!(
        queued_ids(&requests[2]),
        vec![
            RuntimeString::from("older-direction"),
            RuntimeString::from("later-passive")
        ]
    );
    assert!(queue.admitted_snapshot().is_empty());
}
