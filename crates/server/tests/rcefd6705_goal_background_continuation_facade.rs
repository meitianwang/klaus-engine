//! An active goal must audit a persistent blocker while its background shell remains alive.

use super::*;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};
use klaus_agent_core::schemas::message::Usage;

#[tokio::test]
async fn named_goal_submission_automatically_reaches_blocked_with_a_running_shell() {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let calls = Arc::new(AtomicUsize::new(0));
    let captured = calls.clone();
    let agent = make_agent_with_call_model_and_config(
        Box::new(move |_, _| {
            let index = captured.fetch_add(1, Ordering::SeqCst);
            let mut events = vec![ev::message_start(None)];
            let tool = match index {
                0 => Some((
                    "bash",
                    json!({
                        "command": if cfg!(windows) { "ping -n 31 127.0.0.1 > NUL" } else { "exec sleep 30" },
                        "runInBackground": true
                    }),
                )),
                1 => {
                    events.push(ev::text(
                        "External authorization is required: first goal turn.",
                    ));
                    None
                }
                2 => {
                    events.push(ev::text(
                        "The same authorization is required: second goal turn.",
                    ));
                    None
                }
                3 => Some(("update_goal", json!({"status":"blocked"}))),
                4 => {
                    events.push(ev::text(
                        "Authorization remains blocked after three goal turns.",
                    ));
                    None
                }
                _ => panic!("the blocked goal admitted unexpected inference"),
            };
            let stop = if let Some((name, input)) = tool {
                let input = runtime_json::parse(&RuntimeString::from(input.to_string())).unwrap();
                events.extend(ev::tool_use(
                    &format!("background-goal-{index}"),
                    name,
                    &input,
                ));
                ModelStopReason::ToolUse
            } else {
                ModelStopReason::EndTurn
            };
            events.push(ev::stop(
                Some(stop),
                Some(Usage {
                    input_tokens: 1,
                    output_tokens: 1,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                }),
            ));
            Box::pin(futures::stream::iter(events))
        }),
        Some(json!({"goal":true,"autoMemory":false,"autoDream":false,
            "approval_policy":"never","sandbox_mode":"danger-full-access"})),
    );
    let (mut client, _, _, session) = controlled_goal_client_with_agent_events(
        root.path(),
        Arc::new(InstructionFlushControl::new()),
        agent,
        true,
    )
    .await;
    client
        .send(json!({"id":3,"method":"session/setGoal","params":{
            "sessionId":session,"objective":"Complete the authorization workflow.","origin":"user",
            "submit":true,"clientRequestId":"background-goal-audit","expectedGoalId":null
        }}))
        .await;
    let (created, initial) = client.response_for(3).await;
    assert!(created.get("error").is_none(), "{created}");
    let mut frames = initial;
    tokio::time::timeout(REPLY_TIMEOUT, async {
        while !frames.iter().any(|frame| {
            frame["method"] == "turn/completed"
                && frame["params"]["result"]["finalText"]
                    == "Authorization remains blocked after three goal turns."
        }) {
            assert!(frames.len() < 256, "unexpected goal event flood");
            frames.push(client.next_message().await);
        }
    })
    .await
    .unwrap_or_else(|error| {
        panic!(
            "automatic blocked audit did not finish: {error}; calls={}",
            calls.load(Ordering::SeqCst)
        )
    });
    let started: Vec<_> = frames
        .iter()
        .filter(|frame| frame["method"] == "turn/started")
        .collect();
    assert_eq!(
        started.len(),
        3,
        "goal submission must receive two automatic successors"
    );
    for (index, frame) in started.iter().enumerate() {
        assert_eq!(frame["params"]["goalContinuation"], index != 0, "{frame}");
        assert_eq!(
            frame["params"]["goalId"], created["result"]["threadGoal"]["goalId"],
            "{frame}"
        );
    }
    client
        .send(json!({"id":4,"method":"session/goal","params":{"sessionId":session}}))
        .await;
    let (goal, _) = client.response_for(4).await;
    assert_eq!(goal["result"]["threadGoal"]["status"], "blocked", "{goal}");
    client
        .send(json!({"id":5,"method":"session/backgroundProcesses","params":{"sessionId":session}}))
        .await;
    let (background, _) = client.response_for(5).await;
    let processes = background["result"]["backgroundProcesses"]["processes"]
        .as_array()
        .expect("authoritative process snapshot");
    assert_eq!(processes.len(), 1, "{background}");
    assert_eq!(
        processes[0]["status"], "running",
        "goal must block before its shell exits: {background}"
    );
    client
        .send(
            json!({"id":6,"method":"session/stopBackgroundProcess","params":{
                "sessionId":session,"shellId":processes[0]["shellId"],"signal":"SIGKILL"
            }}),
        )
        .await;
    let (stopped, _) = client.response_for(6).await;
    assert!(
        stopped.get("error").is_none(),
        "fixture cleanup must stop its own process: {stopped}"
    );
    client.close().await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        5,
        "blocked goals must not auto-resume"
    );
}
