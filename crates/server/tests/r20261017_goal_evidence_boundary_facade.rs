//! Objective evidence limits measure the JSON string value independently of provenance.

use super::*;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};

#[tokio::test]
async fn named_goal_controls_retain_complete_699_and_700_byte_objective_values_and_omit_701() {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let calls = Arc::new(AtomicUsize::new(0));
    let captured = calls.clone();
    let agent = make_agent_with_call_model_and_config(
        Box::new(move |_, _| {
            captured.fetch_add(1, Ordering::SeqCst);
            Box::pin(futures::stream::iter(text_turn("unexpected inference")))
        }),
        Some(json!({"goal":true,"autoMemory":false,"autoDream":false})),
    );
    let (mut client, store, _agent, session) = controlled_goal_client_with_agent(
        root.path(),
        Arc::new(InstructionFlushControl::new()),
        agent,
    )
    .await;
    let mut cases = Vec::new();
    for prefix in ["", "\"界😀</goal>\\\n"] {
        let prefix_bytes = runtime_json::stringify(&RuntimeJson::String(prefix.into()))
            .to_node_utf8()
            .len();
        for length in [699, 700, 701] {
            let objective = format!("{prefix}{}", "x".repeat(length - prefix_bytes));
            assert_eq!(
                runtime_json::stringify(&RuntimeJson::String(objective.clone().into()))
                    .to_node_utf8()
                    .len(),
                length
            );
            let id = 3 + cases.len() as u64;
            client
                .send(json!({"id":id,"method":"session/setGoal","params":{
                    "sessionId":session,"objective":objective,"status":"paused","origin":"user"
                }}))
                .await;
            let (response, _) = client.response_for(id).await;
            assert!(response.get("error").is_none(), "{response}");
            assert_eq!(response["result"]["threadGoal"]["objective"], objective);
            cases.push((length, objective));
        }
    }
    let saved = store.read_thread("controlled-goal-thread").await.unwrap();
    let instructions: Vec<_> = saved
        .raw_items
        .iter()
        .filter_map(|item| match item {
            RolloutItem::Event(event) if event.event == "user_goal_instruction" => {
                event.payload.as_ref()
            }
            _ => None,
        })
        .collect();
    assert_eq!(instructions.len(), cases.len());
    let mut ids = std::collections::HashSet::new();
    for (instruction, (length, objective)) in instructions.iter().zip(&cases) {
        let id = instruction.get("id").and_then(RuntimeJson::as_str).unwrap();
        assert!(
            ids.insert(id.clone()),
            "every accepted control retains its own identity"
        );
        assert_eq!(
            instruction.get("origin").and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("user"))
        );
        assert_eq!(
            instruction.get("status").and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("paused"))
        );
        let id_bytes = runtime_json::stringify(&RuntimeJson::String(id.clone()))
            .to_node_utf8()
            .len();
        let full_bytes = runtime_json::stringify(instruction).to_node_utf8().len();
        if *length <= 700 {
            assert_eq!(
                instruction.get("objective").and_then(RuntimeJson::as_str),
                Some(&RuntimeString::from(objective.clone()))
            );
            assert!(instruction.get("objectiveOmitted").is_none());
            assert!(
                full_bytes > 700,
                "provenance bytes must not reduce the objective allowance"
            );
            assert!(
                full_bytes <= *length + id_bytes + 256,
                "metadata has a separate fixed wrapper bound"
            );
        } else {
            assert!(instruction.get("objective").is_none());
            assert_eq!(
                instruction
                    .get("objectiveOmitted")
                    .and_then(RuntimeJson::as_bool),
                Some(true)
            );
            assert_eq!(
                instruction
                    .get("objectiveBytes")
                    .and_then(RuntimeJson::as_number)
                    .map(|number| number.as_f64()),
                Some(objective.len() as f64)
            );
            assert!(full_bytes <= id_bytes + 256);
        }
    }
    let replayed =
        klaus_agent_core::goal::retained_context::instructions_from_rollout(&saved.raw_items);
    assert_eq!(replayed.len(), cases.len());
    client
        .send(json!({"id":20,"method":"session/queueSnapshot","params":{"sessionId":session}}))
        .await;
    let (queue, _) = client.response_for(20).await;
    assert!(queue["result"]["queueSnapshot"]["items"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "retaining provenance does not admit work"
    );
    client.close().await;
}
