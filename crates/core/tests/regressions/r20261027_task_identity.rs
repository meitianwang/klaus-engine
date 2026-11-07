//! Defect: hosts could not validate one unresolved task after its plan was archived.
//! Root cause: the facade exposed only the current task-list projection.
//! Regression 20261027: expose durable task instances without plan-wide invalidation.

use super::*;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::tasks::task_store::{
    create_task_bound, delete_task, update_task, write_task_plan, CreateTaskInput,
    TaskPlanWriteEntry, TaskStatus, TaskUpdate,
};
use std::sync::Mutex;

async fn exact_response(client: &mut TestClient, id: u64) -> super::super::runtime_wire::RawJson {
    loop {
        let line = tokio::time::timeout(REPLY_TIMEOUT, client.lines.next_line())
            .await
            .expect("task identity reply within timeout")
            .expect("readable stream")
            .expect("stream still open");
        let message = super::super::runtime_wire::RawJson::from_json(line).unwrap();
        if message.control_u64("id").unwrap() == Some(id)
            && message.get("method").unwrap().is_none()
        {
            return message;
        }
    }
}

async fn exact_identity_response(
    client: &mut TestClient,
    id: u64,
) -> super::super::wire::SessionTaskIdentityResult {
    exact_response(client, id)
        .await
        .get("result")
        .unwrap()
        .unwrap()
        .deserialize()
        .unwrap()
}

#[tokio::test]
async fn task_identity_facade_reads_archived_tasks_and_preserves_exact_semantics() {
    let captured = Arc::new(Mutex::new(None::<Arc<Session>>));
    let base = create_session_for_agent_tests(make_agent_for_tests(Vec::new()));
    let observed = captured.clone();
    let create: CreateSession = Arc::new(move |context| {
        let start = base(context);
        let observed = observed.clone();
        Box::pin(async move {
            let session = start.await?;
            *observed.lock().unwrap() = Some(session.clone());
            Ok(session)
        })
    });
    let mut client = TestClient::start(create);
    client.initialize().await;
    client
        .send(json!({"id":2,"method":"session/start","params":{"init":{}}}))
        .await;
    let (started, _) = client.response_for(2).await;
    let session_id = started["result"]["sessionId"].as_str().unwrap().to_string();
    client
        .send(json!({"id":3,"method":"session/taskIdentity","params":{"sessionId":session_id,"taskId":"1"}}))
        .await;
    let (empty, _) = client.response_for(3).await;
    assert_eq!(empty["result"], json!({"taskIdentity":null}));
    let session = captured.lock().unwrap().clone().unwrap();
    let list = session.task_list_id();
    let created = create_task_bound(
        &list,
        CreateTaskInput {
            subject: RuntimeString::from_utf16(vec![0x0041, 0xd800]),
            description: "Await the requested result".into(),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    write_task_plan(
        &list,
        vec![TaskPlanWriteEntry {
            entry_id: Some("other-work".into()),
            subject: Some("Independent task".into()),
            description: None,
            active_form: None,
            status: None,
        }],
        false,
        None,
    )
    .await
    .unwrap();
    client
        .send(json!({"id":4,"method":"session/taskIdentity","params":{"sessionId":session_id,"taskId":"1"}}))
        .await;
    let identity = exact_identity_response(&mut client, 4)
        .await
        .task_identity
        .unwrap();
    assert_eq!(identity.task_id, "1");
    assert_eq!(identity.status, "pending");
    assert_eq!(identity.subject.as_utf16(), created.subject.as_utf16());
    assert_eq!(identity.description, "Await the requested result");
    assert!(identity.generation.starts_with("sha256:"));
    assert!(identity.fingerprint.starts_with("sha256:"));
    update_task(
        &list,
        &created.id,
        &TaskUpdate {
            status: Some(TaskStatus::Completed),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    client
        .send(json!({"id":5,"method":"session/taskIdentity","params":{"sessionId":session_id,"taskId":"1"}}))
        .await;
    let completed = exact_identity_response(&mut client, 5)
        .await
        .task_identity
        .unwrap();
    assert_eq!(completed.status, "completed");
    assert_eq!(completed.generation, identity.generation);
    assert_eq!(completed.fingerprint, identity.fingerprint);
    delete_task(&list, &created.id).await.unwrap();
    client
        .send(json!({"id":6,"method":"session/taskIdentity","params":{"sessionId":session_id,"taskId":"1"}}))
        .await;
    assert!(exact_identity_response(&mut client, 6)
        .await
        .task_identity
        .is_none());
    client
        .send(json!({"id":7,"method":"session/taskIdentity","params":{"sessionId":session_id,"taskId":"../1"}}))
        .await;
    let invalid = exact_response(&mut client, 7).await;
    assert!(invalid.get("error").unwrap().is_some());
    client.close().await;
}

#[test]
fn task_identity_schema_declares_a_narrow_optional_result() {
    let bundle = schema_bundle();
    let request = &bundle["requests"]["session/taskIdentity"];
    assert!(request["params"].is_object());
    assert!(request["result"].is_object());
    let params: super::super::wire::SessionTaskIdentityParams = serde_json::from_value(json!({
        "sessionId":"session","taskId":"1"
    }))
    .unwrap();
    assert_eq!(params.task_id, "1");
    assert!(
        serde_json::from_value::<super::super::wire::SessionTaskIdentityParams>(json!({
            "sessionId":"session","taskId":"1","path":"untrusted"
        }))
        .is_err()
    );
}
