//! Runtime content was rejected before named-method validation by a scalar JSON
//! parameter detour. Pin exact typed content and the unchanged control guards.
//! Work item: exact runtime content on the named app-server facade.

use super::*;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn raw(text: &str) -> RawJson {
    RawJson::from_json(text.into()).expect("valid JSON fixture")
}

#[test]
fn typed_goal_content_retains_units_and_budget_presence() {
    let params = raw(r#"{"sessionId":"s","objective":"a\ud800b","displayText":"\udc00","expectedGoalId":null,"expectedGoalRevision":7,"tokenBudget":null}"#)
        .deserialize::<wire::SessionSetGoalParams>()
        .unwrap();
    assert_eq!(
        params.objective.as_ref().unwrap().as_utf16(),
        &[0x61, 0xd800, 0x62]
    );
    assert_eq!(params.display_text.as_ref().unwrap().as_utf16(), &[0xdc00]);
    assert_eq!(params.target.expected_goal_id, Some(None));
    assert_eq!(params.target.expected_goal_revision, Some(7));
    assert_eq!(params.token_budget, Some(None));
    let encoded = RawJson::from_serialize(&params).unwrap();
    let reread = encoded.deserialize::<wire::SessionSetGoalParams>().unwrap();
    assert_eq!(reread.objective, params.objective);
    assert_eq!(reread.display_text, params.display_text);
    assert_eq!(
        raw(r#"{"sessionId":"s","objective":"\ud800"}"#)
            .deserialize::<wire::SessionSetGoalParams>()
            .unwrap()
            .token_budget,
        None
    );
    for budget in ["0", "-1", "1.0", "1e0", "9007199254740992"] {
        let text = format!(r#"{{"sessionId":"s","objective":"\ud800","tokenBudget":{budget}}}"#);
        assert!(
            raw(&text)
                .deserialize::<wire::SessionSetGoalParams>()
                .is_err(),
            "budget {budget}"
        );
    }
    for revision in ["null", "-1", "1.0", "1e0", "9007199254740992"] {
        let text = format!(
            r#"{{"sessionId":"s","objective":"\ud800","expectedGoalRevision":{revision}}}"#
        );
        assert!(
            raw(&text)
                .deserialize::<wire::SessionSetGoalParams>()
                .is_err(),
            "revision {revision}"
        );
    }
}

#[test]
fn typed_plan_and_question_content_uses_strings_on_the_wire() {
    let prediction = raw(r#"{"suggestion":"\ud800"}"#)
        .deserialize::<wire::SessionPredictionResult>()
        .unwrap();
    assert_eq!(prediction.suggestion.unwrap().as_utf16(), &[0xd800]);
    let params = raw(r#"{"sessionId":"s","planId":"p","revision":2,"content":"\ud800","expectedContent":"\udc00"}"#)
        .deserialize::<wire::SessionSavePlanDocumentParams>().unwrap();
    assert_eq!(params.content.as_utf16(), &[0xd800]);
    assert_eq!(
        params.expected_content.as_ref().unwrap().as_utf16(),
        &[0xdc00]
    );
    let serialized = RawJson::from_serialize(&params).unwrap();
    let content = serialized
        .get("content")
        .unwrap()
        .unwrap()
        .to_runtime()
        .unwrap();
    assert_eq!(content.as_string().unwrap().as_utf16(), &[0xd800]);
    let request = raw(r#"{"prompt":"\ud800","questions":[{"id":"q","header":"\udc00","question":"\ud800","options":[{"label":"\ud800","description":"\udc00"}],"isOther":null,"isSecret":null}],"metadata":{"kind":"planning","isBlocking":true,"turnId":"t","toolCallId":"c"},"attachmentsSupported":null,"options":["\ud800","\udc00"],"multiSelect":null}"#)
        .deserialize::<wire::PlanningQuestionRequest>().unwrap();
    assert_eq!(request.prompt.as_utf16(), &[0xd800]);
    assert_eq!(request.questions[0].header.as_utf16(), &[0xdc00]);
    assert_eq!(request.options.as_ref().unwrap()[1].as_utf16(), &[0xdc00]);
    let answers = raw(r#"{"sessionId":"s","requestId":"r","turnId":"t","toolCallId":"c","answers":{"q":{"answers":["\ud800","\udc00"]}},"status":"answered"}"#)
        .deserialize::<wire::SessionAnswerUserInputParams>().unwrap();
    assert_eq!(answers.answers["q"].answers[0].as_utf16(), &[0xd800]);
    let saved = RawJson::from_serialize(&request).unwrap();
    assert_eq!(
        saved
            .deserialize::<wire::PlanningQuestionRequest>()
            .unwrap()
            .questions[0]
            .question
            .as_utf16(),
        &[0xd800]
    );
    for revision in ["2.0", "2e0", "-1"] {
        let text =
            format!(r#"{{"sessionId":"s","planId":"p","revision":{revision},"content":"\ud800"}}"#);
        assert!(raw(&text)
            .deserialize::<wire::SessionSavePlanDocumentParams>()
            .is_err());
    }
    assert!(raw(
        r#"{"sessionId":"s","planId":"p","revision":2,"content":"\ud800","unexpected":true}"#
    )
    .deserialize::<wire::SessionSavePlanDocumentParams>()
    .is_err());
}

#[test]
fn raw_init_children_keep_content_and_control_number_tokens() {
    let source = raw(
        r#"{"threadId":"thread","config":{"systemPromptAppend":"\ud800","unchanged":1e0},"skills":[{"name":"skill","content":"\udc00"}],"opaqueNumber":9007199254740993}"#,
    );
    let mut init = exact_requests::InitObject::new(&source).unwrap();
    let config_before = init.get("config").unwrap().as_str().to_owned();
    let skills_before = init.get("skills").unwrap().as_str().to_owned();
    init.set("threadId", raw(r#""other-thread""#));
    let encoded = init.raw();
    assert_eq!(
        encoded.control_string("threadId").unwrap().as_deref(),
        Some("other-thread")
    );
    assert_eq!(
        encoded.get("config").unwrap().unwrap().as_str(),
        config_before
    );
    assert_eq!(
        encoded.get("skills").unwrap().unwrap().as_str(),
        skills_before
    );
    assert_eq!(
        encoded.get("opaqueNumber").unwrap().unwrap().as_str(),
        "9007199254740993"
    );
}

#[tokio::test]
async fn public_named_ingress_validates_controls_after_exact_content() {
    let root = tempfile::tempdir().unwrap();
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (read, mut write) = tokio::io::split(client);
    let (server_read, server_write) = tokio::io::split(server);
    let factory_calls = Arc::new(AtomicU64::new(0));
    let calls = factory_calls.clone();
    let server = tokio::spawn(run_app_server(
        server_read,
        server_write,
        AppServerOptions {
            windows_sandbox_context: None,
            create_session: Arc::new(move |_| {
                calls.fetch_add(1, Ordering::Relaxed);
                Box::pin(async {
                    Err(std::io::Error::other("the fixture never creates a session").into())
                })
            }),
            credential_store: None,
            lifecycle: None,
            plugin_admin: None,
            permission_rules_path: Some(root.path().join("permission-rules.json")),
            config_path: Some(root.path().join("config.json")),
            startup_init_ack_budget: None,
            session_unload_delay: None,
        },
    ));
    let mut lines = BufReader::new(read).lines();
    write
        .write_all(b"{\"id\":1,\"method\":\"initialize\",\"params\":{}}\n")
        .await
        .unwrap();
    let initialized = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(raw(&initialized).get("result").unwrap().is_some());
    let requests = [
        (
            r#"{"id":2,"method":"turn/start","params":{"sessionId":"missing","input":"\ud800"}}"#,
            wire::UNKNOWN_SESSION,
        ),
        (
            r#"{"id":2,"method":"session/setGoal","params":{"objective":"\ud800","displayText":"\udc00","tokenBudget":7}}"#,
            wire::INVALID_PARAMS,
        ),
        (
            r#"{"id":2,"method":"session/setGoal","params":{"sessionId":"missing","objective":"\ud800","tokenBudget":1.0}}"#,
            wire::INVALID_PARAMS,
        ),
        (
            r#"{"id":2,"method":"session/savePlanDocument","params":{"sessionId":"missing","planId":"p","revision":1,"content":"\ud800","expectedContent":"\udc00"}}"#,
            wire::UNKNOWN_SESSION,
        ),
        (
            r#"{"id":2,"method":"session/answerQuestion","params":{"sessionId":"missing","requestId":"r","turnId":"t","clientResponseId":"c","answers":{"q":"\ud800"}}}"#,
            wire::UNKNOWN_SESSION,
        ),
        (
            r#"{"id":2,"method":"session/answerUserInput","params":{"sessionId":"missing","requestId":"r","turnId":"t","toolCallId":"c","answers":{"q":{"answers":["\ud800"]}},"status":"answered"}}"#,
            wire::UNKNOWN_SESSION,
        ),
        (
            r#"{"id":2,"method":"session/steer","params":{"sessionId":"missing","input":"\ud800","displayMetadata":{"text":"\udc00"}}}"#,
            wire::UNKNOWN_SESSION,
        ),
        (
            r#"{"id":2,"method":"session/updateConfig","params":{"sessionId":"missing","update":{"model":"\ud800","maximumGoalTokenBudget":1.0}}}"#,
            wire::INVALID_PARAMS,
        ),
    ];
    for (request, expected) in requests {
        write.write_all(request.as_bytes()).await.unwrap();
        write.write_all(b"\n").await.unwrap();
        let response = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let response = raw(&response).deserialize::<Value>().unwrap();
        assert_eq!(
            response["error"]["code"].as_i64(),
            Some(expected),
            "{request}: {response}"
        );
    }
    assert_eq!(factory_calls.load(Ordering::Relaxed), 0);
    drop(write);
    drop(lines);
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
}
