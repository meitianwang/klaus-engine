//! Defect: MCP snapshot/reload waited for a code that the input reader could not dispatch.
//! Root cause: the reader awaited the ordinary operation before reading its authorization reply.
//! Fixed for the 2026-09-08 Electron MCP authorization work item.
//! This public facade regression belongs to server; core cannot depend on its own facade.

use super::*;
use klaus_agent_core::mcp::client::{McpAuthorizeRequest, McpAuthorizeResponse};
use klaus_agent_core::types::abort::AbortSignal;
use std::sync::Mutex;

type Responses = Arc<Mutex<Vec<McpAuthorizeResponse>>>;

async fn finish_client(mut client: TestClient) {
    client.writer.shutdown().await.unwrap();
    drop(client.writer);
    tokio::time::timeout(REPLY_TIMEOUT, client.server)
        .await
        .expect("the public endpoint must finish teardown")
        .unwrap();
}

async fn waiting_authorization(method: &str) -> (TestClient, String, String, Responses) {
    let agent = make_agent_for_tests(vec![]);
    let responses: Responses = Arc::new(Mutex::new(Vec::new()));
    let captured = responses.clone();
    let create: CreateSession = Arc::new(move |ctx: SessionContext| {
        let agent = agent.clone();
        let captured = captured.clone();
        Box::pin(async move {
            // The real protocol adapter owns this parked response. The browser
            // and HTTP provider are covered separately by the actual stdio gate.
            let response = ctx
                .mcp_authorize_handler
                .authorize(
                    McpAuthorizeRequest {
                        server_name: "fixture".to_string(),
                        authorization_url: "https://auth.example/authorize?state=fixture-state"
                            .to_string(),
                        redirect_uri: "http://127.0.0.1:33418/callback".to_string(),
                    },
                    AbortSignal::new(),
                )
                .await;
            captured.lock().unwrap().push(response);
            Ok::<Arc<Session>, BoxError>(agent.start_session(StartSessionOptions {
                id: ctx.init.thread_id.clone(),
                mcp_authorize_handler: Some(ctx.mcp_authorize_handler),
                ..Default::default()
            }))
        })
    });
    let mut client = TestClient::start(create);
    client.initialize().await;
    client
        .send(json!({"id":2,"method":"session/start","params":{"init":{}}}))
        .await;
    let (started, _) = client.response_for(2).await;
    let session = started["result"]["sessionId"].as_str().unwrap().to_string();
    client
        .send(json!({"id":3,"method":"session/subscribe","params":{"sessionId":session}}))
        .await;
    assert_eq!(client.response_for(3).await.0["result"]["subscribed"], true);
    client.send(json!({"id":4,"method":method,"params":{"sessionId":session,"servers":[],"submissionId":"waiting-operation"}})).await;
    let request = loop {
        let message = client.next_message().await;
        if let Some(request) = message["params"]["event"]["mcpAuthorization"]["requestId"].as_str()
        {
            break request.to_string();
        }
    };
    (client, session, request, responses)
}

async fn resolve(client: &mut TestClient, session: &str, request: &str, code: bool) {
    let response = if code {
        json!({"kind":"code","code":"fixture-secret-code","state":"fixture-state"})
    } else {
        json!({"kind":"decline","reason":"user cancelled"})
    };
    client.send(json!({"id":5,"method":"session/resolveMcpAuthorization","params":{"sessionId":session,"requestId":request,"submissionId":"resolve-authorization","response":response}})).await;
}

#[tokio::test]
async fn mcp_snapshot_and_reload_accept_code_and_decline_while_the_original_request_waits() {
    for method in ["session/mcpRuntime", "session/reloadMcpServers"] {
        for code in [true, false] {
            let (mut client, session, request, responses) = waiting_authorization(method).await;
            resolve(&mut client, &session, &request, code).await;
            let (mut resolved, mut completed) = (false, false);
            while !(resolved && completed) {
                let message = client.next_message().await;
                assert!(
                    !message.to_string().contains("fixture-secret-code"),
                    "authorization code must not be echoed"
                );
                let event = &message["params"]["event"];
                if event["submissionId"] == "resolve-authorization" {
                    assert_eq!(event["mcpAuthorizationResolved"]["requestId"], request);
                    assert_eq!(event["mcpAuthorizationResolved"]["accepted"], code);
                    resolved = true;
                }
                if message["id"] == 4 && message["result"]["mcpRuntime"].is_object()
                    || event["submissionId"] == "waiting-operation"
                        && event["mcpRuntime"].is_object()
                {
                    completed = true;
                }
            }
            {
                let responses = responses.lock().unwrap();
                assert_eq!(responses.len(), 1);
                assert_eq!(
                    matches!(&responses[0], McpAuthorizeResponse::Code { .. }),
                    code
                );
            }
            finish_client(client).await;
        }
    }
}

#[tokio::test]
async fn mcp_interrupt_and_shutdown_decline_the_waiting_authorization() {
    for method in ["turn/interrupt", "session/shutdown"] {
        let (mut client, session, _, responses) = waiting_authorization("session/mcpRuntime").await;
        client
            .send(json!({"id":5,"method":method,"params":{"sessionId":session}}))
            .await;
        let mut control_completed = false;
        while !control_completed || responses.lock().unwrap().is_empty() {
            let message = client.next_message().await;
            if message["id"] == 5 {
                assert!(message.get("error").is_none(), "{message}");
                control_completed = true;
            }
        }
        assert!(matches!(
            &responses.lock().unwrap()[0],
            McpAuthorizeResponse::Decline { .. }
        ));
        finish_client(client).await;
    }
}

#[tokio::test]
async fn mcp_full_ordinary_queue_rejects_overflow_but_still_admits_the_authorization_reply() {
    let (mut client, session, request, _) = waiting_authorization("session/mcpRuntime").await;
    for id in 100..=164 {
        client
            .send(json!({"id":id,"method":"session/history","params":{"sessionId":session}}))
            .await;
    }
    let (overflow, seen) = client.response_for(164).await;
    assert!(
        overflow
            .to_string()
            .contains("protocol request queue is full"),
        "{overflow}"
    );
    assert!(
        seen.iter().all(|row| !row["result"]["history"].is_object()),
        "ordinary operations cannot bypass the pending snapshot"
    );
    resolve(&mut client, &session, &request, true).await;
    let mut histories = std::collections::HashSet::new();
    let mut resolved = false;
    while histories.len() < 64 || !resolved {
        let message = client.next_message().await;
        if let Some(id @ 100..=163) = message["id"].as_u64() {
            assert!(message["result"].get("history").is_some(), "{message}");
            assert!(
                histories.insert(id),
                "one terminal reply per ordinary request"
            );
        }
        if message["params"]["event"]["submissionId"] == "resolve-authorization" {
            assert_eq!(
                message["params"]["event"]["mcpAuthorizationResolved"]["accepted"],
                true
            );
            resolved = true;
        }
    }
    finish_client(client).await;
}
