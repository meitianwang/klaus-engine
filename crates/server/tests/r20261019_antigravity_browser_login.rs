//! Direct browser grants have one named facade and connection-owned callbacks.
//! Work item: 20261019 account login; no model or remote OAuth request is sent.
use super::{make_create_session, TestClient};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn antigravity_browser_login_facade_cancels_and_releases_owned_callbacks() {
    let root = tempfile::tempdir().unwrap();
    let mut client = TestClient::start_full(
        make_create_session(&[]),
        None,
        None,
        Some(root.path().join("config.json")),
    );
    client.initialize().await;
    client
        .send(json!({"id":2,"method":"antigravity/loginStart","params":{}}))
        .await;
    let (start, _) = client.response_for(2).await;
    let result = &start["result"];
    assert_eq!(result["ok"], true);
    let flow = result["flowId"].as_str().unwrap().to_string();
    let url = url::Url::parse(result["authUrl"].as_str().unwrap()).unwrap();
    assert_eq!(url.host_str(), Some("accounts.google.com"));
    assert_eq!(url.path(), "/o/oauth2/auth");
    let redirect = url
        .query_pairs()
        .find(|(key, _)| key == "redirect_uri")
        .unwrap()
        .1
        .into_owned();
    client
        .send(json!({"id":3,"method":"antigravity/loginPoll","params":{"flow":{"flowId":flow}}}))
        .await;
    assert_eq!(client.response_for(3).await.0["result"]["pending"], true);
    client
        .send(json!({"id":4,"method":"antigravity/loginCancel","params":{"flow":{"flowId":flow}}}))
        .await;
    assert_eq!(client.response_for(4).await.0["result"]["cancelled"], true);
    client
        .send(json!({"id":5,"method":"antigravity/loginPoll","params":{"flow":{"flowId":flow}}}))
        .await;
    assert_eq!(
        client.response_for(5).await.0["result"]["error"],
        "antigravity_login_cancelled"
    );
    client.send(json!({"id":6,"method":"antigravity/loginStart","params":{"redirectUri":"https://evil.invalid"}})).await;
    assert!(client.response_for(6).await.0.get("error").is_some());
    client
        .send(json!({"id":7,"method":"antigravity/loginStart","params":{}}))
        .await;
    let (start, _) = client.response_for(7).await;
    let current = url::Url::parse(start["result"]["authUrl"].as_str().unwrap()).unwrap();
    let current_redirect = current
        .query_pairs()
        .find(|(key, _)| key == "redirect_uri")
        .unwrap()
        .1
        .into_owned();
    client.close().await;
    for redirect in [redirect, current_redirect] {
        let redirect = url::Url::parse(&redirect).unwrap();
        let port = redirect.port().unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if tokio::net::TcpStream::connect(("127.0.0.1", port))
                    .await
                    .is_err()
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled and disconnected listeners close promptly");
    }
    assert!(!root.path().join("antigravity-auth.json").exists());
}

#[tokio::test]
async fn aborting_the_facade_future_fences_browser_authorization() {
    let root = tempfile::tempdir().unwrap();
    let mut client = TestClient::start_full(
        make_create_session(&[]),
        None,
        None,
        Some(root.path().join("config.json")),
    );
    client.initialize().await;
    client
        .send(json!({"id":2,"method":"antigravity/loginStart","params":{}}))
        .await;
    let (start, _) = client.response_for(2).await;
    assert_eq!(start["result"]["ok"], true);
    let url = url::Url::parse(start["result"]["authUrl"].as_str().unwrap()).unwrap();
    let redirect = url
        .query_pairs()
        .find(|(key, _)| key == "redirect_uri")
        .unwrap()
        .1
        .into_owned();
    let port = url::Url::parse(&redirect).unwrap().port().unwrap();
    client.server.abort();
    assert!((&mut client.server).await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_err()
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("an aborted facade cannot retain an authorization callback");
    assert!(!root.path().join("antigravity-auth.json").exists());
}
