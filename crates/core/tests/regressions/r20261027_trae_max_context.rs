//! Defect: Max-mode models exposed the smaller dev context through the catalog facade.
//! Root cause: catalog normalization always selected the first dev route and its context.
//! Work item: 20261027 Trae Max context alignment through the public facade.

use super::{make_create_session, TestClient};
use serde_json::{json, Value};
use std::ffi::OsString;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

struct CloudFixture {
    origin: String,
    requests: Arc<Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for CloudFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl CloudFixture {
    async fn start(payload: Value) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let body = payload.to_string();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut received = Vec::new();
                let mut buffer = [0; 4096];
                let target = loop {
                    let count = socket.read(&mut buffer).await.unwrap();
                    assert!(count > 0, "a complete local catalog request");
                    received.extend_from_slice(&buffer[..count]);
                    assert!(received.len() <= 64 * 1024, "bounded catalog headers");
                    let Some(end) = received.windows(4).position(|bytes| bytes == b"\r\n\r\n")
                    else {
                        continue;
                    };
                    break String::from_utf8_lossy(&received[..end])
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap()
                        .to_string();
                };
                captured.lock().unwrap().push(target);
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
                socket.shutdown().await.unwrap();
            }
        });
        Self {
            origin,
            requests,
            task,
        }
    }
}

struct CloudEnvironment(Option<OsString>);

impl CloudEnvironment {
    fn set(origin: &str) -> Self {
        let previous = std::env::var_os("KLAUS_TRAE_CLOUD_URL");
        klaus_agent_core::runtime_process::set_process_environment_var(
            "KLAUS_TRAE_CLOUD_URL",
            origin,
        );
        Self(previous)
    }
}

impl Drop for CloudEnvironment {
    fn drop(&mut self) {
        if let Some(previous) = &self.0 {
            klaus_agent_core::runtime_process::set_process_environment_var(
                "KLAUS_TRAE_CLOUD_URL",
                previous,
            );
        } else {
            klaus_agent_core::runtime_process::remove_process_environment_var(
                "KLAUS_TRAE_CLOUD_URL",
            );
        }
    }
}

#[tokio::test]
async fn catalog_facade_uses_max_context_and_preserves_dev_only_models() {
    let _guard = crate::app_server::trae::test_env_lock().lock().await;
    let profile = tempfile::tempdir().unwrap();
    crate::app_server::trae::set_config_root(profile.path());
    let gateway = CloudFixture::start(json!({"config_info_list":[
        {
            "config_name":"deepseek-v4.1-flash", "config_source":1,
            "config_switch":true, "is_default":true, "usage":"chat_completion",
            "context_window_tokens":{"dev":100000,"max":1000000},
            "display_config":{"display_name":"DeepSeek V4.1 Flash","model_capability":"reasoning_model"},
            "extra_config":"{\"native_function_call\":true}",
            "reasoning_effort_config":{
                "support_thinking":true, "options":["low","high","extra_high"], "default_level":"high"
            },
            "model_detail_list":[
                {"model_name":"deepseek-v4.1-flash__dev","max_tokens":32000,"encrypted_model_params":"fixture-dev-private"},
                {"model_name":"deepseek-v4.1-flash__max","max_tokens":64000,"encrypted_model_params":"fixture-max-private"}
            ]
        },
        {
            "config_name":"doubao-seed-code", "config_source":1,
            "config_switch":true, "usage":"chat_completion",
            "context_window_tokens":{"dev":190000},
            "display_config":{"display_name":"Doubao Seed Code","model_capability":"chat_model"},
            "model_detail_list":[
                {"model_name":"doubao-seed-code__dev","max_tokens":8192,"encrypted_model_params":"fixture-dev-only-private"}
            ]
        }
    ]}))
    .await;
    let _environment = CloudEnvironment::set(&gateway.origin);
    let config = profile.path().join("config.toml");
    std::fs::write(&config, "autoMemory = false\ncheckpointing = false\n").unwrap();
    let mut client = TestClient::start_full(
        make_create_session(&[]),
        None,
        Some(profile.path().join("rules.json")),
        Some(config),
    );
    client.initialize().await;
    client
        .send(json!({
            "id":2, "method":"trae/adoptCredential", "params":{"credential":{
                "accessToken":"fixture-access-token", "account":"Fixture account",
                "userId":"fixture-user", "deviceId":"123456789", "machineId":"fixture-machine",
                "clientVersion":"3.4.7", "region":"cn"
            }}
        }))
        .await;
    let adopted = client.response_for(2).await.0;
    assert_eq!(adopted["result"]["signedIn"], true, "{adopted}");

    client
        .send(json!({"id":3,"method":"trae/models","params":{"read":"warm"}}))
        .await;
    let response = client.response_for(3).await.0;
    assert!(response.get("error").is_none(), "{response}");
    let models = response["result"]["models"].as_array().unwrap();
    assert_eq!(models.len(), 2, "{models:?}");
    let max = models
        .iter()
        .find(|row| row["key"] == "deepseek-v4.1-flash")
        .unwrap();
    assert_eq!(max["contextWindow"], 1000000);
    assert_eq!(max["capabilities"]["contextWindow"], 1000000);
    assert_eq!(max["maxOutputTokens"], 64000);
    assert_eq!(
        max["providerMetadata"]["wireModelName"],
        "deepseek-v4.1-flash__max"
    );
    assert_eq!(max["providerMetadata"]["nativeDefaultEffort"], "high");
    let variants = max["traeVariants"].as_array().unwrap();
    assert!(variants
        .iter()
        .all(|variant| { variant["contextTokens"] == 1000000 }));
    assert!(variants.iter().any(|variant| {
        variant["value"] == "trae:deepseek-v4.1-flash::effort=extra_high"
            && variant["nativeEffort"] == "extra_high"
    }));

    let dev_only = models
        .iter()
        .find(|row| row["key"] == "doubao-seed-code")
        .unwrap();
    assert_eq!(dev_only["contextWindow"], 190000);
    assert_eq!(dev_only["capabilities"]["contextWindow"], 190000);
    assert_eq!(dev_only["maxOutputTokens"], 8192);
    assert_eq!(
        dev_only["providerMetadata"]["wireModelName"],
        "doubao-seed-code__dev"
    );
    let serialized = response.to_string();
    for private in [
        "encrypted_model_params",
        "fixture-dev-private",
        "fixture-max-private",
        "fixture-dev-only-private",
    ] {
        assert!(
            !serialized.contains(private),
            "public facade leaked {private}"
        );
    }
    assert_eq!(
        gateway.requests.lock().unwrap().as_slice(),
        ["/api/ide/v1/get_detail_param?function=chat_v3&need_prompt=false"]
    );
    client.close().await;
}
