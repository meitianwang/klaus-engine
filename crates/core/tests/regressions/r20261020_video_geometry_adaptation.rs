//! Defect: valid generic video dimensions failed before a render could start.
//! Root cause: generic geometry was treated as a provider-specific parameter.
//! Work item: 20261020 adapt geometry inside the named video facade.

use super::*;
use crate::app_server::{qwenwork, workbuddy};
use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper_util::rt::TokioIo;
use std::sync::Mutex;

struct WorkBuddyEnvironment {
    previous_root: std::path::PathBuf,
    previous_base: Option<std::ffi::OsString>,
}

impl Drop for WorkBuddyEnvironment {
    fn drop(&mut self) {
        workbuddy::set_config_root(&self.previous_root);
        if let Some(previous) = &self.previous_base {
            klaus_agent_core::runtime_process::set_process_environment_var(
                "WORKBUDDY_API_BASE",
                previous,
            );
        } else {
            klaus_agent_core::runtime_process::remove_process_environment_var("WORKBUDDY_API_BASE");
        }
    }
}

struct VideoFixture {
    origin: String,
    requests: Arc<Mutex<Vec<(String, Value)>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for VideoFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn video_fixture() -> VideoFixture {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = requests.clone();
    let task = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let captured = captured.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(
                    move |request: hyper::Request<hyper::body::Incoming>| {
                        let captured = captured.clone();
                        async move {
                            let path = request.uri().path().to_owned();
                            let bytes = Limited::new(request.into_body(), 65536)
                                .collect()
                                .await
                                .unwrap()
                                .to_bytes();
                            let body = if bytes.is_empty() {
                                Value::Null
                            } else {
                                serde_json::from_slice(&bytes).unwrap()
                            };
                            captured.lock().unwrap().push((path.clone(), body));
                            let value = match path.as_str() {
                                "/v3/config" => {
                                    json!({"code":0,"data":{"models":[{"id":"quality-video","tags":["text-to-video","image-to-video"],"videoGeneration":{"resolutions":["standard","professional"],"defaultResolution":"professional","minSeconds":4,"maxSeconds":10,"defaultSeconds":5}}],"productFeatures":{"VideoGen":true,"DisableMultimodalGeneration":false}}})
                                }
                                "/v2/videos/generations" => {
                                    json!({"code":0,"data":{"id":"task-1"}})
                                }
                                // Stop after observing the normalized submit, without a network artifact.
                                "/v2/videos/tasks" => json!({"code":0,"data":{"status":"failed"}}),
                                _ => panic!("unexpected fixture route: {path}"),
                            };
                            Ok::<_, std::convert::Infallible>(hyper::Response::new(Full::new(
                                Bytes::from(value.to_string()),
                            )))
                        }
                    },
                );
                hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(socket), service)
                    .await
                    .unwrap();
            });
        }
    });
    VideoFixture {
        origin,
        requests,
        task,
    }
}

#[tokio::test]
async fn named_video_facade_adapts_quality_catalog_dimensions_without_retrying_or_changing_duration(
) {
    let _guard = workbuddy::test_env_lock().lock().await;
    let root = tempfile::tempdir().unwrap();
    let native = video_fixture().await;
    let _environment = WorkBuddyEnvironment {
        previous_root: workbuddy::config_root(),
        previous_base: std::env::var_os("WORKBUDDY_API_BASE"),
    };
    klaus_agent_core::runtime_process::set_process_environment_var(
        "WORKBUDDY_API_BASE",
        &native.origin,
    );
    workbuddy::set_config_root(root.path());
    std::fs::write(root.path().join("workbuddy-subscription-oauth.json"), json!({"deviceId":"fixture-device","userInfo":{"uid":"fixture-user","access_token":"fixture-token","domain":native.origin,"login_method":"adopted","expire_time":4102444800_i64}}).to_string()).unwrap();
    let config = root.path().join("config.toml");
    std::fs::write(&config, "").unwrap();
    let mut client = TestClient::start_full(
        make_create_session(&[]),
        None,
        Some(root.path().join("rules.json")),
        Some(config),
    );
    client.initialize().await;
    for (id, references) in [
        (920, json!([])),
        (
            921,
            json!([{"dataBase64":STANDARD.encode(b"\x89PNG\r\n\x1a\nreference"),"mimeType":"image/png"}]),
        ),
    ] {
        client.send(json!({"id":id,"method":"workbuddy/generateVideo","params":{"prompt":"A moving circle","model":"quality-video","seconds":5,"size":"1280x720","referenceImages":references}})).await;
        let reply = client.response_for(id).await.0;
        assert_eq!(
            reply["error"]["message"], "workbuddy_video_failed",
            "{reply}"
        );
    }
    client.send(json!({"id":922,"method":"workbuddy/generateVideo","params":{"prompt":"A moving circle","model":"quality-video","seconds":3,"size":"1280x720"}})).await;
    assert_eq!(
        client.response_for(922).await.0["error"]["message"],
        "workbuddy_video_duration_unsupported"
    );
    let submits: Vec<_> = native
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|(path, _)| path == "/v2/videos/generations")
        .map(|(_, body)| body.clone())
        .collect();
    assert_eq!(
        submits.len(),
        2,
        "one submission per valid facade operation"
    );
    for body in &submits {
        assert_eq!(body["model"], "quality-video");
        assert_eq!(body["seconds"], 5);
        assert_eq!(body["extra_parameters"]["resolution"], "professional");
    }
    assert_eq!(submits[0]["extra_parameters"]["aspect_ratio"], "16:9");
    assert!(submits[1]["extra_parameters"].get("aspect_ratio").is_none());
    assert!(submits[1]["image_url"]
        .as_str()
        .unwrap()
        .starts_with("data:image/png;base64,"));
    client.writer.shutdown().await.unwrap();
    client.close().await;
}

#[tokio::test]
async fn named_resolution_only_video_facade_adapts_dimensions_in_one_submission() {
    let _guard = qwenwork::test_env_lock().lock().await;
    let root = tempfile::tempdir().unwrap();
    let native = qwenwork::capabilities::tests::fixture().await;
    let _environment = qwenwork::capabilities::tests::setup(&native, root.path());
    let config = root.path().join("config.toml");
    std::fs::write(&config, "").unwrap();
    let mut client = TestClient::start_full(
        make_create_session(&[]),
        None,
        Some(root.path().join("rules.json")),
        Some(config),
    );
    client.initialize().await;
    client.send(json!({"id":923,"method":"qwenwork/generateVideo","params":{"prompt":"A moving circle","model":"happy-horse-1.0","seconds":5,"size":"1280x720"}})).await;
    let reply = client.response_for(923).await.0;
    assert_eq!(reply["result"]["mimeType"], "video/mp4", "{reply}");
    let submits: Vec<_> = native
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|(path, _)| path.ends_with("/generation"))
        .map(|(_, body)| body.clone())
        .collect();
    assert_eq!(
        submits,
        vec![json!({"prompt":"A moving circle","duration":5,"resolution":"720P"})]
    );
    client.writer.shutdown().await.unwrap();
    client.close().await;
}
