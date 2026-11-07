//! Browser login must retain its selected account site through token completion.
use super::*;
use bytes::Bytes;
use http_body_util::Full;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};

struct LoginGateway {
    base: String,
    requests: Arc<Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
    previous_base: Option<std::ffi::OsString>,
}

impl LoginGateway {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let task = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let captured = Arc::clone(&captured);
                let service = hyper::service::service_fn(
                    move |request: Request<hyper::body::Incoming>| {
                        let uri = request.uri().to_string();
                        let mut requests = captured.lock().unwrap();
                        assert!(requests.len() < 16, "login fixture request limit");
                        requests.push(format!("{} {uri}", request.method()));
                        let body = match request.uri().path() {
                            AUTH_STATE_PATH => {
                                let platform = request
                                    .uri()
                                    .query()
                                    .unwrap()
                                    .strip_prefix("platform=")
                                    .unwrap();
                                json!({ "data": { "state": format!("state-{platform}"), "authUrl": format!("https://fixture.invalid/login?platform={platform}") } })
                            }
                            AUTH_TOKEN_PATH => {
                                json!({ "data": { "accessToken": "fixture-token" } })
                            }
                            ACCOUNTS_PATH => {
                                json!({ "data": { "accounts": [{ "uid": "fixture-user", "nickname": "Regional user" }] } })
                            }
                            "/v3/config" => {
                                json!({ "models": [{ "key": "fixture-model", "enable": true, "isDefault": true }] })
                            }
                            other => panic!("unexpected login route: {other}"),
                        };
                        async move {
                            Ok::<_, Infallible>(
                                Response::builder()
                                    .header("Content-Type", "application/json")
                                    .header("Connection", "close")
                                    .body(Full::new(Bytes::from(body.to_string())))
                                    .unwrap(),
                            )
                        }
                    },
                );
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(socket), service)
                    .await;
            }
        });
        let previous_base = std::env::var_os("WORKBUDDY_API_BASE");
        Self {
            base,
            requests,
            task,
            previous_base,
        }
    }

    fn use_override(&self) {
        klaus_agent_core::runtime_process::set_process_environment_var(
            "WORKBUDDY_API_BASE",
            &self.base,
        );
    }

    fn clear_override(&self) {
        klaus_agent_core::runtime_process::remove_process_environment_var("WORKBUDDY_API_BASE");
    }
}

impl Drop for LoginGateway {
    fn drop(&mut self) {
        self.task.abort();
        match &self.previous_base {
            Some(value) => klaus_agent_core::runtime_process::set_process_environment_var(
                "WORKBUDDY_API_BASE",
                value,
            ),
            None => klaus_agent_core::runtime_process::remove_process_environment_var(
                "WORKBUDDY_API_BASE",
            ),
        }
    }
}

#[tokio::test]
async fn explicit_login_sites_override_a_previous_accounts_site() {
    let _lock = super::super::test_env_lock().lock().await;
    let root = tempfile::tempdir().unwrap();
    super::super::set_config_root(root.path());
    let gateway = LoginGateway::start().await;
    gateway.clear_override();
    assert_eq!(
        login_target(None),
        (DEFAULT_API_BASE.into(), DEFAULT_PLATFORM.into())
    );
    adopt_credential(json!({ "accessToken": "old-token", "domain": "www.codebuddy.cn", "platform": "workbuddy" }), None).unwrap();
    assert_eq!(
        login_target(Some(WorkBuddyRegion::Intl)),
        ("https://www.workbuddy.ai".into(), "workbuddy-ai".into())
    );
    adopt_credential(json!({ "accessToken": "old-token", "domain": "www.workbuddy.ai", "platform": "workbuddy-ai" }), None).unwrap();
    assert_eq!(
        login_target(Some(WorkBuddyRegion::Cn)),
        ("https://www.codebuddy.cn".into(), "workbuddy".into())
    );
    assert!(
        serde_json::from_value::<super::super::super::wire::WorkBuddyLoginStartParams>(
            json!({"region":"unknown"})
        )
        .is_err()
    );
}

#[tokio::test]
async fn regional_browser_login_keeps_its_gateway_and_platform_when_tokens_omit_domain() {
    let _lock = super::super::test_env_lock().lock().await;
    let root = tempfile::tempdir().unwrap();
    super::super::set_config_root(root.path());
    let gateway = LoginGateway::start().await;
    for (region, platform) in [
        (WorkBuddyRegion::Cn, "workbuddy"),
        (WorkBuddyRegion::Intl, "workbuddy-ai"),
    ] {
        gateway.use_override();
        let flow = start(Some(region)).await.unwrap();
        assert_eq!(flow["ok"], true);
        assert!(flow["authUrl"].as_str().unwrap().ends_with(platform));
        // A different account may be adopted while the browser page is open.
        // This flow must still finish against the site and platform it started with.
        adopt_credential(json!({"accessToken":"other-token", "domain":"https://other.invalid", "platform":"other-platform"}), None).unwrap();
        gateway.clear_override();
        let result = poll(&flow).await.unwrap();
        assert_eq!(result["ok"], true, "{result}");
        assert_eq!(api_base(), gateway.base);
        let (_, stored) = credential().unwrap();
        assert_eq!(stored["platform"], platform);
        assert_eq!(stored["domain"], gateway.base);
        assert_eq!(stored["login_method"], "browser");
        assert_eq!(
            status()["region"],
            if platform == "workbuddy" {
                "cn"
            } else {
                "intl"
            }
        );
        assert_eq!(status()["loginMethod"], "browser");
        let (provider, _) = super::super::resolve("fixture-model").await.unwrap();
        assert_eq!(provider["baseUrl"], format!("{}/v2", gateway.base));
    }
    let requests = gateway.requests.lock().unwrap();
    assert_eq!(
        requests.as_slice(),
        [
            "POST /v2/plugin/auth/state?platform=workbuddy",
            "GET /v2/plugin/auth/token?state=state-workbuddy",
            "GET /console/accounts",
            "GET /v3/config",
            "POST /v2/plugin/auth/state?platform=workbuddy-ai",
            "GET /v2/plugin/auth/token?state=state-workbuddy-ai",
            "GET /console/accounts",
            "GET /v3/config",
        ]
    );
}

#[tokio::test]
async fn imported_account_status_identifies_its_edition_after_reopening_the_store() {
    let _lock = super::super::test_env_lock().lock().await;
    let root = tempfile::tempdir().unwrap();
    super::super::set_config_root(root.path());
    for (profile, expected) in [
        (json!({"domain":"www.codebuddy.cn"}), "cn"),
        (json!({"domain":"https://www.workbuddy.ai/"}), "intl"),
        (json!({"platform":"workbuddy"}), "cn"),
        (json!({"platform":"workbuddy-ai"}), "intl"),
        (json!({"region":"cn"}), "cn"),
    ] {
        let mut profile = profile;
        profile["accessToken"] = json!("fixture-token");
        adopt_credential(profile, None).unwrap();
        let account = status();
        assert_eq!(account["region"], expected);
        assert_eq!(account["loginMethod"], "adopted");
    }
    adopt_credential(
        json!({"accessToken":"fixture-token","domain":"unknown.invalid"}),
        None,
    )
    .unwrap();
    assert!(status().get("region").is_none());
}
