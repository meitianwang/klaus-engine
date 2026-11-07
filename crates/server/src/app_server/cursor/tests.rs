use super::*;
use serde_json::{json, Value};

#[path = "../../../tests/r20261022_cursor_history_budget.rs"]
mod history_budget_regression;

#[test]
fn native_tool_mismatch_diagnostics_expose_only_fixed_fields_types_and_equality() {
    let actual = json!({"name":"private-name","args":{"private-argument":"private-value"},"toolCallId":"private-id","providerIdentifier":"private-provider","toolName":"private-tool","serverIdentifier":"private-server","skipApproval":false,"path":"/private/path","signature":"private-signature","private-key":"private-value"});
    let expected = json!({"name":"private-name","args":{"private-argument":"private-value"},"toolCallId":"private-id","providerIdentifier":"private-provider","toolName":"private-tool","serverIdentifier":"private-server","path":"/private/path"});
    let detail = super::tool_mismatch_details(&actual, &expected);
    let encoded = detail.to_string();
    for secret in [
        "private-name",
        "private-argument",
        "private-value",
        "private-id",
        "private-provider",
        "private-tool",
        "private-server",
        "/private/path",
        "private-signature",
        "private-key",
    ] {
        assert!(!encoded.contains(secret));
    }
    assert!(encoded.len() < 4096);
    assert_eq!(detail["knownOuter"], "other");
    assert_eq!(detail["actualUnknownFieldCount"], 2);
    assert!(detail["fields"]
        .as_array()
        .unwrap()
        .iter()
        .any(|field| field["field"] == "args" && field["equal"] == true));
    assert!(detail["fields"]
        .as_array()
        .unwrap()
        .iter()
        .any(|field| field["field"] == "skipApproval"
            && field["actualType"] == "boolean"
            && field["expectedType"] == "missing"
            && field["equal"] == false));
    assert_eq!(
        super::tool_mismatch("fixture", &actual, &expected),
        "cursor_native_tool_mismatch"
    );
}

#[tokio::test]
async fn client_identity_is_private_optional_and_required_only_for_native_capabilities() {
    let _guard = test_lock().lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    set_config_root(temp.path());
    assert_eq!(
        adopt_credential(json!({"accessToken":"active"}))["ok"],
        true
    );
    assert_eq!(status()["loggedIn"], true);
    assert_eq!(status()["loginExpired"], false);
    assert_eq!(status()["needsReimport"], true);
    assert_eq!(status()["reason"], credential::METADATA_MISSING);
    let (without, _) = credential::snapshot().expect("legacy credential remains valid");
    assert_eq!(
        without.checksum(0).unwrap_err(),
        credential::METADATA_MISSING
    );
    for identity in ["", "a/b/c", "a\nb", "/b", "a/", "a b"] {
        assert_eq!(
            adopt_credential(json!({"accessToken":"active","clientIdentity":identity}))["ok"],
            false
        );
    }
    assert_eq!(
        adopt_credential(json!({"accessToken":"active","clientIdentity":"machine-id/mac-id"}))
            ["ok"],
        true
    );
    assert_eq!(status()["needsReimport"], false);
    assert!(!status().to_string().contains("machine-id"));
    let (with, _) = credential::snapshot().expect("credential");
    let checksum = with.checksum(0).expect("checksum");
    assert_eq!(checksum.to_str().unwrap(), "paaoq6-0machine-id/mac-id");
    assert_eq!(
        with.checksum(1_720_000_000_000).unwrap().to_str().unwrap(),
        "m1xeR33Cmachine-id/mac-id"
    );
    assert!(checksum.is_sensitive());
    assert_ne!(with.checksum(0).unwrap(), with.checksum(1_000_000).unwrap());
}

#[test]
fn model_transport_reuses_the_native_grant_and_selected_team_without_machine_metadata() {
    let credential: credential::Credential = serde_json::from_value(json!({
        "accessToken":"fixture-native-grant", "teamId":7
    }))
    .unwrap();
    let request = credential::sdk_authenticated(
        reqwest::Client::new().post("https://api2.cursor.sh/agent.v1.AgentService/Run"),
        &credential,
    )
    .build()
    .unwrap();
    assert_eq!(
        request.headers()["authorization"],
        "Bearer fixture-native-grant"
    );
    assert!(request.headers()["authorization"].is_sensitive());
    assert_eq!(request.headers()["x-cursor-client-type"], "sdk");
    assert_eq!(request.headers()["x-cursor-team-id"], "7");
    assert!(!request.headers().contains_key("x-cursor-checksum"));
    assert!(!request.headers().contains_key("cookie"));
    assert_eq!(credential.access_token, "fixture-native-grant");
}

#[tokio::test]
async fn credentials_are_owned_atomic_and_invalid_adoption_preserves_them() {
    let _guard = test_lock().lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    set_config_root(temp.path());
    assert_eq!(status(), json!({"loggedIn":false}));
    let first = adopt_credential(json!({"accessToken":"fixture-access","account":"Fixture"}));
    assert_eq!(first["ok"], true);
    assert_eq!(status()["loggedIn"], true);
    assert!(!status().to_string().contains("fixture-access"));
    assert_eq!(
        adopt_credential(json!({"accessToken":"invalid", "refreshToken":"must-stay-local"}))["ok"],
        false
    );
    assert_eq!(status()["credentialId"], first["credentialId"]);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(temp.path().join("cursor-subscription-oauth.json"))
                .expect("store")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    let second = adopt_credential(json!({"accessToken":"new-access"}));
    assert_ne!(first["credentialId"], second["credentialId"]);
    assert_eq!(
        logout(first["credentialId"].as_str()).expect("conditional logout")["removed"],
        false
    );
    assert_eq!(status()["credentialId"], second["credentialId"]);
    logout(second["credentialId"].as_str()).expect("logout");
    assert_eq!(status(), json!({"loggedIn":false}));
}

#[tokio::test]
async fn expired_and_rejected_snapshots_need_reimport_without_refresh() {
    let _guard = test_lock().lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    set_config_root(temp.path());
    assert_eq!(
        adopt_credential(json!({"accessToken":"expired","expiresAt":1}))["error"],
        credential::NEEDS_REIMPORT
    );
    let adoption = adopt_credential(json!({"accessToken":"active"}));
    assert_eq!(adoption["ok"], true);
    let (_, generation) = credential::snapshot().expect("snapshot");
    let next = adopt_credential(json!({"accessToken":"newer"}));
    let _ = credential::mark_rejected(generation);
    assert_eq!(
        status()["loggedIn"],
        true,
        "old request cannot reject a newer credential"
    );
    assert_eq!(status()["credentialId"], next["credentialId"]);
    let (_, generation) = credential::snapshot().expect("snapshot");
    let _ = credential::mark_rejected(generation);
    assert_eq!(status()["needsReimport"], true);
    assert_eq!(status()["reason"], credential::NEEDS_REIMPORT);
    assert!(status().get("error").is_none());
}

#[tokio::test]
async fn status_keeps_corrupt_credential_errors_explicit() {
    let _guard = test_lock().lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    set_config_root(temp.path());
    std::fs::write(
        temp.path().join("cursor-subscription-oauth.json"),
        "invalid",
    )
    .expect("write corrupt store");
    assert_eq!(
        status(),
        json!({"loggedIn":false,"error":"cursor_client_credentials_invalid"})
    );
    let invalid_root = temp.path().join("not-a-directory");
    std::fs::write(&invalid_root, "fixture").expect("write invalid root");
    set_config_root(&invalid_root);
    assert_eq!(
        status(),
        json!({"loggedIn":false,"error":"cursor_credential_read_failed"})
    );
}

#[test]
fn catalog_keeps_real_variants_without_agent_eligibility_filtering() {
    let models = catalog::normalize(&json!({"models":[
        {"name":"default","clientDisplayName":"Auto","variants":[{}]},
        {"name":"auto-smart","clientDisplayName":"Automatic"},
        {"name":"fixture","serverModelName":"fixture-wire","clientDisplayName":"Fixture","supportsAgent":false,"contextTokenLimit":200000,"variants":[
            {"displayName":"Fixture <span>High Fast\u{200b}</span>","parameterValues":[{"id":"fast","value":"true"},{"id":"effort","value":"high"},{"id":"context","value":"200k"}],"legacySlug":"fixture-high","isDefaultNonMaxConfig":true},
            {"displayName":"Fixture <span>High Fast</span>","isMaxMode":true,"parameterValues":[{"id":"fast","value":"true"},{"id":"effort","value":"high"},{"id":"context","value":"1m"}],"legacySlug":"fixture-high"}
        ]}
    ]}));
    assert_eq!(models.len(), 2);
    assert_ne!(models[0]["key"], models[1]["key"]);
    assert_eq!(models[0]["display_name"], "Fixture High Fast Max");
    assert_eq!(models[0]["max_input_tokens"], 1000000);
    assert_eq!(models[0]["requestedModel"]["maxMode"], true);
    assert_eq!(models[0]["isDefault"], true);
    assert_eq!(models[1]["display_name"], "Fixture High Fast");
    assert_eq!(models[1]["isDefault"], false);
    let short = models
        .iter()
        .find(|model| model["requestedModel"]["maxMode"] == false)
        .expect("short context variant");
    assert_eq!(
        catalog::with_max_context(&models, short)["key"],
        models[0]["key"]
    );
    assert_eq!(models[0]["requestedModel"]["modelId"], "fixture-wire");
    assert_eq!(models[0]["is_reasoning"], true);
}

#[test]
fn connect_decoder_preserves_fragmented_terminal_errors_and_bounds_frames() {
    let mut bytes = bridge::encode_frame(json!({"heartbeat":{}}))
        .expect("frame")
        .to_vec();
    let error = json!({"error":{"code":"permission_denied","message":"InferenceService.RunInference is not enabled for this account"}});
    let mut terminal = bridge::encode_frame(error.clone()).expect("frame").to_vec();
    terminal[0] = 2;
    bytes.extend(terminal);
    let mut decoder = bridge::Decoder::default();
    let mut frames = Vec::new();
    for chunk in bytes.chunks(3) {
        frames.extend(decoder.push(chunk).expect("decode"));
    }
    assert_eq!(
        frames,
        vec![(false, json!({"heartbeat":{}})), (true, error.clone())]
    );
    assert!(decoder.is_empty());
    assert_eq!(
        credential::upstream_error(&error),
        "cursor_inference_not_enabled"
    );
    assert!(decoder.push(&[0, 255, 255, 255, 255]).is_err());
}

pub(crate) fn age_catalog(age: std::time::Duration) {
    let mut state = super::state().lock().unwrap_or_else(|p| p.into_inner());
    state.catalog.as_mut().expect("fixture catalog").0 = std::time::Instant::now() - age;
}

pub(crate) fn forget_catalog_memory() {
    let mut state = super::state().lock().unwrap_or_else(|p| p.into_inner());
    state.catalog = None;
    state.catalog_refreshing = false;
}

pub(crate) fn start_browser_login(origin: &str) -> Value {
    super::auth::start_at(None, origin)
}

fn fixture_catalog() -> Value {
    json!({"models":[{"name":"fixture-model","clientDisplayName":"Fixture","supportsAgent":true,"supportsImage":true,"contextTokenLimit":200000,"variants":[{"displayName":"Fixture High Fast","legacySlug":"fixture-high-fast","isDefaultNonMaxConfig":true,"parameterValues":[{"id":"effort","value":"high"},{"id":"fast","value":"true"}]}]}]})
}

pub(crate) async fn unary_fixture() -> (String, tokio::task::JoinHandle<()>) {
    use bytes::Bytes;
    use http_body_util::Full;
    use hyper::Response;
    use hyper_util::rt::TokioIo;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let origin = format!("http://{}", listener.local_addr().expect("address"));
    let task = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.expect("accept");
            tokio::spawn(async move {
                let service = hyper::service::service_fn(
                    move |request: hyper::Request<hyper::body::Incoming>| async move {
                        use http_body_util::BodyExt;
                        let path = request.uri().path().to_owned();
                        let bytes = http_body_util::Limited::new(request.into_body(), 65_536)
                            .collect()
                            .await
                            .expect("bounded request")
                            .to_bytes();
                        serde_json::from_slice::<Value>(&bytes).expect("request JSON");
                        let value = if path.ends_with("AvailableModels") {
                            fixture_catalog()
                        } else if path.ends_with("GetMe") {
                            json!({"email":"fixture@example.invalid"})
                        } else {
                            assert!(
                                path.ends_with("GetCurrentPeriodUsage"),
                                "unexpected account operation"
                            );
                            json!({"enabled":true,"billingCycleStart":"2026-10-01T00:00:00Z","billingCycleEnd":"2026-11-01T00:00:00Z","planUsage":{"totalPercentUsed":42.5,"apiPercentUsed":40.5,"autoPercentUsed":2},"spendLimitUsage":{"limitType":"limited"}})
                        };
                        Ok::<_, std::convert::Infallible>(Response::new(Full::new(Bytes::from(
                            value.to_string(),
                        ))))
                    },
                );
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(socket), service)
                    .await;
            });
        }
    });
    (origin, task)
}

#[test]
fn upstream_failures_never_disclose_echoed_credentials_or_request_text() {
    for code in [
        "permission_denied",
        "resource_exhausted",
        "private-token-in-code",
    ] {
        let error = credential::upstream_error(
            &json!({"error":{"code":code,"message":"Bearer private-token and private prompt"}}),
        );
        assert!(!error.contains("private"));
        assert!(!error.contains("Bearer"));
    }
}

fn adopt_credential(user_info: Value) -> Value {
    super::adopt_credential(user_info, None)
}

#[tokio::test]
async fn guarded_reimport_cannot_resurrect_or_replace_a_newer_revision() {
    let _guard = test_lock().lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    set_config_root(temp.path());
    let first = adopt_credential(json!({"accessToken":"first"}));
    logout(None).expect("logout");
    let stale = super::adopt_credential(
        json!({"accessToken":"stale"}),
        first["credentialId"].as_str(),
    );
    assert_eq!(stale["error"], "cursor_credentials_changed");
    assert_eq!(status()["loggedIn"], false);
    let current = adopt_credential(json!({"accessToken":"new"}));
    let stale = super::adopt_credential(
        json!({"accessToken":"stale"}),
        first["credentialId"].as_str(),
    );
    assert_eq!(stale["error"], "cursor_credentials_changed");
    assert_eq!(status()["credentialId"], current["credentialId"]);
    assert_eq!(
        super::adopt_credential(
            json!({"accessToken":"renewed"}),
            current["credentialId"].as_str()
        )["ok"],
        true
    );
}

#[test]
fn http_and_connect_capacity_errors_retain_safe_classifications() {
    assert_eq!(credential::http_error(429, None), "cursor_rate_limit");
    assert_eq!(credential::http_error(503, None), "cursor_overloaded");
    assert_eq!(
        credential::http_error(502, Some(&json!({"message":"private token"}))),
        "cursor_overloaded"
    );
    assert_eq!(
        credential::http_error(401, None),
        credential::NEEDS_REIMPORT
    );
    assert_eq!(
        credential::http_error(403, None),
        "cursor_permission_denied"
    );
    assert_eq!(
        credential::upstream_error(&json!({"code":"unavailable","message":"private"})),
        "cursor_overloaded"
    );
    assert_eq!(
        credential::upstream_error(&json!({"code":14,"message":"private"})),
        "cursor_overloaded"
    );
}

#[test]
fn malformed_messages_allow_bounded_recovery_but_integrity_failures_remain_terminal() {
    let error = responses::public_error("cursor_native_message_invalid");
    assert_eq!(error["error"]["type"], "overloaded_error");
    assert_eq!(error["message"], "cursor_model_response_unsupported");
    for guard in [
        "cursor_checkpoint_history_changed",
        "cursor_tool_not_owned",
        "cursor_native_response_mismatch",
        "cursor_tool_batch_duplicate",
        "cursor_native_message_limit",
        "cursor_metadata_not_owned",
        "cursor_metadata_call_mismatch",
    ] {
        let error = responses::public_error(guard);
        assert_eq!(error["error"]["type"], "invalid_request_error", "{guard}");
    }
}

pub(crate) async fn invalid_metadata_fixture(
    helper: bool,
    persistent: bool,
) -> (
    String,
    std::sync::Arc<std::sync::Mutex<Vec<Value>>>,
    tokio::task::JoinHandle<()>,
) {
    model_step_fixture(
        String::new(),
        match (helper, persistent) {
            (false, false) => "terminal-metadata-native-once",
            (false, true) => "terminal-metadata-native-always",
            (true, false) => "terminal-metadata-helper-once",
            (true, true) => "terminal-metadata-helper-always",
        },
    )
    .await
}

pub(crate) async fn default_inventory_fixture(
    read_path: String,
) -> (
    String,
    std::sync::Arc<std::sync::Mutex<Vec<Value>>>,
    tokio::task::JoinHandle<()>,
) {
    model_step_fixture(read_path, "normal").await
}

pub(crate) async fn metadata_inventory_fixture(
    read_path: String,
) -> (
    String,
    std::sync::Arc<std::sync::Mutex<Vec<Value>>>,
    tokio::task::JoinHandle<()>,
) {
    model_step_fixture(read_path, "metadata").await
}

pub(crate) async fn delayed_checkpoint_inventory_fixture(
    read_path: String,
) -> (
    String,
    std::sync::Arc<std::sync::Mutex<Vec<Value>>>,
    tokio::task::JoinHandle<()>,
) {
    model_step_fixture(read_path, "delayed-checkpoint").await
}

pub(crate) async fn permission_inventory_fixture(
    path: String,
) -> (
    String,
    std::sync::Arc<std::sync::Mutex<Vec<Value>>>,
    tokio::task::JoinHandle<()>,
) {
    model_step_fixture(path, "permission").await
}

pub(crate) async fn image_inventory_fixture() -> (
    String,
    std::sync::Arc<std::sync::Mutex<Vec<Value>>>,
    tokio::task::JoinHandle<()>,
) {
    model_step_fixture(String::new(), "image").await
}

pub(crate) async fn malformed_message_fixture(
    persistent: bool,
) -> (
    String,
    std::sync::Arc<std::sync::Mutex<Vec<Value>>>,
    tokio::task::JoinHandle<()>,
) {
    model_step_fixture(
        String::new(),
        if persistent {
            "terminal-malformed-always"
        } else {
            "terminal-malformed-once"
        },
    )
    .await
}

pub(crate) async fn malformed_tool_message_fixture(
    read_path: String,
) -> (
    String,
    std::sync::Arc<std::sync::Mutex<Vec<Value>>>,
    tokio::task::JoinHandle<()>,
) {
    model_step_fixture(read_path, "malformed-tool-once").await
}

pub(crate) async fn invalid_metadata_tool_fixture(
    read_path: String,
) -> (
    String,
    std::sync::Arc<std::sync::Mutex<Vec<Value>>>,
    tokio::task::JoinHandle<()>,
) {
    model_step_fixture(read_path, "metadata-tool-once").await
}

#[tokio::test]
async fn metadata_failure_log_inherits_real_transport_identity_and_certification_state() {
    let _guard = test_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    set_config_root(temp.path());
    let path = temp.path().join("response-failures.jsonl");
    let (api, api_task) = unary_fixture().await;
    let (inference, _, inference_task) = invalid_metadata_fixture(true, true).await;
    adopt_credential(
        json!({"accessToken":"fixture-token","clientIdentity":"fixture-machine","backendUrl":api,"inferenceUrl":inference}),
    );
    let (tx, _rx) = tokio::sync::mpsc::channel(32);
    let (result, events) = diagnostics::capture_failures(
        path.clone(),
        diagnostics::capture(bridge::run(provider_body(), &tx)),
    )
    .await;
    assert_eq!(result.unwrap_err(), metadata::SELECTOR_INVALID);
    let request = events
        .iter()
        .find(|event| event["operation"] == "model_step" && event["phase"] == "request_send")
        .unwrap();
    let raw = std::fs::read_to_string(path).unwrap();
    let records = raw
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 2);
    for record in &records {
        assert_eq!(
            record["request"]["requestId"],
            request["fields"]["requestId"]
        );
        assert_eq!(record["operationId"], request["operationId"]);
        assert_eq!(record["request"]["model"], "fixture-high-fast");
        assert!(record["request"]["generation"].as_u64().is_some());
    }
    assert_eq!(records[0]["phase"], "metadata_rejected");
    assert_eq!(
        records[0]["fields"]["parameters"]["args"]["namespace"]["value"],
        "foreign-fixture"
    );
    assert_eq!(records[1]["phase"], "certification_failed");
    assert_eq!(records[1]["fields"]["error"], metadata::SELECTOR_INVALID);
    assert_eq!(records[1]["fields"]["stage"], "receive");
    for private in [
        "fixture-token",
        "fixture-machine",
        "UNCERTIFIED_PARTIAL",
        "Find the read tool",
    ] {
        assert!(!raw.contains(private), "{private}");
    }
    logout(None).unwrap();
    api_task.abort();
    inference_task.abort();
}

#[tokio::test]
async fn malformed_message_diagnostics_correlate_request_site_and_state_without_content() {
    let _guard = test_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    set_config_root(temp.path());
    let (api, api_task) = unary_fixture().await;
    let (inference, _, inference_task) = malformed_message_fixture(true).await;
    adopt_credential(
        json!({"accessToken":"fixture-token","clientIdentity":"fixture-machine","backendUrl":api,"inferenceUrl":inference}),
    );
    let (tx, _rx) = tokio::sync::mpsc::channel(32);
    let (result, events) = diagnostics::capture(bridge::run(provider_body(), &tx)).await;
    assert_eq!(result.unwrap_err(), "cursor_native_message_invalid");
    let request = events
        .iter()
        .find(|event| event["operation"] == "model_step" && event["phase"] == "request_send")
        .unwrap();
    let request_id = request["fields"]["requestId"].as_str().unwrap();
    assert!(uuid::Uuid::parse_str(request_id).is_ok());
    let rejection = events
        .iter()
        .find(|event| event["phase"] == "native_message_rejected")
        .unwrap();
    assert_eq!(rejection["fields"]["site"], "certify.content");
    assert_eq!(request["operationId"], rejection["operationId"]);
    let state = events
        .iter()
        .find(|event| event["phase"] == "certification_failed")
        .unwrap();
    assert_eq!(state["operationId"], request["operationId"]);
    assert_eq!(state["fields"]["stage"], "finish");
    assert_eq!(state["fields"]["error"], "cursor_native_message_invalid");
    assert!(state["fields"]["packetCount"].as_u64().unwrap() > 0);
    assert_eq!(state["fields"]["heldCalls"], 0);
    let end = events
        .iter()
        .find(|event| event["operation"] == "model_step" && event["phase"] == "end")
        .unwrap();
    assert_eq!(end["fields"]["error"], "cursor_native_message_invalid");
    let encoded = serde_json::to_string(&events).unwrap();
    for private in [
        "fixture-token",
        "fixture-machine",
        "UNCERTIFIED_PARTIAL",
        "Find the read tool",
        "signed-terminal",
    ] {
        assert!(!encoded.contains(private), "{private}");
    }
    logout(None).unwrap();
    api_task.abort();
    inference_task.abort();
}

async fn model_step_fixture(
    read_path: String,
    mode: &'static str,
) -> (
    String,
    std::sync::Arc<std::sync::Mutex<Vec<Value>>>,
    tokio::task::JoinHandle<()>,
) {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use bytes::Bytes;
    use http_body_util::{BodyExt, StreamBody};
    use hyper::{body::Frame, Response};
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use sha2::{Digest, Sha256};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let observed = std::sync::Arc::new(std::sync::Mutex::new(Vec::<Value>::new()));
    let captured = observed.clone();
    let task = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.expect("accept");
            let captured = captured.clone();
            let read_path = read_path.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(
                    move |request: hyper::Request<hyper::body::Incoming>| {
                        let captured = captured.clone();
                        let read_path = read_path.clone();
                        async move {
                            assert_eq!(request.uri().path(), "/agent.v1.AgentService/Run");
                            assert_eq!(request.headers()["authorization"], "Bearer fixture-token");
                            assert_eq!(request.headers()["x-cursor-client-type"], "sdk");
                            assert!(request
                                .headers()
                                .contains_key("x-cursor-agent-allowed-tools"));
                            assert!(!request.headers().contains_key("x-cursor-checksum"));
                            let mut body = request.into_body();
                            let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(32);
                            tokio::spawn(async move {
                                let mut decoder = bridge::Decoder::default();
                                let mut run = Value::Null;
                                let mut roots = Vec::new();
                                let mut messages = std::collections::BTreeMap::new();
                                let mut prepared = false;
                                let mut terminal_pending = false;
                                let mut checkpoint_after_context = None;
                                let send = |value| bridge::encode_frame(value).expect("frame");
                                while let Some(frame) = body.frame().await {
                                    let Ok(frame) = frame else {
                                        break;
                                    };
                                    let Some(data) = frame.data_ref() else {
                                        continue;
                                    };
                                    for (_, value) in decoder.push(data).expect("decode") {
                                        for result in ["mcpResult", "readResult", "writeResult"] {
                                            if let Some(reply) =
                                                value["execClientMessage"].get(result)
                                            {
                                                assert_eq!((mode, result), ("metadata-hang", "mcpResult"), "provider must never receive local execution acknowledgments");
                                                let expected = json!({"mode":"search","pattern":"^unavailable_fixture_tool$","matches":[]});
                                                assert!(super::metadata::mcp_result_matches(
                                                    reply, &expected
                                                ));
                                                {
                                                    let mut captured = captured.lock().unwrap();
                                                    let last = captured.last_mut().unwrap();
                                                    assert_ne!(last["metadataReplied"], true);
                                                    last["metadataReplied"] = json!(true);
                                                }
                                                let args = json!({"name":"klaus_tools-GetDynamicTools","providerIdentifier":step_history::NAMESPACE,"serverIdentifier":step_history::NAMESPACE,"toolName":"GetDynamicTools","toolCallId":"metadata","args":{"namespace":step_history::NAMESPACE,"pattern":"^unavailable_fixture_tool$"}});
                                                if tx.send(send(json!({"interactionUpdate":{"toolCallCompleted":{"modelCallId":"metadata-model","toolCall":{"mcpToolCall":{"args":args,"result":reply}}}}}))).await.is_err() {
                                                    return;
                                                }
                                            }
                                        }
                                        if let Some(request) = value.get("runRequest") {
                                            assert!(
                                                run.is_null(),
                                                "one provider invocation per request"
                                            );
                                            run = request.clone();
                                            assert_eq!(run["conversationState"]["mode"], 1);
                                            assert!(run["action"].get("resumeAction").is_some());
                                            assert!(run["action"]
                                                .get("userMessageAction")
                                                .is_none());
                                            assert_eq!(
                                                run["requestedModel"]["parameters"],
                                                json!([{"id":"effort","value":"high"},{"id":"fast","value":"true"}])
                                            );
                                            roots = run["conversationState"]
                                                ["rootPromptMessagesJson"]
                                                .as_array()
                                                .unwrap()
                                                .clone();
                                            for (index, key) in roots.iter().enumerate() {
                                                tx.send(send(if index==0 {json!({"kvServerMessage":{"getBlobArgs":{"blobId":key}}})}else{json!({"kvServerMessage":{"id":index as u64,"getBlobArgs":{"blobId":key}}})})).await.unwrap();
                                            }
                                        }
                                        if value["execClientMessage"]["id"] == 901 {
                                            let checkpoint = checkpoint_after_context
                                                .take()
                                                .expect("delayed checkpoint");
                                            captured.lock().unwrap().last_mut().unwrap()
                                                ["checkpointBarrierReached"] = json!(true);
                                            if tx.send(send(checkpoint)).await.is_err() {
                                                return;
                                            }
                                        }
                                        if let Some(reply) = value.get("kvClientMessage") {
                                            if let Some(get) = reply.get("getBlobResult") {
                                                let index = reply["id"].as_u64().unwrap() as usize;
                                                let bytes = STANDARD
                                                    .decode(get["blobData"].as_str().unwrap())
                                                    .unwrap();
                                                assert_eq!(
                                                    STANDARD.encode(Sha256::digest(&bytes)),
                                                    roots[index]
                                                );
                                                messages.insert(
                                                    index,
                                                    serde_json::from_slice::<Value>(&bytes)
                                                        .unwrap(),
                                                );
                                            }
                                            if terminal_pending
                                                && reply.get("setBlobResult").is_some()
                                                && reply["id"] == 900
                                            {
                                                if mode == "terminal-missing-trailer" {
                                                    return;
                                                }
                                                let mut trailer = send(if mode=="terminal-error" {json!({"error":{"code":"permission_denied","message":"denied"}})}else{json!({})}).to_vec();
                                                trailer[0] = 2;
                                                if mode == "terminal-trailer-extra" {
                                                    trailer.extend(send(json!({"interactionUpdate":{"heartbeat":{}}})));
                                                }
                                                tx.send(trailer.into()).await.unwrap();
                                                return;
                                            }
                                        }
                                        if !prepared
                                            && !roots.is_empty()
                                            && messages.len() == roots.len()
                                        {
                                            prepared = true;
                                            let history =
                                                messages.values().cloned().collect::<Vec<_>>();
                                            let tools =
                                                run["mcpTools"]["mcpTools"].as_array().unwrap();
                                            let results = history
                                                .iter()
                                                .filter(|message| {
                                                    message["role"] == "tool"
                                                        && matches!(
                                                            message["content"][0]["toolName"]
                                                                .as_str(),
                                                            Some("CallDynamicTool" | "Read")
                                                        )
                                                })
                                                .count();
                                            {
                                                let mut captured = captured.lock().unwrap();
                                                assert!(
                                                    captured.len() < 4,
                                                    "no retry or extra invocation"
                                                );
                                                captured.push(json!({"run":run,"messages":history,"tools":tools,"closed":false,"terminalSent":false}));
                                            }
                                            if mode == "hang" || mode == "metadata-hang" {
                                                if mode == "metadata-hang" {
                                                    let args = json!({"name":"klaus_tools-GetDynamicTools","providerIdentifier":step_history::NAMESPACE,"serverIdentifier":step_history::NAMESPACE,"toolName":"GetDynamicTools","toolCallId":"metadata","args":{"namespace":step_history::NAMESPACE,"pattern":"^unavailable_fixture_tool$"}});
                                                    for update in [
                                                        json!({"interactionUpdate":{"toolCallStarted":{"modelCallId":"metadata-model","toolCall":{"mcpToolCall":{"args":args}}}}}),
                                                        json!({"execServerMessage":{"id":7,"execId":"metadata-exec","mcpArgs":args}}),
                                                        json!({"interactionUpdate":{"toolRequestsListed":{"callCount":1}}}),
                                                    ] {
                                                        if tx.send(send(update)).await.is_err() {
                                                            return;
                                                        }
                                                    }
                                                }
                                                continue;
                                            }
                                            let mut checkpoint_roots = roots.clone();
                                            let reordered_blob = if results > 0 {
                                                let index = history
                                                    .iter()
                                                    .position(|message| {
                                                        message["role"] == "assistant"
                                                    })
                                                    .expect("replayed assistant");
                                                let bytes =
                                                    serde_json::to_vec_pretty(&history[index])
                                                        .unwrap();
                                                let key = STANDARD.encode(Sha256::digest(&bytes));
                                                assert_ne!(
                                                    checkpoint_roots[index], key,
                                                    "fixture changes JSON encoding only"
                                                );
                                                checkpoint_roots[index] = json!(key);
                                                Some(
                                                    json!({"kvServerMessage":{"id":899,"setBlobArgs":{"blobId":key,"blobData":STANDARD.encode(bytes)}}}),
                                                )
                                            } else {
                                                None
                                            };
                                            if results
                                                < if mode.starts_with("terminal-") {
                                                    0
                                                } else if mode == "permission" || mode == "image" {
                                                    1
                                                } else {
                                                    2
                                                }
                                            {
                                                let name = if mode == "image" {
                                                    "generate_image"
                                                } else if mode == "permission" {
                                                    "apply_patch"
                                                } else if results == 0 {
                                                    "tool_search"
                                                } else {
                                                    "read"
                                                };
                                                let tool = tools
                                                    .iter()
                                                    .find(|tool| tool["toolName"] == name)
                                                    .expect("default tool");
                                                let args = if mode == "image" {
                                                    json!({"prompt":"A blue square"})
                                                } else if mode == "permission" {
                                                    json!({"patch":format!("*** Begin Patch\n*** Add File: {read_path}\n+should not exist\n*** End Patch")})
                                                } else if results == 0 {
                                                    json!({"query":"read"})
                                                } else {
                                                    json!({"path":read_path})
                                                };
                                                let raw = format!("raw-{name}\nfunction-{results}");
                                                let held = json!({"name":tool["name"],"providerIdentifier":step_history::NAMESPACE,"serverIdentifier":step_history::NAMESPACE,"toolName":tool["toolName"],"args":args,"toolCallId":raw});
                                                let native = json!({"id":"assistant","role":"assistant","content":[{"type":"reasoning","text":"Reason","signature":"signed-facade","providerOptions":{"cursor":{"modelName":"fixture-high-fast"}}},{"type":"tool-call","toolCallId":raw,"toolName":"CallDynamicTool","args":{"namespace":step_history::NAMESPACE,"toolName":tool["toolName"],"arguments":args}}],"providerOptions":{"cursor":{"pendingToolExecutionContracts":{raw.clone():{"toolCallId":raw,"outerToolName":"CallDynamicTool","toolIdentifier":"MCP","isDynamic":true}}}}});
                                                let mut exec = held.clone();
                                                if mode == "foreign-tool" {
                                                    exec["providerIdentifier"] = json!("foreign");
                                                }
                                                let native_read = matches!(
                                                    mode,
                                                    "normal"
                                                        | "malformed-tool-once"
                                                        | "metadata-tool-once"
                                                ) && results == 1;
                                                let mut native = native;
                                                if mode == "malformed-tool-once"
                                                    && captured.lock().unwrap().len() == 1
                                                {
                                                    native["content"][0]["text"] = Value::Null;
                                                }
                                                let (started, execution) = if native_read {
                                                    native["content"][1]["toolName"] =
                                                        json!("Read");
                                                    native["content"][1]["args"] = args.clone();
                                                    native["providerOptions"]["cursor"]
                                                        ["pendingToolExecutionContracts"][&raw]
                                                        ["outerToolName"] = json!("Read");
                                                    native["providerOptions"]["cursor"]
                                                        ["pendingToolExecutionContracts"][&raw]
                                                        ["toolIdentifier"] = json!("READ");
                                                    native["providerOptions"]["cursor"]
                                                        ["pendingToolExecutionContracts"][&raw]
                                                        ["isDynamic"] = json!(false);
                                                    (
                                                        json!({"readToolCall":{"args":args},"toolCallId":raw}),
                                                        json!({"execServerMessage":{"readArgs":{"path":read_path,"toolCallId":raw},"acceptHookAdditionalContexts":true}}),
                                                    )
                                                } else {
                                                    (
                                                        json!({"mcpToolCall":{"args":held}}),
                                                        json!({"execServerMessage":{"id":7,"execId":"exec","mcpArgs":exec,"acceptHookAdditionalContexts":true}}),
                                                    )
                                                };
                                                if mode == "metadata-tool-once"
                                                    && captured.lock().unwrap().len() == 1
                                                {
                                                    // Hold an ordinary execution intent before the
                                                    // rejected discovery, as in a mixed model batch.
                                                    for update in [
                                                        json!({"interactionUpdate":{"toolCallStarted":{"modelCallId":"model-call","toolCall":started}}}),
                                                        execution,
                                                        json!({"interactionUpdate":{"toolCallStarted":{"modelCallId":"model-call","toolCall":{"getMcpToolsToolCall":{"args":{"server":"foreign-fixture","toolCallId":"query-1"}}}}}}),
                                                    ] {
                                                        tx.send(send(update)).await.unwrap();
                                                    }
                                                    return;
                                                }
                                                let mut metadata_updates = Vec::new();
                                                if mode == "metadata"
                                                    || mode == "delayed-checkpoint"
                                                {
                                                    let id = format!("metadata-{results}");
                                                    let selector = if results == 0 {
                                                        json!({"pattern":"^unavailable_fixture_tool$"})
                                                    } else {
                                                        json!({"namespace":step_history::NAMESPACE,"toolName":"read"})
                                                    };
                                                    let wire = if results == 0 {
                                                        json!({"pattern":"^unavailable_fixture_tool$","toolCallId":id})
                                                    } else {
                                                        json!({"server":step_history::NAMESPACE,"toolName":"read","toolCallId":id})
                                                    };
                                                    let result = if results == 0 {
                                                        json!({"mode":"search","pattern":"^unavailable_fixture_tool$","matches":[]})
                                                    } else {
                                                        let description = tool["description"]
                                                            .as_str()
                                                            .unwrap_or_default()
                                                            .split_whitespace()
                                                            .collect::<Vec<_>>()
                                                            .join(" ");
                                                        json!({"tool":"read","inputSchema":tool["inputSchema"],"description":description})
                                                    };
                                                    metadata_updates.extend([
                                                        json!({"interactionUpdate":{"toolCallStarted":{"modelCallId":"metadata-model","toolCall":{"getMcpToolsToolCall":{"args":wire}}}}}),
                                                        json!({"interactionUpdate":{"toolRequestsListed":{"callCount":1}}}),
                                                        json!({"interactionUpdate":{"toolCallCompleted":{"modelCallId":"metadata-model","toolCall":{"getMcpToolsToolCall":{"args":wire,"result":{"success":{"content":result.to_string()}}}}}}}),
                                                    ]);
                                                    if mode == "delayed-checkpoint" {
                                                        let helper = json!({"role":"assistant","content":[{"type":"reasoning","text":"","signature":"metadata-signature"},{"type":"tool-call","toolCallId":id,"toolName":"GetDynamicTools","args":selector}]});
                                                        metadata_updates.push(json!({"conversationCheckpointUpdate":{"rootPromptMessagesJson":checkpoint_roots,"pendingToolCalls":[helper.to_string()]}}));
                                                    }
                                                    for (index, message) in [
                                                        json!({"role":"assistant","content":[{"type":"reasoning","text":"","signature":"metadata-signature"},{"type":"tool-call","toolCallId":id,"toolName":"GetDynamicTools","args":selector}]}),
                                                        json!({"role":"tool","content":[{"type":"tool-result","toolCallId":id,"toolName":"GetDynamicTools","result":result.to_string()}]}),
                                                    ].into_iter().enumerate() {
                                                        let bytes = serde_json::to_vec(&message).unwrap();
                                                        let key = STANDARD.encode(Sha256::digest(&bytes));
                                                        checkpoint_roots.push(json!(key));
                                                        metadata_updates.push(json!({"kvServerMessage":{"id":890+index,"setBlobArgs":{"blobId":key,"blobData":STANDARD.encode(bytes)}}}));
                                                    }
                                                }
                                                let updates = [
                                                    json!({"interactionUpdate":{"toolCallStarted":{"modelCallId":"model-call","toolCall":started}}}),
                                                    execution,
                                                    json!({"interactionUpdate":{"toolRequestsListed":{"callCount":1}}}),
                                                    json!({"conversationCheckpointUpdate":{"rootPromptMessagesJson":checkpoint_roots,"pendingToolCalls":[native.to_string()]}}),
                                                ];
                                                // Both batches deliberately share one HTTP data chunk.
                                                // Protocol validity cannot depend on transport chunking.
                                                let mut chunk = Vec::new();
                                                let mut updates = updates.to_vec();
                                                if mode == "delayed-checkpoint" {
                                                    checkpoint_after_context = updates.pop();
                                                    // The reply proves that this chunk was processed
                                                    // before the current checkpoint can be delivered.
                                                    updates.push(json!({"execServerMessage":{"id":901,"execId":"checkpoint-barrier","requestContextArgs":{}}}));
                                                }
                                                for update in
                                                    metadata_updates.into_iter().chain(updates)
                                                {
                                                    chunk.extend_from_slice(&send(update));
                                                }
                                                if tx.send(chunk.into()).await.is_err() {
                                                    return;
                                                }
                                                if let Some(blob) = reordered_blob {
                                                    if tx.send(send(blob)).await.is_err() {
                                                        return;
                                                    }
                                                }
                                            } else {
                                                let invalid_metadata = mode
                                                    .starts_with("terminal-metadata-")
                                                    && (mode.ends_with("-always")
                                                        || captured.lock().unwrap().len() == 1);
                                                if invalid_metadata {
                                                    tx.send(send(json!({"interactionUpdate":{"textDelta":{"text":"UNCERTIFIED_PARTIAL"}}}))).await.unwrap();
                                                    let call = if mode.contains("-helper-") {
                                                        json!({"mcpToolCall":{"args":{"name":"klaus_tools-GetDynamicTools","providerIdentifier":"klaus_tools","toolName":"GetDynamicTools","toolCallId":"query-1","args":{"namespace":"foreign-fixture"}}}})
                                                    } else {
                                                        json!({"getMcpToolsToolCall":{"args":{"server":"foreign-fixture","toolCallId":"query-1"}}})
                                                    };
                                                    tx.send(send(json!({"interactionUpdate":{"toolCallStarted":{"modelCallId":"query-model","toolCall":call}}}))).await.unwrap();
                                                    return;
                                                }
                                                let malformed = mode == "terminal-malformed-always"
                                                    || mode == "terminal-malformed-once"
                                                        && captured.lock().unwrap().len() == 1;
                                                let mut native = json!({"role":"assistant","id":"terminal","content":[{"type":"reasoning","text":"Reason","signature":"signed-terminal","providerOptions":{"cursor":{"modelName":"fixture-high-fast"}}},{"type":"text","text":"FACADE_TOOL_OK"}],"providerOptions":{"cursor":{"modelName":"fixture-high-fast"}}});
                                                if malformed {
                                                    native["content"] = Value::Null;
                                                }
                                                let bytes = serde_json::to_vec(&native).unwrap();
                                                let key = STANDARD.encode(Sha256::digest(&bytes));
                                                let mut final_roots = checkpoint_roots;
                                                final_roots.push(json!(key));
                                                tx.send(send(json!({"interactionUpdate":{"textDelta":{"text":if malformed {"UNCERTIFIED_PARTIAL"} else {"FACADE_TOOL_OK"}}},"ttftBreakdown":{"providerTtftMs":2.5,"clientClock":false}}))).await.unwrap();
                                                if mode == "terminal-streaming" {
                                                    tokio::time::sleep(
                                                        std::time::Duration::from_millis(250),
                                                    )
                                                    .await;
                                                }
                                                captured.lock().unwrap().last_mut().unwrap()
                                                    ["terminalSent"] = json!(true);
                                                if let Some(blob) = reordered_blob {
                                                    tx.send(send(blob)).await.unwrap();
                                                }
                                                // Final KV/checkpoint follow turnEnded. Completion must
                                                // drain and acknowledge them before accepting the trailer.
                                                for update in [
                                                    json!({"interactionUpdate":{"turnEnded":{"inputTokens":"11","outputTokens":"5","cacheReadTokens":"2","reasoningTokens":"1"}}}),
                                                    json!({"kvServerMessage":{"id":900,"setBlobArgs":{"blobId":key,"blobData":STANDARD.encode(bytes)}}}),
                                                    json!({"conversationCheckpointUpdate":{"rootPromptMessagesJson":final_roots,"pendingToolCalls":[]}}),
                                                ] {
                                                    tx.send(send(update)).await.unwrap();
                                                }
                                                terminal_pending = true;
                                            }
                                        }
                                    }
                                }
                                if prepared {
                                    let mut captured = captured.lock().unwrap();
                                    if let Some(last) = captured.last_mut() {
                                        last["closed"] = json!(true);
                                    }
                                }
                            });
                            let stream = futures::stream::unfold(rx, |mut rx| async move {
                                rx.recv().await.map(|bytes| {
                                    (Ok::<_, std::convert::Infallible>(Frame::data(bytes)), rx)
                                })
                            });
                            Ok::<_, std::convert::Infallible>(
                                Response::builder()
                                    .header("content-type", "application/connect+json")
                                    .body(StreamBody::new(stream))
                                    .unwrap(),
                            )
                        }
                    },
                );
                let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(socket), service)
                    .await;
            });
        }
    });
    (origin, observed, task)
}

fn provider_body() -> Value {
    json!({"model":"auto","stream":true,"input":[{"role":"user","content":"Find the read tool"}],"tools":[{"type":"function","name":"tool_search","description":"Search tools","parameters":{"type":"object"}},{"type":"function","name":"read","description":"Read a file","parameters":{"type":"object"}}]})
}

async fn wait_for_request(captured: &std::sync::Arc<std::sync::Mutex<Vec<Value>>>) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while captured.lock().unwrap().is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("provider request observed");
}

#[tokio::test]
async fn credential_epoch_and_downstream_disconnect_cancel_the_entire_model_step() {
    use futures::StreamExt;
    let _guard = test_lock().lock().await;
    for mode in ["hang", "metadata-hang"] {
        let temp = tempfile::tempdir().unwrap();
        set_config_root(temp.path());
        let (api, api_task) = unary_fixture().await;
        let (inference, captured, inference_task) = model_step_fixture("unused".into(), mode).await;
        assert_eq!(
            adopt_credential(
                json!({"accessToken":"fixture-token","clientIdentity":"fixture-machine","backendUrl":api,"inferenceUrl":inference})
            )["ok"],
            true
        );
        let (tx, mut rx) = tokio::sync::mpsc::channel(32);
        let running = tokio::spawn(async move { bridge::run(provider_body(), &tx).await });
        wait_for_request(&captured).await;
        if mode == "metadata-hang" {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while captured
                    .lock()
                    .unwrap()
                    .last()
                    .is_none_or(|request| request["metadataReplied"] != true)
                {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("frozen metadata reply observed before cancellation");
        }
        logout(None).unwrap();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(5), running)
                .await
                .unwrap()
                .unwrap()
                .unwrap_err(),
            "cursor_credential_changed"
        );
        while let Some(event) = rx.recv().await {
            let event = String::from_utf8(event.to_vec()).unwrap();
            assert!(!event.contains("function_call"));
            assert!(!event.contains("encrypted_content"));
            assert!(!event.contains("response.completed"));
        }
        assert_eq!(
            adopt_credential(
                json!({"accessToken":"fixture-token","clientIdentity":"fixture-machine","backendUrl":api,"inferenceUrl":inference})
            )["ok"],
            true
        );
        captured.lock().unwrap().clear();
        let (base, token) = bridge::ensure_started().await.unwrap();
        let response = reqwest::Client::new()
            .post(format!("{base}/responses"))
            .bearer_auth(token)
            .json(&provider_body())
            .send()
            .await
            .unwrap();
        let mut stream = response.bytes_stream();
        assert!(stream.next().await.unwrap().is_ok());
        wait_for_request(&captured).await;
        if mode == "metadata-hang" {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while captured
                    .lock()
                    .unwrap()
                    .last()
                    .is_none_or(|request| request["metadataReplied"] != true)
                {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("frozen metadata reply observed before cancellation");
        }
        drop(stream);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !captured
                .lock()
                .unwrap()
                .last()
                .is_some_and(|request| request["closed"] == true)
            {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("upstream HTTP/2 request closes when downstream disconnects");
        logout(None).unwrap();
        api_task.abort();
        inference_task.abort();
    }
}

#[tokio::test]
async fn unowned_model_tool_intents_never_reach_the_local_executor_or_opaque_history() {
    let _guard = test_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    set_config_root(temp.path());
    let (api, api_task) = unary_fixture().await;
    let (inference, captured, inference_task) =
        model_step_fixture("unused".into(), "foreign-tool").await;
    adopt_credential(
        json!({"accessToken":"fixture-token","clientIdentity":"fixture-machine","backendUrl":api,"inferenceUrl":inference}),
    );
    let (tx, mut rx) = tokio::sync::mpsc::channel(32);
    let error = bridge::run(provider_body(), &tx).await.unwrap_err();
    drop(tx);
    assert_eq!(error, "cursor_tool_not_owned");
    assert_eq!(
        responses::public_error(&error)["message"],
        "cursor_model_response_unsupported"
    );
    while let Some(event) = rx.recv().await {
        let event = String::from_utf8(event.to_vec()).unwrap();
        assert!(!event.contains("function_call"));
        assert!(!event.contains("encrypted_content"));
        assert!(!event.contains("response.completed"));
    }
    assert_eq!(captured.lock().unwrap().len(), 1);
    logout(None).unwrap();
    api_task.abort();
    inference_task.abort();
}

#[tokio::test]
async fn ordinary_text_streams_before_terminal_certification_without_publishing_tool_or_context_events(
) {
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let mut output = responses::Output::new("fixture");
    output.start(&tx).await.unwrap();
    output
        .delta(&json!({"textDelta":{"text":"First"}}), &tx)
        .await
        .unwrap();
    output
        .delta(&json!({"thinkingDelta":{"text":"Thinking"}}), &tx)
        .await
        .unwrap();
    let mut observed = String::new();
    for _ in 0..4 {
        observed.push_str(std::str::from_utf8(&rx.recv().await.unwrap()).unwrap());
    }
    assert!(observed.contains("First"));
    assert!(observed.contains("Thinking"));
    assert!(!observed.contains("function_call"));
    assert!(!observed.contains("encrypted_content"));
    assert!(!observed.contains("response.completed"));
}

#[tokio::test]
async fn http2_text_streams_before_final_checkpoint_and_terminal_errors_never_commit_native_history(
) {
    let _guard = test_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    set_config_root(temp.path());
    let (api, api_task) = unary_fixture().await;
    for mode in [
        "terminal-streaming",
        "terminal-missing-trailer",
        "terminal-error",
        "terminal-trailer-extra",
    ] {
        let (inference, captured, inference_task) = model_step_fixture("unused".into(), mode).await;
        adopt_credential(
            json!({"accessToken":"fixture-token","clientIdentity":"fixture-machine","backendUrl":api,"inferenceUrl":inference}),
        );
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let running = tokio::spawn(async move { bridge::run(provider_body(), &tx).await });
        let mut events = String::new();
        while let Some(event) = rx.recv().await {
            let event = std::str::from_utf8(&event).unwrap();
            if mode == "terminal-streaming" && event.contains("response.output_text.delta") {
                assert_eq!(
                    captured.lock().unwrap().last().unwrap()["terminalSent"],
                    false,
                    "text is emitted before the final checkpoint"
                );
            }
            events.push_str(event);
        }
        let result = running.await.unwrap();
        if mode == "terminal-streaming" {
            result.unwrap();
            assert!(events.contains("response.completed"));
            assert!(events.contains("encrypted_content"));
            assert!(events.contains("\"input_tokens\":11"));
            assert!(events.contains("\"output_tokens\":5"));
        } else {
            let error = result.unwrap_err();
            assert_eq!(
                error,
                match mode {
                    "terminal-error" => "cursor_permission_denied",
                    "terminal-missing-trailer" => "cursor_terminal_trailer_missing",
                    _ => "cursor_terminal_trailer_invalid",
                }
            );
            assert!(!events.contains("response.completed"));
            assert!(!events.contains("encrypted_content"));
        }
        inference_task.abort();
    }
    logout(None).unwrap();
    api_task.abort();
}

#[tokio::test]
async fn credential_epoch_cancels_final_output_under_backpressure_and_rejects_stale_completion() {
    let _guard = test_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    set_config_root(temp.path());
    adopt_credential(json!({"accessToken":"fixture-token","clientIdentity":"fixture-machine"}));
    let (_, generation) = credential::snapshot().unwrap();
    let certified = || step::Certified {
        text: String::new(),
        reasoning: "Reason".into(),
        calls: vec![json!({"toolCallId":"safe-call","toolName":"read","args":{"path":"fixture"}})],
        context: json!({"format":step_history::CONTEXT_FORMAT}),
        usage: json!({}),
    };
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let running = tokio::spawn(async move {
        bridge::finish_output(
            responses::Output::new("fixture"),
            certified(),
            generation,
            &tx,
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while rx.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("final output is backpressured before opaque context");
    logout(None).unwrap();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(5), running)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err(),
        "cursor_credential_changed"
    );
    while let Some(event) = rx.recv().await {
        let event = std::str::from_utf8(&event).unwrap();
        assert!(!event.contains("function_call"));
        assert!(!event.contains("encrypted_content"));
        assert!(!event.contains("response.completed"));
    }
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    assert_eq!(
        bridge::finish_output(
            responses::Output::new("fixture"),
            certified(),
            generation,
            &tx
        )
        .await
        .unwrap_err(),
        "cursor_credential_changed"
    );
    assert!(
        rx.try_recv().is_err(),
        "an already stale completion emits nothing"
    );
}

#[test]
fn unsupported_read_intents_are_model_response_errors_without_authentication_blame() {
    for error in [
        "cursor_read_intent_not_declared",
        "cursor_read_intent_schema_unsupported",
        "cursor_read_intent_arguments_unsupported",
    ] {
        assert_eq!(
            responses::public_error(error)["message"],
            "cursor_model_response_unsupported"
        );
    }
}

#[test]
fn catalog_exposes_family_metadata_and_real_parameter_definitions_without_extra_variants() {
    let value = json!({"models":[{"name":"family","serverModelName":"actual-family","clientDisplayName":"Family <b>Name</b>","parameterDefinitions":[{"id":"effort","name":"Reasoning","parameterType":{"enumParameter":{"values":[{"value":"low","displayName":"Low"},{"value":"high","displayName":"High","increasesModelCost":true}]}}}],"variants":[{"displayName":"Family High","parameterValues":[{"id":"effort","value":"high"}]}]}]});
    let rows = catalog::normalize(&value);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["modelId"], "actual-family");
    assert_eq!(rows[0]["modelName"], "Family Name");
    assert_eq!(rows[0]["parameterDefinitions"][0]["type"], "enum");
    assert_eq!(
        rows[0]["parameterDefinitions"][0]["values"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        rows[0]["requestedModel"]["parameters"],
        json!([{"id":"effort","value":"high"}])
    );
}

#[test]
fn grok_4_7_reads_images_when_the_catalog_flag_is_false() {
    let models = catalog::normalize(&json!({"models":[
        {"name":"grok-4.7","serverModelName":"grok-4.7","clientDisplayName":"Grok 4.7","supportsImages":false,"supportsAgent":true,"contextTokenLimit":256000,"variants":[
            {"displayName":"Grok 4.7 High","parameterValues":[{"id":"context","value":"256k"},{"id":"reasoning_effort","value":"high"},{"id":"fast","value":"false"}],"isDefaultNonMaxConfig":true}
        ]},
        {"name":"text-only","serverModelName":"text-only","clientDisplayName":"Text Only","supportsImages":false,"supportsAgent":true,"variants":[
            {"displayName":"Text Only","parameterValues":[{"id":"effort","value":"high"}]}
        ]},
        {"name":"sees","serverModelName":"sees","clientDisplayName":"Sees","supportsImage":true,"supportsAgent":true,"variants":[
            {"displayName":"Sees","parameterValues":[{"id":"effort","value":"high"}]}
        ]}
    ]}));
    let row = |id: &str| {
        models
            .iter()
            .find(|model| model["modelId"] == id)
            .unwrap_or_else(|| panic!("missing {id}"))
    };
    assert_eq!(row("grok-4.7")["is_vl"], json!(true));
    assert_eq!(row("text-only")["is_vl"], json!(false));
    assert_eq!(row("sees")["is_vl"], json!(true));
}

#[test]
fn browser_inference_uses_the_model_service_origin_and_preserves_explicit_overrides() {
    let browser: credential::Credential = serde_json::from_value(
        json!({"accessToken":"fixture-access","clientIdentity":"fixture-own-install"}),
    )
    .unwrap();
    assert_eq!(
        browser.inference_origin().unwrap(),
        "https://agentn.api5.cursor.sh"
    );
    let imported: credential::Credential=serde_json::from_value(json!({"accessToken":"fixture-access","clientIdentity":"fixture-external-install","inferenceUrl":"https://models.cursor.sh"})).unwrap();
    assert_eq!(
        imported.inference_origin().unwrap(),
        "https://models.cursor.sh"
    );
    assert_eq!(browser.api_origin().unwrap(), "https://api2.cursor.sh");
}
