//! Bounded PKCE browser login with owner-scoped callback publication.

use super::{auth, config_root, http_json};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use rand::RngCore;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, OnceLock,
};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

#[derive(Clone)]
enum Outcome {
    Pending,
    Complete(String),
    Failed(String),
}
struct Flow {
    id: String,
    owner: usize,
    root: PathBuf,
    expected_generation: Option<String>,
    expected_revision: u64,
    deadline: Instant,
    cancelled: watch::Sender<bool>,
    outcome: Mutex<Outcome>,
}
fn flows() -> &'static Mutex<VecDeque<Arc<Flow>>> {
    static FLOWS: OnceLock<Mutex<VecDeque<Arc<Flow>>>> = OnceLock::new();
    FLOWS.get_or_init(|| Mutex::new(VecDeque::new()))
}
fn fail(flow: &Flow, message: &str) {
    let mut outcome = flow.outcome.lock().unwrap_or_else(|p| p.into_inner());
    if matches!(*outcome, Outcome::Pending) {
        *outcome = Outcome::Failed(message.into());
        flow.cancelled.send_replace(true);
    }
}
pub(super) fn invalidate_pending() {
    for flow in flows().lock().unwrap_or_else(|p| p.into_inner()).iter() {
        fail(flow, "trae_login_credential_changed");
    }
}
pub fn disconnect(owner: usize) {
    for flow in flows()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .filter(|flow| flow.owner == owner)
    {
        fail(flow, "trae_login_cancelled");
    }
}
pub(crate) struct ConnectionGuard {
    owner: usize,
}
impl ConnectionGuard {
    pub(crate) fn new(owner: usize) -> Self {
        Self { owner }
    }
}
impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        disconnect(self.owner);
    }
}

fn failure(message: &str) -> Value {
    json!({"ok":false,"status":"error","error":message})
}

pub async fn start(
    owner: usize,
    closed: Arc<AtomicBool>,
    excluded_device_ids: &[String],
    local_device_proof: Option<&auth::LocalDeviceProof>,
) -> Value {
    if closed.load(Ordering::Acquire) {
        return failure("trae_login_cancelled");
    }
    if excluded_device_ids.len() > 32
        || excluded_device_ids
            .iter()
            .any(|id| id.is_empty() || id.len() > 256 || id.chars().any(char::is_control))
    {
        return failure("trae_device_identity_invalid");
    }
    if let Some(proof) = local_device_proof {
        if let Err(error) = auth::validate_device_proof(proof) {
            return failure(&error);
        }
    }
    let root = config_root();
    let expected_generation = auth::generation();
    let expected_revision = auth::revision();
    let mut changes = auth::changes();
    let cancelled = async {
        loop {
            if closed.load(Ordering::Acquire) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    let registered = tokio::select! {
        biased;
        _=cancelled=>return failure("trae_login_cancelled"),
        _=changes.changed()=>return failure("trae_login_credential_changed"),
        result=tokio::time::timeout(Duration::from_secs(30),super::device::register())=>
            result.unwrap_or_else(|_|Err("trae_device_registration_timed_out".into())),
    };
    let device_id = match registered {
        Ok(id) => id,
        Err(error) => return failure(&error),
    };
    if closed.load(Ordering::Acquire)
        || root != config_root()
        || expected_revision != auth::revision()
    {
        return failure("trae_login_credential_changed");
    }
    let matching_proof = local_device_proof.filter(|proof| proof.device_id == device_id);
    if matching_proof.is_none()
        && (excluded_device_ids.contains(&device_id)
            || auth::current_device_id().as_deref() == Some(&device_id))
    {
        return failure("trae_device_identity_conflict");
    }
    let mut random = [0u8; 64];
    if rand::rngs::OsRng.try_fill_bytes(&mut random).is_err() {
        return failure("trae_login_random_failed");
    }
    let state = URL_SAFE_NO_PAD.encode(&random[..24]);
    let verifier = URL_SAFE_NO_PAD.encode(&random[24..]);
    let (private, public) = match matching_proof {
        Some(proof) => (
            proof.device_private_key.clone(),
            proof.device_public_key.clone(),
        ),
        None => match auth::generated_keys() {
            Ok(pair) => pair,
            Err(error) => return failure(&error),
        },
    };
    let listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(_) => return failure("trae_login_callback_unavailable"),
    };
    let port = match listener.local_addr() {
        Ok(address) => address.port(),
        Err(_) => return failure("trae_login_callback_unavailable"),
    };
    let redirect = format!("http://127.0.0.1:{port}/authorize?state={state}");
    let id = uuid::Uuid::new_v4().to_string();
    let (cancelled, mut cancellation) = watch::channel(false);
    let flow = Arc::new(Flow {
        id: id.clone(),
        owner,
        root,
        expected_generation,
        expected_revision,
        deadline: Instant::now() + Duration::from_secs(300),
        cancelled,
        outcome: Mutex::new(Outcome::Pending),
    });
    {
        let mut list = flows().lock().unwrap_or_else(|p| p.into_inner());
        list.retain(|flow| flow.deadline > Instant::now());
        if list.len() >= 16 {
            return failure("trae_login_capacity");
        }
        list.push_back(flow.clone());
    }
    let machine_id = matching_proof
        .map(|proof| proof.machine_id.clone())
        .unwrap_or_else(|| format!("{:x}", Sha256::digest(random)));
    let credential = auth::Credential {
        access_token: String::new(),
        refresh_token: None,
        expires_at: None,
        refresh_expires_at: None,
        account: None,
        user_id: None,
        device_id: device_id.clone(),
        machine_id: machine_id.clone(),
        device_private_key: Some(private),
        device_public_key: Some(public),
        client_version: "3.4.7".into(),
        region: "cn".into(),
    };
    let mut authorize = match url::Url::parse("https://www.trae.cn/authorization") {
        Ok(url) => url,
        Err(_) => return failure("trae_login_endpoint_invalid"),
    };
    authorize.query_pairs_mut().extend_pairs([
        ("login_version", "1"),
        ("auth_from", "trae"),
        ("login_channel", "native_ide"),
        ("plugin_version", "2.3.96150"),
        ("auth_type", "local"),
        ("client_id", auth::CLIENT_ID),
        ("redirect", "0"),
        ("login_trace_id", &id),
        ("auth_callback_url", &redirect),
        ("machine_id", &machine_id),
        ("device_id", &device_id),
        ("x_device_id", &device_id),
        ("x_machine_id", &machine_id),
        ("x_device_brand", ""),
        (
            "x_device_type",
            if cfg!(target_os = "macos") {
                "MacOS"
            } else {
                std::env::consts::OS
            },
        ),
        ("x_os_version", &super::device::os_version()),
        ("x_app_version", "3.4.7"),
        ("x_app_type", "stable"),
        (
            "code_challenge",
            &URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes())),
        ),
        ("code_challenge_method", "S256"),
    ]);
    let background = flow.clone();
    tokio::spawn(async move {
        let operation = async {
            loop {
                let (mut socket, address) = listener
                    .accept()
                    .await
                    .map_err(|_| "trae_login_callback_unavailable".to_string())?;
                if !address.ip().is_loopback() {
                    continue;
                }
                let query = match callback(&mut socket, port, &state).await {
                    Ok(query) => query,
                    Err(_) => {
                        let _ = reply(
                            &mut socket,
                            "400 Bad Request",
                            "Invalid authorization callback.",
                        )
                        .await;
                        continue;
                    }
                };
                if query
                    .get("error_code")
                    .is_some_and(|code| !code.is_empty() && code != "0")
                {
                    let _ = reply(
                        &mut socket,
                        "400 Bad Request",
                        "Authorization was declined.",
                    )
                    .await;
                    return Err("trae_login_declined".into());
                }
                let code: Value = serde_json::from_str(
                    query
                        .get("authCodeInfo")
                        .ok_or("trae_login_callback_invalid")?,
                )
                .map_err(|_| "trae_login_callback_invalid")?;
                let code = code["AuthCode"]
                    .as_str()
                    .filter(|code| !code.is_empty() && code.len() <= 8192)
                    .ok_or("trae_login_callback_invalid")?;
                let exchange = http_json(&super::account_base(),auth::EXCHANGE_PATH,reqwest::Method::POST,
                    Some(json!({"ClientID":auth::CLIENT_ID,"AuthCode":code,"CodeVerifier":verifier,
                        "DeviceInfo":auth::device_info(&credential),"IDEVersion":credential.client_version})),Some(("",true))).await?;
                let mut connected = auth::exchanged(&exchange, credential)?;
                let profile = http_json(
                    &super::account_base(),
                    "/cloudide/api/v3/trae/GetUserInfo",
                    reqwest::Method::POST,
                    Some(json!({"ReqSource":"IDE","IDEVersion":connected.client_version})),
                    Some((&connected.access_token, true)),
                )
                .await?;
                connected.account = profile
                    .pointer("/Result/ScreenName")
                    .and_then(Value::as_str)
                    .filter(|label| !label.is_empty() && label.len() <= 320)
                    .map(str::to_owned);
                connected.user_id = profile
                    .pointer("/Result/UserID")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty() && id.len() <= 256)
                    .map(str::to_owned);
                publish(&background, &closed, connected).await?;
                let _ = reply(
                    &mut socket,
                    "200 OK",
                    "Your account is connected. You can return to Klaus.",
                )
                .await;
                return Ok(());
            }
        };
        let result: Result<(), String> = tokio::select! {
            result=operation=>result,
            _=cancellation.changed()=>Err("trae_login_cancelled".into()),
            _=tokio::time::sleep_until(tokio::time::Instant::from_std(flow.deadline))=>Err("trae_login_expired".into()),
        };
        if let Err(error) = result {
            fail(&flow, &error);
        }
    });
    json!({"ok":true,"flowId":id,"authUrl":authorize.as_str(),"expiresAt":auth::now()+300_000})
}

async fn callback(
    socket: &mut TcpStream,
    port: u16,
    state: &str,
) -> Result<std::collections::HashMap<String, String>, String> {
    let mut bytes = Vec::new();
    loop {
        if bytes.len() >= 16 * 1024 {
            return Err("trae_login_callback_invalid".into());
        }
        let mut part = [0u8; 1024];
        let length = tokio::time::timeout(Duration::from_secs(3), socket.read(&mut part))
            .await
            .map_err(|_| "trae_login_callback_invalid")?
            .map_err(|_| "trae_login_callback_invalid")?;
        if length == 0 {
            return Err("trae_login_callback_invalid".into());
        }
        bytes.extend_from_slice(&part[..length]);
        if bytes.windows(4).any(|part| part == b"\r\n\r\n") {
            break;
        }
    }
    let request = std::str::from_utf8(&bytes).map_err(|_| "trae_login_callback_invalid")?;
    let mut lines = request.split("\r\n");
    let first = lines
        .next()
        .ok_or("trae_login_callback_invalid")?
        .split_whitespace()
        .collect::<Vec<_>>();
    if first.len() != 3 || first[0] != "GET" || !first[1].starts_with("/authorize?") {
        return Err("trae_login_callback_invalid".into());
    }
    let expected = format!("127.0.0.1:{port}");
    if !lines.any(|line| {
        line.split_once(':').is_some_and(|(key, value)| {
            key.eq_ignore_ascii_case("host") && value.trim() == expected
        })
    }) {
        return Err("trae_login_callback_invalid".into());
    }
    let parsed = url::Url::parse(&format!("http://{expected}{}", first[1]))
        .map_err(|_| "trae_login_callback_invalid")?;
    let mut values = std::collections::HashMap::new();
    for (key, value) in parsed.query_pairs() {
        if values
            .insert(key.into_owned(), value.into_owned())
            .is_some()
        {
            return Err("trae_login_callback_invalid".into());
        }
    }
    if values.get("state").map(String::as_str) != Some(state) {
        return Err("trae_login_callback_invalid".into());
    }
    Ok(values)
}
async fn reply(socket: &mut TcpStream, status: &str, text: &str) -> std::io::Result<()> {
    socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n{text}",text.len()).as_bytes()).await
}
fn find(owner: usize, id: &str) -> Option<Arc<Flow>> {
    flows()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .find(|flow| flow.owner == owner && flow.id == id && flow.root == config_root())
        .cloned()
}
pub async fn poll(owner: usize, id: &str) -> Value {
    let Some(flow) = find(owner, id) else {
        return failure("trae_login_flow_unavailable");
    };
    if flow.deadline <= Instant::now() {
        fail(&flow, "trae_login_expired");
    }
    if matches!(
        *flow.outcome.lock().unwrap_or_else(|p| p.into_inner()),
        Outcome::Pending
    ) && (flow.expected_generation != auth::generation()
        || flow.expected_revision != auth::revision())
    {
        fail(&flow, "trae_login_credential_changed");
    }
    let outcome = flow
        .outcome
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone();
    match outcome {
        Outcome::Pending => json!({"ok":false,"pending":true,"status":"pending"}),
        Outcome::Complete(generation) if auth::is_current(&generation) => {
            json!({"ok":true,"status":"success","account":auth::status()["account"]})
        }
        Outcome::Complete(_) => failure("trae_login_credential_changed"),
        Outcome::Failed(message) => failure(&message),
    }
}

async fn publish(
    flow: &Flow,
    closed: &AtomicBool,
    credential: auth::Credential,
) -> Result<(), String> {
    let _lock = auth::lock().lock().await;
    let mut outcome = flow.outcome.lock().unwrap_or_else(|p| p.into_inner());
    if !matches!(*outcome, Outcome::Pending)
        || flow.root != config_root()
        || closed.load(Ordering::Acquire)
        || flow.deadline <= Instant::now()
    {
        return Err("trae_login_cancelled".into());
    }
    if flow.expected_generation != auth::generation() || flow.expected_revision != auth::revision()
    {
        return Err("trae_login_credential_changed".into());
    }
    let generation = auth::store_login(credential)?;
    *outcome = Outcome::Complete(generation);
    Ok(())
}
pub async fn cancel(owner: usize, id: &str) -> Value {
    if let Some(flow) = find(owner, id) {
        fail(&flow, "trae_login_cancelled");
    }
    json!({"ok":true})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn browser_start_registers_an_independent_device_and_validates_the_pkce_callback() {
        use bytes::Bytes;
        use http_body_util::{BodyExt, Full};
        use hyper::{body::Incoming, service::service_fn, Request, Response};
        let _guard = super::super::tests::root_lock().lock().await;
        let temp = tempfile::tempdir().unwrap();
        super::super::set_config_root(temp.path());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let exchanges = Arc::new(Mutex::new(Vec::<Value>::new()));
        let captured = exchanges.clone();
        let registrations = Arc::new(std::sync::atomic::AtomicU64::new(9123456789012345678));
        let server = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let captured = captured.clone();
                let registrations = registrations.clone();
                tokio::spawn(async move {
                    let handler = service_fn(move |request: Request<Incoming>| {
                        let captured = captured.clone();
                        let registrations = registrations.clone();
                        async move {
                            let path = request.uri().path().to_owned();
                            let body = request.into_body().collect().await.unwrap().to_bytes();
                            let value = match path.as_str() {
                                "/service/2/desktop/device_register/" => {
                                    assert_eq!(&body[..6], &[116, 99, 5, 16, 0, 0]);
                                    json!({"device_id_str":registrations.fetch_add(1,Ordering::AcqRel).to_string(),"install_id_str":"7123456789012345678"})
                                }
                                auth::EXCHANGE_PATH => {
                                    captured
                                        .lock()
                                        .unwrap()
                                        .push(serde_json::from_slice(&body).unwrap());
                                    json!({"Result":{"Token":"independent-access","RefreshToken":"independent-refresh","TokenExpireAt":auth::now()+3_600_000,"RefreshExpireAt":auth::now()+7_200_000}})
                                }
                                "/cloudide/api/v3/trae/GetUserInfo" => {
                                    json!({"Result":{"UserID":"browser-user","ScreenName":"Browser account"}})
                                }
                                _ => panic!("unexpected test request: {path}"),
                            };
                            Ok::<_, std::convert::Infallible>(Response::new(Full::new(
                                Bytes::from(value.to_string()),
                            )))
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(hyper_util::rt::TokioIo::new(socket), handler)
                        .await;
                });
            }
        });
        std::env::set_var("KLAUS_TRAE_DEVICE_URL", &origin);
        std::env::set_var("KLAUS_TRAE_ACCOUNT_URL", &origin);
        struct Environment;
        impl Drop for Environment {
            fn drop(&mut self) {
                std::env::remove_var("KLAUS_TRAE_DEVICE_URL");
                std::env::remove_var("KLAUS_TRAE_ACCOUNT_URL");
            }
        }
        let _environment = Environment;
        let closed = Arc::new(AtomicBool::new(false));
        let (private, public) = auth::generated_keys().unwrap();
        let proof = auth::LocalDeviceProof {
            device_id: "9123456789012345678".into(),
            machine_id: "existing-device-machine".into(),
            device_private_key: private,
            device_public_key: public,
        };
        let result = start(
            31,
            closed.clone(),
            std::slice::from_ref(&proof.device_id),
            Some(&proof),
        )
        .await;
        assert_eq!(result["ok"], true);
        let id = result["flowId"].as_str().unwrap();
        let authorize = url::Url::parse(result["authUrl"].as_str().unwrap()).unwrap();
        assert_eq!(
            authorize.origin().ascii_serialization(),
            "https://www.trae.cn"
        );
        let query = authorize
            .query_pairs()
            .into_owned()
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(query["device_id"], "9123456789012345678");
        assert_eq!(query["machine_id"], proof.machine_id);
        assert_eq!(query["code_challenge_method"], "S256");
        let mut redirect = url::Url::parse(&query["auth_callback_url"]).unwrap();
        assert_eq!(poll(31, id).await["pending"], true);
        let mut invalid = redirect.clone();
        invalid.set_query(Some(
            "state=invalid&authCodeInfo=%7B%22AuthCode%22%3A%22ignored%22%7D",
        ));
        assert_eq!(
            super::super::client()
                .get(invalid)
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::BAD_REQUEST
        );
        assert_eq!(poll(31, id).await["ok"], false);
        redirect
            .query_pairs_mut()
            .append_pair("authCodeInfo", r#"{"AuthCode":"test-code"}"#);
        assert_eq!(
            super::super::client()
                .get(redirect)
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::OK
        );
        assert_eq!(poll(31, id).await["ok"], true);
        assert_eq!(auth::status()["authMethod"], "browser");
        assert_eq!(auth::status()["userId"], "browser-user");
        let saved = auth::credential(None).await.unwrap();
        assert_eq!(
            saved.device_private_key.as_deref(),
            Some(proof.device_private_key.as_str())
        );
        assert_eq!(
            saved.device_public_key.as_deref(),
            Some(proof.device_public_key.as_str())
        );
        assert_eq!(saved.access_token, "independent-access");
        assert_eq!(saved.refresh_token.as_deref(), Some("independent-refresh"));
        {
            let exchanges = exchanges.lock().unwrap();
            assert_eq!(exchanges.len(), 1);
            assert_eq!(exchanges[0]["AuthCode"], "test-code");
            assert_eq!(exchanges[0]["DeviceInfo"]["DeviceID"], query["device_id"]);
            assert_eq!(
                exchanges[0]["DeviceInfo"]["DevicePublicKey"],
                proof.device_public_key
            );
            assert!(exchanges[0].get("RefreshToken").is_none());
            assert!(exchanges[0].get("DeviceProof").is_none());
            assert_eq!(
                URL_SAFE_NO_PAD.encode(Sha256::digest(
                    exchanges[0]["CodeVerifier"].as_str().unwrap().as_bytes()
                )),
                query["code_challenge"]
            );
        }
        let cancelled = start(31, closed.clone(), &[], Some(&proof)).await;
        let second_authorize = url::Url::parse(cancelled["authUrl"].as_str().unwrap()).unwrap();
        assert!(second_authorize
            .query_pairs()
            .any(|(key, value)| key == "machine_id" && value != proof.machine_id));
        let cancelled_id = cancelled["flowId"].as_str().unwrap();
        cancel(31, cancelled_id).await;
        assert_eq!(
            poll(31, cancelled_id).await["error"],
            "trae_login_cancelled"
        );
        let rejected = start(31, closed, &["9123456789012345680".into()], Some(&proof)).await;
        assert_eq!(rejected["error"], "trae_device_identity_conflict");
        server.abort();
    }

    fn pending(owner: usize) -> Arc<Flow> {
        let flow = Arc::new(Flow {
            id: uuid::Uuid::new_v4().to_string(),
            owner,
            root: config_root(),
            expected_generation: auth::generation(),
            expected_revision: auth::revision(),
            deadline: Instant::now() + Duration::from_secs(60),
            cancelled: watch::channel(false).0,
            outcome: Mutex::new(Outcome::Pending),
        });
        flows().lock().unwrap().push_back(flow.clone());
        flow
    }

    #[tokio::test]
    async fn pending_poll_never_signals_success_and_newer_login_fences_late_callback() {
        let _guard = super::super::tests::root_lock().lock().await;
        let temp = tempfile::tempdir().unwrap();
        super::super::set_config_root(temp.path());
        let old = pending(17);
        let current = pending(19);
        let waiting = poll(17, &old.id).await;
        assert_eq!(
            waiting,
            json!({"ok":false,"pending":true,"status":"pending"})
        );
        assert_eq!(
            poll(19, &old.id).await["error"],
            "trae_login_flow_unavailable"
        );
        let closed = AtomicBool::new(false);
        let mut credential = super::super::tests::credential();
        credential.user_id = Some("new-account".into());
        publish(&current, &closed, credential).await.unwrap();
        let generation = auth::generation().unwrap();
        assert_eq!(poll(19, &current.id).await["status"], "success");
        assert_eq!(
            publish(&old, &closed, super::super::tests::credential())
                .await
                .unwrap_err(),
            "trae_login_credential_changed"
        );
        assert!(auth::is_current(&generation));
        assert_eq!(auth::status()["userId"], "new-account");
        assert_eq!(poll(17, &old.id).await["ok"], false);
    }

    #[tokio::test]
    async fn adoption_logout_and_disconnect_prevent_callback_publication() {
        let _guard = super::super::tests::root_lock().lock().await;
        let temp = tempfile::tempdir().unwrap();
        super::super::set_config_root(temp.path());
        let closed = AtomicBool::new(false);
        let adopted = pending(21);
        auth::adopt(super::super::tests::credential())
            .await
            .unwrap();
        assert!(
            publish(&adopted, &closed, super::super::tests::credential())
                .await
                .is_err()
        );
        let logged_out = pending(21);
        auth::logout().await.unwrap();
        assert!(
            publish(&logged_out, &closed, super::super::tests::credential())
                .await
                .is_err()
        );
        assert_eq!(auth::status()["signedIn"], false);
        let absent_generation_revision = auth::revision();
        auth::logout().await.unwrap();
        assert_ne!(auth::revision(), absent_generation_revision);
        let disconnected = pending(23);
        let other = pending(24);
        disconnect(23);
        assert!(
            publish(&disconnected, &closed, super::super::tests::credential())
                .await
                .is_err()
        );
        publish(&other, &closed, super::super::tests::credential())
            .await
            .unwrap();
        assert_eq!(auth::status()["authMethod"], "browser");
    }
}
