//! Connection-owned PKCE browser authorization.
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
static EPOCH: AtomicU64 = AtomicU64::new(0);
struct Flow {
    owner: usize,
    epoch: u64,
    result: Value,
    abort: tokio::sync::watch::Sender<bool>,
}
fn flows() -> &'static Mutex<HashMap<String, Flow>> {
    static FLOWS: OnceLock<Mutex<HashMap<String, Flow>>> = OnceLock::new();
    FLOWS.get_or_init(|| Mutex::new(HashMap::new()))
}
pub(super) fn epoch() -> u64 {
    EPOCH.load(Ordering::SeqCst)
}
pub(super) fn is_epoch(epoch: u64) -> bool {
    self::epoch() == epoch
}
pub(super) fn ownership_gate() -> std::sync::MutexGuard<'static, ()> {
    static GATE: OnceLock<Mutex<()>> = OnceLock::new();
    GATE.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|p| p.into_inner())
}
pub(super) fn invalidate_pending_locked() {
    EPOCH.fetch_add(1, Ordering::SeqCst);
    let mut f = flows().lock().unwrap_or_else(|p| p.into_inner());
    for flow in f.values() {
        flow.abort.send_replace(true);
    }
    f.clear();
}
pub fn invalidate_pending() {
    let _gate = ownership_gate();
    invalidate_pending_locked();
}
pub(super) fn with_invalidation<T>(work: impl FnOnce() -> T) -> T {
    let _gate = ownership_gate();
    invalidate_pending_locked();
    work()
}
fn begin_epoch() -> u64 {
    let _gate = ownership_gate();
    invalidate_pending_locked();
    EPOCH.load(Ordering::SeqCst)
}
pub struct ConnectionGuard(usize);
impl ConnectionGuard {
    pub fn new(owner: usize) -> Self {
        Self(owner)
    }
}
impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        let _gate = ownership_gate();
        let mut f = flows().lock().unwrap_or_else(|p| p.into_inner());
        f.retain(|_, v| {
            if v.owner == self.0 {
                if v.result["pending"] == true {
                    let _ = EPOCH.compare_exchange(
                        v.epoch,
                        v.epoch.wrapping_add(1),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    );
                }
                v.abort.send_replace(true);
                false
            } else {
                true
            }
        });
    }
}
pub async fn start(owner: usize, closed: Arc<AtomicBool>) -> Value {
    if closed.load(Ordering::Acquire) {
        return json!({"ok":false,"error":"kiro_login_cancelled"});
    }
    let epoch = begin_epoch();
    let mut listener = None;
    for port in [
        3128, 4649, 6588, 8008, 9091, 49153, 50153, 51153, 52153, 53153,
    ] {
        if let Ok(l) = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).await {
            listener = Some(l);
            break;
        }
    }
    let Some(listener) = listener else {
        return json!({"ok":false,"error":"kiro_login_port_unavailable"});
    };
    let Ok(address) = listener.local_addr() else {
        return json!({"ok":false,"error":"kiro_login_port_unavailable"});
    };
    let base = format!("http://localhost:{}", address.port());
    let id = uuid::Uuid::new_v4().to_string();
    let state = uuid::Uuid::new_v4().to_string();
    let verifier = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let mut url = url::Url::parse("https://app.kiro.dev/signin").expect("static URL");
    url.query_pairs_mut()
        .append_pair("state", &state)
        .append_pair("code_challenge", &challenge)
        .append_pair("code_challenge_method", "S256")
        .append_pair("redirect_uri", &base)
        .append_pair("redirect_from", "KiroIDE");
    let ownership = ownership_gate();
    if !is_epoch(epoch) || closed.load(Ordering::Acquire) {
        return json!({"ok":false,"error":"kiro_login_cancelled"});
    }
    let (abort, mut aborted) = tokio::sync::watch::channel(false);
    flows().lock().unwrap_or_else(|p| p.into_inner()).insert(
        id.clone(),
        Flow {
            owner,
            epoch,
            result: json!({"ok":true,"status":"pending","pending":true}),
            abort,
        },
    );
    drop(ownership);
    let task_id = id.clone();
    tokio::spawn(async move {
        let result = tokio::select! {result=authorize(listener,&base,&state,&verifier,epoch)=>result,_=aborted.changed()=>return,_=tokio::time::sleep(Duration::from_secs(600))=>Err("kiro_login_expired".into())};
        let result = match result {
            Ok(value) => value,
            Err(error) => json!({"ok":false,"status":"failed","error":error}),
        };
        if let Some(flow) = flows()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get_mut(&task_id)
            .filter(|flow| flow.epoch == epoch)
        {
            flow.result = result;
        }
    });
    json!({"ok":true,"flowId":id,"authUrl":url.as_str(),"expiresAt":super::auth::now()+600_000,"interval":1})
}
async fn authorize(
    listener: TcpListener,
    base: &str,
    state: &str,
    verifier: &str,
    epoch: u64,
) -> Result<Value, String> {
    loop {
        let (mut socket, remote) = listener
            .accept()
            .await
            .map_err(|_| "kiro_login_listener_failed")?;
        if !remote.ip().is_loopback() {
            continue;
        }
        let mut bytes = vec![0; 16384];
        let n = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut bytes))
            .await
            .map_err(|_| "kiro_login_callback_timeout")?
            .map_err(|_| "kiro_login_callback_invalid")?;
        let text = std::str::from_utf8(&bytes[..n]).map_err(|_| "kiro_login_callback_invalid")?;
        let line = text.lines().next().unwrap_or_default();
        let target = line
            .strip_prefix("GET ")
            .and_then(|v| v.split_once(' '))
            .map(|v| v.0)
            .unwrap_or_default();
        let url = url::Url::parse(&format!("{base}{target}"))
            .map_err(|_| "kiro_login_callback_invalid")?;
        let params: HashMap<_, _> = url.query_pairs().into_owned().collect();
        if !matches!(url.path(), "/oauth/callback" | "/signin/callback")
            || params.get("state").map(String::as_str) != Some(state)
        {
            let _=socket.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 16\r\nConnection: close\r\n\r\nInvalid callback").await;
            continue;
        }
        let option = params
            .get("login_option")
            .map(String::as_str)
            .ok_or("kiro_login_callback_invalid")?;
        let provider = match option {
            "google" => "Google",
            "github" => "Github",
            _ => return Err("kiro_login_method_unsupported".into()),
        };
        let code = params
            .get("code")
            .filter(|v| !v.is_empty() && v.len() <= 8192)
            .ok_or("kiro_login_callback_invalid")?;
        let redirect = format!("{base}{}?login_option={option}", url.path());
        let response = super::client()
            .post(format!("{}/oauth/token", super::auth_base()))
            .header("user-agent", "KiroIDE-1.4.26-Klaus")
            .json(&json!({"code":code,"code_verifier":verifier,"redirect_uri":redirect}))
            .send()
            .await
            .map_err(|_| "kiro_login_exchange_failed")?;
        let value = super::bounded_json(response).await?;
        let seconds = value["expiresIn"]
            .as_i64()
            .filter(|v| *v > 0 && *v <= 604800)
            .ok_or("kiro_login_response_invalid")?;
        let c = super::auth::Credential {
            access_token: value["accessToken"]
                .as_str()
                .ok_or("kiro_login_response_invalid")?
                .into(),
            refresh_token: value["refreshToken"].as_str().map(str::to_owned),
            profile_arn: value["profileArn"].as_str().map(str::to_owned),
            expires_at: Some(
                (chrono::Utc::now() + chrono::Duration::seconds(seconds)).to_rfc3339(),
            ),
            auth_method: "social".into(),
            provider: provider.into(),
            region: None,
            client_id: None,
            client_secret: None,
            account: None,
        };
        super::auth::store_login(c, epoch).await?;
        let body =
            "<!doctype html><title>Klaus</title><p>Kiro is connected. You can return to Klaus.</p>";
        let reply=format!("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
        let _ = socket.write_all(reply.as_bytes()).await;
        return Ok(
            json!({"ok":true,"status":"complete","pending":false,"account":super::auth::status()["account"],"statusInfo":super::auth::status()}),
        );
    }
}
pub async fn poll(owner: usize, id: &str) -> Value {
    flows()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(id)
        .filter(|flow| flow.owner == owner)
        .map(|flow| flow.result.clone())
        .unwrap_or_else(|| json!({"ok":false,"error":"kiro_login_cancelled"}))
}
pub async fn cancel(owner: usize, id: &str) -> Value {
    let _gate = ownership_gate();
    let mut flows = flows().lock().unwrap_or_else(|p| p.into_inner());
    if flows.get(id).is_some_and(|flow| flow.owner == owner) {
        if let Some(flow) = flows.remove(id) {
            if flow.result["pending"] == true {
                let _ = EPOCH.compare_exchange(
                    flow.epoch,
                    flow.epoch.wrapping_add(1),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
            }
            flow.abort.send_replace(true);
        }
    }
    json!({"ok":true,"cancelled":true})
}
