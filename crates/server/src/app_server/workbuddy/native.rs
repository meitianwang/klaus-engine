//! Bounded account services shared by the media and search facade.
use super::auth;
use base64::{engine::general_purpose::STANDARD, Engine};
use futures::StreamExt;
use klaus_agent_core::types::abort::AbortSignal;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

pub(crate) const MAX_IMAGE: usize = 16 * 1024 * 1024;
pub(crate) const MAX_AUDIO: usize = 16 * 1024 * 1024;
pub(crate) const MAX_VIDEO: usize = 48 * 1024 * 1024;
pub(crate) const MAX_JSON: usize = 2 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct Identity {
    fingerprint: Vec<u8>,
    base: String,
}
impl Identity {
    fn current() -> Result<Self, String> {
        let credential = auth::credential().ok_or("workbuddy_not_signed_in")?;
        Self::from_credential(&credential, auth::api_base())
    }
    fn from_credential(credential: &(String, Value), base: String) -> Result<Self, String> {
        let bytes = serde_json::to_vec(credential).map_err(|_| "workbuddy_credential_invalid")?;
        Ok(Self {
            fingerprint: Sha256::digest(bytes).to_vec(),
            base,
        })
    }
    fn generation(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(&self.fingerprint);
        digest.update(self.base.as_bytes());
        format!("{:x}", digest.finalize())
    }
    fn check(&self) -> Result<(), String> {
        let current = Self::current().map_err(|_| "workbuddy_credential_changed")?;
        if self.fingerprint != current.fingerprint || self.base != current.base {
            return Err("workbuddy_credential_changed".into());
        }
        Ok(())
    }
}
pub struct NativeResponse {
    pub value: Value,
    pub(crate) identity: Identity,
}
#[derive(Default)]
pub(crate) struct Operations {
    pending: Mutex<HashMap<String, AbortSignal>>,
}
pub(crate) struct Operation {
    owner: Arc<Operations>,
    id: String,
    signal: AbortSignal,
    identity: Option<Identity>,
}
fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
}
impl Operations {
    pub fn register(self: &Arc<Self>, id: Option<&str>) -> Result<Operation, String> {
        if id.is_some_and(|id| !valid_id(id)) {
            return Err("workbuddy_operation_id_invalid".into());
        }
        let id = id
            .map(str::to_owned)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        if pending.len() >= 4 {
            return Err("workbuddy_operation_limit".into());
        }
        if pending.contains_key(&id) {
            return Err("workbuddy_operation_id_in_use".into());
        }
        let signal = AbortSignal::new();
        pending.insert(id.clone(), signal.clone());
        Ok(Operation {
            owner: self.clone(),
            id,
            signal,
            identity: None,
        })
    }
    pub fn cancel(&self, id: &str) -> Result<Value, String> {
        if !valid_id(id) {
            return Err("workbuddy_operation_id_invalid".into());
        }
        let pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        let found = pending.get(id);
        if let Some(signal) = found {
            signal.cancel();
        }
        Ok(json!({"ok":true,"cancelled":found.is_some()}))
    }
    pub fn cancel_all(&self) {
        for signal in self
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
        {
            signal.cancel();
        }
    }
}
impl Operation {
    pub fn signal(&self) -> AbortSignal {
        self.signal.clone()
    }
    pub fn bind(&mut self, identity: Identity) {
        self.identity = Some(identity);
    }
    pub fn publication_error(&self) -> Option<&'static str> {
        if self.signal.is_cancelled() {
            return Some("workbuddy_operation_cancelled");
        }
        if self.identity.as_ref().is_some_and(|i| i.check().is_err()) {
            return Some("workbuddy_credential_changed");
        }
        None
    }
}
impl Drop for Operation {
    fn drop(&mut self) {
        self.owner
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.id);
    }
}
pub(crate) async fn bounded<T>(
    signal: &AbortSignal,
    seconds: u64,
    work: impl Future<Output = Result<T, String>>,
) -> Result<T, String> {
    tokio::select! { biased; _=signal.cancelled()=>Err("workbuddy_operation_cancelled".into()), r=tokio::time::timeout(Duration::from_secs(seconds),work)=>{let value=r.map_err(|_|"workbuddy_operation_timed_out")??;if signal.is_cancelled(){Err("workbuddy_operation_cancelled".into())}else{Ok(value)}} }
}
pub(crate) fn text<'a>(s: &'a str, max: usize, error: &str) -> Result<&'a str, String> {
    if s.trim().is_empty() || s.len() > max || s.contains('\0') {
        Err(error.into())
    } else {
        Ok(s)
    }
}
pub(crate) fn decode(s: &str, max: usize) -> Result<Vec<u8>, String> {
    if s.len() > max.div_ceil(3) * 4 {
        return Err("workbuddy_media_too_large".into());
    }
    let bytes = STANDARD.decode(s).map_err(|_| "workbuddy_media_invalid")?;
    if bytes.is_empty() || bytes.len() > max {
        return Err("workbuddy_media_too_large".into());
    }
    Ok(bytes)
}
pub(crate) fn image_mime(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("image/png")
    } else if bytes.starts_with(&[255, 216, 255]) {
        Some("image/jpeg")
    } else if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
        Some("image/webp")
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        Some("image/gif")
    } else {
        None
    }
}
fn field(v: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|k| match &v[*k] {
        Value::String(s) if !s.is_empty() => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    })
}
pub(crate) struct NativeSession {
    token: String,
    profile: Value,
    pub base: String,
    pub client: reqwest::Client,
    identity: Identity,
}
impl NativeSession {
    pub async fn start() -> Result<Self, String> {
        auth::ensure_fresh_token().await?;
        let credential = auth::credential().ok_or("workbuddy_not_signed_in")?;
        let profile = credential.1.clone();
        let token =
            field(&profile, &["access_token", "accessToken"]).ok_or("workbuddy_not_signed_in")?;
        let base = auth::api_base();
        let url = reqwest::Url::parse(&base).map_err(|_| "workbuddy_origin_invalid")?;
        if url.username() != ""
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || !(url.scheme() == "https"
                || url.scheme() == "http"
                    && matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "[::1]")))
        {
            return Err("workbuddy_origin_invalid".into());
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(120))
            .build()
            .map_err(|_| "workbuddy_network_error")?;
        let identity = Identity::from_credential(&credential, base.clone())?;
        identity.check()?;
        Ok(Self {
            token,
            profile,
            base,
            client,
            identity,
        })
    }
    pub async fn start_expected(expected: Option<&str>) -> Result<Self, String> {
        if let Some(expected) = expected {
            let identity = Identity::current().map_err(|_| "workbuddy_credential_changed")?;
            if identity.generation() != expected {
                return Err("workbuddy_credential_changed".into());
            }
        }
        let session = Self::start().await?;
        if expected.is_some_and(|expected| session.generation() != expected) {
            return Err("workbuddy_credential_changed".into());
        }
        Ok(session)
    }
    pub fn generation(&self) -> String {
        self.identity.generation()
    }
    pub fn check(&self) -> Result<(), String> {
        self.identity.check()
    }
    pub fn response(&self, value: Value) -> Result<NativeResponse, String> {
        self.check()?;
        Ok(NativeResponse {
            value,
            identity: self.identity.clone(),
        })
    }
    pub fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        session_id: Option<&str>,
    ) -> Result<reqwest::RequestBuilder, String> {
        self.check()?;
        if session_id.is_some_and(|id| !valid_id(id)) {
            return Err("workbuddy_session_id_invalid".into());
        }
        let request_id = uuid::Uuid::new_v4().to_string();
        let mut r = self
            .client
            .request(
                method,
                format!("{}{}", self.base.trim_end_matches('/'), path),
            )
            .bearer_auth(&self.token)
            .header("User-Agent", super::catalog::user_agent())
            .header("Accept", "application/json")
            .header("X-Requested-With", "XMLHttpRequest")
            .header("X-Request-Id", &request_id)
            .header("X-Conversation-Id", session_id.unwrap_or(&request_id))
            .header("X-Conversation-Request-Id", &request_id)
            .header("X-Conversation-Message-Id", &request_id);
        for (keys, header) in [
            (&["uid"][..], "X-User-Id"),
            (&["domain"][..], "X-Domain"),
            (&["enterprise_id", "enterpriseId"][..], "X-Enterprise-Id"),
        ] {
            if let Some(value) = field(&self.profile, keys) {
                r = r.header(header, &value);
                if header == "X-Enterprise-Id" {
                    r = r.header("X-Tenant-Id", value);
                }
            }
        }
        let uid = field(&self.profile, &["uid"]);
        let enterprise = field(&self.profile, &["enterprise_id", "enterpriseId"]);
        let id_source = field(&self.profile, &["id_source", "idSource"]);
        let method = field(&self.profile, &["auth_method", "authMethod"]);
        if let Some(value) = &id_source {
            r = r.header("X-Id-Source", value);
        }
        if let Some(value) = &method {
            r = r.header("X-Auth-Method", value);
        }
        if let Some(uid) =
            uid.filter(|_| enterprise.is_some() || id_source.is_some() || method.is_some())
        {
            let mut info = json!({"uin":uid});
            if let Some(value) = enterprise {
                info["owner_uin"] = json!(value);
            }
            if let Some(value) = id_source {
                info["id_source"] = json!(value);
            }
            if let Some(value) = method {
                info["token_source"] = json!(value);
            }
            r = r.header(
                "X-Userinfo",
                STANDARD
                    .encode(serde_json::to_vec(&info).map_err(|_| "workbuddy_credential_invalid")?),
            );
        }
        Ok(r)
    }
    pub async fn json(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
        session_id: Option<&str>,
        limit: usize,
    ) -> Result<Value, String> {
        let mut request = self.request(method, path, session_id)?;
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request
            .send()
            .await
            .map_err(|_| "workbuddy_network_error")?;
        let status = response.status().as_u16();
        if matches!(status, 401 | 403 | 429) {
            check_error(status, &Value::Null)?;
        }
        let bytes = read_bytes(response, limit).await?;
        self.check()?;
        let value =
            serde_json::from_slice::<Value>(&bytes).map_err(|_| "workbuddy_invalid_response")?;
        check_error(status, &value)?;
        Ok(value)
    }
    pub async fn config(&self) -> Result<Value, String> {
        let v = self
            .json(reqwest::Method::GET, "/v3/config", None, None, MAX_JSON)
            .await?;
        Ok(super::metadata::effective(&v))
    }
    pub async fn download(&self, url: &str, max: usize) -> Result<Vec<u8>, String> {
        let mut url = reqwest::Url::parse(url).map_err(|_| "workbuddy_artifact_url_invalid")?;
        for _ in 0..4 {
            if url.scheme() != "https" || url.username() != "" || url.password().is_some() {
                return Err("workbuddy_artifact_url_invalid".into());
            }
            if let Some(host) = url.host_str() {
                if host == "localhost" || host.ends_with(".localhost") || host.ends_with(".local") {
                    return Err("workbuddy_artifact_url_invalid".into());
                }
                if let Ok(ip) = host.trim_matches(['[', ']']).parse::<std::net::IpAddr>() {
                    match ip {
                        std::net::IpAddr::V4(ip)
                            if ip.is_private()
                                || ip.is_loopback()
                                || ip.is_link_local()
                                || ip.is_unspecified() =>
                        {
                            return Err("workbuddy_artifact_url_invalid".into())
                        }
                        std::net::IpAddr::V6(ip)
                            if ip.is_loopback()
                                || ip.is_unspecified()
                                || ip.segments()[0] & 0xfe00 == 0xfc00
                                || ip.segments()[0] & 0xffc0 == 0xfe80 =>
                        {
                            return Err("workbuddy_artifact_url_invalid".into())
                        }
                        _ => {}
                    }
                }
            }
            let r = self
                .client
                .get(url.clone())
                .send()
                .await
                .map_err(|_| "workbuddy_download_failed")?;
            if r.status().is_redirection() {
                let next = r
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|v| v.to_str().ok())
                    .ok_or("workbuddy_artifact_url_invalid")?;
                url = url
                    .join(next)
                    .map_err(|_| "workbuddy_artifact_url_invalid")?;
                continue;
            }
            if !r.status().is_success() {
                return Err("workbuddy_download_failed".into());
            }
            let bytes = read_bytes(r, max).await?;
            self.check()?;
            return Ok(bytes);
        }
        Err("workbuddy_artifact_redirect_limit".into())
    }
}
pub(crate) async fn read_bytes(response: reqwest::Response, max: usize) -> Result<Vec<u8>, String> {
    if response.content_length().is_some_and(|n| n > max as u64) {
        return Err("workbuddy_response_too_large".into());
    }
    let mut out = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| "workbuddy_response_read_failed")?;
        if chunk.len() > max.saturating_sub(out.len()) {
            return Err("workbuddy_response_too_large".into());
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}
pub(crate) fn check_error(status: u16, v: &Value) -> Result<(), String> {
    if status == 401 {
        return Err("workbuddy_login_expired".into());
    }
    if status == 403 {
        return Err("workbuddy_service_forbidden".into());
    }
    if status == 429 {
        return Err("workbuddy_rate_limit".into());
    }
    if !(200..300).contains(&status) {
        return Err("workbuddy_service_unavailable".into());
    }
    if v.get("code").is_some_and(|code| code != 0 && code != "0") {
        return Err("workbuddy_service_rejected".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn operation_slots_survive_cancellation_until_publication_guard_drops() {
        let ops = Arc::new(Operations::default());
        let first = ops.register(Some("owned")).unwrap();
        assert!(ops.register(Some("owned")).is_err());
        let others: Vec<_> = (0..3).map(|_| ops.register(None).unwrap()).collect();
        assert!(ops.register(None).is_err());
        assert_eq!(ops.cancel("owned").unwrap()["cancelled"], true);
        assert_eq!(
            first.publication_error(),
            Some("workbuddy_operation_cancelled")
        );
        assert!(ops.register(None).is_err());
        drop(first);
        assert!(ops.register(None).is_ok());
        drop(others);
    }
    #[tokio::test]
    async fn cancellation_drops_inflight_work_without_waiting_for_deadline() {
        let signal = AbortSignal::new();
        let child = signal.clone();
        let work = bounded(&signal, 10, async {
            child.cancel();
            std::future::pending::<Result<(), String>>().await
        });
        assert_eq!(work.await.unwrap_err(), "workbuddy_operation_cancelled");
    }
    #[test]
    fn response_bounds_and_image_magic_reject_invalid_payloads() {
        assert!(decode("not base64", 10).is_err());
        assert!(decode(&STANDARD.encode([1; 11]), 10).is_err());
        assert_eq!(image_mime(b"\x89PNG\r\n\x1a\n"), Some("image/png"));
        assert_eq!(image_mime(b"<html>"), None);
        assert!(valid_id("owned-id_1"));
        assert!(!valid_id("injected\nheader"));
    }
    #[tokio::test]
    async fn queued_publication_refuses_changed_or_removed_credentials() {
        let _lock = super::super::test_env_lock().lock().await;
        let root = tempfile::tempdir().unwrap();
        super::super::set_config_root(root.path());
        let path = root.path().join("workbuddy-subscription-oauth.json");
        std::fs::write(
            &path,
            json!({"deviceId":"fixture","userInfo":{"access_token":"private","uid":"fixture"}})
                .to_string(),
        )
        .unwrap();
        let ops = Arc::new(Operations::default());
        let mut operation = ops.register(Some("publication")).unwrap();
        operation.bind(Identity::current().unwrap());
        assert_eq!(operation.publication_error(), None);
        std::fs::remove_file(path).unwrap();
        assert_eq!(
            operation.publication_error(),
            Some("workbuddy_credential_changed")
        );
    }
    #[tokio::test]
    async fn blocked_output_stops_on_cancellation_or_credential_rotation() {
        use tokio::io::AsyncReadExt;
        let _lock = super::super::test_env_lock().lock().await;
        let root = tempfile::tempdir().unwrap();
        super::super::set_config_root(root.path());
        let path = root.path().join("workbuddy-subscription-oauth.json");
        for cancelled in [true, false] {
            std::fs::write(&path, json!({"deviceId":"fixture","userInfo":{"access_token":"old-secret","uid":"fixture"}}).to_string()).unwrap();
            let ops = Arc::new(Operations::default());
            let mut operation = ops.register(Some("blocked-output")).unwrap();
            operation.bind(Identity::current().unwrap());
            let (tx, rx) = tokio::sync::mpsc::channel(128);
            tx.send(super::super::super::service::Outgoing {
                message: json!({"id":1,"result":{"audioBase64":format!("{}PRIVATE_END", "x".repeat(65536))}}).into(),
                cursor_operation: None,
                workbuddy_operation: Some(operation),
                provider_operation: None,
            }).await.unwrap_or_else(|_| panic!("output queue"));
            drop(tx);
            let (mut reader, output) = tokio::io::duplex(32);
            let writer = tokio::spawn(super::super::super::service::write_app_output(output, rx));
            let mut prefix = [0; 32];
            reader.read_exact(&mut prefix).await.unwrap();
            assert!(!writer.is_finished());
            if cancelled {
                ops.cancel("blocked-output").unwrap();
            } else {
                std::fs::remove_file(&path).unwrap();
            }
            tokio::time::timeout(Duration::from_secs(1), writer)
                .await
                .unwrap()
                .unwrap();
            let mut tail = Vec::new();
            reader.read_to_end(&mut tail).await.unwrap();
            assert!(!String::from_utf8_lossy(&tail).contains("PRIVATE_END"));
            assert!(ops.register(Some("blocked-output")).is_ok());
        }
    }

    #[tokio::test]
    async fn enterprise_headers_preserve_the_real_uid_and_tenant_without_invented_sources() {
        let _lock = super::super::test_env_lock().lock().await;
        let root = tempfile::tempdir().unwrap();
        super::super::set_config_root(root.path());
        std::fs::write(root.path().join("workbuddy-subscription-oauth.json"),json!({"deviceId":"fixture","userInfo":{"access_token":"private","uid":"uid-fixture","uin":"different-legacy-number","enterprise_id":"tenant-fixture","domain":"https://example.com","login_method":"adopted"}}).to_string()).unwrap();
        let session = NativeSession::start().await.unwrap();
        let request = session
            .request(reqwest::Method::GET, "/v3/config", None)
            .unwrap()
            .build()
            .unwrap();
        let info = request
            .headers()
            .get("X-Userinfo")
            .unwrap()
            .to_str()
            .unwrap();
        let info: Value = serde_json::from_slice(&STANDARD.decode(info).unwrap()).unwrap();
        assert_eq!(
            info,
            json!({"uin":"uid-fixture","owner_uin":"tenant-fixture"})
        );
        assert!(request.headers().get("X-Auth-Method").is_none());
        assert!(request.headers().get("X-Id-Source").is_none());
    }
    #[test]
    fn endpoint_refusal_is_distinct_from_an_expired_login() {
        assert_eq!(
            check_error(401, &Value::Null).unwrap_err(),
            "workbuddy_login_expired"
        );
        assert_eq!(
            check_error(403, &json!({"code":12005})).unwrap_err(),
            "workbuddy_service_forbidden"
        );
        assert_eq!(
            check_error(429, &Value::Null).unwrap_err(),
            "workbuddy_rate_limit"
        );
    }
}
