//! Independent native media and search operations using the account's service grant.
use super::{auth, catalog};
use crate::app_server::wire::{
    QwenWorkImageParams, QwenWorkMusicParams, QwenWorkSearchParams, QwenWorkVideoParams,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures::StreamExt;
use klaus_agent_core::types::abort::AbortSignal;
use serde_json::{json, Value};
use std::{future::Future, sync::Arc, time::Duration};

const MAX_JSON: usize = 2 * 1024 * 1024;
const MAX_IMAGE: usize = 16 * 1024 * 1024;
const MAX_VIDEO: usize = 48 * 1024 * 1024;
const MAX_AUDIO: usize = 32 * 1024 * 1024;
const IMAGE_MODELS: &[&str] = &["open-image-2", "banana-image-2"];
const VIDEO_MODELS: &[&str] = &["seedance-2.0", "happy-horse-1.0"];
const MUSIC_MODELS: &[&str] = &["minimax-music-2.5"];
const ACTIVE: &[&str] = &["pending", "queued", "running", "processing"];

pub(crate) fn changes() -> &'static tokio::sync::watch::Sender<u64> {
    static CHANGES: std::sync::OnceLock<tokio::sync::watch::Sender<u64>> =
        std::sync::OnceLock::new();
    CHANGES.get_or_init(|| tokio::sync::watch::channel(0).0)
}

pub(crate) fn credential_changed() {
    changes().send_modify(|generation| *generation = generation.wrapping_add(1));
}

fn generation() -> u64 {
    *changes().borrow()
}

pub struct NativeResponse {
    pub(crate) value: Value,
    pub(crate) generation: u64,
}

pub(crate) fn current_generation() -> u64 {
    generation()
}

pub fn capabilities() -> Value {
    json!({"authenticated":auth::status()["loggedIn"] == true,
        "image":{"available":true,"models":IMAGE_MODELS,"defaultModel":IMAGE_MODELS[0],"referenceImages":4},
        "imageEdit":{"available":true,"models":IMAGE_MODELS,"defaultModel":IMAGE_MODELS[0],"referenceImages":4},
        "video":{"available":true,"models":VIDEO_MODELS,"defaultModel":VIDEO_MODELS[0],"minSeconds":4,"maxSeconds":15},
        "music":{"available":true,"models":MUSIC_MODELS,"defaultModel":MUSIC_MODELS[0]},
        "audio":{"available":false},"transcription":{"available":true,"streaming":true},"search":{"available":true},"fetch":{"available":false}})
}

fn text(value: &str, limit: usize, name: &str) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty() || value.chars().count() > limit || value.contains('\0') {
        return Err(format!("qwenwork_{name}_invalid"));
    }
    Ok(value.to_string())
}
fn model(value: Option<&str>, allowed: &[&str]) -> Result<String, String> {
    match value {
        None => Ok(allowed[0].into()),
        Some(value) if allowed.contains(&value) => Ok(value.into()),
        _ => Err("qwenwork_media_model_unsupported".into()),
    }
}
fn session_id(value: Option<&str>) -> Result<String, String> {
    match value {
        None => Ok(uuid::Uuid::new_v4().to_string()),
        Some(value) if value.chars().any(char::is_control) => {
            Err("qwenwork_session_id_invalid".into())
        }
        Some(value) => text(value, 200, "session_id"),
    }
}

fn mime(bytes: &[u8], kind: &str) -> Result<&'static str, String> {
    match kind {
        "image" if bytes.starts_with(b"\x89PNG\r\n\x1a\n") => Ok("image/png"),
        "image" if bytes.starts_with(b"\xff\xd8\xff") => Ok("image/jpeg"),
        "image" if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") => {
            Ok("image/webp")
        }
        "music"
            if bytes.starts_with(b"ID3")
                || bytes.len() >= 2 && bytes[0] == 0xff && bytes[1] & 0xe0 == 0xe0 =>
        {
            Ok("audio/mpeg")
        }
        "video" if bytes.get(4..8) == Some(b"ftyp") && bytes.len() >= 12 => Ok("video/mp4"),
        _ => Err("qwenwork_media_format_invalid".into()),
    }
}
fn image_request(params: QwenWorkImageParams) -> Result<MediaRequest, String> {
    let selected = model(params.model.as_deref(), IMAGE_MODELS)?;
    let mut body = json!({"prompt":text(&params.prompt, 2500, "prompt")?});
    if let Some(size) = params.size {
        let known = [
            "1024x1024",
            "1536x1024",
            "1024x1536",
            "2048x2048",
            "2048x1152",
            "3840x2160",
            "2160x3840",
        ];
        if !known.contains(&size.as_str()) {
            return Err("qwenwork_image_size_unsupported".into());
        }
        if selected == "banana-image-2" {
            return Err("qwenwork_image_size_unsupported".into());
        }
        body["size"] = json!(size);
    }
    if params.reference_images.len() > 4 {
        return Err("qwenwork_reference_images_invalid".into());
    }
    let mut total = 0;
    let mut images = Vec::new();
    for image in params.reference_images {
        if image.data_base64.len() > 4 * 1024 * 1024 * 4 / 3 + 4 {
            return Err("qwenwork_reference_images_too_large".into());
        }
        let bytes = STANDARD
            .decode(&image.data_base64)
            .map_err(|_| "qwenwork_reference_image_invalid")?;
        total += bytes.len();
        if bytes.is_empty() || bytes.len() > 4 * 1024 * 1024 || total > 8 * 1024 * 1024 {
            return Err("qwenwork_reference_images_too_large".into());
        }
        if mime(&bytes, "image")? != image.mime_type {
            return Err("qwenwork_reference_image_invalid".into());
        }
        images.push(format!(
            "data:{};base64,{}",
            image.mime_type,
            STANDARD.encode(bytes)
        ));
    }
    let operation = if images.is_empty() {
        "generation"
    } else {
        body["images"] = json!(images);
        "edit"
    };
    let path = match selected.as_str() {
        "open-image-2" => format!("/vendors/openai/v1/gpt-image-2/{operation}"),
        "banana-image-2" => format!("/vendors/google/v1/nano-banana-2/{operation}"),
        _ => unreachable!(),
    };
    Ok(MediaRequest {
        kind: "image",
        path,
        body,
        session: session_id(params.session_id.as_deref())?,
    })
}
fn video_request(params: QwenWorkVideoParams) -> Result<MediaRequest, String> {
    let selected = model(params.model.as_deref(), VIDEO_MODELS)?;
    let mut body = json!({"prompt":text(&params.prompt, 2500, "prompt")?});
    if let Some(seconds) = params.seconds {
        if !(4..=15).contains(&seconds) {
            return Err("qwenwork_video_seconds_unsupported".into());
        }
        body["duration"] = json!(seconds);
    }
    if let Some(size) = params.size {
        let (resolution, aspect) = match size.as_str() {
            "480p" | "720p" | "1080p" | "4k" => (size.as_str(), None),
            "1280x720" => ("720p", Some("16:9")),
            "720x1280" => ("720p", Some("9:16")),
            "1920x1080" => ("1080p", Some("16:9")),
            "1080x1920" => ("1080p", Some("9:16")),
            "854x480" => ("480p", Some("16:9")),
            "480x854" => ("480p", Some("9:16")),
            _ => return Err("qwenwork_video_size_unsupported".into()),
        };
        if selected == "happy-horse-1.0" {
            if !["720p", "1080p"].contains(&resolution) {
                return Err("qwenwork_video_size_unsupported".into());
            }
            // This route accepts a resolution while deriving its own geometry.
            body["resolution"] = json!(resolution.to_uppercase());
        } else {
            body["resolution"] = json!(resolution);
            if let Some(aspect) = aspect {
                body["aspect_ratio"] = json!(aspect);
            }
        }
    }
    let path = if selected == "seedance-2.0" {
        "/vendors/bytedance/v1/seedance-2.0/text-to-video/generation"
    } else {
        "/vendors/alibaba/v1/happy-horse-1-0-t2v/generation"
    };
    Ok(MediaRequest {
        kind: "video",
        path: path.into(),
        body,
        session: session_id(params.session_id.as_deref())?,
    })
}
fn music_request(params: QwenWorkMusicParams) -> Result<MediaRequest, String> {
    model(params.model.as_deref(), MUSIC_MODELS)?;
    let mut body = json!({"prompt":text(&params.prompt, 2000, "prompt")?,"lyrics_optimizer":params.lyrics.is_none(),"audio_setting":{"format":"mp3","sample_rate":44100,"bitrate":256000}});
    if let Some(lyrics) = params.lyrics {
        let lyrics = text(&lyrics, 3000, "lyrics")?;
        if lyrics.chars().count() < 10 {
            return Err("qwenwork_lyrics_invalid".into());
        }
        body["lyrics_prompt"] = json!(lyrics);
    }
    Ok(MediaRequest {
        kind: "music",
        path: "/vendors/minimax/v1/music-2.5/text-to-music/generation".into(),
        body,
        session: session_id(params.session_id.as_deref())?,
    })
}
struct MediaRequest {
    kind: &'static str,
    path: String,
    body: Value,
    session: String,
}
struct NativeSession {
    host_token: String,
    token: String,
    base: String,
    session: String,
    generation: u64,
}

fn checked_base(value: &str) -> Result<String, String> {
    let url = reqwest::Url::parse(value).map_err(|_| "qwenwork_service_origin_invalid")?;
    let host = url.host_str().ok_or("qwenwork_service_origin_invalid")?;
    let trusted =
        host == "maas.xiaoluozi.cn" || host == "qwenwork.cn" || host.ends_with(".qwenwork.cn");
    #[cfg(test)]
    let trusted = trusted || host == "127.0.0.1";
    let secure = url.scheme() == "https";
    #[cfg(test)]
    let secure = secure || url.scheme() == "http" && host == "127.0.0.1";
    if !trusted
        || !secure
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !["", "/", "/v1", "/v1/"].contains(&url.path())
        || url
            .port()
            .is_some_and(|port| port != 443 && host != "127.0.0.1")
    {
        return Err("qwenwork_service_origin_invalid".into());
    }
    Ok(format!(
        "{}/v1",
        value.trim_end_matches('/').trim_end_matches("/v1")
    ))
}
fn client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(15))
        .build()
        .map_err(|_| "qwenwork_network_error".into())
}
fn authorized(
    request: reqwest::RequestBuilder,
    token: &str,
) -> Result<reqwest::RequestBuilder, String> {
    let mut value = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
        .map_err(|_| "qwenwork_credential_invalid")?;
    value.set_sensitive(true);
    Ok(request
        .header(reqwest::header::AUTHORIZATION, value)
        .header("accept", "application/json"))
}
async fn bytes(response: reqwest::Response, max: usize) -> Result<Vec<u8>, String> {
    if response
        .content_length()
        .is_some_and(|size| size > max as u64)
    {
        return Err("qwenwork_response_too_large".into());
    }
    let mut result = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| "qwenwork_response_read_failed")?;
        if result.len().saturating_add(chunk.len()) > max {
            return Err("qwenwork_response_too_large".into());
        }
        result.extend_from_slice(&chunk);
    }
    Ok(result)
}
fn upstream_error(status: u16, value: &Value) -> String {
    if value["code"] == "112" || value["code"] == 112 || status == 402 {
        return "qwenwork_quota_exceeded".into();
    }
    match status {
        401 => "qwenwork_login_expired",
        403 => "qwenwork_permission_denied",
        408 | 504 => "qwenwork_operation_timed_out",
        429 => "qwenwork_rate_limit",
        500..=599 => "qwenwork_service_unavailable",
        _ => "qwenwork_upstream_rejected",
    }
    .into()
}
async fn json_response(response: reqwest::Response) -> Result<Value, String> {
    let status = response.status().as_u16();
    let body = bytes(response, MAX_JSON).await?;
    let value: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    if !(200..300).contains(&status) {
        return Err(upstream_error(status, &value));
    }
    if !value.is_object() {
        return Err("qwenwork_response_invalid".into());
    }
    Ok(value)
}
impl NativeSession {
    async fn start(session: String) -> Result<Self, String> {
        auth::ensure_fresh_token()
            .await
            .map_err(|_| "qwenwork_login_expired")?;
        let (user, generation) = auth::native_snapshot()?;
        let host_token = user["security_oauth_token"]
            .as_str()
            .filter(|s| !s.is_empty())
            .or_else(|| user["access_token"].as_str().filter(|s| !s.is_empty()))
            .ok_or("qwenwork_not_signed_in")?
            .to_string();
        let mut result = Self {
            host_token,
            token: String::new(),
            base: String::new(),
            session,
            generation,
        };
        let mut changed = changes().subscribe();
        result.check()?;
        tokio::select! { biased;
            _ = changed.changed() => return Err("qwenwork_credential_changed".into()),
            minted = result.mint() => minted?,
        }
        Ok(result)
    }
    fn check(&self) -> Result<(), String> {
        if generation() != self.generation {
            return Err("qwenwork_credential_changed".into());
        }
        Ok(())
    }
    async fn mint(&mut self) -> Result<(), String> {
        self.check()?;
        let response = authorized(
            client()?.post(format!("{}/api/desktop-sessions", catalog::api_base())),
            &self.host_token,
        )?
        .json(&json!({"session_id":self.session}))
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .map_err(|_| "qwenwork_network_error")?;
        let value = json_response(response).await?;
        self.check()?;
        if !value["session_id"]
            .as_str()
            .map(str::trim)
            .is_some_and(|id| id.eq_ignore_ascii_case(&self.session))
        {
            return Err("qwenwork_session_mismatch".into());
        }
        let token = value["access_token"]
            .as_str()
            .filter(|token| {
                !token.is_empty() && token.len() <= 16384 && !token.chars().any(char::is_control)
            })
            .ok_or("qwenwork_service_credential_invalid")?;
        let base = checked_base(
            value["base_url"]
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or(if self.base.is_empty() {
                    "https://maas.xiaoluozi.cn"
                } else {
                    &self.base
                }),
        )?;
        if !self.base.is_empty() && self.base != base {
            return Err("qwenwork_service_origin_changed".into());
        }
        self.token = token.into();
        self.base = base;
        Ok(())
    }
    async fn request(
        &mut self,
        method: reqwest::Method,
        path: &str,
        body: Option<&Value>,
        search: bool,
    ) -> Result<Value, String> {
        for attempt in 0..=1 {
            self.check()?;
            let root = if search {
                self.base.as_str()
            } else {
                self.base.trim_end_matches("/v1")
            };
            let mut request = authorized(
                client()?.request(method.clone(), format!("{root}{path}")),
                &self.token,
            )?
            .header(
                "user-agent",
                if search {
                    "QwenWork WebSearch"
                } else {
                    "QwenWork/media-generation"
                },
            )
            .header("x-mm-client-request-source", "qwenwork_desktop")
            .header("x-mm-device-type", std::env::consts::OS)
            .timeout(Duration::from_secs(60));
            if !search {
                request = request.header("X-Agent-Skills", "qwenwork-router");
            }
            if let Some(body) = body {
                request = request.json(body);
            }
            let response = request.send().await.map_err(|_| {
                if method == reqwest::Method::POST && !search {
                    "qwenwork_submission_unknown"
                } else {
                    "qwenwork_network_error"
                }
            })?;
            if response.status() == reqwest::StatusCode::UNAUTHORIZED
                && (search || method == reqwest::Method::GET)
                && attempt == 0
            {
                drop(response);
                self.mint().await?;
                continue;
            }
            let value = json_response(response).await?;
            self.check()?;
            return Ok(value);
        }
        unreachable!()
    }
}
async fn run<T>(
    signal: &AbortSignal,
    deadline: Duration,
    work: impl Future<Output = Result<T, String>>,
) -> Result<T, String> {
    tokio::select! { biased;
        _ = signal.cancelled() => Err("qwenwork_operation_cancelled".into()),
        result = tokio::time::timeout(deadline, work) => {
            let value = result.map_err(|_| "qwenwork_operation_timed_out")??;
            if signal.is_cancelled() { return Err("qwenwork_operation_cancelled".into()); }
            Ok(value)
        }
    }
}
async fn epoch<T>(
    session: &mut NativeSession,
    work: impl for<'a> FnOnce(
        &'a mut NativeSession,
    )
        -> std::pin::Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>,
) -> Result<T, String> {
    let mut change = changes().subscribe();
    session.check()?;
    tokio::select! { biased;
        _ = change.changed() => Err("qwenwork_credential_changed".into()),
        result = work(session) => result,
    }
}

fn task(value: &Value, expected: Option<&str>) -> Result<(String, String), String> {
    let info = value["task_info"]
        .as_object()
        .ok_or("qwenwork_task_invalid")?;
    let id = info
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| {
            !id.is_empty()
                && ![".", ".."].contains(id)
                && id.len() <= 200
                && id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._:-".contains(&b))
        })
        .ok_or("qwenwork_task_invalid")?;
    if expected.is_some_and(|expected| expected != id) {
        return Err("qwenwork_task_mismatch".into());
    }
    let status = info
        .get("status")
        .and_then(Value::as_str)
        .ok_or("qwenwork_task_invalid")?;
    if !ACTIVE.contains(&status)
        && ![
            "completed",
            "succeeded",
            "failed",
            "rejected",
            "cancelled",
            "canceled",
            "error",
            "timeout",
        ]
        .contains(&status)
    {
        return Err("qwenwork_task_status_invalid".into());
    }
    Ok((id.to_string(), status.to_string()))
}
fn artifact(value: &Value, kind: &str) -> Result<String, String> {
    let plural = match kind {
        "image" => "images",
        "video" => "videos",
        _ => "audios",
    };
    let singular = match kind {
        "music" => "audio",
        other => other,
    };
    let values = value
        .get(plural)
        .or_else(|| value.get(singular))
        .ok_or("qwenwork_media_missing")?;
    let item = if let Some(values) = values.as_array() {
        if values.len() != 1 {
            return Err("qwenwork_media_count_invalid".into());
        }
        &values[0]
    } else {
        values
    };
    item.as_str()
        .or_else(|| item["url"].as_str())
        .filter(|url| !url.is_empty() && url.len() <= 16384)
        .map(str::to_string)
        .ok_or_else(|| "qwenwork_media_missing".into())
}
fn public_host(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost")
        || host.ends_with(".localhost")
        || host.ends_with(".local")
        || host.ends_with(".internal")
    {
        return false;
    }
    match host.trim_matches(['[', ']']).parse::<std::net::IpAddr>() {
        Ok(ip) => public_ip(ip),
        Err(_) => host.contains('.'),
    }
}
fn public_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(ip) => {
            let b = ip.octets();
            !(ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_unspecified()
                || ip.is_broadcast()
                || ip.is_multicast()
                || b[0] == 0
                || b[0] >= 240
                || b[0] == 100 && (64..=127).contains(&b[1])
                || b[0] == 198 && [18, 19].contains(&b[1])
                || b[0] == 192 && b[1] == 0 && b[2] == 0)
        }
        std::net::IpAddr::V6(ip) => {
            if let Some(ip) = ip.to_ipv4_mapped() {
                return public_ip(std::net::IpAddr::V4(ip));
            }
            !(ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_multicast()
                || (ip.segments()[0] & 0xfe00) == 0xfc00
                || (ip.segments()[0] & 0xffc0) == 0xfe80)
        }
    }
}
fn public_url(value: &str, https_only: bool) -> Result<reqwest::Url, String> {
    let url = reqwest::Url::parse(value).map_err(|_| "qwenwork_media_url_invalid")?;
    let host = url.host_str().ok_or("qwenwork_media_url_invalid")?;
    let valid = (url.scheme() == "https" || !https_only && url.scheme() == "http")
        && public_host(host)
        && url.port().is_none_or(|port| [80, 443].contains(&port));
    #[cfg(test)]
    let valid = valid || https_only && url.scheme() == "http" && host == "127.0.0.1";
    if !valid || !url.username().is_empty() || url.password().is_some() || url.fragment().is_some()
    {
        return Err("qwenwork_media_url_invalid".into());
    }
    Ok(url)
}
fn artifact_dns_address(host: &str, address: std::net::IpAddr) -> bool {
    if public_ip(address) {
        return true;
    }
    if !public_host(host) || host.parse::<std::net::IpAddr>().is_ok() {
        return false;
    }
    let address = match address {
        std::net::IpAddr::V4(address) => Some(address),
        std::net::IpAddr::V6(address) => address.to_ipv4_mapped(),
    };
    // A tunnel may synthesize DNS answers while TLS remains bound to the public hostname.
    address.is_some_and(|address| {
        let bytes = address.octets();
        bytes[0] == 198 && [18, 19].contains(&bytes[1])
    })
}

async fn download_client(url: &reqwest::Url) -> Result<reqwest::Client, String> {
    let host = url.host_str().ok_or("qwenwork_media_url_invalid")?;
    #[cfg(test)]
    if host == "127.0.0.1" {
        return client();
    }
    let addresses: Vec<_> =
        tokio::time::timeout(Duration::from_secs(5), tokio::net::lookup_host((host, 443)))
            .await
            .map_err(|_| "qwenwork_download_failed")?
            .map_err(|_| "qwenwork_download_failed")?
            .take(16)
            .collect();
    if addresses.is_empty()
        || addresses
            .iter()
            .any(|address| !artifact_dns_address(host, address.ip()))
    {
        return Err("qwenwork_media_url_invalid".into());
    }
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .resolve_to_addrs(host, &addresses)
        .connect_timeout(Duration::from_secs(15))
        .build()
        .map_err(|_| "qwenwork_download_failed".into())
}
async fn download(value: &str, kind: &str) -> Result<(Vec<u8>, &'static str), String> {
    let max = match kind {
        "image" => MAX_IMAGE,
        "video" => MAX_VIDEO,
        _ => MAX_AUDIO,
    };
    let mut url = public_url(value, true)?;
    for redirect in 0..=4 {
        let response = download_client(&url)
            .await?
            .get(url.clone())
            .timeout(Duration::from_secs(120))
            .send()
            .await
            .map_err(|_| "qwenwork_download_failed")?;
        if response.status().is_redirection() {
            if redirect == 4 {
                return Err("qwenwork_media_url_invalid".into());
            }
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .ok_or("qwenwork_media_url_invalid")?;
            url = public_url(
                url.join(location)
                    .map_err(|_| "qwenwork_media_url_invalid")?
                    .as_str(),
                true,
            )?;
            continue;
        }
        if !response.status().is_success() {
            return Err("qwenwork_download_failed".into());
        }
        let bytes = bytes(response, max).await?;
        let mime = mime(&bytes, kind)?;
        return Ok((bytes, mime));
    }
    unreachable!()
}
async fn media(request: MediaRequest, signal: AbortSignal) -> Result<NativeResponse, String> {
    run(&signal, Duration::from_secs(1800), async move {
        let mut session = NativeSession::start(request.session.clone()).await?;
        let generation = session.generation;
        let value = epoch(&mut session, |session| {
            Box::pin(async move {
                let mut value = session
                    .request(
                        reqwest::Method::POST,
                        &request.path,
                        Some(&request.body),
                        false,
                    )
                    .await?;
                let (id, mut status) = task(&value, None)?;
                let mut round = 0;
                while ACTIVE.contains(&status.as_str()) {
                    let delay = polling_delay(round);
                    tokio::time::sleep(delay).await;
                    round += 1;
                    value = session
                        .request(
                            reqwest::Method::GET,
                            &format!("{}/{}", request.path, id),
                            None,
                            false,
                        )
                        .await?;
                    status = task(&value, Some(&id))?.1;
                }
                if !["completed", "succeeded"].contains(&status.as_str()) {
                    return Err("qwenwork_media_generation_failed".into());
                }
                let (bytes, mime_type) =
                    download(&artifact(&value, request.kind)?, request.kind).await?;
                session.check()?;
                let field = match request.kind {
                    "image" => "imageBase64",
                    "video" => "videoBase64",
                    _ => "audioBase64",
                };
                let mut result = json!({"mimeType":mime_type});
                result[field] = json!(STANDARD.encode(bytes));
                if request.kind == "video" {
                    if let Some(seconds) = value
                        .get("duration")
                        .and_then(Value::as_f64)
                        .filter(|n| n.is_finite() && *n > 0.0 && *n <= 3600.0)
                    {
                        result["seconds"] = json!(seconds);
                    }
                }
                Ok(result)
            })
        })
        .await?;
        Ok(NativeResponse { value, generation })
    })
    .await
}
fn polling_delay(round: u64) -> Duration {
    #[cfg(test)]
    {
        let _ = round;
        Duration::from_millis(1)
    }
    #[cfg(not(test))]
    {
        Duration::from_secs((2 + round).min(10))
    }
}
pub async fn generate_image(
    params: QwenWorkImageParams,
    signal: AbortSignal,
) -> Result<NativeResponse, String> {
    media(image_request(params)?, signal).await
}
pub async fn generate_video(
    params: QwenWorkVideoParams,
    signal: AbortSignal,
) -> Result<NativeResponse, String> {
    media(video_request(params)?, signal).await
}
pub async fn generate_music(
    params: QwenWorkMusicParams,
    signal: AbortSignal,
) -> Result<NativeResponse, String> {
    media(music_request(params)?, signal).await
}

fn search_request(params: &QwenWorkSearchParams) -> Result<(Value, usize), String> {
    let query = text(&params.query, 70, "search_query")?;
    let count = params.count.unwrap_or(5);
    if !(1..=50).contains(&count) {
        return Err("qwenwork_search_count_invalid".into());
    }
    let mut body = json!({"query":query,"count":count.div_ceil(10)*10});
    if let Some(range) = params.time_range.as_deref() {
        body["recency"] = json!(match range {
            "day" => "oneDay",
            "week" => "oneWeek",
            "month" => "oneMonth",
            _ => return Err("qwenwork_search_time_range_invalid".into()),
        });
    }
    Ok((body, count as usize))
}
fn search_result(value: Value, count: usize) -> Result<Value, String> {
    let rows = value["results"]
        .as_array()
        .filter(|rows| rows.len() <= 50)
        .ok_or("qwenwork_search_response_invalid")?;
    let mut results = Vec::new();
    for row in rows.iter().take(count) {
        let url = row["url"]
            .as_str()
            .ok_or("qwenwork_search_response_invalid")?;
        public_url(url, false).map_err(|_| "qwenwork_search_response_invalid")?;
        let title = row["title"].as_str().unwrap_or(url);
        let snippet = row["content"]
            .as_str()
            .or_else(|| row["snippet"].as_str())
            .or_else(|| row["mainBody"].as_str())
            .unwrap_or_default();
        if title.len() > 4096 || snippet.len() > 128 * 1024 {
            return Err("qwenwork_search_response_invalid".into());
        }
        results.push(json!({"title":title,"url":url,"snippet":snippet}));
    }
    Ok(json!({"results":results}))
}
pub async fn search(
    params: QwenWorkSearchParams,
    signal: AbortSignal,
) -> Result<NativeResponse, String> {
    let (body, count) = search_request(&params)?;
    let id = session_id(params.session_id.as_deref())?;
    run(&signal, Duration::from_secs(120), async move {
        let mut session = NativeSession::start(id).await?;
        let generation = session.generation;
        let value = epoch(&mut session, |session| {
            Box::pin(async move {
                let response = session
                    .request(
                        reqwest::Method::POST,
                        "/tools/web-search",
                        Some(&body),
                        true,
                    )
                    .await?;
                search_result(response, count)
            })
        })
        .await?;
        Ok(NativeResponse { value, generation })
    })
    .await
}

fn host_token(user: &Value) -> Result<&str, String> {
    user["security_oauth_token"]
        .as_str()
        .filter(|token| !token.is_empty())
        .or_else(|| {
            user["access_token"]
                .as_str()
                .filter(|token| !token.is_empty())
        })
        .ok_or_else(|| "qwenwork_not_signed_in".into())
}
async fn transcription_profile(user: &Value) -> Result<Value, String> {
    let business = user["is_biz"] == true;
    let path = if business {
        "/api/v2/quota/usage"
    } else {
        "/api/v1/adapter/user/account-context?include=user,plan,quota,page,data_sharing"
    };
    let response = authorized(
        client()?.get(format!("{}{path}", catalog::api_base())),
        host_token(user)?,
    )?
    .timeout(Duration::from_secs(30))
    .send()
    .await
    .map_err(|_| "qwenwork_network_error")?;
    let value = json_response(response).await?;
    let data = value.get("data").unwrap_or(&value);
    let id = if business {
        if data
            .get("user_quota")
            .or_else(|| data.get("userQuota"))
            .is_none()
        {
            return Err("qwenwork_transcription_identity_invalid".into());
        }
        data.get("user_id").or_else(|| data.get("userId"))
    } else {
        if !data["quota"].is_object() {
            return Err("qwenwork_transcription_identity_invalid".into());
        }
        data.get("user").unwrap_or(data).get("id")
    }
    .and_then(Value::as_str)
    .filter(|id| !id.is_empty() && id.len() <= 256 && !id.chars().any(char::is_control))
    .ok_or("qwenwork_transcription_identity_invalid")?;
    Ok(
        json!({"uid":id,"security_oauth_token":host_token(user)?,"name":user["name"].as_str().unwrap_or_default(),"email":user["email"].as_str().unwrap_or_default()}),
    )
}
pub async fn transcribe_audio(
    params: crate::app_server::wire::SubscriptionTranscribeParams,
    signal: AbortSignal,
) -> Result<NativeResponse, String> {
    run(&signal, Duration::from_secs(420), async {
        auth::ensure_fresh_token()
            .await
            .map_err(|_| "qwenwork_login_expired")?;
        let (user, generation) = auth::native_snapshot()?;
        if params
            .expected_credential_generation
            .as_ref()
            .is_some_and(|expected| expected != &generation.to_string())
        {
            return Err("qwenwork_credential_changed".into());
        }
        let mut changed = changes().subscribe();
        if generation != *changed.borrow() {
            return Err("qwenwork_credential_changed".into());
        }
        let work = async {
            let profile = transcription_profile(&user).await?;
            let response = super::super::qoder::speech::transcribe_prepared(
                super::super::qoder::speech::Source::QwenWork,
                params,
                signal.clone(),
                super::super::qoder::speech::CredentialSnapshot {
                    profile,
                    origin: Some(catalog::api_base()),
                    publication_check: Arc::new(move || generation == *changes().borrow()),
                },
            )
            .await
            .map_err(|error| super::super::qoder::speech::Source::QwenWork.source_error(&error))?;
            if !(response.publication_check)() {
                return Err("qwenwork_credential_changed".into());
            }
            Ok(NativeResponse {
                value: response.value,
                generation,
            })
        };
        tokio::select! { biased;
            _ = changed.changed() => Err("qwenwork_credential_changed".into()),
            result = work => result,
        }
    })
    .await
}

pub(crate) async fn stream_dictation(
    input: tokio::sync::mpsc::Receiver<Vec<u8>>,
    ready: tokio::sync::oneshot::Sender<String>,
    publish: crate::app_server::qoder::speech::TextCallback,
    signal: AbortSignal,
    refinement: Option<super::super::wire::DictationRefinementConfig>,
    recognized: Option<crate::app_server::qoder::speech::TextCallback>,
) -> Result<super::super::qoder::speech::SpeechResult, String> {
    auth::ensure_fresh_token()
        .await
        .map_err(|_| "qwenwork_login_expired")?;
    let (user, generation) = auth::native_snapshot()?;
    let mut changed = changes().subscribe();
    if generation != *changed.borrow() {
        return Err("qwenwork_credential_changed".into());
    }
    let work = async {
        let profile = transcription_profile(&user).await?;
        let valid: Arc<dyn Fn() -> bool + Send + Sync> =
            Arc::new(move || generation == *changes().borrow());
        super::super::qoder::speech::stream_prepared(
            super::super::qoder::speech::Source::QwenWork,
            input,
            signal.clone(),
            super::super::qoder::speech::CredentialSnapshot {
                profile,
                origin: Some(catalog::api_base()),
                publication_check: valid.clone(),
            },
            super::super::qoder::speech::LiveCallbacks {
                ready,
                generation: generation.to_string(),
                publish,
                recognized,
                valid,
            },
            refinement,
        )
        .await
        .map_err(|error| super::super::qoder::speech::Source::QwenWork.source_error(&error))
    };
    let result = tokio::select! {
        biased;
        _ = signal.cancelled() => Err("qwenwork_transcription_cancelled".into()),
        _ = changed.changed() => Err("qwenwork_credential_changed".into()),
        result = work => result,
    };
    if signal.is_cancelled() {
        return Err("qwenwork_transcription_cancelled".into());
    }
    if generation != *changes().borrow() {
        return Err("qwenwork_credential_changed".into());
    }
    result
}

#[cfg(test)]
pub(crate) mod tests;
