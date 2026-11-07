//! Account-bound native search and media transport.

use super::auth;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use futures::StreamExt;
use klaus_agent_core::types::abort::AbortSignal;
use serde_json::{json, Value};
use std::{future::Future, sync::Arc, time::Duration};

pub(crate) const MAX_JSON: usize = 2 * 1024 * 1024;
const MAX_QUERY: usize = 8192;

pub struct NativeResponse {
    pub value: Value,
    pub publication_check: Arc<dyn Fn() -> bool + Send + Sync>,
}

pub(super) struct Session {
    pub credential: auth::Credential,
    pub generation: String,
}

impl Session {
    pub async fn start(expected: Option<&str>) -> Result<Self, String> {
        let generation = auth::generation().ok_or("trae_not_signed_in")?;
        if expected.is_some_and(|expected| expected != generation) {
            return Err("trae_credential_changed".into());
        }
        let credential = auth::credential(Some(&generation)).await?;
        let session = Self {
            credential,
            generation,
        };
        session.check()?;
        Ok(session)
    }

    pub fn check(&self) -> Result<(), String> {
        if auth::is_current(&self.generation) {
            Ok(())
        } else {
            Err("trae_credential_changed".into())
        }
    }

    pub fn response(&self, value: Value) -> Result<NativeResponse, String> {
        self.check()?;
        let generation = self.generation.clone();
        Ok(NativeResponse {
            value,
            publication_check: Arc::new(move || auth::is_current(&generation)),
        })
    }

    pub async fn json(&self, path: &str, body: &Value) -> Result<Value, String> {
        self.check()?;
        let response = super::client()
            .post(format!("{}{path}", super::cloud_base()))
            .headers(super::cloud_headers(&self.credential)?)
            .json(body)
            .send()
            .await
            .map_err(|_| "trae_request_transport_failed")?;
        let value = read_json(response, MAX_JSON).await?;
        self.check()?;
        Ok(value)
    }
}

pub(super) async fn bounded<T>(
    signal: &AbortSignal,
    generation: Option<&str>,
    seconds: u64,
    work: impl Future<Output = Result<T, String>>,
) -> Result<T, String> {
    let invalidated = async {
        loop {
            if generation.is_some_and(|generation| !auth::is_current(generation)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    tokio::select! {
        biased;
        _ = signal.cancelled() => Err("trae_operation_cancelled".into()),
        _ = invalidated => Err("trae_credential_changed".into()),
        result = tokio::time::timeout(Duration::from_secs(seconds), work) => {
            let result = result.map_err(|_| "trae_operation_timed_out")??;
            if signal.is_cancelled() { return Err("trae_operation_cancelled".into()); }
            if generation.is_some_and(|generation| !auth::is_current(generation)) { return Err("trae_credential_changed".into()); }
            Ok(result)
        }
    }
}

pub(super) async fn read_json(response: reqwest::Response, max: usize) -> Result<Value, String> {
    let status = response.status();
    if !status.is_success() {
        return Err(super::errors::from_response(response)
            .await
            .provider_error()
            .message
            .try_to_utf8()
            .unwrap_or_else(|_| "trae_request_rejected".into()));
    }
    if response
        .content_length()
        .is_some_and(|length| length > max as u64)
    {
        return Err("trae_response_too_large".into());
    }
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| "trae_response_transport_failed")?;
        if bytes.len().saturating_add(chunk.len()) > max {
            return Err("trae_response_too_large".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    let value: Value = serde_json::from_slice(&bytes).map_err(|_| "trae_response_invalid")?;
    service_error(&value)?;
    Ok(value)
}

pub(super) fn service_error(value: &Value) -> Result<(), String> {
    for key in ["code", "error_code"] {
        if value
            .get(key)
            .and_then(Value::as_i64)
            .filter(|code| *code != 0)
            .is_some()
        {
            let error = super::errors::ServiceError::from_native(value, None, None);
            return Err(format!(
                "{} (service code {})",
                error.category,
                error.native_code.as_deref().unwrap_or("unknown")
            ));
        }
    }
    if value.get("success") == Some(&Value::Bool(false))
        || value.get("error").is_some_and(|value| !value.is_null())
    {
        return Err(super::errors::ServiceError::from_native(value, None, None).category);
    }
    Ok(())
}

fn text<'a>(value: &'a Value, key: &str, max: usize) -> Result<&'a str, String> {
    value[key]
        .as_str()
        .filter(|value| !value.trim().is_empty() && value.len() <= max && !value.contains('\0'))
        .ok_or_else(|| format!("trae_{key}_invalid"))
}

pub(super) fn public_url(raw: &str) -> Result<(), String> {
    let url = reqwest::Url::parse(raw).map_err(|_| "trae_result_url_invalid")?;
    let host = url.host_str().ok_or("trae_result_url_invalid")?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || host.eq_ignore_ascii_case("localhost")
        || host.ends_with(".localhost")
        || host.ends_with(".local")
        || host
            .trim_matches(['[', ']'])
            .parse::<std::net::IpAddr>()
            .is_ok()
    {
        return Err("trae_result_url_invalid".into());
    }
    Ok(())
}

fn search_body(params: &Value) -> Result<Value, String> {
    if params
        .get("model")
        .and_then(Value::as_str)
        .is_some_and(|model| !model.is_empty() && !["default", "service-default"].contains(&model))
    {
        return Err("trae_search_model_unsupported".into());
    }
    if params
        .get("freshness")
        .and_then(Value::as_str)
        .is_some_and(|value| !value.is_empty())
    {
        return Err("trae_search_freshness_unsupported".into());
    }
    let query = params
        .get("query")
        .or_else(|| params.get("user_input"))
        .and_then(Value::as_str)
        .filter(|value| {
            !value.trim().is_empty() && value.len() <= MAX_QUERY && !value.contains('\0')
        })
        .ok_or("trae_search_query_invalid")?;
    let count = params
        .get("maxResults")
        .or_else(|| params.get("top_k"))
        .and_then(Value::as_u64)
        .unwrap_or(5);
    if !(1..=20).contains(&count) {
        return Err("trae_search_count_invalid".into());
    }
    let mut body = json!({"user_input":query,"top_k":count,"access_type":0,
        "session_id":uuid::Uuid::new_v4().to_string(),"conversation_id":uuid::Uuid::new_v4().to_string()});
    for key in ["ModelName", "language", "lr"] {
        if params.get(key).is_some_and(|value| !value.is_null()) {
            body[key] = Value::String(text(params, key, 256)?.to_owned());
        }
    }
    Ok(body)
}

fn domains(params: &Value, key: &str) -> Result<Vec<String>, String> {
    let Some(value) = params.get(key).filter(|value| !value.is_null()) else {
        return Ok(Vec::new());
    };
    let values = value
        .as_array()
        .filter(|values| values.len() <= 20)
        .ok_or("trae_search_domains_invalid")?;
    values
        .iter()
        .map(|value| {
            let domain = value
                .as_str()
                .ok_or("trae_search_domains_invalid")?
                .to_ascii_lowercase();
            if domain.len() > 253
                || !domain.contains('.')
                || domain.split('.').any(|label| {
                    label.is_empty()
                        || label.len() > 63
                        || label.starts_with('-')
                        || label.ends_with('-')
                        || !label
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                })
            {
                return Err("trae_search_domains_invalid".to_owned());
            }
            public_url(&format!("https://{domain}/"))?;
            Ok(domain)
        })
        .collect()
}

fn search_results(value: &Value, params: &Value, count: usize) -> Result<Value, String> {
    let allowed = domains(params, "allowedDomains")?;
    let blocked = domains(params, "blockedDomains")?;
    let rows = value["references"]
        .as_array()
        .filter(|rows| rows.len() <= 100)
        .ok_or("trae_search_response_invalid")?;
    let mut results = Vec::new();
    for row in rows {
        let url = text(row, "uri", 16_384)?;
        public_url(url)?;
        let title = row["title"].as_str().unwrap_or("");
        let snippets = row["snippets"]
            .as_array()
            .ok_or("trae_search_response_invalid")?;
        if title.len() > 16_384 || snippets.len() > 100 {
            return Err("trae_search_response_invalid".into());
        }
        let snippets = snippets
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .filter(|value| value.len() <= 128 * 1024)
                    .ok_or("trae_search_response_invalid")
            })
            .collect::<Result<Vec<_>, _>>()?;
        let parsed = reqwest::Url::parse(url).map_err(|_| "trae_result_url_invalid")?;
        let host = parsed.host_str().unwrap_or("").to_ascii_lowercase();
        let matches = |domain: &String| host == *domain || host.ends_with(&format!(".{domain}"));
        if (!allowed.is_empty() && !allowed.iter().any(matches)) || blocked.iter().any(matches) {
            continue;
        }
        results.push(json!({"title":title,"url":url,"snippet":snippets.join("\n\n")}));
    }
    let total = results.len();
    results.truncate(count);
    Ok(json!({"results":results,"totalResults":total}))
}

pub async fn search(params: Value, signal: AbortSignal) -> Result<NativeResponse, String> {
    let allowed = domains(&params, "allowedDomains")?;
    let blocked = domains(&params, "blockedDomains")?;
    let mut body = search_body(&params)?;
    let count = body["top_k"].as_u64().unwrap_or(5) as usize;
    if !allowed.is_empty() || !blocked.is_empty() {
        body["top_k"] = json!(20);
    }
    bounded(&signal, None, 45, async {
        let session = Session::start(None).await?;
        bounded(&signal, Some(&session.generation), 30, async {
            let value = session.json("/api/ide/v1/web_search", &body).await?;
            session.response(search_results(&value, &params, count)?)
        })
        .await
    })
    .await
}

const MAX_IMAGE: usize = 24 * 1024 * 1024;
const MAX_VIDEO: usize = 96 * 1024 * 1024;

fn image_mime(bytes: &[u8]) -> Option<&'static str> {
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

fn gcd(mut left: u32, mut right: u32) -> u32 {
    while right != 0 {
        (left, right) = (right, left % right);
    }
    left
}

fn video_dimensions(value: &str, separator: char) -> Result<(u32, u32), String> {
    let (width, height) = value
        .split_once(separator)
        .ok_or("trae_video_size_invalid")?;
    let number = |value: &str| {
        value.parse::<u32>().ok().filter(|number| {
            *number > 0 && !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
        })
    };
    let width = number(width).ok_or("trae_video_size_invalid")?;
    let height = number(height).ok_or("trae_video_size_invalid")?;
    Ok((width, height))
}

fn reduced_ratio(width: u32, height: u32) -> Result<String, String> {
    let divisor = gcd(width, height);
    let width = width / divisor;
    let height = height / divisor;
    if width > 100 || height > 100 {
        return Err("trae_video_ratio_invalid".into());
    }
    Ok(format!("{width}:{height}"))
}

fn video_size(params: &Value) -> Result<(Option<String>, Option<String>), String> {
    let optional = |key: &str| -> Result<Option<&str>, String> {
        params
            .get(key)
            .filter(|value| !value.is_null())
            .map(|value| {
                value
                    .as_str()
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| format!("trae_video_{key}_invalid"))
            })
            .transpose()
    };
    let mut resolution = optional("resolution")?.map(str::to_owned);
    if resolution
        .as_deref()
        .is_some_and(|value| !matches!(value, "480p" | "720p" | "1080p" | "4k"))
    {
        return Err("trae_video_resolution_invalid".into());
    }
    let mut ratio = optional("ratio")?
        .map(|value| {
            let (width, height) = video_dimensions(value, ':')?;
            reduced_ratio(width, height)
        })
        .transpose()?;
    if let Some(size) = optional("size")?.filter(|value| *value != "auto") {
        if size.len() > 32 {
            return Err("trae_video_size_invalid".into());
        }
        let (width, height) = video_dimensions(size, if size.contains('x') { 'x' } else { ':' })?;
        let size_ratio = reduced_ratio(width, height)?;
        if ratio.as_ref().is_some_and(|ratio| ratio != &size_ratio) {
            return Err("trae_video_size_conflict".into());
        }
        ratio = Some(size_ratio);
        if size.contains('x') {
            if width > 8192 || height > 8192 {
                return Err("trae_video_size_invalid".into());
            }
            let size_resolution = match width.min(height) {
                480 => "480p",
                720 => "720p",
                1080 => "1080p",
                2160 => "4k",
                _ => return Err("trae_video_resolution_unsupported".into()),
            };
            if resolution
                .as_deref()
                .is_some_and(|resolution| resolution != size_resolution)
            {
                return Err("trae_video_size_conflict".into());
            }
            resolution = Some(size_resolution.to_owned());
        }
    }
    Ok((resolution, ratio))
}

const DEFAULT_IMAGE_SIZE: &str = "2048x2048";
const MIN_IMAGE_PIXELS: u64 = 3_686_400;
const DEFAULT_IMAGE_PIXELS: u64 = 2048 * 2048;

fn image_size(params: &Value) -> Result<String, String> {
    let Some(value) = params.get("image_size").or_else(|| params.get("size")) else {
        return Ok(DEFAULT_IMAGE_SIZE.into());
    };
    let size = value.as_str().ok_or("trae_image_size_invalid")?;
    if size == "auto" {
        return Ok(DEFAULT_IMAGE_SIZE.into());
    }
    if size.len() > 32
        || size.is_empty()
        || !size
            .bytes()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, b'x' | b':' | b'_'))
    {
        return Err("trae_image_size_invalid".into());
    }
    let Some((width, height)) = size.split_once('x') else {
        return Ok(size.into());
    };
    let dimension = |value: &str| {
        value
            .parse::<u32>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or("trae_image_size_invalid")
    };
    let (width, height) = (dimension(width)?, dimension(height)?);
    let pixels = u64::from(width) * u64::from(height);
    if pixels >= MIN_IMAGE_PIXELS {
        return Ok(size.into());
    }
    // Adapt undersized requests before submission while retaining their aspect ratio.
    // The default area leaves room above the service minimum after pixel rounding.
    let scale = (DEFAULT_IMAGE_PIXELS as f64 / pixels as f64).sqrt();
    let width = (f64::from(width) * scale).ceil() as u32;
    let height = (f64::from(height) * scale).ceil() as u32;
    Ok(format!("{width}x{height}"))
}

fn media_body(params: &Value, video: bool) -> Result<Value, String> {
    let prompt = text(params, "prompt", 32_768)?;
    if params
        .get("model")
        .or_else(|| params.get("modelSelection"))
        .and_then(Value::as_str)
        .is_some_and(|model| !matches!(model, "" | "default" | "service-default"))
    {
        return Err("trae_media_model_unsupported".into());
    }
    let mut body = json!({"prompt":prompt,"access_type":0});
    if video {
        let (resolution, ratio) = video_size(params)?;
        let duration = params
            .get("duration")
            .or_else(|| params.get("seconds"))
            .and_then(Value::as_u64)
            .unwrap_or(5);
        if !(1..=60).contains(&duration) {
            return Err("trae_video_duration_invalid".into());
        }
        if let Some(resolution) = resolution {
            body["resolution"] = json!(resolution);
        }
        if let Some(ratio) = ratio {
            body["ratio"] = json!(ratio);
        }
        body["duration"] = json!(duration);
    } else {
        body["image_size"] = json!(image_size(params)?);
    }
    for (native_key, alias) in [("image_urls", "imageUrls"), ("video_urls", "videoUrls")] {
        if !video && native_key == "video_urls" {
            continue;
        }
        if let Some(values) = params
            .get(native_key)
            .or_else(|| params.get(alias))
            .filter(|value| !value.is_null())
        {
            let rows = values
                .as_array()
                .filter(|values| values.len() <= 8)
                .ok_or("trae_reference_media_invalid")?;
            let mut urls = Vec::new();
            for value in rows {
                let url = value
                    .as_str()
                    .filter(|value| value.len() <= 16_384)
                    .ok_or("trae_reference_media_invalid")?;
                public_url(url)?;
                if !url.starts_with("https://") {
                    return Err("trae_reference_media_invalid".into());
                }
                urls.push(url.to_owned());
            }
            body[native_key] = json!(urls);
        }
    }
    if let Some(images) = params
        .get("referenceImages")
        .filter(|value| !value.is_null())
    {
        let images = images
            .as_array()
            .filter(|values| values.len() <= 4)
            .ok_or("trae_reference_images_invalid")?;
        let mut urls = body["image_urls"].as_array().cloned().unwrap_or_default();
        let mut total = 0usize;
        for image in images {
            let encoded = image["dataBase64"]
                .as_str()
                .filter(|value| value.len() <= (8 * 1024 * 1024usize).div_ceil(3) * 4)
                .ok_or("trae_reference_images_invalid")?;
            let bytes = STANDARD
                .decode(encoded)
                .map_err(|_| "trae_reference_images_invalid")?;
            total = total.saturating_add(bytes.len());
            if bytes.len() > 8 * 1024 * 1024 || total > 16 * 1024 * 1024 {
                return Err("trae_reference_images_too_large".into());
            }
            let mime = image_mime(&bytes).ok_or("trae_reference_images_invalid")?;
            if image["mimeType"].as_str() != Some(mime) {
                return Err("trae_reference_images_invalid".into());
            }
            urls.push(json!(format!(
                "data:{mime};base64,{}",
                STANDARD.encode(bytes)
            )));
        }
        if urls.len() > 8 {
            return Err("trae_reference_images_invalid".into());
        }
        if !urls.is_empty() {
            body["image_urls"] = json!(urls);
        }
    }
    if let Some(value) = params
        .get("total_input_video_duration_ms")
        .or_else(|| params.get("totalInputVideoDurationMs"))
    {
        let duration = value
            .as_u64()
            .filter(|value| *value <= 600_000)
            .ok_or("trae_reference_media_invalid")?;
        body["total_input_video_duration_ms"] = json!(duration);
    }
    Ok(body)
}

#[derive(Default)]
struct MediaStream {
    pending: Vec<u8>,
    event: String,
    data: Vec<String>,
    results: Vec<Value>,
    done: bool,
}

impl MediaStream {
    fn push(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.pending.extend_from_slice(bytes);
        while let Some(end) = self.pending.iter().position(|value| *value == b'\n') {
            if end > 64 * 1024 {
                return Err("trae_media_event_too_large".into());
            }
            let mut line: Vec<u8> = self.pending.drain(..=end).collect();
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            let line = String::from_utf8(line).map_err(|_| "trae_media_stream_invalid")?;
            if line.is_empty() {
                self.dispatch()?;
            } else if let Some(event) = line.strip_prefix("event:") {
                self.event = event.trim().to_owned();
            } else if let Some(data) = line.strip_prefix("data:") {
                if self
                    .data
                    .iter()
                    .map(String::len)
                    .sum::<usize>()
                    .saturating_add(data.len())
                    > 64 * 1024
                {
                    return Err("trae_media_event_too_large".into());
                }
                self.data
                    .push(data.strip_prefix(' ').unwrap_or(data).to_owned());
            }
        }
        if self.pending.len() > 64 * 1024 {
            return Err("trae_media_event_too_large".into());
        }
        Ok(())
    }
    fn dispatch(&mut self) -> Result<(), String> {
        if self.data.is_empty() {
            self.event.clear();
            return Ok(());
        }
        let raw = self.data.join("\n");
        self.data.clear();
        let value: Value = serde_json::from_str(&raw).map_err(|_| "trae_media_stream_invalid")?;
        service_error(&value)?;
        match self.event.as_str() {
            "heartbeat" => {}
            "error" => return Err("trae_media_generation_failed".into()),
            "result" => {
                if self.done || self.results.len() >= 8 {
                    return Err("trae_media_stream_invalid".into());
                }
                let uri = text(&value, "uri", 4096)?;
                if !uri.starts_with("tos-")
                    || uri.contains("..")
                    || uri.chars().any(char::is_control)
                {
                    return Err("trae_media_uri_invalid".into());
                }
                self.results.push(value);
            }
            "done" => {
                if self.done || self.results.is_empty() || value["finish_reason"] != "stop" {
                    return Err("trae_media_incomplete".into());
                }
                self.done = true;
            }
            _ => return Err("trae_media_stream_invalid".into()),
        }
        self.event.clear();
        Ok(())
    }
}

async fn media_stream(session: &Session, body: &Value, video: bool) -> Result<Vec<Value>, String> {
    let path = if video {
        "/api/ide/v1/tool_text_to_video_stream"
    } else {
        "/api/ide/v1/tool_text_to_image_stream"
    };
    let response = super::client()
        .post(format!("{}{path}", super::cloud_base()))
        .headers(super::cloud_headers(&session.credential)?)
        .json(body)
        .timeout(Duration::from_secs(if video { 600 } else { 180 }))
        .send()
        .await
        .map_err(|_| "trae_media_request_failed")?;
    if !response.status().is_success() {
        return Err(format!(
            "trae_request_refused (HTTP {})",
            response.status().as_u16()
        ));
    }
    if !response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("text/event-stream"))
    {
        read_json(response, MAX_JSON).await?;
        return Err("trae_media_stream_invalid".into());
    }
    let mut decoder = MediaStream::default();
    let mut total = 0usize;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        session.check()?;
        let chunk = chunk.map_err(|_| "trae_media_receive_failed")?;
        total = total.saturating_add(chunk.len());
        if total > MAX_JSON {
            return Err("trae_response_too_large".into());
        }
        decoder.push(&chunk)?;
        if decoder.done {
            return Ok(decoder.results);
        }
    }
    Err("trae_media_incomplete".into())
}

async fn download_media(
    session: &Session,
    uri: &str,
    video: bool,
) -> Result<(Vec<u8>, &'static str), String> {
    let value = session
        .json("/api/ide/v1/get_resource_url", &json!({"uri_list":[uri]}))
        .await?;
    let url = value["url_map"]
        .get(uri)
        .and_then(Value::as_str)
        .ok_or("trae_media_url_missing")?;
    public_url(url)?;
    let parsed = reqwest::Url::parse(url).map_err(|_| "trae_result_url_invalid")?;
    if parsed.scheme() != "https"
        || !parsed
            .host_str()
            .is_some_and(|host| host.ends_with(".byteimg.com"))
    {
        return Err("trae_media_url_invalid".into());
    }
    // The returned CDN URL carries its own authorization; account headers stay on the API host.
    let response = super::client()
        .get(url)
        .timeout(Duration::from_secs(90))
        .send()
        .await
        .map_err(|_| "trae_media_download_failed")?;
    if !response.status().is_success() {
        return Err("trae_media_download_failed".into());
    }
    let max = if video { MAX_VIDEO } else { MAX_IMAGE };
    if response
        .content_length()
        .is_some_and(|length| length > max as u64)
    {
        return Err("trae_media_too_large".into());
    }
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        session.check()?;
        let chunk = chunk.map_err(|_| "trae_media_download_failed")?;
        if bytes.len().saturating_add(chunk.len()) > max {
            return Err("trae_media_too_large".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    let mime = if video {
        if bytes.get(4..8) != Some(b"ftyp") {
            return Err("trae_video_response_invalid".into());
        }
        "video/mp4"
    } else {
        image_mime(&bytes).ok_or("trae_image_response_invalid")?
    };
    Ok((bytes, mime))
}

fn media_result(mut media: Vec<Value>, video: bool) -> Result<Value, String> {
    if media.is_empty() {
        return Err("trae_media_incomplete".into());
    }
    if media.len() == 1 {
        return Ok(media.pop().unwrap());
    }
    if !video {
        return Ok(json!({"images":media}));
    }
    let mut value = media[0].clone();
    value["outputs"] = json!(media);
    Ok(value)
}

async fn generate_media(
    params: Value,
    signal: AbortSignal,
    video: bool,
) -> Result<NativeResponse, String> {
    let body = media_body(&params, video)?;
    let seconds = if video { 720 } else { 300 };
    bounded(&signal, None, seconds, async {
        let session = Session::start(None).await?;
        bounded(&signal, Some(&session.generation), seconds - 10, async {
            let results = media_stream(&session, &body, video).await?;
            let mut media = Vec::new();
            let mut total_bytes = 0usize;
            // Validate every output before exposing any artifact to its caller.
            for result in results {
                let uri = result["uri"].as_str().ok_or("trae_media_uri_invalid")?;
                let (bytes, mime) = download_media(&session, uri, video).await?;
                total_bytes = total_bytes.saturating_add(bytes.len());
                if total_bytes > if video { MAX_VIDEO } else { MAX_IMAGE } { return Err("trae_media_too_large".into()); }
                media.push(if video { json!({"videoBase64":STANDARD.encode(bytes),"mimeType":mime,"duration":result["video_duration"]}) }
                    else { json!({"imageBase64":STANDARD.encode(bytes),"mimeType":mime}) });
            }
            session.response(media_result(media, video)?)
        }).await
    }).await
}

pub async fn generate_image(params: Value, signal: AbortSignal) -> Result<NativeResponse, String> {
    generate_media(params, signal, false).await
}

pub async fn generate_video(params: Value, signal: AbortSignal) -> Result<NativeResponse, String> {
    generate_media(params, signal, true).await
}

#[cfg(test)]
#[path = "native_tests.rs"]
mod tests;
