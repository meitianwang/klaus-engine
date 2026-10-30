//! Bounded single-operation account services, independent of the agent runtime.
use super::{auth, changes, credential};
use crate::app_server::wire::{
    CursorAudioParams, CursorImageParams, CursorSearchParams, CursorVideoParams,
};
use base64::{engine::general_purpose, Engine};
use futures::StreamExt;
use klaus_agent_core::types::abort::AbortSignal;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const MAX_OPERATIONS: usize = 4;
const MAX_IMAGE_BYTES: usize = 16 * 1024 * 1024;
const MAX_AUDIO_BYTES: usize = 16 * 1024 * 1024;
const MAX_VIDEO_BYTES: usize = 48 * 1024 * 1024;
const MAX_REFERENCES: usize = 4;
const MAX_REFERENCE_BYTES: usize = 4 * 1024 * 1024;
const MAX_SEARCH_BYTES: usize = 2 * 1024 * 1024;
const GATE_REASON: &str = "Developer, Sand, training, or local agent loop access required";

#[derive(Default)]
pub(crate) struct Operations {
    pending: Mutex<HashMap<String, AbortSignal>>,
}

pub(crate) struct Operation {
    owner: Arc<Operations>,
    id: String,
    signal: AbortSignal,
    generation: Option<u64>,
}

/// Credential identity stays internal while its payload is published over the facade.
pub struct NativeResponse {
    pub(crate) value: Value,
    pub(crate) generation: u64,
}

impl std::fmt::Debug for NativeResponse {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("NativeResponse")
    }
}

fn valid_operation_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

impl Operations {
    /// Register synchronously before spawning so a following cancel cannot race it.
    pub fn register(self: &Arc<Self>, id: Option<&str>) -> Result<Operation, String> {
        if id.is_some_and(|id| !valid_operation_id(id)) {
            return Err("cursor_operation_id_invalid".into());
        }
        let id = id
            .map(str::to_owned)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if pending.len() >= MAX_OPERATIONS {
            return Err("cursor_operation_limit".into());
        }
        if pending.contains_key(&id) {
            return Err("cursor_operation_id_in_use".into());
        }
        let signal = AbortSignal::new();
        pending.insert(id.clone(), signal.clone());
        Ok(Operation {
            owner: Arc::clone(self),
            id,
            signal,
            generation: None,
        })
    }

    pub fn cancel(&self, id: &str) -> Result<Value, String> {
        if !valid_operation_id(id) {
            return Err("cursor_operation_id_invalid".into());
        }
        let pending = self
            .pending
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
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
            .unwrap_or_else(|poison| poison.into_inner())
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

    pub fn bind_generation(&mut self, generation: u64) {
        self.generation = Some(generation);
    }

    pub fn publication_error(&self) -> Option<&'static str> {
        if self.signal.is_cancelled() {
            return Some("cursor_operation_cancelled");
        }
        if self
            .generation
            .is_some_and(|generation| credential::check_generation(generation).is_err())
        {
            return Some("cursor_credential_changed");
        }
        None
    }
}

impl Drop for Operation {
    fn drop(&mut self) {
        self.owner
            .pending
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .remove(&self.id);
    }
}

pub(super) async fn cancellable<T>(
    signal: &AbortSignal,
    timeout: Duration,
    work: impl Future<Output = Result<T, String>>,
) -> Result<T, String> {
    tokio::select! {
        biased;
        _ = signal.cancelled() => Err("cursor_operation_cancelled".into()),
        result = tokio::time::timeout(timeout, work) => {
            let value = result.map_err(|_| "cursor_operation_timed_out")??;
            if signal.is_cancelled() { return Err("cursor_operation_cancelled".into()); }
            Ok(value)
        }
    }
}

struct NativeSession {
    credential: credential::Credential,
    generation: u64,
}

impl NativeSession {
    async fn start() -> Result<Self, String> {
        auth::ensure_valid().await?;
        let (credential, generation) = credential::snapshot()?;
        Ok(Self {
            credential,
            generation,
        })
    }

    async fn serving_variant(&self, selection: Option<&str>) -> Result<Value, String> {
        if selection == Some("") {
            return Err("cursor_model_unavailable".into());
        }
        let selection = selection.unwrap_or("");
        if selection.len() > 2048 || selection.chars().any(char::is_control) {
            return Err("cursor_model_unavailable".into());
        }
        let models = super::fetch_models(false).await?;
        let exact = selection.strip_prefix("cursor:").unwrap_or(selection);
        let model = if exact.is_empty() {
            models
                .iter()
                .find(|model| model["isDefault"] == true)
                .or_else(|| models.first())
        } else {
            models.iter().find(|model| model["key"] == exact)
        }
        .ok_or("cursor_model_unavailable")?;
        credential::check_generation(self.generation)?;
        Ok(super::catalog::with_max_context(&models, model).clone())
    }

    async fn serving_model(&self, selection: Option<&str>) -> Result<(String, bool), String> {
        let model = self.serving_variant(selection).await?;
        // This service attributes the operation to the concrete serving selection.
        let id = model["legacySlug"]
            .as_str()
            .filter(|id| !id.is_empty())
            .ok_or("cursor_model_variant_unavailable")?
            .to_string();
        Ok((id, model["requestedModel"]["maxMode"] == true))
    }

    async fn request(&self, method: &str, body: Value, max_bytes: usize) -> Result<Value, String> {
        let mut changed = changes().subscribe();
        credential::check_generation(self.generation)?;
        let work = async {
            let client = credential::client()?;
            let request = client.post(format!(
                "{}/aiserver.v1.AiService/{method}",
                self.credential.api_origin()?
            ));
            let response = credential::authenticated(request, &self.credential)
                .header("content-type", "application/json")
                .header("x-cursor-streaming", "true")
                .header(
                    "x-cursor-checksum",
                    self.credential
                        .checksum(chrono::Utc::now().timestamp_millis())?,
                )
                .json(&body)
                .send()
                .await
                .map_err(|_| "cursor_network_error")?;
            let status = response.status().as_u16();
            if response
                .content_length()
                .is_some_and(|length| length > max_bytes as u64)
            {
                return Err("cursor_response_too_large".into());
            }
            let mut bytes = Vec::new();
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|_| "cursor_response_read_failed")?;
                if chunk.len() > max_bytes.saturating_sub(bytes.len()) {
                    return Err("cursor_response_too_large".into());
                }
                bytes.extend_from_slice(&chunk);
            }
            credential::check_generation(self.generation)?;
            let value = serde_json::from_slice::<Value>(&bytes);
            if !(200..300).contains(&status) {
                let error = native_error(status, value.as_ref().ok());
                return Err(if error == credential::NEEDS_REIMPORT {
                    credential::mark_rejected(self.generation)
                } else {
                    error
                });
            }
            let value = value.map_err(|_| "cursor_invalid_response")?;
            if value.get("error").is_some() {
                let error = native_error(status, Some(&value));
                return Err(if error == credential::NEEDS_REIMPORT {
                    credential::mark_rejected(self.generation)
                } else {
                    error
                });
            }
            Ok(value)
        };
        let result = tokio::select! {
            biased;
            _ = changed.changed() => Err("cursor_credential_changed".into()),
            result = work => result,
        };
        if result.as_ref().is_err_and(|error| {
            error == credential::NEEDS_REIMPORT || error == credential::NEEDS_LOGIN
        }) {
            return result;
        }
        credential::check_generation(self.generation)?;
        result
    }
}

fn native_error(status: u16, value: Option<&Value>) -> String {
    let error = value.map(|value| value.get("error").unwrap_or(value));
    if error
        .and_then(|value| value.get("message"))
        .and_then(Value::as_str)
        .is_some_and(|message| message.trim_end_matches('.') == GATE_REASON)
        && (status == 403
            || error
                .and_then(|value| value.get("code"))
                .is_some_and(|code| code == "permission_denied"))
    {
        return "cursor_native_capability_not_enabled".into();
    }
    if let Some(error) = error {
        if error.get("contentSafetyBlocked") == Some(&json!(true)) {
            return "cursor_content_filter".into();
        }
        if error.get("modelRestricted") == Some(&json!(true)) {
            return "cursor_model_capability_unsupported".into();
        }
        if error.get("code").is_some() {
            let classified = credential::http_error(status, value);
            return if classified == "cursor_upstream_error" {
                "cursor_native_service_failed".into()
            } else {
                classified
            };
        }
        if let Some(provider_status) = error.get("providerStatusCode").and_then(Value::as_u64) {
            if let Ok(status) = u16::try_from(provider_status) {
                return match status {
                    429 => "cursor_rate_limit".into(),
                    408 | 500..=599 => "cursor_overloaded".into(),
                    _ => "cursor_native_service_failed".into(),
                };
            }
        }
    }
    if (200..300).contains(&status) {
        "cursor_native_service_failed".into()
    } else {
        credential::http_error(status, value)
    }
}

fn required_text<'a>(text: &'a str, max: usize, error: &str) -> Result<&'a str, String> {
    if text.trim().is_empty() || text.len() > max || text.contains('\0') {
        return Err(error.into());
    }
    Ok(text)
}

fn proto_text<'a>(value: &'a Value, field: &str, max: usize) -> Result<&'a str, String> {
    match value.get(field) {
        None => Ok(""),
        Some(value) => value
            .as_str()
            .filter(|text| text.len() <= max)
            .ok_or_else(|| "cursor_search_response_invalid".into()),
    }
}

fn aspect_ratio(size: Option<&str>) -> Result<Option<String>, String> {
    let Some(size) = size else {
        return Ok(None);
    };
    if size == "auto" {
        return Ok(None);
    }
    let ratio = if size.contains('x') {
        let (width, height) = size
            .split_once('x')
            .ok_or("cursor_image_size_unsupported")?;
        let width: u32 = width.parse().map_err(|_| "cursor_image_size_unsupported")?;
        let height: u32 = height
            .parse()
            .map_err(|_| "cursor_image_size_unsupported")?;
        if width == 0 || height == 0 || width > 8192 || height > 8192 {
            return Err("cursor_image_size_unsupported".into());
        }
        let (mut a, mut b) = (width, height);
        while b != 0 {
            (a, b) = (b, a % b);
        }
        format!("{}:{}", width / a, height / a)
    } else {
        size.to_string()
    };
    if !["1:1", "4:3", "3:4", "16:9", "9:16"].contains(&ratio.as_str()) {
        return Err("cursor_image_size_unsupported".into());
    }
    Ok(Some(ratio))
}

pub(super) fn decode_base64(value: &str, limit: usize) -> Result<Vec<u8>, String> {
    if value.is_empty() || value.len() > limit.div_ceil(3) * 4 {
        return Err("cursor_native_media_invalid".into());
    }
    let bytes = general_purpose::STANDARD
        .decode(value)
        .or_else(|_| general_purpose::STANDARD_NO_PAD.decode(value))
        .map_err(|_| "cursor_native_media_invalid")?;
    if bytes.is_empty() || bytes.len() > limit {
        return Err("cursor_native_media_invalid".into());
    }
    Ok(bytes)
}

pub(super) fn valid_image(bytes: &[u8], mime: &str) -> bool {
    match mime {
        "image/png" => bytes.starts_with(b"\x89PNG\r\n\x1a\n"),
        "image/jpeg" => bytes.starts_with(&[0xff, 0xd8, 0xff]),
        "image/gif" => bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a"),
        "image/webp" => bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP"),
        _ => false,
    }
}

fn valid_audio(bytes: &[u8], mime: &str) -> bool {
    match mime {
        "audio/mpeg" | "audio/mp3" => {
            bytes.starts_with(b"ID3")
                || bytes
                    .get(..2)
                    .is_some_and(|head| head[0] == 0xff && head[1] & 0xe0 == 0xe0)
        }
        "audio/wav" | "audio/x-wav" => {
            bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WAVE")
        }
        "audio/ogg" => bytes.starts_with(b"OggS"),
        "audio/flac" => bytes.starts_with(b"fLaC"),
        "audio/mp4" => bytes.get(4..8) == Some(b"ftyp"),
        "audio/webm" => bytes.starts_with(&[0x1a, 0x45, 0xdf, 0xa3]),
        _ => false,
    }
}

pub async fn generate_image(
    params: CursorImageParams,
    signal: AbortSignal,
) -> Result<NativeResponse, String> {
    cancellable(&signal, Duration::from_secs(120), async {
        required_text(&params.prompt, 16_384, "cursor_image_prompt_invalid")?;
        let ratio = aspect_ratio(params.size.as_deref())?;
        if params.reference_images.len() > MAX_REFERENCES { return Err("cursor_reference_images_invalid".into()); }
        let mut references = Vec::new();
        let mut total = 0usize;
        for image in &params.reference_images {
            let data = decode_base64(&image.data_base64, MAX_REFERENCE_BYTES)?;
            total += data.len();
            if total > 8 * 1024 * 1024 || !valid_image(&data, &image.mime_type) {
                return Err("cursor_reference_images_invalid".into());
            }
            references.push(json!({"data":general_purpose::STANDARD.encode(data),"mimeType":image.mime_type}));
        }
        let session = NativeSession::start().await?;
        let variant = session.serving_variant(params.model_selection.as_deref()).await?;
        let model = variant["legacySlug"].as_str().filter(|id|!id.is_empty()).ok_or("cursor_model_variant_unavailable")?;
        let max_mode = variant["requestedModel"]["maxMode"] == true;
        let mut body = json!({"description":params.prompt,"referenceImages":references,"modelId":model,"maxMode":max_mode});
        if let Some(ratio) = &ratio { body["aspectRatio"] = json!(ratio); }
        let result = match session.request("RunGenerateImage", body, 24 * 1024 * 1024).await {
            Err(error) if error == "cursor_native_capability_not_enabled" => {
                let result = super::capability_step::generate_image(&session.credential,session.generation,&variant,&params.prompt,ratio.as_deref(),&references).await?;
                credential::check_generation(session.generation)?;
                return Ok(NativeResponse { value:result,generation:session.generation });
            }
            result => result?,
        };
        let success = result.get("success").ok_or("cursor_native_media_invalid")?;
        let mime = success["mimeType"].as_str().ok_or("cursor_native_media_invalid")?;
        let data = decode_base64(success["imageData"].as_str().ok_or("cursor_native_media_invalid")?, MAX_IMAGE_BYTES)?;
        if !valid_image(&data, mime) { return Err("cursor_native_media_invalid".into()); }
        credential::check_generation(session.generation)?;
        Ok(NativeResponse { value: json!({"imageBase64":general_purpose::STANDARD.encode(data),"mimeType":mime,"aspectRatio":ratio,"pixelSize":"provider-controlled"}), generation: session.generation })
    }).await
}

pub async fn generate_audio(
    params: CursorAudioParams,
    signal: AbortSignal,
) -> Result<NativeResponse, String> {
    cancellable(&signal, Duration::from_secs(90), async {
        required_text(&params.text, 16_384, "cursor_audio_text_invalid")?;
        let mut body = json!({"text":params.text});
        if let Some(voice) = &params.voice {
            if voice.is_empty()
                || voice.len() > 64
                || !voice
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            {
                return Err("cursor_audio_voice_invalid".into());
            }
            body["voiceId"] = json!(voice);
        }
        if let Some(speed) = params.speed {
            if !speed.is_finite() || !(0.75..=2.0).contains(&speed) {
                return Err("cursor_audio_speed_invalid".into());
            }
            body["speed"] = json!(speed);
        }
        let session = NativeSession::start().await?;
        let result = session
            .request("TextToSpeech", body, 24 * 1024 * 1024)
            .await?;
        let mime = result["mimeType"]
            .as_str()
            .ok_or("cursor_native_media_invalid")?;
        let data = decode_base64(
            result["audio"]
                .as_str()
                .ok_or("cursor_native_media_invalid")?,
            MAX_AUDIO_BYTES,
        )?;
        if !valid_audio(&data, mime) {
            return Err("cursor_native_media_invalid".into());
        }
        credential::check_generation(session.generation)?;
        Ok(NativeResponse {
            value: json!({"audioBase64":general_purpose::STANDARD.encode(data),"mimeType":mime}),
            generation: session.generation,
        })
    })
    .await
}

fn video_size(size: Option<&str>) -> Result<(Option<String>, Option<&'static str>), String> {
    let Some(size) = size else {
        return Ok((None, None));
    };
    if size == "auto" {
        return Ok((None, None));
    }
    if let Some((width, height)) = size.split_once('x') {
        let width: u32 = width.parse().map_err(|_| "cursor_video_size_unsupported")?;
        let height: u32 = height
            .parse()
            .map_err(|_| "cursor_video_size_unsupported")?;
        let resolution = match width.min(height) {
            480 => "480p",
            720 => "720p",
            _ => return Err("cursor_video_size_unsupported".into()),
        };
        let (mut a, mut b) = (width, height);
        while b != 0 {
            (a, b) = (b, a % b);
        }
        let ratio = format!("{}:{}", width / a, height / a);
        if !["1:1", "16:9", "9:16", "4:3", "3:4", "3:2", "2:3"].contains(&ratio.as_str()) {
            return Err("cursor_video_size_unsupported".into());
        }
        return Ok((Some(ratio), Some(resolution)));
    }
    if !["1:1", "16:9", "9:16", "4:3", "3:4", "3:2", "2:3"].contains(&size) {
        return Err("cursor_video_size_unsupported".into());
    }
    Ok((Some(size.into()), None))
}

pub async fn generate_video(
    params: CursorVideoParams,
    signal: AbortSignal,
) -> Result<NativeResponse, String> {
    cancellable(&signal, Duration::from_secs(300), async {
        required_text(&params.prompt, 16_384, "cursor_video_prompt_invalid")?;
        if params.seconds.is_some_and(|seconds| !(1..=15).contains(&seconds)) {
            return Err("cursor_video_seconds_unsupported".into());
        }
        let (ratio, resolution) = video_size(params.size.as_deref())?;
        let session = NativeSession::start().await?;
        let (model, max_mode) = session.serving_model(params.model_selection.as_deref()).await?;
        let mut body = json!({"prompt":params.prompt,"referenceImages":[],"modelId":model,"maxMode":max_mode});
        if let Some(seconds) = params.seconds { body["durationSeconds"] = json!(seconds); }
        if let Some(ratio) = &ratio { body["aspectRatio"] = json!(ratio); }
        if let Some(resolution) = resolution { body["resolution"] = json!(resolution); }
        let value = session.request("RunGenerateVideo", body, 64 * 1024 * 1024).await?;
        let success = value.get("success").ok_or("cursor_native_media_invalid")?;
        let mime = success["mimeType"].as_str().ok_or("cursor_native_media_invalid")?;
        let seconds = success["durationSeconds"].as_u64().filter(|seconds| (1..=15).contains(seconds))
            .ok_or("cursor_native_media_invalid")?;
        let data = decode_base64(success["videoData"].as_str().ok_or("cursor_native_media_invalid")?, MAX_VIDEO_BYTES)?;
        if mime != "video/mp4" || data.get(4..8) != Some(b"ftyp") {
            return Err("cursor_native_media_invalid".into());
        }
        credential::check_generation(session.generation)?;
        Ok(NativeResponse { value: json!({"videoBase64":general_purpose::STANDARD.encode(data),"mimeType":mime,"seconds":seconds,"pixelSize":"provider-controlled"}), generation: session.generation })
    }).await
}

pub async fn search(
    params: CursorSearchParams,
    signal: AbortSignal,
) -> Result<NativeResponse, String> {
    cancellable(&signal, Duration::from_secs(25), async {
        required_text(&params.query, 4096, "cursor_search_query_invalid")?;
        let session = NativeSession::start().await?;
        let variant = session
            .serving_variant(params.model_selection.as_deref())
            .await?;
        let model = variant["legacySlug"]
            .as_str()
            .filter(|id| !id.is_empty())
            .ok_or("cursor_model_variant_unavailable")?;
        let value = session
            .request(
                "RunWebSearch",
                json!({"searchTerm":params.query,"modelId":model}),
                MAX_SEARCH_BYTES,
            )
            .await;
        let value = match value {
            Err(error) if error == "cursor_native_capability_not_enabled" => {
                let result = super::capability_step::search(
                    &session.credential,
                    session.generation,
                    &variant,
                    &params.query,
                )
                .await?;
                credential::check_generation(session.generation)?;
                return Ok(NativeResponse {
                    value: result,
                    generation: session.generation,
                });
            }
            result => result?,
        };
        let empty = Vec::new();
        let documents = match value.get("documents") {
            None => &empty,
            Some(value) => value.as_array().ok_or("cursor_search_response_invalid")?,
        };
        if documents.len() > 50 {
            return Err("cursor_search_response_invalid".into());
        }
        let mut results = Vec::new();
        for document in documents {
            let title = proto_text(document, "title", 4096)?;
            let snippet = proto_text(document, "text", 32_768)?;
            let address = proto_text(document, "url", 8192)?;
            let url = url::Url::parse(address).map_err(|_| "cursor_search_response_invalid")?;
            if !matches!(url.scheme(), "https" | "http")
                || !url.username().is_empty()
                || url.password().is_some()
            {
                return Err("cursor_search_response_invalid".into());
            }
            results.push(json!({"title":title,"url":address,"snippet":snippet}));
        }
        let mut result = json!({"results":results});
        if let Some(answer) = value.get("answer") {
            let answer = answer
                .as_str()
                .filter(|text| text.len() <= 128 * 1024)
                .ok_or("cursor_search_response_invalid")?;
            result["answer"] = json!(answer);
        }
        credential::check_generation(session.generation)?;
        Ok(NativeResponse {
            value: result,
            generation: session.generation,
        })
    })
    .await
}

#[cfg(test)]
#[path = "capabilities_tests.rs"]
pub(crate) mod tests;
