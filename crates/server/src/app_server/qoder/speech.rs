//! Standalone signed audio recognition with bounded input, output and lifetime.

use crate::app_server::wire::{DictationRefinementConfig, SubscriptionTranscribeParams};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use futures::{SinkExt, StreamExt};
use klaus_agent_core::types::abort::AbortSignal;
use serde_json::{json, Value};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::tungstenite::{
    client::IntoClientRequest, protocol::WebSocketConfig, Message,
};

const AUDIO_PATH: &str = "/api/v2/service/ws/asr";
const REGION_URL: &str = "https://center.qoder.sh/algo/api/v2/service/region/endpoints";
const DEFAULT_ORIGIN: &str = "https://api2.qoder.sh";
const MAX_AUDIO_BYTES: usize = 12 * 1024 * 1024;
const MAX_PCM_BYTES: usize = 6 * 60 * 16_000 * 2;
const MAX_RESULT_BYTES: usize = 64 * 1024;
// A native completion carries both raw and finalized recording text.
const MAX_EVENT_BYTES: usize = MAX_RESULT_BYTES * 2 + 4096;
const MAX_REGION_BYTES: usize = 64 * 1024;
const FRAME_BYTES: usize = 3_200;

#[derive(Clone, Copy, Debug)]
pub(crate) enum Source {
    Qoder,
    QwenWork,
}

impl Source {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Qoder => "qoder",
            Self::QwenWork => "qwenwork",
        }
    }

    pub(crate) fn source_error(self, error: &str) -> String {
        match error.strip_prefix("qoder_") {
            Some(code) => format!("{}_{code}", self.name()),
            None => format!("{}_transcription_failed", self.name()),
        }
    }

    fn credential(self) -> Option<(String, Value)> {
        match self {
            Self::Qoder => super::auth::credential(),
            Self::QwenWork => None,
        }
    }
}

/// The source owner resolves its own quota identity and credential lineage.
pub(crate) struct CredentialSnapshot {
    pub profile: Value,
    pub origin: Option<String>,
    pub publication_check: Arc<dyn Fn() -> bool + Send + Sync>,
}

pub(crate) struct NativeResponse {
    pub value: Value,
    pub publication_check: Arc<dyn Fn() -> bool + Send + Sync>,
}

/// One recording-level result, distinct from the display-only ASR snapshots.
#[derive(Debug)]
pub(crate) struct SpeechResult {
    pub text: String,
    pub raw_text: String,
    pub polished: bool,
    pub refinement_complete: bool,
}

impl SpeechResult {
    pub(crate) fn unrefined(text: String) -> Self {
        Self {
            raw_text: text.clone(),
            text,
            polished: false,
            refinement_complete: false,
        }
    }
}

fn credential_identity(profile: &(String, Value)) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(profile.0.as_bytes());
    digest.update([0]);
    digest.update(profile.1.to_string().as_bytes());
    format!("{:x}", digest.finalize())
}

fn usable_profile(profile: &Value) -> Result<(), String> {
    if !["security_oauth_token", "access_token"].iter().any(|key| {
        profile[*key]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    }) || !profile.get("uid").is_some_and(|value| {
        value.as_i64().is_some() || value.as_str().is_some_and(|text| !text.is_empty())
    }) {
        return Err("qoder_transcription_signed_out".into());
    }
    if let Some(expiry) = profile.get("expire_time").and_then(Value::as_i64) {
        let expiry = if expiry > 100_000_000_000 {
            expiry / 1000
        } else {
            expiry
        };
        if expiry <= chrono::Utc::now().timestamp() {
            // A copied login is never refreshed here: its owner may rotate it.
            return Err("qoder_transcription_login_expired".into());
        }
    }
    Ok(())
}

pub(crate) async fn transcribe_audio(
    source: Source,
    params: SubscriptionTranscribeParams,
    signal: AbortSignal,
) -> Result<NativeResponse, String> {
    let profile = source
        .credential()
        .ok_or("qoder_transcription_signed_out")?;
    let identity = credential_identity(&profile);
    if params
        .expected_credential_generation
        .as_ref()
        .is_some_and(|expected| expected != &identity)
    {
        return Err("qoder_transcription_credential_changed".into());
    }
    let publication_check: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(move || {
        source
            .credential()
            .is_some_and(|profile| credential_identity(&profile) == identity)
    });
    transcribe_prepared(
        source,
        params,
        signal,
        CredentialSnapshot {
            profile: profile.1,
            origin: None,
            publication_check,
        },
    )
    .await
}

pub(crate) type TextCallback =
    Arc<dyn Fn(String) -> futures::future::BoxFuture<'static, ()> + Send + Sync>;

pub(crate) struct LiveCallbacks {
    pub ready: tokio::sync::oneshot::Sender<String>,
    pub generation: String,
    pub publish: crate::app_server::qoder::speech::TextCallback,
    pub recognized: Option<crate::app_server::qoder::speech::TextCallback>,
    pub valid: Arc<dyn Fn() -> bool + Send + Sync>,
}

pub(crate) async fn stream_dictation(
    source: Source,
    input: tokio::sync::mpsc::Receiver<Vec<u8>>,
    ready: tokio::sync::oneshot::Sender<String>,
    publish: crate::app_server::qoder::speech::TextCallback,
    signal: AbortSignal,
    refinement: Option<DictationRefinementConfig>,
    recognized: Option<crate::app_server::qoder::speech::TextCallback>,
) -> Result<SpeechResult, String> {
    let profile = source
        .credential()
        .ok_or("qoder_transcription_signed_out")?;
    let generation = credential_identity(&profile);
    let expected = generation.clone();
    let valid: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(move || {
        source
            .credential()
            .is_some_and(|value| credential_identity(&value) == expected)
    });
    stream_prepared(
        source,
        input,
        signal,
        CredentialSnapshot {
            profile: profile.1,
            origin: None,
            publication_check: valid.clone(),
        },
        LiveCallbacks {
            ready,
            generation,
            publish,
            recognized,
            valid,
        },
        refinement,
    )
    .await
}

pub(crate) async fn stream_prepared(
    source: Source,
    input: tokio::sync::mpsc::Receiver<Vec<u8>>,
    signal: AbortSignal,
    credential: CredentialSnapshot,
    callbacks: LiveCallbacks,
    refinement: Option<DictationRefinementConfig>,
) -> Result<SpeechResult, String> {
    if let Some(config) = &refinement {
        config.validate()?;
    }
    usable_profile(&credential.profile)?;
    let valid = callbacks.valid.clone();
    let work = async {
        let origin = match credential.origin {
            Some(origin) if source_origin(source, &origin) => origin,
            Some(_) => return Err("qoder_transcription_region_invalid".into()),
            None if matches!(source, Source::QwenWork) => {
                return Err("qoder_transcription_region_invalid".into())
            }
            None => audio_origin().await?,
        };
        if !valid() {
            return Err("qoder_transcription_credential_changed".into());
        }
        let session_id = uuid::Uuid::new_v4().to_string();
        let mut profile = credential.profile;
        if profile["security_oauth_token"]
            .as_str()
            .is_none_or(str::is_empty)
        {
            profile["security_oauth_token"] = profile["access_token"].clone();
        }
        let request = audio_request(source, &origin, &profile, &session_id)?;
        let config = WebSocketConfig {
            max_message_size: Some(MAX_EVENT_BYTES),
            max_frame_size: Some(MAX_EVENT_BYTES),
            ..WebSocketConfig::default()
        };
        let (socket, _) = tokio_tungstenite::connect_async_with_config(request, Some(config), true)
            .await
            .map_err(|error| connection_error(source, error))?;
        transcribe_feed_complete(
            source,
            socket,
            AudioFeed::Live(input),
            &session_id,
            Some(callbacks),
            refinement,
        )
        .await
    };
    let changed = async {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await;
            if !valid() {
                break;
            }
        }
    };
    let result = tokio::select! {
        biased;
        _ = signal.cancelled() => Err("qoder_transcription_cancelled".into()),
        _ = changed => Err("qoder_transcription_credential_changed".into()),
        result = tokio::time::timeout(Duration::from_secs(360), work) => result.map_err(|_| "qoder_transcription_timed_out")?,
    };
    if signal.is_cancelled() {
        return Err("qoder_transcription_cancelled".into());
    }
    if !valid() {
        return Err("qoder_transcription_credential_changed".into());
    }
    result
}

pub(crate) async fn transcribe_prepared(
    source: Source,
    params: SubscriptionTranscribeParams,
    signal: AbortSignal,
    credential: CredentialSnapshot,
) -> Result<NativeResponse, String> {
    if signal.is_cancelled() {
        return Err("qoder_transcription_cancelled".into());
    }
    if params.language.is_some() {
        return Err("qoder_transcription_language_override_unavailable".into());
    }
    let pcm = decode_audio(&params.audio_base64, params.mime_type.as_deref())?;
    usable_profile(&credential.profile)?;
    if matches!(source, Source::QwenWork) && credential.origin.is_none() {
        return Err("qoder_transcription_region_invalid".into());
    }
    let publication_check = credential.publication_check;
    if !publication_check() {
        return Err("qoder_transcription_credential_changed".into());
    }
    let timeout = Duration::from_secs(30 + (pcm.len() / 32_000) as u64);
    let result = cancellable(&signal, timeout, async {
        let origin = match credential.origin {
            Some(origin) if source_origin(source, &origin) => origin,
            Some(_) => return Err("qoder_transcription_region_invalid".into()),
            None => audio_origin().await?,
        };
        let session_id = uuid::Uuid::new_v4().to_string();
        let mut profile = credential.profile;
        if profile["security_oauth_token"]
            .as_str()
            .is_none_or(str::is_empty)
        {
            profile["security_oauth_token"] = profile["access_token"].clone();
        }
        let request = audio_request(source, &origin, &profile, &session_id)?;
        let config = WebSocketConfig {
            max_message_size: Some(MAX_RESULT_BYTES),
            max_frame_size: Some(MAX_RESULT_BYTES),
            ..WebSocketConfig::default()
        };
        let (socket, _) = tokio_tungstenite::connect_async_with_config(request, Some(config), true)
            .await
            .map_err(|error| connection_error(source, error))?;
        transcribe_socket(source, socket, &pcm, &session_id).await
    })
    .await?;
    if signal.is_cancelled() {
        return Err("qoder_transcription_cancelled".into());
    }
    if !publication_check() {
        return Err("qoder_transcription_credential_changed".into());
    }
    Ok(NativeResponse {
        value: json!({"text": result}),
        publication_check,
    })
}

fn connection_error(source: Source, error: tokio_tungstenite::tungstenite::Error) -> String {
    use tokio_tungstenite::tungstenite::Error;
    let code = match &error {
        Error::Http(response) => {
            let status = response.status().as_u16();
            eprintln!(
                "[dictation-transport] source={} websocket_status={status}",
                source.name()
            );
            match status {
                401 => "authentication_required",
                402 => "billing_required",
                403 => "permission_denied",
                429 => "rate_limited",
                _ => "provider_rejected",
            }
        }
        _ => {
            let kind = match error {
                Error::Tls(_) => "tls",
                Error::Io(_) => "io",
                Error::Url(_) => "url",
                _ => "protocol",
            };
            eprintln!(
                "[dictation-transport] source={} connection_kind={kind}",
                source.name()
            );
            "connection_failed"
        }
    };
    format!("qoder_transcription_{code}")
}

async fn cancellable<T>(
    signal: &AbortSignal,
    timeout: Duration,
    work: impl Future<Output = Result<T, String>>,
) -> Result<T, String> {
    tokio::select! {
        biased;
        _ = signal.cancelled() => Err("qoder_transcription_cancelled".into()),
        result = tokio::time::timeout(timeout, work) => {
            let value = result.map_err(|_| "qoder_transcription_timed_out")??;
            if signal.is_cancelled() { return Err("qoder_transcription_cancelled".into()); }
            Ok(value)
        }
    }
}

fn audio_request(
    source: Source,
    origin: &str,
    profile: &Value,
    session_id: &str,
) -> Result<http::Request<()>, String> {
    let url = format!("{}{AUDIO_PATH}", origin.trim_end_matches('/'));
    let signed = super::sign::sign_audio_get(&url, profile)
        .map_err(|_| "qoder_transcription_authentication_failed")?;
    let socket_url = url
        .replacen("https:", "wss:", 1)
        .replacen("http:", "ws:", 1);
    let mut request = socket_url
        .into_client_request()
        .map_err(|_| "qoder_transcription_request_invalid")?;
    let business_id = uuid::Uuid::new_v4().to_string();
    let business = json!({
        "product":if matches!(source, Source::QwenWork) { "qoder_work" } else { "ide" },
        "type":"asr_chat", "id":business_id,
        "begin_at":chrono::Utc::now().timestamp_millis(),
        "name":format!("asr_chat-{business_id}"),
    })
    .to_string();
    let headers = signed.headers.into_iter().filter(|(name, _)| {
        !matches!(source, Source::QwenWork)
            || !matches!(name.as_str(), "X-Machine-Id" | "X-Machine-Type")
    });
    for (name, value) in headers.chain([
        (
            "X-IDE-Platform".into(),
            if matches!(source, Source::QwenWork) {
                "qoder_work"
            } else {
                "Klaus"
            }
            .into(),
        ),
        ("SampleRate".into(), "16000".into()),
        ("FrameDurationMs".into(), "100".into()),
        ("BitDepth".into(), "16".into()),
        ("Channels".into(), "1".into()),
        ("X-Asr-Session-Id".into(), session_id.into()),
        ("X-Business".into(), business),
    ]) {
        request.headers_mut().insert(
            http::HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| "qoder_transcription_request_invalid")?,
            http::HeaderValue::from_str(&value)
                .map_err(|_| "qoder_transcription_request_invalid")?,
        );
    }
    Ok(request)
}

fn valid_origin(text: &str) -> bool {
    url::Url::parse(text).is_ok_and(|url| {
        url.scheme() == "https"
            && url.username().is_empty()
            && url.password().is_none()
            && url.port().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && matches!(url.path(), "" | "/")
            && url
                .host_str()
                .is_some_and(|host| host == "qoder.sh" || host.ends_with(".qoder.sh"))
    })
}

fn source_origin(source: Source, text: &str) -> bool {
    #[cfg(test)]
    if url::Url::parse(text).is_ok_and(|url| {
        url.scheme() == "http"
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && matches!(url.path(), "" | "/")
            && url
                .host_str()
                .is_some_and(|host| matches!(host, "127.0.0.1" | "[::1]"))
    }) {
        return true;
    }
    if matches!(source, Source::QwenWork) {
        url::Url::parse(text).is_ok_and(|url| {
            url.scheme() == "https"
                && url.username().is_empty()
                && url.password().is_none()
                && url.port().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
                && matches!(url.path(), "" | "/")
                && url.host_str() == Some("gateway.qwenwork.cn")
        })
    } else {
        valid_origin(text)
    }
}

pub(crate) async fn audio_origin() -> Result<String, String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| "qoder_transcription_connection_failed")?;
    let response = match client.get(REGION_URL).send().await {
        Ok(response) if response.status().is_success() => response,
        _ => return Ok(DEFAULT_ORIGIN.into()),
    };
    let mut bytes = Vec::new();
    let mut chunks = response.bytes_stream();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(|_| "qoder_transcription_region_invalid")?;
        if bytes.len().saturating_add(chunk.len()) > MAX_REGION_BYTES {
            return Err("qoder_transcription_region_invalid".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    let region: Value =
        serde_json::from_slice(&bytes).map_err(|_| "qoder_transcription_region_invalid")?;
    let Some(origin) = region
        .get("inferNodes")
        .and_then(Value::as_array)
        .and_then(|nodes| nodes.first())
        .and_then(Value::as_str)
    else {
        return Ok(DEFAULT_ORIGIN.into());
    };
    if !valid_origin(origin) {
        return Err("qoder_transcription_region_invalid".into());
    }
    Ok(origin.trim_end_matches('/').to_string())
}

fn decode_audio(encoded: &str, mime_type: Option<&str>) -> Result<Vec<u8>, String> {
    if encoded.len() > MAX_AUDIO_BYTES.div_ceil(3) * 4 {
        return Err("qoder_transcription_audio_too_large".into());
    }
    let bytes = STANDARD
        .decode(encoded)
        .map_err(|_| "qoder_transcription_audio_invalid")?;
    if bytes.len() > MAX_AUDIO_BYTES {
        return Err("qoder_transcription_audio_too_large".into());
    }
    let pcm = match mime_type.unwrap_or("audio/wav") {
        "audio/wav" | "audio/x-wav" => wav_pcm(&bytes)?,
        "audio/pcm" => bytes.as_slice(),
        _ => return Err("qoder_transcription_mime_type_unsupported".into()),
    };
    if pcm.is_empty() || pcm.len() % 2 != 0 {
        return Err("qoder_transcription_audio_invalid".into());
    }
    if pcm.len() > MAX_PCM_BYTES {
        return Err("qoder_transcription_audio_too_large".into());
    }
    Ok(pcm.to_vec())
}

fn wav_pcm(bytes: &[u8]) -> Result<&[u8], String> {
    let invalid = || "qoder_transcription_wav_format_invalid".to_string();
    if bytes.len() < 12
        || &bytes[..4] != b"RIFF"
        || &bytes[8..12] != b"WAVE"
        || u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize != bytes.len() - 8
    {
        return Err(invalid());
    }
    let mut offset = 12;
    let mut format = false;
    let mut pcm = None;
    while offset < bytes.len() {
        let header = bytes.get(offset..offset + 8).ok_or_else(invalid)?;
        let size = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        offset += 8;
        let end = offset.checked_add(size).ok_or_else(invalid)?;
        let chunk = bytes.get(offset..end).ok_or_else(invalid)?;
        match &header[..4] {
            b"fmt " => {
                if format
                    || chunk.len() < 16
                    || u16::from_le_bytes(chunk[0..2].try_into().unwrap()) != 1
                    || u16::from_le_bytes(chunk[2..4].try_into().unwrap()) != 1
                    || u32::from_le_bytes(chunk[4..8].try_into().unwrap()) != 16_000
                    || u32::from_le_bytes(chunk[8..12].try_into().unwrap()) != 32_000
                    || u16::from_le_bytes(chunk[12..14].try_into().unwrap()) != 2
                    || u16::from_le_bytes(chunk[14..16].try_into().unwrap()) != 16
                {
                    return Err(invalid());
                }
                format = true;
            }
            b"data" => {
                if pcm.replace(chunk).is_some() {
                    return Err(invalid());
                }
            }
            _ => {}
        }
        offset = end.checked_add(size % 2).ok_or_else(invalid)?;
        if offset > bytes.len() {
            return Err(invalid());
        }
    }
    if !format {
        return Err(invalid());
    }
    pcm.ok_or_else(invalid)
}

fn pcm_clock(bytes: usize) -> Duration {
    Duration::from_micros(bytes as u64 * 1_000_000 / 32_000)
}

async fn transcribe_socket<S>(
    source: Source,
    socket: tokio_tungstenite::WebSocketStream<S>,
    pcm: &[u8],
    session_id: &str,
) -> Result<String, String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    transcribe_feed(source, socket, AudioFeed::Buffered(pcm), session_id, None).await
}

enum AudioFeed<'a> {
    Buffered(&'a [u8]),
    Live(tokio::sync::mpsc::Receiver<Vec<u8>>),
}

async fn transcribe_feed<S>(
    source: Source,
    socket: tokio_tungstenite::WebSocketStream<S>,
    feed: AudioFeed<'_>,
    session_id: &str,
    callbacks: Option<LiveCallbacks>,
) -> Result<String, String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    transcribe_feed_complete(source, socket, feed, session_id, callbacks, None)
        .await
        .map(|result| result.text)
}

#[derive(Default)]
struct RefinementResult {
    text: String,
    raw_text: String,
    polished: bool,
    failed: bool,
}

fn recording_result(
    raw_text: &str,
    refinement: Option<&RefinementResult>,
) -> Result<SpeechResult, String> {
    let raw_text = raw_text.trim();
    if let Some(refinement) = refinement.filter(|value| !value.failed) {
        let raw_text = if refinement.raw_text.trim().is_empty() {
            raw_text
        } else {
            refinement.raw_text.trim()
        };
        let text = if refinement.text.trim().is_empty() {
            raw_text
        } else {
            refinement.text.trim()
        };
        if text.is_empty() {
            return Err("qoder_transcription_no_speech".into());
        }
        return Ok(SpeechResult {
            text: text.to_owned(),
            raw_text: raw_text.to_owned(),
            polished: refinement.polished,
            refinement_complete: true,
        });
    }
    if raw_text.is_empty() {
        return Err("qoder_transcription_no_speech".into());
    }
    Ok(SpeechResult::unrefined(raw_text.to_owned()))
}

async fn transcribe_feed_complete<S>(
    source: Source,
    socket: tokio_tungstenite::WebSocketStream<S>,
    feed: AudioFeed<'_>,
    session_id: &str,
    callbacks: Option<LiveCallbacks>,
    refinement: Option<DictationRefinementConfig>,
) -> Result<SpeechResult, String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if let Some(config) = &refinement {
        config.validate()?;
    }
    let refinement = refinement.map(|config| (uuid::Uuid::new_v4().to_string(), config));
    let clock = tokio::time::Instant::now();
    let diagnostic =
        |stage: &str, enabled: Option<bool>, polished: Option<bool>, failed: Option<bool>| {
            eprintln!(
                "[dictation-transport] {}",
                json!({
                    "source":source.name(), "session":session_id, "stage":stage,
                    "elapsed_ms":clock.elapsed().as_millis(),
                    "enabled":enabled, "polished":polished, "failed":failed,
                })
            );
        };
    let retain_interim_tail = callbacks.is_some() && matches!(source, Source::QwenWork);
    let (mut writer, mut reader) = socket.split();
    let (publish, recognized, valid) = if let Some(callbacks) = callbacks {
        if !(callbacks.valid)() {
            return Err("qoder_transcription_credential_changed".into());
        }
        let _ = callbacks.ready.send(callbacks.generation);
        (
            Some(callbacks.publish),
            callbacks.recognized,
            Some(callbacks.valid),
        )
    } else {
        (None, None, None)
    };
    let uploaded = Arc::new(AtomicBool::new(false));
    let upload_done = uploaded.clone();
    let send = async {
        if let Some((request_id, config)) = &refinement {
            writer
                .send(Message::Text(
                    json!({
                        "type":"asr_polish_config",
                        "enabled":true,
                        "request_id":request_id,
                        "client_type":6,
                        "source":"chat_input",
                        "messages":config.messages,
                    })
                    .to_string(),
                ))
                .await
                .map_err(|_| "qoder_transcription_connection_failed".to_string())?;
            diagnostic("refinement_config_sent", Some(true), None, None);
        }
        let started = tokio::time::Instant::now();
        let mut sent = 0usize;
        match feed {
            AudioFeed::Buffered(pcm) => {
                for frame in pcm.chunks(FRAME_BYTES) {
                    if matches!(source, Source::QwenWork) {
                        tokio::time::sleep_until(started + pcm_clock(sent)).await;
                    }
                    writer
                        .send(Message::Binary(frame.to_vec()))
                        .await
                        .map_err(|_| "qoder_transcription_connection_failed".to_string())?;
                    sent += frame.len();
                }
                if matches!(source, Source::QwenWork) {
                    tokio::time::sleep_until(started + pcm_clock(sent)).await;
                }
            }
            AudioFeed::Live(mut input) => {
                while let Some(frame) = input.recv().await {
                    if valid.as_ref().is_some_and(|check| !check()) {
                        return Err("qoder_transcription_credential_changed".into());
                    }
                    if frame.is_empty()
                        || frame.len() > 32768
                        || !frame.len().is_multiple_of(2)
                        || sent.saturating_add(frame.len()) > MAX_PCM_BYTES
                    {
                        return Err("qoder_transcription_audio_invalid".into());
                    }
                    sent += frame.len();
                    writer
                        .send(Message::Binary(frame))
                        .await
                        .map_err(|_| "qoder_transcription_connection_failed".to_string())?;
                }
            }
        }
        upload_done.store(true, Ordering::Release);
        writer
            .send(Message::Text(
                json!({"type":"voice_completed","message":"close by user"}).to_string(),
            ))
            .await
            .map_err(|_| "qoder_transcription_connection_failed".to_string())?;
        diagnostic("voice_completed_sent", None, None, None);
        Ok::<(), String>(())
    };
    let receive = async {
        let mut result = String::new();
        let mut interim = String::new();
        let mut speech_done = false;
        let mut refinement_enabled = false;
        let mut refined: Option<RefinementResult> = None;
        let mut refinement_deadline = None;
        let snapshot = async |committed: &str, interim: &str| {
            if let Some(publish) = &publish {
                if valid.as_ref().is_some_and(|check| !check()) {
                    return;
                }
                publish(format!("{committed}{interim}")).await;
            }
        };
        loop {
            let message = if let Some(deadline) = refinement_deadline {
                tokio::time::timeout_at(deadline, reader.next())
                    .await
                    .map_err(|_| "qoder_transcription_refinement_timed_out")?
            } else {
                reader.next().await
            };
            let Some(message) = message else { break };
            let message = message.map_err(|_| "qoder_transcription_connection_failed")?;
            let Message::Text(text) = message else {
                if matches!(message, Message::Close(_)) {
                    break;
                }
                continue;
            };
            if text.len() > MAX_EVENT_BYTES {
                return Err("qoder_transcription_result_too_large".into());
            }
            let event: Value =
                serde_json::from_str(&text).map_err(|_| "qoder_transcription_response_invalid")?;
            let event_type = event.get("type").and_then(Value::as_str);
            let refinement_event = matches!(
                event_type,
                Some("asr_polish_config_ack" | "polish_completed")
            );
            if !refinement_event
                && event
                    .get("asr_session_id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| id != session_id)
            {
                continue;
            }
            match event_type {
                Some("speech_err") => return Err("qoder_transcription_refused".into()),
                Some("speech_processing" | "speech_delta") => {
                    if speech_done {
                        continue;
                    }
                    interim = event
                        .get("message")
                        .and_then(Value::as_str)
                        .ok_or("qoder_transcription_response_invalid")?
                        .to_owned();
                    if result.len().saturating_add(interim.len()) > MAX_RESULT_BYTES {
                        return Err("qoder_transcription_result_too_large".into());
                    }
                    snapshot(&result, &interim).await;
                }
                Some("speech_completed") => {
                    if speech_done {
                        continue;
                    }
                    let segment = event
                        .get("message")
                        .and_then(Value::as_str)
                        .ok_or("qoder_transcription_response_invalid")?;
                    if result.len().saturating_add(segment.len()) > MAX_RESULT_BYTES {
                        return Err("qoder_transcription_result_too_large".into());
                    }
                    // Completion commits one utterance; only the current interim
                    // is replaceable while the recording continues.
                    result.push_str(segment);
                    interim.clear();
                    snapshot(&result, "").await;
                }
                Some("speech_done") => {
                    diagnostic("speech_done", Some(refinement_enabled), None, None);
                    if valid.as_ref().is_some_and(|check| !check()) {
                        return Err("qoder_transcription_credential_changed".into());
                    }
                    if !uploaded.load(Ordering::Acquire) {
                        return Err("qoder_transcription_incomplete".into());
                    }
                    if !speech_done && retain_interim_tail {
                        result.push_str(&interim);
                        interim.clear();
                    }
                    if !speech_done && refinement_enabled {
                        if let Some(recognized) = &recognized {
                            if !result.trim().is_empty() {
                                recognized(result.trim().to_owned()).await;
                            }
                        }
                    }
                    speech_done = true;
                    if !refinement_enabled || refined.is_some() {
                        return recording_result(
                            &result,
                            refinement_enabled.then_some(refined.as_ref()).flatten(),
                        );
                    }
                    // This is a failure boundary for a missing complete response,
                    // not a latency shortcut that publishes an unfinished result.
                    refinement_deadline.get_or_insert_with(|| {
                        tokio::time::Instant::now() + Duration::from_secs(20)
                    });
                }
                Some("asr_polish_config_ack") => {
                    if refinement.as_ref().is_some_and(|(request_id, _)| {
                        event.get("request_id").and_then(Value::as_str) == Some(request_id)
                    }) {
                        refinement_enabled =
                            event.get("enabled").and_then(Value::as_bool) == Some(true);
                        diagnostic("refinement_ack", Some(refinement_enabled), None, None);
                        if speech_done && !refinement_enabled {
                            if valid.as_ref().is_some_and(|check| !check()) {
                                return Err("qoder_transcription_credential_changed".into());
                            }
                            return recording_result(&result, None);
                        }
                    }
                }
                Some("polish_completed") => {
                    if !refinement.as_ref().is_some_and(|(request_id, _)| {
                        event.get("request_id").and_then(Value::as_str) == Some(request_id)
                    }) {
                        continue;
                    }
                    let text = event
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let raw_text = event
                        .get("raw_text")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    if text.len() > MAX_RESULT_BYTES || raw_text.len() > MAX_RESULT_BYTES {
                        return Err("qoder_transcription_result_too_large".into());
                    }
                    let error = event.get("error");
                    let polished = event.get("polished").and_then(Value::as_bool) == Some(true);
                    let failed = error
                        .is_some_and(|value| !value.is_null() && value != false && value != "");
                    diagnostic(
                        "polish_completed",
                        Some(refinement_enabled),
                        Some(polished),
                        Some(failed),
                    );
                    refined = Some(RefinementResult {
                        text: text.to_owned(),
                        raw_text: raw_text.to_owned(),
                        polished,
                        failed,
                    });
                    if speech_done && refinement_enabled {
                        if valid.as_ref().is_some_and(|check| !check()) {
                            return Err("qoder_transcription_credential_changed".into());
                        }
                        return recording_result(&result, refined.as_ref());
                    }
                }
                Some("speech_cost") => {}
                _ => {}
            }
        }
        Err("qoder_transcription_incomplete".into())
    };
    let (_, text) = tokio::try_join!(send, receive)?;
    Ok(text)
}

#[cfg(test)]
#[path = "speech_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "speech_refinement_tests.rs"]
mod refinement_tests;
