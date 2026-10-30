//! Bounded standalone subscription dictation. Audio never enters a model turn.

use super::{claude_usage, model_sources, wire};
use base64::Engine;
use futures::{SinkExt, StreamExt};
use klaus_agent_core::credential_store::CredentialStore;
use klaus_agent_core::provider::auth::AuthSource;
use klaus_agent_core::types::abort::AbortSignal;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};

const MAX_AUDIO: usize = 25 * 1024 * 1024;
const MAX_TEXT: usize = 48_000;
const MAX_RESPONSE: usize = 1024 * 1024;
const BUDGET: Duration = Duration::from_secs(120);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Source {
    Claude,
    Codex,
}
impl Source {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Codex => "codex",
        }
    }
    fn error(self, category: &str) -> String {
        format!("{}_transcription_{category}", self.name())
    }
}

#[derive(Clone)]
pub(super) struct Options {
    claude_url: String,
    codex_url: String,
    pub(super) codex_stream_url: String,
    budget: Duration,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            claude_url: "wss://api.anthropic.com/api/ws/speech_to_text/voice_stream".into(),
            codex_url: "https://chatgpt.com/backend-api/transcribe".into(),
            codex_stream_url: "wss://chatgpt.com/backend-api/dictation/stream?dictation_surface=composer&client_type=desktop".into(),
            budget: BUDGET,
        }
    }
}

pub(super) struct Response {
    pub(super) value: wire::SubscriptionTranscriptResult,
    pub(super) publication_check: Arc<dyn Fn() -> bool + Send + Sync>,
}

pub(super) fn language(value: Option<&str>) -> Result<Option<String>, String> {
    value
        .map(|value| {
            if !(2..=3).contains(&value.len()) || !value.bytes().all(|b| b.is_ascii_alphabetic()) {
                return Err("provider_transcription_language_invalid".into());
            }
            Ok(value.to_ascii_lowercase())
        })
        .transpose()
}

fn decode_audio(params: &wire::SubscriptionTranscribeParams) -> Result<Vec<u8>, String> {
    if params.audio_base64.is_empty() || params.audio_base64.len() > MAX_AUDIO.div_ceil(3) * 4 {
        return Err("provider_transcription_audio_invalid".into());
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(&params.audio_base64)
        .map_err(|_| "provider_transcription_audio_invalid")?;
    if bytes.is_empty() || bytes.len() > MAX_AUDIO {
        return Err("provider_transcription_audio_invalid".into());
    }
    Ok(bytes)
}

fn mime(value: Option<&str>) -> Result<(&'static str, &'static str), String> {
    let value = value.unwrap_or("audio/webm");
    if value.len() > 128 || value.bytes().any(|b| b.is_ascii_control()) {
        return Err("provider_transcription_mime_invalid".into());
    }
    if value
        .split(';')
        .next()
        .is_some_and(|kind| kind.trim().eq_ignore_ascii_case("audio/pcm"))
    {
        for parameter in value.split(';').skip(1) {
            let valid = parameter
                .trim()
                .split_once('=')
                .is_some_and(|(key, value)| {
                    matches!(
                        (key.trim(), value.trim().trim_matches('"')),
                        ("rate", "16000") | ("channels", "1") | ("encoding", "s16le")
                    )
                });
            if !valid {
                return Err("provider_transcription_format_unsupported".into());
            }
        }
    }
    match value
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "audio/webm" | "video/webm" => Ok(("audio/webm", "webm")),
        "audio/ogg" | "application/ogg" => Ok(("audio/ogg", "ogg")),
        "audio/wav" | "audio/wave" | "audio/x-wav" => Ok(("audio/wav", "wav")),
        "audio/mp4" | "audio/x-m4a" => Ok(("audio/mp4", "mp4")),
        "audio/mpeg" | "audio/mp3" => Ok(("audio/mpeg", "mp3")),
        "audio/pcm" => Ok(("audio/pcm", "pcm")),
        _ => Err("provider_transcription_format_unsupported".into()),
    }
}

/// The native stream accepts signed little-endian PCM16, mono, 16 kHz only.
fn pcm_audio(bytes: &[u8], kind: &str) -> Result<Vec<u8>, String> {
    if kind == "audio/pcm" {
        return (!bytes.is_empty() && bytes.len().is_multiple_of(2))
            .then(|| bytes.to_vec())
            .ok_or_else(|| "claude_transcription_audio_invalid".into());
    }
    if kind != "audio/wav" || bytes.len() < 12 || &bytes[..4] != b"RIFF" || &bytes[8..12] != b"WAVE"
    {
        return Err("claude_transcription_format_unsupported".into());
    }
    let u16le = |offset| u16::from_le_bytes([bytes[offset], bytes[offset + 1]]);
    let u32le = |offset| u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    if u32le(4) as usize != bytes.len() - 8 {
        return Err("claude_transcription_audio_invalid".into());
    }
    let mut offset = 12usize;
    let mut format = false;
    let mut data = None;
    while offset < bytes.len() {
        let header_end = offset
            .checked_add(8)
            .filter(|end| *end <= bytes.len())
            .ok_or("claude_transcription_audio_invalid")?;
        let length = u32le(offset + 4) as usize;
        let end = header_end
            .checked_add(length)
            .filter(|end| *end <= bytes.len())
            .ok_or("claude_transcription_audio_invalid")?;
        match &bytes[offset..offset + 4] {
            b"fmt " => {
                if format || length < 16 {
                    return Err("claude_transcription_audio_invalid".into());
                }
                if u16le(header_end) != 1
                    || u16le(header_end + 2) != 1
                    || u32le(header_end + 4) != 16_000
                    || u32le(header_end + 8) != 32_000
                    || u16le(header_end + 12) != 2
                    || u16le(header_end + 14) != 16
                {
                    return Err("claude_transcription_format_unsupported".into());
                }
                format = true;
            }
            b"data" => {
                if data.is_some() || length == 0 || !length.is_multiple_of(2) {
                    return Err("claude_transcription_audio_invalid".into());
                }
                data = Some(bytes[header_end..end].to_vec());
            }
            _ => {}
        }
        offset = end
            .checked_add(length % 2)
            .filter(|end| *end <= bytes.len())
            .ok_or("claude_transcription_audio_invalid")?;
    }
    if !format {
        return Err("claude_transcription_audio_invalid".into());
    }
    data.ok_or_else(|| "claude_transcription_audio_invalid".into())
}

fn credential_identity(root: &Path, source: Source) -> Option<String> {
    let paths = match source {
        Source::Claude => vec![
            model_sources::claude_subscription_path(root),
            root.join(".credentials.json"),
        ],
        Source::Codex => vec![model_sources::codex_store_path(root)],
    };
    if paths
        .iter()
        .any(|path| std::fs::metadata(path).is_ok_and(|meta| meta.len() > MAX_RESPONSE as u64))
    {
        return None;
    }
    match source {
        Source::Claude => {
            let accounts = model_sources::claude_accounts(root).ok()?;
            let account = accounts.first()?;
            let tokens = account.get("tokens")?.as_object()?;
            Some(format!(
                "{}:{}",
                account["id"].as_str()?,
                model_sources::account_credential_id("subscription", tokens)
            ))
        }
        Source::Codex => {
            let account = model_sources::codex_account(root)?;
            Some(format!(
                "{}:{}",
                account["accountId"].as_str()?,
                model_sources::account_credential_id("codex", &account)
            ))
        }
    }
}

pub(super) fn pin(
    root: &Path,
    source: Source,
) -> Result<Arc<dyn Fn() -> bool + Send + Sync>, String> {
    let expected =
        credential_identity(root, source).ok_or_else(|| source.error("authentication_required"))?;
    Ok(pin_identity(root, source, expected))
}

fn pin_identity(
    root: &Path,
    source: Source,
    expected: String,
) -> Arc<dyn Fn() -> bool + Send + Sync> {
    let root = root.to_path_buf();
    Arc::new(move || credential_identity(&root, source).as_ref() == Some(&expected))
}

struct ClaudeCredential {
    headers: std::collections::HashMap<String, String>,
    generation: String,
    current: Arc<dyn Fn() -> bool + Send + Sync>,
}

async fn claude_credential(
    root: &Path,
    store: Option<Arc<dyn CredentialStore>>,
    usage: &claude_usage::UsageOptions,
    expected_generation: Option<&str>,
) -> Result<ClaudeCredential, String> {
    let source = Source::Claude;
    let identity =
        credential_identity(root, source).ok_or_else(|| source.error("authentication_required"))?;
    let generation = format!("{:x}", Sha256::digest(identity.as_bytes()));
    if expected_generation.is_some_and(|expected| expected != generation) {
        return Err(source.error("credential_changed"));
    }
    let current = pin_identity(root, source, identity);
    let auth = claude_usage::current_auth(root, store, usage)
        .await
        .map_err(|_| source.error("authentication_required"))?;
    // Once a refresh starts its rotated grant must be stored even on cancellation.
    let headers = tokio::spawn(async move { auth.get_headers(&AbortSignal::new()).await })
        .await
        .map_err(|_| source.error("authentication_required"))?
        .map_err(|_| source.error("authentication_required"))?;
    if !current() {
        return Err(source.error("credential_changed"));
    }
    Ok(ClaudeCredential {
        headers,
        generation,
        current,
    })
}

pub(super) struct ClaudeDictationOptions {
    pub(super) root: PathBuf,
    pub(super) store: Option<Arc<dyn CredentialStore>>,
    pub(super) speech: Options,
    pub(super) usage: claude_usage::UsageOptions,
    pub(super) language: Option<String>,
}

pub(super) async fn stream_claude_dictation(
    context: ClaudeDictationOptions,
    input: tokio::sync::mpsc::Receiver<Vec<u8>>,
    ready: tokio::sync::oneshot::Sender<String>,
    publish: super::qoder::speech::TextCallback,
    signal: AbortSignal,
) -> Result<String, String> {
    claude_dictation::stream(context, input, ready, publish, signal).await
}

#[path = "claude_dictation.rs"]
mod claude_dictation;

pub(super) struct CodexCredential {
    pub(super) token: super::codex_catalog::Token,
    pub(super) generation: String,
    pub(super) current: Arc<dyn Fn() -> bool + Send + Sync>,
}

/// Bind streaming and retained-audio recovery to the same selected login.
pub(super) async fn codex_credential(
    root: &Path,
    expected_generation: Option<&str>,
) -> Result<CodexCredential, String> {
    let source = Source::Codex;
    let expected =
        credential_identity(root, source).ok_or_else(|| source.error("authentication_required"))?;
    let generation = format!("{:x}", Sha256::digest(expected.as_bytes()));
    if expected_generation.is_some_and(|value| value != generation) {
        return Err(source.error("credential_changed"));
    }
    let refresh_root = root.to_path_buf();
    let token = tokio::spawn(async move {
        super::codex_catalog::token_for_credential(&refresh_root, Some(&expected)).await
    })
    .await
    .map_err(|_| source.error("authentication_required"))?
    .ok_or_else(|| source.error("authentication_required"))?;
    let account =
        model_sources::codex_account(root).ok_or_else(|| source.error("credential_changed"))?;
    if account.get("accessToken").and_then(Value::as_str) != Some(token.access_token.as_str())
        || account.get("accountId").and_then(Value::as_str) != Some(token.account_id.as_str())
    {
        return Err(source.error("credential_changed"));
    }
    let identity = format!(
        "{}:{}",
        token.account_id,
        model_sources::account_credential_id("codex", &account)
    );
    Ok(CodexCredential {
        token,
        generation: format!("{:x}", Sha256::digest(identity.as_bytes())),
        current: pin_identity(root, source, identity),
    })
}

pub(super) async fn transcribe(
    root: PathBuf,
    store: Option<Arc<dyn CredentialStore>>,
    source: Source,
    params: wire::SubscriptionTranscribeParams,
    signal: AbortSignal,
    options: Options,
    usage_options: claude_usage::UsageOptions,
) -> Result<Response, String> {
    if signal.is_cancelled() {
        return Err(source.error("cancelled"));
    }
    let audio = decode_audio(&params)?;
    let default_mime = if source == Source::Claude {
        "audio/wav"
    } else {
        "audio/webm"
    };
    let (kind, extension) = mime(params.mime_type.as_deref().or(Some(default_mime)))?;
    let language = language(params.language.as_deref())?;
    if source == Source::Codex && kind == "audio/pcm" {
        return Err(source.error("format_unsupported"));
    }
    let audio = if source == Source::Claude {
        pcm_audio(&audio, kind)?
    } else {
        audio
    };
    let work = async {
        match source {
            Source::Claude => {
                let credential = claude_credential(
                    &root,
                    store,
                    &usage_options,
                    params.expected_credential_generation.as_deref(),
                )
                .await?;
                let headers = credential.headers;
                let publication_check = credential.current;
                let text = while_current(
                    source,
                    publication_check.clone(),
                    claude_stream(&options.claude_url, headers, audio, language.as_deref()),
                )
                .await?;
                Ok::<Response, String>(Response {
                    value: wire::SubscriptionTranscriptResult { text },
                    publication_check,
                })
            }
            Source::Codex => {
                let credential =
                    codex_credential(&root, params.expected_credential_generation.as_deref())
                        .await?;
                let token = credential.token;
                let publication_check = credential.current;
                let text = while_current(
                    source,
                    publication_check.clone(),
                    codex_request(
                        &options.codex_url,
                        &token.access_token,
                        &token.account_id,
                        audio,
                        kind,
                        extension,
                        language.as_deref(),
                    ),
                )
                .await?;
                Ok::<Response, String>(Response {
                    value: wire::SubscriptionTranscriptResult { text },
                    publication_check,
                })
            }
        }
    };
    tokio::select! {
        biased;
        _ = signal.cancelled() => Err(source.error("cancelled")),
        result = tokio::time::timeout(options.budget, work) => {
            let response = result.map_err(|_| source.error("timeout"))??;
            if !(response.publication_check)() { return Err(source.error("credential_changed")); }
            Ok(response)
        }
    }
}

async fn while_current<T>(
    source: Source,
    check: Arc<dyn Fn() -> bool + Send + Sync>,
    work: impl std::future::Future<Output = Result<T, String>>,
) -> Result<T, String> {
    let changed = async {
        loop {
            if !check() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    tokio::select! {
        biased;
        _ = changed => Err(source.error("credential_changed")),
        result = work => result,
    }
}

async fn claude_connect(
    endpoint: &str,
    headers: std::collections::HashMap<String, String>,
    language: Option<&str>,
    forward_interims: bool,
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    String,
> {
    let source = Source::Claude;
    let mut url = url::Url::parse(endpoint).map_err(|_| source.error("transport_failed"))?;
    url.query_pairs_mut().extend_pairs([
        ("encoding", "linear16"),
        ("sample_rate", "16000"),
        ("channels", "1"),
        ("endpointing_ms", "300"),
        ("utterance_end_ms", "1000"),
        ("language", language.unwrap_or("en")),
        ("use_conversation_engine", "true"),
    ]);
    if forward_interims {
        url.query_pairs_mut()
            .append_pair("forward_interims", "typed");
    }
    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(|_| source.error("transport_failed"))?;
    for (name, value) in headers {
        let name = http::header::HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| source.error("authentication_required"))?;
        let value = http::HeaderValue::from_str(&value)
            .map_err(|_| source.error("authentication_required"))?;
        request.headers_mut().insert(name, value);
    }
    request
        .headers_mut()
        .insert("x-app", http::HeaderValue::from_static("cli"));
    request.headers_mut().insert(
        "anthropic-client-platform",
        http::HeaderValue::from_static("claude_code_cli"),
    );
    request
        .headers_mut()
        .insert("user-agent", http::HeaderValue::from_static("Klaus/1.0"));
    let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
        max_message_size: Some(128 * 1024),
        max_frame_size: Some(128 * 1024),
        ..Default::default()
    };
    let (socket, _) = tokio_tungstenite::connect_async_with_config(request, Some(config), false)
        .await
        .map_err(|error| match error {
            tokio_tungstenite::tungstenite::Error::Http(response) => {
                http_error(source, response.status().as_u16())
            }
            _ => source.error("transport_failed"),
        })?;
    Ok(socket)
}

async fn claude_stream(
    endpoint: &str,
    headers: std::collections::HashMap<String, String>,
    audio: Vec<u8>,
    language: Option<&str>,
) -> Result<String, String> {
    let source = Source::Claude;
    let socket = claude_connect(endpoint, headers, language, false).await?;
    let (mut sink, mut stream) = socket.split();
    let uploaded = Arc::new(AtomicBool::new(false));
    let upload_done = uploaded.clone();
    let upload = async move {
        sink.send(Message::Text(r#"{"type":"KeepAlive"}"#.into()))
            .await
            .map_err(|_| source.error("transport_failed"))?;
        for chunk in audio.chunks(8192) {
            sink.send(Message::Binary(chunk.to_vec()))
                .await
                .map_err(|_| source.error("transport_failed"))?;
        }
        // Mark finalization before awaiting a flush: the server may answer immediately.
        upload_done.store(true, Ordering::Release);
        sink.send(Message::Text(r#"{"type":"CloseStream"}"#.into()))
            .await
            .map_err(|_| source.error("transport_failed"))?;
        Ok::<_, String>(sink)
    };
    let read = async move {
        let mut text = String::new();
        let mut interim = String::new();
        let mut bytes = 0usize;
        let mut frames = 0usize;
        while let Some(message) = stream.next().await {
            frames += 1;
            if frames > 4096 {
                return Err(source.error("response_invalid"));
            }
            let message = message.map_err(|_| source.error("transport_failed"))?;
            match message {
                Message::Text(value) => {
                    bytes = bytes.saturating_add(value.len());
                    if bytes > MAX_RESPONSE {
                        return Err(source.error("response_invalid"));
                    }
                    let value: Value = serde_json::from_str(&value)
                        .map_err(|_| source.error("response_invalid"))?;
                    match value["type"].as_str() {
                        Some("TranscriptInterim" | "TranscriptText") => {
                            interim = value["data"]
                                .as_str()
                                .ok_or_else(|| source.error("response_invalid"))?
                                .to_owned();
                            if text.len().saturating_add(interim.len()) > MAX_TEXT {
                                return Err(source.error("response_invalid"));
                            }
                        }
                        Some("TranscriptEndpoint") => {
                            append_text(&mut text, &interim)?;
                            interim.clear();
                            if uploaded.load(Ordering::Acquire) {
                                return Ok(text);
                            }
                        }
                        Some("TranscriptError" | "error") => {
                            return Err(source.error("provider_rejected"))
                        }
                        Some(_) => {}
                        None => return Err(source.error("response_invalid")),
                    }
                }
                Message::Close(frame) => {
                    if !uploaded.load(Ordering::Acquire) || frame.is_some_and(|frame| {
                        frame.code != tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal
                    }) { return Err(source.error("transport_failed")); }
                    append_text(&mut text, &interim)?;
                    return Ok(text);
                }
                Message::Ping(_) | Message::Pong(_) => {}
                _ => return Err(source.error("response_invalid")),
            }
        }
        Err(source.error("transport_failed"))
    };
    let (sink, text) = tokio::try_join!(upload, read)?;
    drop(sink);
    Ok(text)
}

fn append_text(text: &mut String, next: &str) -> Result<(), String> {
    let next = next.trim();
    if text.len().saturating_add(next.len()).saturating_add(1) > MAX_TEXT {
        return Err("claude_transcription_response_invalid".into());
    }
    if !next.is_empty() {
        if !text.is_empty() {
            text.push(' ');
        }
        text.push_str(next);
    }
    Ok(())
}

pub(super) fn http_error(source: Source, status: u16) -> String {
    source.error(match status {
        401 => "authentication_required",
        403 => "permission_denied",
        402 => "billing_required",
        429 => "rate_limited",
        _ => "provider_rejected",
    })
}

#[cfg(any(target_os = "macos", test))]
fn desktop_bundle_version(path: &std::path::Path) -> Option<String> {
    use std::io::Read;
    let file = std::fs::File::open(path).ok()?;
    let mut bytes = Vec::new();
    file.take(131_073).read_to_end(&mut bytes).ok()?;
    if bytes.len() > 131_072 {
        return None;
    }
    let plist = std::str::from_utf8(&bytes).ok()?;
    let value = plist
        .split_once("<key>CFBundleShortVersionString</key>")?
        .1
        .trim_start()
        .strip_prefix("<string>")?
        .split_once("</string>")?
        .0;
    (value.len() <= 64
        && value.split('.').count() >= 3
        && value
            .split('.')
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit())))
    .then(|| value.to_owned())
}
pub(super) fn codex_transcription_user_agent() -> String {
    static VERSION: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    let version = VERSION.get_or_init(|| {
        #[cfg(target_os = "macos")]
        {
            let mut roots = vec![std::path::PathBuf::from("/Applications")];
            if let Some(home) = std::env::var_os("HOME") {
                roots.push(std::path::PathBuf::from(home).join("Applications"));
            }
            for root in roots {
                for name in ["Codex.app", "ChatGPT.app"] {
                    if let Some(version) =
                        desktop_bundle_version(&root.join(name).join("Contents/Info.plist"))
                    {
                        return version;
                    }
                }
            }
        }
        super::model_sources::codex_client_version()
    });
    let os = match std::env::consts::OS {
        "macos" => "Mac OS",
        "windows" => "Windows",
        "linux" => "Linux",
        other => other,
    };
    let arch = match std::env::consts::ARCH {
        "aarch64" => "arm64",
        "x86_64" => "x64",
        other => other,
    };
    format!("Codex Desktop/{version} ({os}; {arch})")
}

async fn codex_request(
    endpoint: &str,
    access: &str,
    account_id: &str,
    audio: Vec<u8>,
    kind: &str,
    extension: &str,
    language: Option<&str>,
) -> Result<String, String> {
    let source = Source::Codex;
    let boundary = format!("klaus-{}", uuid::Uuid::new_v4());
    let mut body = Vec::with_capacity(audio.len() + 1024);
    if let Some(language) = language {
        body.extend_from_slice(format!("--{boundary}\r\nContent-Disposition: form-data; name=\"language\"\r\n\r\n{language}\r\n").as_bytes());
    }
    body.extend_from_slice(format!("--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"audio.{extension}\"\r\nContent-Type: {kind}\r\n\r\n").as_bytes());
    body.extend_from_slice(&audio);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(BUDGET)
        .build()
        .map_err(|_| source.error("transport_failed"))?;
    let response = client
        .post(endpoint)
        .bearer_auth(access)
        .header("ChatGPT-Account-Id", account_id)
        .header("originator", "Codex Desktop")
        .header("user-agent", codex_transcription_user_agent())
        .header("accept", "application/json")
        .header(
            "content-type",
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(body)
        .send()
        .await
        .map_err(|_| source.error("transport_failed"))?;
    if !response.status().is_success() {
        if response
            .headers()
            .get("cf-mitigated")
            .is_some_and(|value| value == "challenge")
        {
            return Err(source.error("network_challenge"));
        }
        return Err(http_error(source, response.status().as_u16()));
    }
    if response
        .content_length()
        .is_some_and(|len| len > MAX_RESPONSE as u64)
    {
        return Err(source.error("response_invalid"));
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| source.error("transport_failed"))?;
        if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE {
            return Err(source.error("response_invalid"));
        }
        bytes.extend_from_slice(&chunk);
    }
    let value: Value =
        serde_json::from_slice(&bytes).map_err(|_| source.error("response_invalid"))?;
    let text = value["text"]
        .as_str()
        .filter(|text| text.len() <= MAX_TEXT)
        .ok_or_else(|| source.error("response_invalid"))?;
    Ok(text.to_owned())
}

#[cfg(test)]
#[path = "subscription_speech_tests.rs"]
mod tests;
