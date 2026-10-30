//! Facade-owned realtime calls. Browser media never receives provider credentials.
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use super::{exact_payload, wire, AppServerState, SessionHandle};
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
#[path = "realtime_wire.rs"]
mod wire_protocol;
use klaus_agent_core::config_store;
use klaus_agent_core::credential_store::{is_valid_model_provider_id, model_credential_key};
use klaus_agent_core::engine::session::Session;
use wire_protocol::{Protocol, TranscriptNormalizer};

const MAX_TEXT: usize = 1024 * 1024;
const MAX_CONTEXT_BYTES: usize = 16 * 1024;
const MAX_APPEND_AUDIO: usize = 128 * 1024;
const CONTEXT_PREFIX: &str = "Current conversation context (reference data):\n";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_MODEL_OBSERVATIONS: usize = 32;
const MAX_MODEL_NAME: usize = 256;

#[derive(Default)]
pub(super) struct RealtimeSessions {
    sessions: Mutex<HashMap<String, Arc<RealtimeHandle>>>,
    observed_models: Mutex<VecDeque<(ModelObservationKey, String)>>,
}

#[derive(PartialEq, Eq)]
struct ModelObservationKey {
    source_id: String,
    credential_identity: [u8; 32],
    endpoint: String,
    sideband_endpoint: String,
    protocol: &'static str,
}

impl RealtimeSessions {
    fn resolved_model(&self, source: &Provider) -> Option<String> {
        source.model.clone().or_else(|| {
            let key = source.model_observation_key();
            self.observed_models
                .lock()
                .unwrap()
                .iter()
                .find(|(candidate, _)| candidate == &key)
                .map(|(_, model)| model.clone())
        })
    }

    fn observe_model(&self, source: &Provider, event: &Value) -> Option<String> {
        if event["type"] != "session.updated" {
            return None;
        }
        let model = event
            .pointer("/session/model")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|model| !model.is_empty() && model.len() <= MAX_MODEL_NAME)?
            .to_string();
        let key = source.model_observation_key();
        let mut observations = self.observed_models.lock().unwrap();
        observations.retain(|(candidate, _)| candidate != &key);
        observations.push_back((key, model.clone()));
        while observations.len() > MAX_MODEL_OBSERVATIONS {
            observations.pop_front();
        }
        Some(model)
    }
}

struct RealtimeHandle {
    request_id: String,
    protocol: Protocol,
    commands: mpsc::Sender<Value>,
    cancel: watch::Sender<bool>,
    ready: watch::Receiver<Option<Result<wire::RealtimeStartResult, String>>>,
    done: watch::Receiver<Option<Result<(), String>>>,
    done_sender: watch::Sender<Option<Result<(), String>>>,
    finalization: tokio::sync::Mutex<Option<Finalization>>,
    ownership: Mutex<ResponseOwnership>,
}
#[derive(Default)]
struct ResponseOwnership {
    epoch: u64,
    tokens: HashMap<String, bool>,
    delegations: HashMap<String, String>,
    borrowed: HashMap<String, String>,
    context_waiters: std::collections::HashSet<String>,
}
#[derive(Clone)]
struct Finalization {
    source: Provider,
    call_id: String,
    core: Arc<Session>,
}
#[derive(Clone)]
struct Provider {
    base_url: url::Url,
    sideband_base_url: url::Url,
    api_key: String,
    credential_identity: [u8; 32],
    model: Option<String>,
    protocol: Protocol,
    source_id: String,
    account_id: Option<String>,
    voice: Option<String>,
    transcription_model: Option<String>,
    /// Pre-signed transport headers for lines whose authentication is not a
    /// plain bearer key (the signed voice gateway envelope). Empty elsewhere.
    extra_headers: Vec<(String, String)>,
}

impl Provider {
    fn model_observation_key(&self) -> ModelObservationKey {
        ModelObservationKey {
            source_id: self.source_id.clone(),
            credential_identity: self.credential_identity,
            endpoint: self.base_url.to_string(),
            sideband_endpoint: self.sideband_base_url.to_string(),
            protocol: self.protocol.name(),
        }
    }
}

fn credential_identity(parts: &[&str]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    for part in parts {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part.as_bytes());
    }
    digest.finalize().into()
}

fn qoder_credential_identity(profile_id: &str, profile: &Value) -> [u8; 32] {
    // Signing adds a fresh envelope to every request; bind observations to the
    // stored account and token generation before preparing that envelope.
    credential_identity(&[
        profile_id,
        &profile["uid"].to_string(),
        profile["security_oauth_token"].as_str().unwrap_or_default(),
        profile["access_token"].as_str().unwrap_or_default(),
    ])
}

pub(super) async fn dispatch(state: Arc<AppServerState>, id: Value, method: String, params: Value) {
    tokio::spawn(async move {
        let _guard = if matches!(
            method.as_str(),
            "session/realtimeStart" | "session/realtimeHandoff"
        ) {
            match super::HeavyRequestGuard::acquire(&state, &id).await {
                Some(guard) => Some(guard),
                None => return,
            }
        } else {
            None
        };
        let result = match method.as_str() {
            "session/realtimeCapabilities" => {
                match serde_json::from_value::<wire::RealtimeCapabilitiesParams>(params) {
                    Ok(params) => {
                        let capability = match provider(
                            &state,
                            params.auth_mode.as_deref(),
                            params.model_provider_id.as_deref(),
                        )
                        .await
                        {
                            Ok(source) => {
                                let model = state.realtime.resolved_model(&source);
                                wire::RealtimeCapabilitiesResult {
                                    available: true,
                                    reason: None,
                                    source_id: source.source_id,
                                    protocol: source.protocol.name().into(),
                                    voices: source.protocol.voices(),
                                    default_voice: source.voice.unwrap_or_default(),
                                    transcript_source: if source.protocol == Protocol::Public {
                                        "peer"
                                    } else {
                                        "engine"
                                    }
                                    .into(),
                                    model,
                                }
                            }
                            Err(reason) => wire::RealtimeCapabilitiesResult {
                                available: false,
                                source_id: match params.auth_mode.as_deref() {
                                    Some("codex") => "subscription:codex".into(),
                                    Some("zhipu") => "subscription:zhipu".into(),
                                    Some("qoder") => "subscription:qoder".into(),
                                    _ => params.model_provider_id.clone().unwrap_or_default(),
                                },
                                protocol: match params.auth_mode.as_deref() {
                                    Some("codex") => "frameless",
                                    Some("zhipu") => "glm",
                                    Some("qoder") => "qoder",
                                    _ => "public",
                                }
                                .into(),
                                voices: if params.auth_mode.as_deref() == Some("codex") {
                                    Protocol::Frameless.voices()
                                } else {
                                    Vec::new()
                                },
                                default_voice: if params.auth_mode.as_deref() == Some("codex") {
                                    "cove"
                                } else {
                                    ""
                                }
                                .into(),
                                transcript_source: "engine".into(),
                                reason: Some(reason),
                                model: None,
                            },
                        };
                        Ok(serde_json::to_value(capability).unwrap())
                    }
                    Err(error) => Err(error.to_string()),
                }
            }
            "session/realtimeStart" => {
                match serde_json::from_value::<wire::RealtimeStartParams>(params) {
                    Ok(params) => start(state.clone(), params).await.and_then(|value| {
                        serde_json::to_value(value).map_err(|error| error.to_string())
                    }),
                    Err(error) => Err(error.to_string()),
                }
            }
            "session/realtimeStop" => {
                match serde_json::from_value::<wire::RealtimeSessionParams>(params) {
                    Ok(params) => stop(&state, &params.session_id, &params.request_id)
                        .await
                        .map(|()| json!({})),
                    Err(error) => Err(error.to_string()),
                }
            }
            "session/realtimeInterrupt" => {
                match serde_json::from_value::<wire::RealtimeSessionParams>(params) {
                    Ok(params) => interrupt_tasks(&state, &params.session_id, &params.request_id)
                        .and_then(|()| {
                            command(
                                &state,
                                &params.session_id,
                                &params.request_id,
                                json!({"type":"response.cancel"}),
                            )
                        }),
                    Err(error) => Err(error.to_string()),
                }
            }
            "session/realtimeAppendText" => {
                match serde_json::from_value::<wire::RealtimeAppendTextParams>(params) {
                    Ok(params) => {
                        if !matches!(params.role.as_str(), "user" | "assistant" | "system")
                            || params.text.trim().is_empty()
                            || params.text.len() > MAX_TEXT
                        {
                            Err("invalid realtime text item".into())
                        } else {
                            command(
                                &state,
                                &params.session_id,
                                &params.request_id,
                                json!({"type":"conversation.item.create","item":{"id":format!("text_{}", uuid::Uuid::new_v4().simple()),"type":"message","role":params.role,"content":[{"type":if params.role == "assistant" {"output_text"} else {"input_text"},"text":params.text}]}}),
                            )
                        }
                    }
                    Err(error) => Err(error.to_string()),
                }
            }
            "session/realtimeAppendSpeech" => {
                match serde_json::from_value::<wire::RealtimeAppendSpeechParams>(params) {
                    Ok(params) => {
                        if params.text.trim().is_empty() || params.text.len() > MAX_TEXT {
                            Err("Realtime speech requires bounded text".into())
                        } else {
                            command(
                                &state,
                                &params.session_id,
                                &params.request_id,
                                json!({"type":"response.create","response":{"conversation":"none","output_modalities":["audio"],"instructions":"Say the provided text aloud without taking actions.","tool_choice":"none","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":params.text}]}]}}),
                            )
                        }
                    }
                    Err(error) => Err(error.to_string()),
                }
            }
            "session/realtimeAppendAudio" => {
                match serde_json::from_value::<wire::RealtimeAppendAudioParams>(params) {
                    Ok(params) => {
                        if params.audio_base64.is_empty()
                            || params.audio_base64.len() > MAX_APPEND_AUDIO
                        {
                            Err("Realtime audio requires one bounded encoded frame".into())
                        } else {
                            append_audio(
                                &state,
                                &params.session_id,
                                &params.request_id,
                                &params.audio_base64,
                            )
                        }
                    }
                    Err(error) => Err(error.to_string()),
                }
            }
            "session/realtimeHandoff" => {
                match serde_json::from_value::<wire::RealtimeHandoffParams>(params) {
                    Ok(params) => handoff(&state, params).await,
                    Err(error) => Err(error.to_string()),
                }
            }
            _ => Err("unsupported realtime operation".into()),
        };
        match result {
            Ok(value) => state.send(wire::response(&id, value)).await,
            Err(error) => {
                state
                    .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                    .await
            }
        }
    });
}

fn valid_identity(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 160
        && value.bytes().all(|value| {
            value.is_ascii_alphanumeric() || matches!(value, b'_' | b'-' | b'.' | b':')
        })
}
async fn provider(
    state: &AppServerState,
    auth_mode: Option<&str>,
    requested: Option<&str>,
) -> Result<Provider, String> {
    if let Some(mode) = auth_mode {
        if requested.is_some() {
            return Err("Choose one realtime authentication source".into());
        }
        match mode {
            "codex" => {
                let token = super::super::codex_catalog::token(
                    &super::super::model_sources::config_root_of(&state.config_path),
                )
                .await
                .ok_or("Sign in to the selected Codex subscription before starting voice")?;
                return Ok(Provider {
                    credential_identity: credential_identity(&[
                        &token.account_id,
                        &token.access_token,
                    ]),
                    base_url: url::Url::parse("https://chatgpt.com/backend-api/codex/").unwrap(),
                    sideband_base_url: url::Url::parse("https://api.openai.com/v1/").unwrap(),
                    api_key: token.access_token,
                    account_id: Some(token.account_id),
                    source_id: "subscription:codex".into(),
                    protocol: Protocol::Frameless,
                    model: Some("gpt-live-1-codex".into()),
                    voice: Some("cove".into()),
                    transcription_model: None,
                    extra_headers: Vec::new(),
                });
            }
            "zhipu" => {
                let api_key = crate::app_server::zhipu::auth::api_key()
                    .ok_or("Sign in to the selected Zhipu subscription before starting voice")?;
                let base = url::Url::parse("https://open.bigmodel.cn/api/paas/v4/").unwrap();
                return Ok(Provider {
                    credential_identity: credential_identity(&[&api_key]),
                    sideband_base_url: base.clone(),
                    base_url: base,
                    api_key,
                    account_id: None,
                    source_id: "subscription:zhipu".into(),
                    protocol: Protocol::Glm,
                    model: Some("glm-realtime-air".into()),
                    voice: None,
                    transcription_model: None,
                    extra_headers: Vec::new(),
                });
            }
            "qoder" => {
                let (profile_id, mut profile) = crate::app_server::qoder::auth::credential()
                    .ok_or("Sign in to the selected Qoder subscription before starting voice")?;
                if profile["security_oauth_token"]
                    .as_str()
                    .is_none_or(str::is_empty)
                {
                    profile["security_oauth_token"] = profile["access_token"].clone();
                }
                if profile["security_oauth_token"]
                    .as_str()
                    .is_none_or(str::is_empty)
                    || !profile.get("uid").is_some_and(|value| {
                        value.as_i64().is_some()
                            || value.as_str().is_some_and(|text| !text.is_empty())
                    })
                {
                    return Err(
                        "Sign in to the selected Qoder subscription before starting voice".into(),
                    );
                }
                if let Some(expiry) = profile.get("expire_time").and_then(Value::as_i64) {
                    let expiry = if expiry > 100_000_000_000 {
                        expiry / 1000
                    } else {
                        expiry
                    };
                    if expiry <= chrono::Utc::now().timestamp() {
                        return Err("The Qoder subscription login has expired; sign in again before starting voice".into());
                    }
                }
                let credential_identity = qoder_credential_identity(&profile_id, &profile);
                let origin = crate::app_server::qoder::speech::audio_origin()
                    .await
                    .map_err(|_| "The Qoder voice endpoint is unavailable".to_string())?;
                let endpoint = format!("{}/api/v2/service/ws/asr", origin.trim_end_matches('/'));
                let signed = crate::app_server::qoder::sign::sign_audio_get(&endpoint, &profile)
                    .map_err(|_| "Qoder voice authentication could not be prepared".to_string())?;
                let mut api_key = String::new();
                let mut extra_headers: Vec<(String, String)> = Vec::new();
                for (name, value) in signed.headers {
                    if name == "Authorization" {
                        api_key = value.strip_prefix("Bearer ").unwrap_or(&value).to_string();
                    } else {
                        extra_headers.push((name, value));
                    }
                }
                if api_key.is_empty() {
                    return Err("Qoder voice authentication could not be prepared".into());
                }
                let connection = uuid::Uuid::new_v4().simple().to_string();
                extra_headers.extend([
                    (
                        "X-Qoder-Voice-Protocol".to_string(),
                        "realtime_voice.v1".to_string(),
                    ),
                    (
                        "X-Qoder-Voiceprint-Injection".to_string(),
                        "disabled".to_string(),
                    ),
                    (
                        "X-Asr-Session-Id".to_string(),
                        uuid::Uuid::new_v4().to_string(),
                    ),
                    (
                        "X-Business".to_string(),
                        json!({
                            "product": "ide",
                            "type": "realtime_voice",
                            "id": connection,
                            "begin_at": chrono::Utc::now().timestamp_millis(),
                            "name": format!("realtime_voice-{connection}"),
                        })
                        .to_string(),
                    ),
                ]);
                let base = url::Url::parse(&format!("{}/", origin.trim_end_matches('/')))
                    .map_err(|_| "The Qoder voice endpoint is invalid".to_string())?;
                return Ok(Provider {
                    sideband_base_url: base.clone(),
                    base_url: base,
                    api_key,
                    account_id: None,
                    source_id: "subscription:qoder".into(),
                    protocol: Protocol::Qoder,
                    credential_identity,
                    model: None,
                    voice: Some("longanqian".into()),
                    transcription_model: None,
                    extra_headers,
                });
            }
            _ => return Err("Choose one realtime authentication source".into()),
        }
    }
    let id = requested
        .filter(|id| is_valid_model_provider_id(id))
        .ok_or("Select a configured API model source with an explicit realtime model")?;
    let document = config_store::load(&state.config_path)
        .await
        .map_err(|error| format!("Cannot read realtime model configuration: {error}"))?;
    let providers = document.product_section("modelProviders");
    let entry = providers
        .get(id)
        .ok_or("The configured realtime model source is unavailable")?;
    if entry["provider"] != "openai" {
        return Err("The selected source does not declare the supported realtime protocol".into());
    }
    let realtime = entry
        .get("realtime")
        .filter(|value| value.is_object())
        .ok_or("The selected source has no explicit realtime capability")?;
    let model = realtime["model"]
        .as_str()
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .ok_or("The selected source has no configured realtime model")?;
    let protocol = Protocol::parse(realtime["protocol"].as_str().unwrap_or("public"))?;
    let transcription_model = realtime["transcriptionModel"]
        .as_str()
        .map(str::trim)
        .filter(|model| !model.is_empty());
    if protocol == Protocol::Public && transcription_model.is_none() {
        return Err("The selected source needs an explicit transcription model to retain spoken conversation text".into());
    }
    let root = entry["baseUrl"]
        .as_str()
        .unwrap_or("https://api.openai.com/v1/");
    let mut base_url = url::Url::parse(root).map_err(|_| "The realtime source URL is invalid")?;
    if !matches!(base_url.scheme(), "https" | "http")
        || !base_url.username().is_empty()
        || base_url.password().is_some()
        || base_url.query().is_some()
        || base_url.fragment().is_some()
    {
        return Err("The realtime source URL is invalid".into());
    }
    let suffix = base_url
        .path()
        .trim_end_matches('/')
        .strip_suffix("/chat/completions")
        .or_else(|| {
            base_url
                .path()
                .trim_end_matches('/')
                .strip_suffix("/responses")
        })
        .unwrap_or(base_url.path().trim_end_matches('/'))
        .to_string();
    base_url.set_path(&format!("{suffix}/"));
    let store = state
        .credential_store
        .as_ref()
        .ok_or("The realtime source credential store is unavailable")?;
    let api_key = store
        .get(&model_credential_key(id))
        .await
        .map_err(|_| "The realtime source credentials could not be read")?
        .filter(|key| !key.trim().is_empty())
        .ok_or("The selected realtime source has no stored API credential")?;
    Ok(Provider {
        credential_identity: credential_identity(&[&api_key]),
        sideband_base_url: base_url.clone(),
        base_url,
        api_key,
        model: Some(model.into()),
        protocol,
        source_id: id.into(),
        account_id: None,
        voice: realtime["voice"]
            .as_str()
            .map(str::to_string)
            .or_else(|| match protocol {
                Protocol::Public => None,
                Protocol::Qoder => Some("longanqian".into()),
                _ => Some("cove".into()),
            }),
        transcription_model: transcription_model.map(str::to_string),
        extra_headers: Vec::new(),
    })
}
fn http_client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .timeout(CONNECT_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| error.to_string())
}
async fn negotiate(
    source: &Provider,
    params: &wire::RealtimeStartParams,
    core: &Session,
) -> Result<wire::RealtimeStartResult, String> {
    if let Some(transport) = &params.transport {
        if transport.r#type != "existingCall" || !valid_identity(&transport.call_id) {
            return Err("invalid existing realtime call".into());
        }
        if params.offer_sdp.is_some() {
            return Err("an existing realtime call cannot include a new SDP offer".into());
        }
        return Ok(wire::RealtimeStartResult {
            protocol: source.protocol.name().into(),
            transcript_source: if source.protocol == Protocol::Public {
                "peer"
            } else {
                "engine"
            }
            .into(),
            answer_sdp: None,
            call_id: transport.call_id.clone(),
        });
    }
    if source.protocol == Protocol::Qoder {
        // The signed voice gateway line is a plain WebSocket; there is no SDP
        // exchange and the call identity is minted locally.
        if params.offer_sdp.is_some() {
            return Err("The Qoder voice line does not use a WebRTC offer".into());
        }
        return Ok(wire::RealtimeStartResult {
            protocol: source.protocol.name().into(),
            transcript_source: "engine".into(),
            answer_sdp: None,
            call_id: uuid::Uuid::new_v4().simple().to_string(),
        });
    }
    let sdp = params
        .offer_sdp
        .as_deref()
        .filter(|value| !value.trim().is_empty() && value.len() <= 256 * 1024)
        .ok_or("A valid realtime SDP offer is required")?;
    let session = session_configuration(source, params, core);
    let boundary = format!("klaus-{}", uuid::Uuid::new_v4().simple());
    let body = format!("--{boundary}\r\nContent-Disposition: form-data; name=\"sdp\"\r\nContent-Type: application/sdp\r\n\r\n{sdp}\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"session\"\r\nContent-Type: application/json\r\n\r\n{session}\r\n--{boundary}--\r\n");
    let backend = source.base_url.path().contains("/backend-api");
    let path = if source.protocol == Protocol::Frameless && !backend {
        "live"
    } else {
        "realtime/calls"
    };
    let mut url = source
        .base_url
        .join(path)
        .map_err(|error| error.to_string())?;
    if source.protocol == Protocol::V1 || (source.protocol == Protocol::Frameless && backend) {
        url.query_pairs_mut()
            .append_pair("intent", "quicksilver")
            .append_pair("architecture", "avas");
    }
    let mut request = http_client()?.post(url).bearer_auth(&source.api_key);
    request = request.header("x-session-id", &params.session_id);
    if source.protocol != Protocol::Public {
        request = request.header(
            "OpenAI-Alpha",
            if source.protocol == Protocol::V1 {
                "quicksilver=v1"
            } else {
                "quicksilver=v2"
            },
        );
    }
    if let Some(account) = &source.account_id {
        request = request.header("ChatGPT-Account-Id", account);
        let version = super::super::model_sources::codex_client_version();
        request = request
            .header("originator", "codex_cli_rs")
            .header("version", &version)
            .header(
                "User-Agent",
                super::super::codex_catalog::user_agent(&version),
            );
    }
    if backend && source.protocol != Protocol::Public {
        request = request.json(&json!({"sdp": sdp, "session": session}));
    } else {
        request = request
            .header(
                "Content-Type",
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(body);
    }
    let response = request
        .send()
        .await
        .map_err(|error| format!("Realtime SDP negotiation failed: {error}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "The realtime provider refused SDP negotiation ({})",
            response.status()
        ));
    }
    let location = response
        .headers()
        .get("location")
        .and_then(|value| value.to_str().ok())
        .ok_or("The realtime provider did not return a call identity")?
        .to_string();
    let call_id = location
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .filter(|value| valid_identity(value))
        .ok_or("The realtime provider returned an invalid call identity")?
        .to_string();
    let answer = async {
        let mut stream = response.bytes_stream();
        let mut answer = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| error.to_string())?;
            if answer.len() + chunk.len() > 256 * 1024 {
                return Err("The realtime SDP answer is too large".to_string());
            }
            answer.extend_from_slice(&chunk);
        }
        let answer = String::from_utf8(answer)
            .map_err(|_| "The realtime provider returned invalid SDP".to_string())?;
        if !answer.trim_start().starts_with("v=0") {
            return Err("The realtime provider returned invalid SDP".into());
        }
        Ok(answer)
    }
    .await;
    match answer {
        Ok(answer_sdp) => Ok(wire::RealtimeStartResult {
            protocol: source.protocol.name().into(),
            transcript_source: if source.protocol == Protocol::Public {
                "peer"
            } else {
                "engine"
            }
            .into(),
            answer_sdp: Some(answer_sdp),
            call_id,
        }),
        Err(error) => {
            let _ = hangup(source, &call_id).await;
            Err(error)
        }
    }
}
async fn wait_value<T: Clone>(
    mut receiver: watch::Receiver<Option<Result<T, String>>>,
) -> Result<T, String> {
    loop {
        if let Some(result) = receiver.borrow().clone() {
            return result;
        }
        receiver
            .changed()
            .await
            .map_err(|_| "The realtime lifecycle ended without a receipt".to_string())?;
    }
}
async fn start(
    state: Arc<AppServerState>,
    params: wire::RealtimeStartParams,
) -> Result<wire::RealtimeStartResult, String> {
    if !valid_identity(&params.request_id) {
        return Err("invalid realtime request identity".into());
    }
    let session = state
        .session(&params.session_id)
        .ok_or("The realtime conversation is unavailable")?;
    let mut source = provider(
        &state,
        params.auth_mode.as_deref(),
        params.model_provider_id.as_deref(),
    )
    .await?;
    if let Some(protocol) = params.protocol.as_deref() {
        let protocol = Protocol::parse(protocol)?;
        if source.protocol == Protocol::Public && protocol != Protocol::Public {
            return Err("The selected source does not declare this wire protocol".into());
        }
        if source.protocol != Protocol::Public && protocol == Protocol::Public {
            return Err("The selected source requires its hosted voice protocol".into());
        }
        source.protocol = protocol;
        if params.auth_mode.as_deref() == Some("codex") {
            source.model = Some(
                if protocol == Protocol::Frameless {
                    "gpt-live-1-codex"
                } else {
                    "gpt-realtime-1.5"
                }
                .into(),
            );
        }
    }
    if let Some(voice) = params.voice.as_deref() {
        if source.protocol != Protocol::Public
            && !source
                .protocol
                .voices()
                .iter()
                .any(|candidate| candidate == voice)
        {
            return Err("The selected voice is unavailable for this protocol".into());
        }
        source.voice = Some(voice.into());
    }
    let core = session.ps.realtime_session().await?;
    let (commands, receiver) = mpsc::channel(64);
    let (cancel, cancellation) = watch::channel(false);
    let (ready_tx, ready) = watch::channel(None);
    let (done_tx, done) = watch::channel(None);
    let handle = Arc::new(RealtimeHandle {
        protocol: source.protocol,
        request_id: params.request_id.clone(),
        commands,
        cancel,
        ready,
        done,
        done_sender: done_tx.clone(),
        finalization: tokio::sync::Mutex::new(None),
        ownership: Mutex::new(ResponseOwnership::default()),
    });
    let existing = {
        let mut sessions = state.realtime.sessions.lock().unwrap();
        if let Some(existing) = sessions.get(&params.session_id) {
            Some(existing.clone())
        } else {
            sessions.insert(params.session_id.clone(), handle.clone());
            None
        }
    };
    if let Some(existing) = existing {
        if existing.request_id != params.request_id {
            return Err("A realtime call already owns this conversation".into());
        }
        if let Some(Err(error)) = existing.done.borrow().as_ref() {
            return Err(format!(
                "The previous realtime call requires finalization: {error}"
            ));
        }
        return wait_value(existing.ready.clone()).await;
    }
    let task_state = state.clone();
    let task_handle = handle.clone();
    tokio::spawn(async move {
        let result = run(
            CallContext {
                state: task_state.clone(),
                session,
                handle: task_handle.clone(),
                core,
                source,
                params: params.clone(),
            },
            receiver,
            cancellation,
            ready_tx,
        )
        .await;
        let _ = done_tx.send(Some(result.clone()));
        if result.is_err() {
            publish(
                &task_state,
                &params,
                json!({"type":"error","error":{"message":result.as_ref().err()}}),
            )
            .await;
        }
        let retain = result.is_err() && task_handle.finalization.lock().await.is_some();
        let mut sessions = task_state.realtime.sessions.lock().unwrap();
        if !retain
            && sessions
                .get(&params.session_id)
                .is_some_and(|entry| Arc::ptr_eq(entry, &task_handle))
        {
            sessions.remove(&params.session_id);
        }
    });
    wait_value(handle.ready.clone()).await
}
fn command(
    state: &AppServerState,
    session_id: &str,
    request_id: &str,
    mut value: Value,
) -> Result<Value, String> {
    let handle = state
        .realtime
        .sessions
        .lock()
        .unwrap()
        .get(session_id)
        .cloned()
        .ok_or("The realtime call is unavailable")?;
    if handle.request_id != request_id || *handle.cancel.borrow() {
        return Err("The realtime call generation is no longer active".into());
    }
    if value["type"] == "response.create"
        && matches!(handle.protocol, Protocol::Public | Protocol::Qoder)
    {
        value = owned_response(&handle, request_id, value)?;
    }
    handle
        .commands
        .try_send(value)
        .map_err(|_| "The realtime control channel is busy or closed".to_string())?;
    Ok(json!({}))
}
/// One encoded microphone frame for an engine-mediated voice line. Only a
/// transport that carries its own audio accepts frames; a peer-to-peer media
/// line refuses instead of silently dropping them.
fn append_audio(
    state: &AppServerState,
    session_id: &str,
    request_id: &str,
    audio: &str,
) -> Result<Value, String> {
    let handle = state
        .realtime
        .sessions
        .lock()
        .unwrap()
        .get(session_id)
        .cloned()
        .ok_or("The realtime call is unavailable")?;
    if handle.request_id != request_id || *handle.cancel.borrow() {
        return Err("The realtime call generation is no longer active".into());
    }
    if handle.protocol != Protocol::Qoder {
        return Err("The realtime transport does not accept engine audio".into());
    }
    handle
        .commands
        .try_send(json!({"type":"input_audio_buffer.append","audio":audio}))
        .map_err(|_| "The realtime control channel is busy or closed".to_string())?;
    Ok(json!({}))
}
async fn stop(state: &AppServerState, session_id: &str, request_id: &str) -> Result<(), String> {
    let handle = state
        .realtime
        .sessions
        .lock()
        .unwrap()
        .get(session_id)
        .cloned();
    let Some(handle) = handle else {
        return Ok(());
    };
    if handle.request_id != request_id {
        return Err("The realtime call generation is no longer active".into());
    }
    retire_tasks(state, session_id, request_id);
    if matches!(handle.done.borrow().as_ref(), Some(Err(_))) {
        let finalized = {
            let mut pending = handle.finalization.lock().await;
            if let Some(finalization) = pending.as_ref() {
                hangup(&finalization.source, &finalization.call_id).await?;
                finalization.core.flush_rollout().await?;
                *pending = None;
                let _ = handle.done_sender.send(Some(Ok(())));
                true
            } else {
                false
            }
        };
        if finalized {
            {
                let mut sessions = state.realtime.sessions.lock().unwrap();
                if sessions
                    .get(session_id)
                    .is_some_and(|entry| Arc::ptr_eq(entry, &handle))
                {
                    sessions.remove(session_id);
                }
            }
            state.notify_session(session_id,"session/realtimeEvent",json!({"sessionId":session_id,"requestId":request_id,"event":{"type":"closed","reason":"stopped","finalizationComplete":true,"transcriptFlushed":true}})).await;
            return Ok(());
        }
    }
    let _ = handle.cancel.send(true);
    wait_value(handle.done.clone()).await
}
pub(super) async fn stop_session(state: &AppServerState, session_id: &str) -> Result<(), String> {
    let request = state
        .realtime
        .sessions
        .lock()
        .unwrap()
        .get(session_id)
        .map(|handle| handle.request_id.clone());
    match request {
        Some(request) => stop(state, session_id, &request).await,
        None => Ok(()),
    }
}
pub(super) async fn stop_all(state: &AppServerState) {
    let handles: Vec<_> = state
        .realtime
        .sessions
        .lock()
        .unwrap()
        .values()
        .cloned()
        .collect();
    for handle in handles {
        let _ = handle.cancel.send(true);
        let _ = tokio::time::timeout(
            CONNECT_TIMEOUT + DRAIN_TIMEOUT,
            wait_value(handle.done.clone()),
        )
        .await;
    }
}
async fn publish(state: &AppServerState, params: &wire::RealtimeStartParams, event: Value) {
    publish_media(state, params, event, false).await;
}
/// Engine-mediated voice lines deliver audio inside provider events; every
/// other line exchanges media peer-to-peer, so the payload is stripped before
/// the event reaches the host.
async fn publish_media(
    state: &AppServerState,
    params: &wire::RealtimeStartParams,
    mut event: Value,
    keep_audio: bool,
) {
    if !keep_audio
        && event["type"]
            .as_str()
            .is_some_and(|kind| kind.ends_with("audio.delta"))
    {
        if let Some(event) = event.as_object_mut() {
            event.remove("delta");
            event.remove("audio");
            event.remove("data");
        }
    }
    state
        .notify_session(
            &params.session_id,
            "session/realtimeEvent",
            json!({"sessionId":params.session_id,"requestId":params.request_id,"event":event}),
        )
        .await;
}

#[derive(Default)]
struct Transcript {
    pending: HashMap<String, (String, String)>,
    order: std::collections::VecDeque<String>,
    ready: std::collections::HashSet<String>,
    delivered: std::collections::HashSet<String>,
}
impl Transcript {
    fn item(&mut self, id: &str, role: &str) -> &mut (String, String) {
        if !self.pending.contains_key(id) {
            self.order.push_back(id.into());
        }
        self.pending
            .entry(id.into())
            .or_insert_with(|| (role.into(), String::new()))
    }
    async fn receive(
        &mut self,
        state: &AppServerState,
        core: &Session,
        source: &Provider,
        params: &wire::RealtimeStartParams,
        event: &Value,
    ) -> Result<(), String> {
        let kind = event["type"].as_str().unwrap_or("");
        let Some(id) = event["item_id"]
            .as_str()
            .or_else(|| event.pointer("/item/id").and_then(Value::as_str))
        else {
            return Ok(());
        };
        if self.delivered.contains(id) {
            return Ok(());
        }
        let role = if kind.starts_with("conversation.item.input_audio_transcription") {
            "user"
        } else {
            "assistant"
        };
        if matches!(
            kind,
            "conversation.item.input_audio_transcription.delta"
                | "response.output_audio_transcript.delta"
                | "response.audio_transcript.delta"
                | "response.output_text.delta"
                | "response.text.delta"
        ) {
            if let Some(delta) = event["delta"].as_str() {
                let item = self.item(id, role);
                if item.1.len() + delta.len() > MAX_TEXT {
                    return Err("The realtime transcript exceeded its limit".into());
                }
                item.1.push_str(delta);
            }
        }
        if matches!(
            kind,
            "conversation.item.input_audio_transcription.completed"
                | "response.output_audio_transcript.done"
                | "response.audio_transcript.done"
                | "response.output_text.done"
                | "response.text.done"
        ) {
            let text = event["transcript"]
                .as_str()
                .or_else(|| event["text"].as_str())
                .unwrap_or("");
            if text.len() > MAX_TEXT {
                return Err("The realtime transcript exceeded its limit".into());
            }
            let item = self.item(id, role);
            if !text.is_empty() {
                item.1 = text.into();
            }
            self.ready.insert(id.into());
        }
        if kind == "conversation.item.input_audio_transcription.failed" {
            self.item(id, "user");
            self.ready.insert(id.into());
        }
        if matches!(kind, "conversation.item.created" | "conversation.item.done") {
            if let Some(item) = event.get("item").filter(|item| item["type"] == "message") {
                let role = item["role"].as_str().unwrap_or("");
                if matches!(role, "user" | "assistant") {
                    let text = item["content"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|part| {
                            part["text"]
                                .as_str()
                                .or_else(|| part["transcript"].as_str())
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                    if text.len() > MAX_TEXT {
                        return Err("The realtime transcript exceeded its limit".into());
                    }
                    let pending = self.item(id, role);
                    if !text.is_empty() {
                        pending.1 = text;
                    }
                    if kind == "conversation.item.done"
                        || item["status"] == "completed"
                        || (role == "user" && !pending.1.is_empty())
                    {
                        self.ready.insert(id.into());
                    }
                }
            }
        }
        self.drain(state, core, source, params).await
    }
    async fn drain(
        &mut self,
        state: &AppServerState,
        core: &Session,
        source: &Provider,
        params: &wire::RealtimeStartParams,
    ) -> Result<(), String> {
        while self.order.front().is_some_and(|id| self.ready.contains(id)) {
            let id = self.order.pop_front().unwrap();
            self.ready.remove(&id);
            let Some((role, text)) = self.pending.remove(&id) else {
                continue;
            };
            if text.trim().is_empty() || self.delivered.contains(&id) {
                continue;
            }
            let identity = format!("realtime:{}:{id}", params.request_id);
            let appended = core
                .append_realtime_transcript(
                    &identity,
                    &role,
                    &text,
                    source.model.as_deref().unwrap_or(""),
                )
                .await?;
            self.delivered.insert(id);
            if appended {
                let item = if role == "assistant" {
                    json!({"id":identity,"type":"agent_message","text":text,"phase":"final_answer","status":"completed"})
                } else {
                    json!({"id":identity,"type":"user_message","text":text,"status":"completed"})
                };
                if role == "assistant" {
                    state.notify_session(&params.session_id,"item/started",json!({"sessionId":params.session_id,"submissionId":params.request_id,"source":"realtime","item":{"id":identity,"type":"agent_message","text":"","phase":"final_answer","status":"in_progress"}})).await;
                    state.notify_session(&params.session_id,"item/agentMessage/delta",json!({"sessionId":params.session_id,"submissionId":params.request_id,"source":"realtime","messageId":identity,"phase":"final_answer","delta":text})).await;
                }
                state.notify_session(&params.session_id,"item/completed",json!({"sessionId":params.session_id,"submissionId":params.request_id,"source":"realtime","item":item})).await;
            }
        }
        Ok(())
    }
    async fn flush(
        &mut self,
        state: &AppServerState,
        core: &Session,
        source: &Provider,
        params: &wire::RealtimeStartParams,
    ) -> Result<(), String> {
        self.ready.extend(self.order.iter().cloned());
        self.drain(state, core, source, params).await?;
        core.flush_rollout().await
    }
}
async fn hangup(source: &Provider, call_id: &str) -> Result<(), String> {
    if source.protocol != Protocol::Public {
        return Ok(());
    }
    let response = http_client()?
        .post(
            source
                .base_url
                .join(&format!("realtime/calls/{call_id}/hangup"))
                .map_err(|error| error.to_string())?,
        )
        .bearer_auth(&source.api_key)
        .send()
        .await
        .map_err(|error| format!("Realtime call finalization failed: {error}"))?;
    if response.status().is_success() || response.status() == reqwest::StatusCode::NOT_FOUND {
        Ok(())
    } else {
        Err(format!(
            "The realtime provider refused call finalization ({})",
            response.status()
        ))
    }
}
/// The Qoder voice gateway refuses an exhausted account during the handshake
/// with a provider code inside the refusal body. Surface it as an actionable
/// account-state error instead of an opaque transport failure.
fn qoder_connect_message(
    source: &Provider,
    error: &tokio_tungstenite::tungstenite::Error,
) -> Option<String> {
    if source.protocol != Protocol::Qoder {
        return None;
    }
    let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
        return None;
    };
    let code = response
        .body()
        .as_ref()
        .and_then(|bytes| serde_json::from_slice::<Value>(bytes).ok())
        .and_then(|body| {
            body["code"]
                .as_str()
                .map(str::to_string)
                .or_else(|| body["code"].as_i64().map(|code| code.to_string()))
        });
    if code
        .as_deref()
        .is_some_and(|code| matches!(code, "112" | "116" | "117" | "118"))
    {
        return Some("The Qoder subscription has no realtime voice quota left".into());
    }
    Some(format!(
        "The Qoder voice line refused the connection (HTTP {}{})",
        response.status().as_u16(),
        code.map(|code| format!(", code {code}"))
            .unwrap_or_default()
    ))
}
fn session_configuration(
    source: &Provider,
    params: &wire::RealtimeStartParams,
    core: &Session,
) -> Value {
    let delegation = if source.protocol == Protocol::Public {
        "Use delegate_task"
    } else {
        "Use task delegation"
    };
    let mut instructions = format!("{}\nYou are speaking with the user in their current task. {delegation} whenever a request requires work by the task agent, access to files, tools, apps, or verification. Report task progress and results only from confirmed task results and context. Do not claim an action has happened before the task agent confirms it.", params.instructions.as_deref().unwrap_or(""));
    let context = core.realtime_context_text(MAX_CONTEXT_BYTES - CONTEXT_PREFIX.len());
    if !context.is_empty() {
        instructions.push_str("\n\n");
        instructions.push_str(CONTEXT_PREFIX);
        instructions.push_str(&context);
    }
    source.protocol.session(
        &instructions,
        source.model.as_deref(),
        source.voice.as_deref(),
        source.transcription_model.as_deref(),
    )
}
fn interrupt_tasks(
    state: &AppServerState,
    session_id: &str,
    request_id: &str,
) -> Result<(), String> {
    let handle = state
        .realtime
        .sessions
        .lock()
        .unwrap()
        .get(session_id)
        .cloned()
        .ok_or("The realtime call is unavailable")?;
    if handle.request_id != request_id {
        return Err("The realtime call generation is no longer active".into());
    }
    let mut ownership = handle.ownership.lock().unwrap();
    if *handle.cancel.borrow() {
        return Err("The realtime call generation is no longer active".into());
    }
    ownership.epoch = ownership
        .epoch
        .checked_add(1)
        .expect("realtime interruption generation exhausted");
    ownership.tokens.clear();
    if let Some(session) = state.session(session_id) {
        for turn in ownership.delegations.values() {
            session.ps.interrupt_realtime_submission(turn);
        }
    }
    Ok(())
}
fn retire_tasks(state: &AppServerState, session_id: &str, request_id: &str) {
    let handle = state
        .realtime
        .sessions
        .lock()
        .unwrap()
        .get(session_id)
        .cloned();
    let Some(handle) = handle.filter(|handle| handle.request_id == request_id) else {
        return;
    };
    let mut ownership = handle.ownership.lock().unwrap();
    let _ = handle.cancel.send(true);
    ownership.epoch = ownership
        .epoch
        .checked_add(1)
        .expect("realtime interruption generation exhausted");
    ownership.tokens.clear();
    if let Some(session) = state.session(session_id) {
        for turn in ownership.delegations.values() {
            session.ps.retire_queued_realtime_submission(turn);
        }
        for submission in ownership.context_waiters.drain() {
            session.waiters.lock().unwrap().remove(&submission);
        }
    }
}
async fn handoff(
    state: &AppServerState,
    params: wire::RealtimeHandoffParams,
) -> Result<Value, String> {
    if !valid_identity(&params.handoff_id)
        || params.text.trim().is_empty()
        || params.text.len() > MAX_TEXT
    {
        return Err("invalid realtime task context".into());
    }
    let handle = state
        .realtime
        .sessions
        .lock()
        .unwrap()
        .get(&params.session_id)
        .cloned()
        .ok_or("The realtime call is unavailable")?;
    if handle.request_id != params.request_id || *handle.cancel.borrow() {
        return Err("The realtime call generation is no longer active".into());
    }
    let session = state
        .session(&params.session_id)
        .ok_or("The realtime conversation is unavailable")?;
    let submission_id = format!("realtime-context:{}", uuid::Uuid::new_v4());
    let mut receipts = session.realtime_events.subscribe();
    let (mut receiver, turn_id, epoch) = {
        let mut ownership = handle.ownership.lock().unwrap();
        if *handle.cancel.borrow() {
            return Err("The realtime call generation is no longer active".into());
        }
        let turn_id = ownership
            .delegations
            .get(&params.handoff_id)
            .or_else(|| ownership.borrowed.get(&params.handoff_id))
            .cloned()
            .ok_or("The realtime task is no longer active")?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        session.register_waiter(submission_id.clone(), sender);
        ownership.context_waiters.insert(submission_id.clone());
        if session.op_tx.send(json!({"id":submission_id,"op":{"type":"steer","input":params.text,"priority":"now","expectedTurnId":turn_id,"displayMetadata":{"source":"realtime","requestId":params.request_id,"callId":params.handoff_id,"expectedTurnId":turn_id}}}).into()).is_err(){session.waiters.lock().unwrap().remove(&submission_id);ownership.context_waiters.remove(&submission_id);return Err("The realtime task transport is closed".into());}
        (receiver, turn_id, ownership.epoch)
    };
    let outcome = tokio::time::timeout(CONNECT_TIMEOUT, async {
        loop {
            tokio::select! {
                reply = &mut receiver => return reply.map_err(|_|"The realtime call ended before accepting context".to_string()).and_then(|reply|exact_payload::scalar(&reply.value)),
                receipt = receipts.recv() => {
                    let receipt=receipt.map_err(|_|"The realtime task acceptance stream is unavailable".to_string())?;
                    let snapshot=exact_payload::field(&receipt,"queueSnapshot");
                    let scalar_eq = |value: &RuntimeJson, expected: &str| value.as_string().is_some_and(|value| value == &klaus_agent_core::runtime_values::RuntimeString::from(expected));
                    if scalar_eq(exact_payload::field(snapshot,"status"),"authoritative") && session.ps.realtime_steer_target().as_deref()==Some(turn_id.as_str()) && exact_payload::field(snapshot,"items").as_array().is_some_and(|items|items.iter().any(|item| {
                        let message=exact_payload::field(item,"message");
                        let metadata=exact_payload::field(message,"displayMetadata");
                        scalar_eq(exact_payload::field(item,"state"),"pending") && scalar_eq(exact_payload::field(message,"sessionId"),&params.session_id) && scalar_eq(exact_payload::field(metadata,"source"),"realtime") && scalar_eq(exact_payload::field(metadata,"requestId"),&params.request_id) && scalar_eq(exact_payload::field(metadata,"callId"),&params.handoff_id) && scalar_eq(exact_payload::field(metadata,"expectedTurnId"),&turn_id)
                    })) {
                        return Ok(json!({"queued":{"accepted":true}}));
                    }
                }
            }
        }
    }).await;
    let stale = {
        let mut ownership = handle.ownership.lock().unwrap();
        ownership.context_waiters.remove(&submission_id);
        *handle.cancel.borrow() || ownership.epoch != epoch
    };
    session.waiters.lock().unwrap().remove(&submission_id);
    if stale {
        return Err("The realtime call generation is no longer active".into());
    }
    let reply =
        outcome.map_err(|_| "The realtime task context was not accepted before its deadline")??;
    if reply.pointer("/queued/accepted") == Some(&Value::Bool(true)) {
        return Ok(json!({}));
    }
    if let Some(error) = reply.get("error") {
        return Err(error["message"]
            .as_str()
            .or_else(|| error.as_str())
            .unwrap_or("The realtime task context was rejected")
            .into());
    }
    if reply.pointer("/steered/applied") != Some(&Value::Bool(true)) {
        return Err("The realtime task did not accept context".into());
    }
    Ok(json!({}))
}
#[derive(Default)]
struct DelegationState {
    calls: HashMap<String, Delegation>,
    superseded: Vec<(String, String)>,
}
struct Delegation {
    turn_id: String,
    owned: bool,
    delivered: bool,
    progress: RuntimeString,
    last_context: std::time::Instant,
}
impl DelegationState {
    async fn begin_hosted(
        &mut self,
        state: &AppServerState,
        session: &SessionHandle,
        handle: &RealtimeHandle,
        params: &wire::RealtimeStartParams,
        id: &str,
        prompt: &str,
    ) -> Result<Option<Value>, String> {
        if self.calls.contains_key(id) {
            return Ok(None);
        }
        if !valid_identity(id) || prompt.trim().is_empty() || prompt.len() > MAX_TEXT {
            return Err("The realtime handoff requires a valid identity and bounded prompt".into());
        }
        if self.calls.len() >= 256
            || self.calls.values().filter(|call| !call.delivered).count() >= 8
        {
            return Ok(Some(function_output(
                id,
                &json!({"status":"failed","error":"The realtime task limit was reached"})
                    .to_string(),
            )));
        }
        let Some(target) = session.ps.realtime_steer_target() else {
            let epoch = handle.ownership.lock().unwrap().epoch;
            return self.begin_call(session,handle,params,&json!({"call_id":id,"name":"delegate_task","arguments":json!({"prompt":prompt}).to_string()}),epoch);
        };
        let (epoch, owned) = {
            let mut ownership = handle.ownership.lock().unwrap();
            if *handle.cancel.borrow() {
                return Ok(None);
            }
            let owned = ownership.delegations.values().any(|turn| turn == &target);
            ownership.borrowed.insert(id.into(), target.clone());
            (ownership.epoch, owned)
        };
        let outcome = handoff(
            state,
            wire::RealtimeHandoffParams {
                session_id: params.session_id.clone(),
                request_id: params.request_id.clone(),
                handoff_id: id.into(),
                text: prompt.into(),
            },
        )
        .await;
        let mut ownership = handle.ownership.lock().unwrap();
        ownership.borrowed.remove(id);
        if *handle.cancel.borrow() || ownership.epoch != epoch {
            return Ok(None);
        }
        if let Err(error) = outcome {
            return Ok(Some(function_output(
                id,
                &json!({"status":"failed","error":error}).to_string(),
            )));
        }
        for (previous, call) in &mut self.calls {
            if !call.delivered && call.turn_id == target {
                call.delivered = true;
                self.superseded.push((previous.clone(), target.clone()));
                ownership.delegations.remove(previous);
                ownership.borrowed.remove(previous);
            }
        }
        if owned {
            ownership.delegations.insert(id.into(), target.clone());
        } else {
            ownership.borrowed.insert(id.into(), target.clone());
        }
        self.calls.insert(
            id.into(),
            Delegation {
                turn_id: target,
                owned,
                delivered: false,
                progress: RuntimeString::default(),
                last_context: std::time::Instant::now(),
            },
        );
        Ok(None)
    }
    fn begin(
        &mut self,
        session: &SessionHandle,
        handle: &RealtimeHandle,
        params: &wire::RealtimeStartParams,
        event: &Value,
    ) -> Result<Vec<Value>, String> {
        if event["type"] != "response.done" {
            return Ok(Vec::new());
        }
        let token = event
            .pointer("/response/metadata/taskResponseToken")
            .and_then(Value::as_str);
        let epoch = event
            .pointer("/response/metadata/taskResponseEpoch")
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<u64>().ok());
        let valid = {
            let mut ownership = handle.ownership.lock().unwrap();
            token.is_some_and(|token| ownership.tokens.remove(token) == Some(true))
                && epoch == Some(ownership.epoch)
                && event.pointer("/response/metadata/taskRequestId")
                    == Some(&json!(params.request_id))
        };
        if !valid || event.pointer("/response/status") != Some(&json!("completed")) {
            return Ok(Vec::new());
        }
        let mut outputs = Vec::new();
        for call in event
            .pointer("/response/output")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|item| item["type"] == "function_call")
        {
            if let Some(output) = self.begin_call(session, handle, params, call, epoch.unwrap())? {
                outputs.push(output);
            }
        }
        Ok(outputs)
    }
    fn begin_call(
        &mut self,
        session: &SessionHandle,
        handle: &RealtimeHandle,
        params: &wire::RealtimeStartParams,
        call: &Value,
        response_epoch: u64,
    ) -> Result<Option<Value>, String> {
        let Some(id) = call["call_id"].as_str().filter(|id| valid_identity(id)) else {
            return Err("The realtime tool call has no valid identity".into());
        };
        if self.calls.contains_key(id) {
            return Ok(None);
        }
        let failure = |message: &str| {
            Some(function_output(
                id,
                &json!({"status":"failed","error":message}).to_string(),
            ))
        };
        if call["name"] != "delegate_task" {
            return Ok(failure("The requested realtime tool is unavailable"));
        }
        if self.calls.len() >= 256
            || self.calls.values().filter(|call| !call.delivered).count() >= 8
        {
            return Ok(failure("The realtime task limit was reached"));
        }
        let args = call["arguments"]
            .as_str()
            .and_then(|value| serde_json::from_str::<Value>(value).ok());
        let Some(args) = args.filter(|value| {
            value.is_object() && value.as_object().is_some_and(|value| value.len() == 1)
        }) else {
            return Ok(failure("The task arguments are invalid"));
        };
        let Some(prompt) = args["prompt"]
            .as_str()
            .filter(|value| !value.trim().is_empty() && value.len() <= MAX_TEXT)
        else {
            return Ok(failure("A bounded task prompt is required"));
        };
        let turn_id = format!("realtime:{}:{id}", params.request_id);
        if session.turns.lock().unwrap().contains(&turn_id)
            || session.waiters.lock().unwrap().contains_key(&turn_id)
        {
            return Err("The realtime task identity is already in flight".into());
        }
        let mut ownership = handle.ownership.lock().unwrap();
        if ownership.epoch != response_epoch || *handle.cancel.borrow() {
            return Ok(None);
        }
        ownership.delegations.insert(id.into(), turn_id.clone());
        if let Err(error)=session.admit_turn(&turn_id,json!({"type":"run","input":prompt,"options":{"metaInput":true,"displayMetadata":{"source":"realtime","requestId":params.request_id,"callId":id}}})) {ownership.delegations.remove(id);return Err(error.into());}
        drop(ownership);
        self.calls.insert(
            id.into(),
            Delegation {
                turn_id,
                owned: true,
                delivered: false,
                progress: RuntimeString::default(),
                last_context: std::time::Instant::now(),
            },
        );
        Ok(None)
    }
    fn observe(
        &mut self,
        handle: &RealtimeHandle,
        event: &RuntimeJson,
    ) -> Option<(String, RuntimeJson)> {
        let submission = exact_payload::text(exact_payload::field(event, "submissionId"))?;
        let (id, call) = self
            .calls
            .iter_mut()
            .find(|(_, call)| !call.delivered && call.turn_id == submission)?;
        let output = if let Some(result) = event.get("result") {
            exact_payload::object([
                (
                    "status",
                    if exact_payload::text(exact_payload::field(result, "stopReason")).as_deref()
                        == Some("completed")
                    {
                        "completed".into()
                    } else {
                        "failed".into()
                    },
                ),
                (
                    "stopReason",
                    exact_payload::field(result, "stopReason").clone(),
                ),
                ("text", exact_payload::field(result, "finalText").clone()),
            ])
        } else if let Some(error) = event.get("error") {
            exact_payload::object([("status", "failed".into()), ("error", error.clone())])
        } else {
            return None;
        };
        call.delivered = true;
        handle.ownership.lock().unwrap().delegations.remove(id);
        handle.ownership.lock().unwrap().borrowed.remove(id);
        Some((
            id.clone(),
            Protocol::Public
                .context_exact(&runtime_json::stringify(&output), Some(id), true, true)
                .remove(0),
        ))
    }

    fn context(&mut self, event: &RuntimeJson) -> Option<RuntimeJson> {
        let submission = exact_payload::text(exact_payload::field(event, "submissionId"))?;
        let (_, call) = self
            .calls
            .iter_mut()
            .find(|(_, call)| !call.delivered && call.turn_id == submission)?;
        let inner = event.get("event")?;
        let kind = exact_payload::text(exact_payload::field(inner, "type")).unwrap_or_default();
        if let Some(delta) = inner
            .get("delta")
            .and_then(RuntimeJson::as_string)
            .or_else(|| inner.get("text").and_then(RuntimeJson::as_string))
        {
            let bytes = std::char::decode_utf16(
                call.progress
                    .as_utf16()
                    .iter()
                    .chain(delta.as_utf16())
                    .copied(),
            )
            .map(|character| character.map_or(3, |character| character.len_utf8()))
            .sum::<usize>();
            if bytes <= 16 * 1024 {
                call.progress = call.progress.concat(delta);
            }
        }
        if !matches!(
            kind.as_str(),
            "item.started" | "item.completed" | "approval.requested" | "turn.failed"
        ) && call.last_context.elapsed() < Duration::from_millis(750)
        {
            return None;
        }
        call.last_context = std::time::Instant::now();
        let text = if call.progress.is_empty() {
            RuntimeString::from(format!("Task {}: {}", call.turn_id, kind))
        } else {
            RuntimeString::from(format!("Task {} progress: ", call.turn_id))
                .concat(&std::mem::take(&mut call.progress))
        };
        Some(
            Protocol::Public
                .context_exact(&text, None, true, false)
                .remove(0),
        )
    }
}
fn owned_response(
    handle: &RealtimeHandle,
    request_id: &str,
    mut event: Value,
) -> Result<Value, String> {
    let mut ownership = handle.ownership.lock().unwrap();
    if ownership.tokens.len() >= 256 {
        return Err("The realtime response limit was reached".into());
    }
    let token = uuid::Uuid::new_v4().to_string();
    ownership.tokens.insert(
        token.clone(),
        event.pointer("/response/tool_choice") != Some(&json!("none"))
            && event.pointer("/response/conversation") != Some(&json!("none")),
    );
    event["response"]["metadata"] = json!({"taskRequestId":request_id,"taskResponseEpoch":ownership.epoch.to_string(),"taskResponseToken":token});
    Ok(event)
}
fn function_output(call_id: &str, text: &str) -> Value {
    json!({"type":"conversation.item.create","item":{"type":"function_call_output","call_id":call_id,"output":text}})
}
async fn append_current_context<S>(
    socket: &mut S,
    core: &Session,
    protocol: Protocol,
) -> Result<(), String>
where
    S: futures::Sink<WsMessage> + Unpin,
    S::Error: std::fmt::Display,
{
    let text = core.realtime_context_text(MAX_CONTEXT_BYTES - CONTEXT_PREFIX.len());
    if text.is_empty() {
        return Ok(());
    }
    for frame in protocol.context(&format!("{CONTEXT_PREFIX}{text}"), None, false, false) {
        socket
            .send(WsMessage::Text(frame.to_string()))
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}
fn sideband_request(
    source: &Provider,
    params: &wire::RealtimeStartParams,
    call_id: &str,
) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request, String> {
    let mut url = source
        .sideband_base_url
        .join(match source.protocol {
            Protocol::Frameless => "live",
            Protocol::Qoder => "api/v2/service/ws/asr",
            _ => "realtime",
        })
        .map_err(|error| error.to_string())?;
    url.set_scheme(if source.sideband_base_url.scheme() == "https" {
        "wss"
    } else {
        "ws"
    })
    .map_err(|_| "The realtime sideband URL is invalid")?;
    if source.protocol == Protocol::Frameless {
        url.path_segments_mut()
            .map_err(|_| "The realtime sideband URL is invalid")?
            .push(call_id);
    } else if source.protocol != Protocol::Qoder {
        if source.protocol == Protocol::V1 {
            url.query_pairs_mut().append_pair("intent", "quicksilver");
        }
        url.query_pairs_mut().append_pair("call_id", call_id);
    }
    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(|error| error.to_string())?;
    request.headers_mut().insert(
        "Authorization",
        format!("Bearer {}", source.api_key)
            .parse()
            .map_err(|_| "The realtime credential is invalid")?,
    );
    if source.protocol != Protocol::Qoder {
        request.headers_mut().insert(
            "x-session-id",
            params
                .session_id
                .parse()
                .map_err(|_| "The realtime session identity is invalid")?,
        );
    }
    if let Some(account) = &source.account_id {
        let version = super::super::model_sources::codex_client_version();
        request
            .headers_mut()
            .insert("originator", "codex_cli_rs".parse().unwrap());
        request.headers_mut().insert(
            "version",
            version
                .parse()
                .map_err(|_| "The realtime client version is invalid")?,
        );
        request.headers_mut().insert(
            "User-Agent",
            super::super::codex_catalog::user_agent(&version)
                .parse()
                .map_err(|_| "The realtime user agent is invalid")?,
        );
        request.headers_mut().insert(
            "ChatGPT-Account-Id",
            account
                .parse()
                .map_err(|_| "The realtime account identity is invalid")?,
        );
    }
    if !matches!(source.protocol, Protocol::Public | Protocol::Qoder) {
        request.headers_mut().insert(
            "OpenAI-Alpha",
            if source.protocol == Protocol::V1 {
                "quicksilver=v1"
            } else {
                "quicksilver=v2"
            }
            .parse()
            .map_err(|_| "The realtime protocol header is invalid")?,
        );
    }
    for (name, value) in &source.extra_headers {
        request.headers_mut().insert(
            http::HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| "The realtime credential is invalid")?,
            http::HeaderValue::from_str(value).map_err(|_| "The realtime credential is invalid")?,
        );
    }
    Ok(request)
}
struct CallContext {
    state: Arc<AppServerState>,
    session: Arc<SessionHandle>,
    handle: Arc<RealtimeHandle>,
    core: Arc<Session>,
    source: Provider,
    params: wire::RealtimeStartParams,
}
async fn run(
    context: CallContext,
    mut commands: mpsc::Receiver<Value>,
    mut cancellation: watch::Receiver<bool>,
    ready: watch::Sender<Option<Result<wire::RealtimeStartResult, String>>>,
) -> Result<(), String> {
    let CallContext {
        state,
        session,
        handle,
        core,
        mut source,
        params,
    } = context;
    let has_configured_model = source.model.is_some();
    let params = &params;
    let init = tokio::select! {
        value = negotiate(&source, params, &core) => value,
        _ = cancellation.changed() => Err("Realtime startup was cancelled".into()),
    };
    let call = match init {
        Ok(call) => call,
        Err(error) => {
            let _ = ready.send(Some(Err(error.clone())));
            return Err(error);
        }
    };
    *handle.finalization.lock().await = Some(Finalization {
        source: source.clone(),
        call_id: call.call_id.clone(),
        core: core.clone(),
    });
    let request = sideband_request(&source, params, &call.call_id)?;
    let connect = async {
        let delays = [100, 300, 800];
        let mut attempt = 0;
        loop {
            let result = tokio_tungstenite::connect_async(request.clone()).await;
            let activating = matches!(&result,Err(tokio_tungstenite::tungstenite::Error::Http(response)) if response.status().as_u16()==404);
            if activating && attempt < delays.len() {
                tokio::time::sleep(Duration::from_millis(delays[attempt])).await;
                attempt += 1;
                continue;
            }
            break result;
        }
    };
    let socket = tokio::select! {
        value = tokio::time::timeout(CONNECT_TIMEOUT, connect) => value.map_err(|_| "The realtime sideband connection timed out".to_string()).and_then(|value| value.map_err(|error| qoder_connect_message(&source, &error).unwrap_or_else(|| format!("The realtime sideband connection failed: {error}")))),
        _ = cancellation.changed() => Err("Realtime startup was cancelled".into()),
    };
    let (mut socket, _) = match socket {
        Ok(socket) => socket,
        Err(error) => {
            if hangup(&source, &call.call_id).await.is_ok() {
                *handle.finalization.lock().await = None;
            }
            let _ = ready.send(Some(Err(error.clone())));
            return Err(error);
        }
    };
    let trace_path = super::super::model_sources::config_root_of(&state.config_path)
        .join("realtime")
        .join(format!("{}.jsonl", params.request_id));
    let mut failure = trace(
        &trace_path,
        &json!({"type":"sideband.connected","sessionId":params.session_id,"callId":call.call_id}),
    )
    .await
    .err();
    let configuration = session_configuration(&source, params, &core);
    if matches!(source.protocol, Protocol::Public | Protocol::Qoder) {
        if let Err(error) = socket
            .send(WsMessage::Text(
                json!({"type":"session.update","session":configuration}).to_string(),
            ))
            .await
        {
            failure = Some(error.to_string());
        }
    }
    let _ = ready.send(Some(if let Some(error) = &failure {
        Err(error.clone())
    } else {
        Ok(call.clone())
    }));
    let started_timeout = tokio::time::sleep(Duration::from_secs(15));
    tokio::pin!(started_timeout);
    let mut transcript = Transcript::default();
    let mut tasks = DelegationState::default();
    let mut task_events = session.realtime_events.subscribe();
    let mut started = !matches!(source.protocol, Protocol::Public | Protocol::Qoder);
    let mut normalizer = TranscriptNormalizer::default();
    if started {
        publish(
            &state,
            params,
            json!({"type":"started","callId":call.call_id,"model":source.model,"protocol":source.protocol.name(),"transcriptSource":"engine"}),
        ).await;
    }
    let mut stopped = false;
    let mut provider_closed = false;
    while failure.is_none() {
        tokio::select! {
            _ = &mut started_timeout, if !started => { failure = Some("The realtime provider did not confirm session startup".into()); break; }
            _ = cancellation.changed(), if !stopped => {
                stopped = true;
                if source.protocol == Protocol::Public {
                    let _ = socket.send(WsMessage::Text(json!({"type":"response.cancel"}).to_string())).await;
                } else if source.protocol == Protocol::Frameless {
                    let _ = socket.send(WsMessage::Text(json!({"type":"session.close"}).to_string())).await;
                }
                break;
            }
            _ = session.ps.wait_until_closing() => { stopped = true; break; }
            command = commands.recv(), if !stopped => {
                match command {
                    Some(command) => {
                        for command in source.protocol.outgoing(command) {
                            if let Err(error) = socket.send(WsMessage::Text(command.to_string())).await {
                                failure = Some(error.to_string());
                                break;
                            }
                        }
                    }
                    None => { stopped = true; break; }
                }
            }
            task_event = task_events.recv() => {
                match task_event {
                    Ok(event) => {
                        if *handle.cancel.borrow(){stopped=true;break;}
                        let identity = exact_payload::text(exact_payload::field(&event,"submissionId")).and_then(|id| tasks.calls.iter().find(|(_,call)| !call.delivered && call.turn_id==id).map(|(id,call)|(id.clone(),call.turn_id.clone(),call.owned)));
                        let Some((handoff, turn, owned)) = identity else { continue; };
                        if exact_payload::text(exact_payload::field(exact_payload::field(&event,"event"),"type")).as_deref()==Some("turn.started") {
                            publish(&state,params,json!({"type":"task.started","handoffId":handoff,"turnId":turn,"owned":owned})).await;
                        }
                        state.notify_session_runtime(&params.session_id,"session/realtimeEvent",exact_payload::object([
                            ("sessionId",params.session_id.clone().into()),("requestId",params.request_id.clone().into()),
                            ("event",exact_payload::object([("type","task.event".into()),("handoffId",handoff.clone().into()),("turnId",turn.clone().into()),("event",event.clone())]))
                        ]),None).await;
                        let output = tasks.observe(&handle, &event).map(|(_, output)| output).or_else(|| tasks.context(&event));
                        if let Some(output) = output {
                            let terminal = exact_payload::text(exact_payload::field(exact_payload::field(&output,"item"),"type")).as_deref()==Some("function_call_output");
                            let outputs = if source.protocol==Protocol::Public { vec![output] } else {
                                let text = if terminal {
                                    let value = output.pointer("/item/output").and_then(RuntimeJson::as_string).and_then(|text|runtime_json::parse(text).ok()).unwrap_or(RuntimeJson::Null);
                                    value.get("text").and_then(RuntimeJson::as_string).cloned().unwrap_or_else(||runtime_json::stringify(&value))
                                } else { output.pointer("/item/content/0/text").and_then(RuntimeJson::as_string).cloned().unwrap_or_default() };
                                source.protocol.context_exact(&text,Some(&handoff),exact_payload::text(exact_payload::field(exact_payload::field(&event,"event"),"phase")).as_deref()!=Some("commentary"),terminal)
                            };
                            for output in outputs {
                                if let Err(error)=socket.send(WsMessage::Text(runtime_json::stringify(&output).try_to_utf8().expect("JSON encoder output is scalar"))).await {failure=Some(error.to_string());break;}
                            }
                            if terminal && matches!(source.protocol, Protocol::Public | Protocol::Qoder) { match owned_response(&handle,&params.request_id,json!({"type":"response.create"})) {Ok(response)=>if let Err(error)=socket.send(WsMessage::Text(response.to_string())).await{failure=Some(error.to_string());break;},Err(error)=>{failure=Some(error);break;}} }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => { if !handle.ownership.lock().unwrap().delegations.is_empty() {failure = Some("Realtime task progress fell behind the authoritative session stream".into()); break;} }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => { stopped = true; break; }
                }
            }
            message = socket.next() => {
                match message {
                    Some(Ok(WsMessage::Text(text))) => {
                        if *handle.cancel.borrow(){stopped=true;break;}
                        if text.len() > 4 * MAX_TEXT { failure = Some("The realtime provider event exceeded its limit".into()); break; }
                        let event: Value = match serde_json::from_str(&text) { Ok(event) => event, Err(_) => { failure = Some("The realtime provider sent invalid JSON".into()); break; } };
                        if let Err(error) = trace(&trace_path, &event).await { failure = Some(error); break; }
                        if !started && event["type"]=="error" {failure=Some(event.pointer("/error/message").and_then(Value::as_str).unwrap_or("The realtime provider rejected session configuration").into());break;}
                        let confirms_start = event["type"] == "session.updated" && (source.protocol == Protocol::Qoder || event.pointer("/session/tools").and_then(Value::as_array).is_some_and(|tools| tools.iter().any(|tool| tool["name"] == "delegate_task")));
                        if started || confirms_start {
                            if let Some(model) = state.realtime.observe_model(&source, &event) {
                                if !has_configured_model { source.model = Some(model); }
                            }
                        }
                        if !started && confirms_start { started = true; publish(&state, params, json!({"type":"started","callId":call.call_id,"model":source.model})).await; }
                        if started && source.protocol==Protocol::Public && (event["type"]=="input_audio_buffer.committed" || (event["type"]=="conversation.item.created"&&event.pointer("/item/role")==Some(&json!("user"))&&event.pointer("/item/content").and_then(Value::as_array).is_some_and(|content|content.iter().any(|part|part["type"]=="input_text")))) {
                            let context=core.realtime_context_text(MAX_CONTEXT_BYTES-CONTEXT_PREFIX.len());
                            if !context.is_empty(){if let Err(error)=socket.send(WsMessage::Text(json!({"type":"conversation.item.create","item":{"type":"message","role":"system","content":[{"type":"input_text","text":format!("{CONTEXT_PREFIX}{context}")}]}}).to_string())).await{failure=Some(error.to_string());break;}}
                            match owned_response(&handle,&params.request_id,json!({"type":"response.create"})) {Ok(response)=>if let Err(error)=socket.send(WsMessage::Text(response.to_string())).await {failure=Some(error.to_string());break;},Err(error)=>{failure=Some(error);break;}}
                        }
                        if let Some((handoff, prompt)) = source.protocol.handoff(&event) {
                            if let Some(input)=normalizer.handoff_input(source.protocol,&event,&prompt) {
                                if let Err(error)=transcript.receive(&state,&core,&source,params,&input).await {failure=Some(error);break;}
                                publish(&state,params,input).await;
                                if let Err(error)=append_current_context(&mut socket,&core,source.protocol).await {failure=Some(error);break;}
                            }
                            let prompt=normalizer.task_prompt(&prompt);
                            let duplicate = tasks.calls.contains_key(&handoff);
                            match tasks.begin_hosted(&state, &session, &handle, params, &handoff, &prompt).await {
                                Ok(Some(output)) => {
                                    let text = output.pointer("/item/output").and_then(Value::as_str).unwrap_or("");
                                    for output in source.protocol.context(text, Some(&handoff), true, true) {
                                        if let Err(error) = socket.send(WsMessage::Text(output.to_string())).await { failure=Some(error.to_string()); break; }
                                    }
                                }
                                Ok(None) => {
                                    for (previous, turn) in tasks.superseded.drain(..) {
                                        publish(&state,params,json!({"type":"task.superseded","handoffId":previous,"turnId":turn,"replacementHandoffId":handoff})).await;
                                    }
                                    if !duplicate {
                                        if let Some(call) = tasks.calls.get(&handoff) {
                                            publish(&state, params, json!({"type":"task.queued","handoffId":handoff,"turnId":call.turn_id,"owned":call.owned})).await;
                                        }
                                    }
                                }
                                Err(error) => { failure=Some(error); break; }
                            }
                        }
                        if matches!(source.protocol, Protocol::Public | Protocol::Qoder) { match tasks.begin(&session, &handle, params, &event) {
                            Ok(outputs) => { for output in outputs { if let Err(error) = socket.send(WsMessage::Text(output.to_string())).await { failure = Some(error.to_string()); break; } match owned_response(&handle,&params.request_id,json!({"type":"response.create"})) {Ok(response)=>if let Err(error)=socket.send(WsMessage::Text(response.to_string())).await{failure=Some(error.to_string());break;},Err(error)=>{failure=Some(error);break;}} } },
                            Err(error) => { failure = Some(error); break; }
                        }
                        }
                        if let Some(caption) = normalizer.normalize(source.protocol, &event) {
                            if let Err(error) = transcript.receive(&state, &core, &source, params, &caption).await { failure = Some(error); break; }
                            if caption["type"]=="conversation.item.input_audio_transcription.completed" {
                                if let Err(error)=append_current_context(&mut socket,&core,source.protocol).await {failure=Some(error);break;}
                            }
                            publish(&state, params, caption).await;
                            continue;
                        }
                        if let Err(error) = transcript.receive(&state, &core, &source, params, &event).await { failure = Some(error); break; }
                        publish_media(&state, params, event, source.protocol == Protocol::Qoder).await;
                    }
                    Some(Ok(WsMessage::Ping(value))) => { let _ = socket.send(WsMessage::Pong(value)).await; }
                    Some(Ok(WsMessage::Close(_))) | None => { provider_closed = true; break; }
                    Some(Err(error)) => { failure = Some(error.to_string()); break; }
                    _ => {}
                }
            }
        }
    }
    retire_tasks(&state, &params.session_id, &params.request_id);
    let finalized = hangup(&source, &call.call_id).await;
    if let Err(error) = &finalized {
        failure = Some(error.clone());
    }
    if !provider_closed {
        let drain = async {
            while let Some(message) = socket.next().await {
                match message {
                    Ok(WsMessage::Text(text)) => {
                        if text.len() > 4 * MAX_TEXT {
                            return Err(
                                "The realtime provider event exceeded its limit".to_string()
                            );
                        }
                        let event: Value =
                            serde_json::from_str(&text).map_err(|error| error.to_string())?;
                        if let Some(caption) = normalizer.normalize(source.protocol, &event) {
                            transcript
                                .receive(&state, &core, &source, params, &caption)
                                .await?;
                            publish(&state, params, caption).await;
                            continue;
                        }
                        transcript
                            .receive(&state, &core, &source, params, &event)
                            .await?;
                        let closed = event["type"] == "session.closed";
                        publish_media(&state, params, event, source.protocol == Protocol::Qoder)
                            .await;
                        if closed {
                            return Ok(true);
                        }
                    }
                    Ok(WsMessage::Close(_)) => return Ok(true),
                    Ok(_) => {}
                    Err(error) => return Err(error.to_string()),
                }
            }
            Ok(true)
        };
        match tokio::time::timeout(DRAIN_TIMEOUT, drain).await {
            Ok(Ok(closed)) => provider_closed = closed,
            Ok(Err(error)) => {
                failure = Some(error);
            }
            Err(_) => {}
        }
    }
    let tail = if params.flush_transcript_tail_on_session_end {
        transcript.flush(&state, &core, &source, params).await
    } else {
        core.flush_rollout().await
    };
    let _ = socket.close(None).await;
    if let Err(error) = trace(&trace_path, &json!({"type":"sideband.finalized","transcriptFlushed":tail.is_ok(),"providerClosed":provider_closed})).await { failure = Some(error); }
    if finalized.is_ok() && tail.is_ok() {
        *handle.finalization.lock().await = None;
    }
    publish(
        &state,
        params,
        json!({"type":"closed","reason":if stopped {"stopped"} else {"transport-closed"},"finalizationComplete":finalized.is_ok(),"transcriptFlushed":tail.is_ok()}),
    ).await;
    tail?;
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(())
}

async fn trace(path: &std::path::Path, event: &Value) -> Result<(), String> {
    let kind = event["type"].as_str().unwrap_or("");
    if kind.ends_with("audio.delta") || kind == "input_audio_buffer.append" {
        return Ok(());
    }
    tokio::fs::create_dir_all(path.parent().ok_or("invalid realtime trace path")?)
        .await
        .map_err(|error| error.to_string())?;
    let mut options = tokio::fs::OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options
        .open(path)
        .await
        .map_err(|error| error.to_string())?;
    let mut encoded = serde_json::to_vec(event).map_err(|error| error.to_string())?;
    encoded.push(b'\n');
    file.write_all(&encoded)
        .await
        .map_err(|error| error.to_string())?;
    file.flush().await.map_err(|error| error.to_string())
}

#[cfg(test)]
#[path = "realtime_model_tests.rs"]
mod observed_model_tests;

#[cfg(test)]
mod wire_contract_tests {
    use super::*;
    fn params() -> wire::RealtimeStartParams {
        serde_json::from_value(json!({"sessionId":"selected-chat","requestId":"voice-one","authMode":"codex","offerSdp":"v=0\r\n"})).unwrap()
    }
    fn source(protocol: Protocol) -> Provider {
        Provider {
            base_url: url::Url::parse("https://chatgpt.com/backend-api/codex/").unwrap(),
            sideband_base_url: url::Url::parse("https://api.openai.com/v1/").unwrap(),
            api_key: "synthetic-access-token".into(),
            credential_identity: credential_identity(&[
                "synthetic-account",
                "synthetic-access-token",
            ]),
            account_id: Some("synthetic-account".into()),
            source_id: "subscription:codex".into(),
            protocol,
            model: Some("voice-model".into()),
            voice: Some("cove".into()),
            transcription_model: None,
            extra_headers: Vec::new(),
        }
    }
    #[test]
    fn existing_call_request_retains_selected_auth_and_exact_protocol_routing() {
        for (protocol, expected, alpha) in [
            (
                Protocol::V1,
                "wss://api.openai.com/v1/realtime?intent=quicksilver&call_id=rtc_one",
                "quicksilver=v1",
            ),
            (
                Protocol::Frameless,
                "wss://api.openai.com/v1/live/rtc_one",
                "quicksilver=v2",
            ),
        ] {
            let request = sideband_request(&source(protocol), &params(), "rtc_one").unwrap();
            assert_eq!(request.uri().to_string(), expected);
            assert_eq!(
                request.headers()["Authorization"],
                "Bearer synthetic-access-token"
            );
            assert_eq!(request.headers()["ChatGPT-Account-Id"], "synthetic-account");
            assert_eq!(request.headers()["x-session-id"], "selected-chat");
            assert_eq!(request.headers()["OpenAI-Alpha"], alpha);
        }
    }
    #[test]
    fn configured_compatible_source_keeps_credentials_on_its_selected_host() {
        let mut source = source(Protocol::Frameless);
        source.account_id = None;
        source.base_url = url::Url::parse("http://127.0.0.1:12345/backend-api/codex/").unwrap();
        source.sideband_base_url = source.base_url.clone();
        let request = sideband_request(&source, &params(), "rtc_one").unwrap();
        assert_eq!(
            request.uri().to_string(),
            "ws://127.0.0.1:12345/backend-api/codex/live/rtc_one"
        );
        assert!(request.headers().get("ChatGPT-Account-Id").is_none());
    }
}
