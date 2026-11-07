//! Native streaming speech recognition using the connected account.

use super::native::{self, NativeResponse, Session};
use crate::app_server::wire::SubscriptionTranscribeParams;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use flate2::read::GzDecoder;
use futures::{SinkExt, StreamExt};
use klaus_agent_core::types::abort::AbortSignal;
use serde_json::{json, Value};
use std::{io::Read, sync::Arc, time::Duration};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::{
    tungstenite::{protocol::WebSocketConfig, Message},
    WebSocketStream,
};

const MAX_PCM: usize = 6 * 60 * 16_000 * 2;
const MAX_AUDIO: usize = MAX_PCM + 64 * 1024;
const MAX_EVENT: usize = 256 * 1024;
const MAX_TEXT: usize = 64 * 1024;
const FRAME_BYTES: usize = 3200;

pub(crate) struct Transcript {
    pub text: String,
    pub sequence: Option<i32>,
    pub definite: bool,
}

pub async fn transcribe(
    params: SubscriptionTranscribeParams,
    signal: AbortSignal,
) -> Result<NativeResponse, String> {
    // Check input before decoding credentials or starting a network operation.
    let pcm = decode_audio(&params.audio_base64, params.mime_type.as_deref())?;
    native::bounded(&signal, None, 420, async {
        let session = Session::start(params.expected_credential_generation.as_deref()).await?;
        native::bounded(&signal, Some(&session.generation), 410, async {
            let socket = open(&session).await?;
            let transcript = recognize(socket, Feed::Buffered(&pcm), None, None).await?;
            session.response(json!({"text":transcript.text,"rawText":transcript.text,
                "polished":false,"refinementComplete":false,"definite":transcript.definite,
                "sequence":transcript.sequence,"credentialGeneration":session.generation}))
        })
        .await
    })
    .await
}

pub async fn stream_dictation(
    input: tokio::sync::mpsc::Receiver<Vec<u8>>,
    ready: tokio::sync::oneshot::Sender<String>,
    publish: crate::app_server::qoder::speech::TextCallback,
    signal: AbortSignal,
) -> Result<String, String> {
    native::bounded(&signal, None, 420, async {
        let session = Session::start(None).await?;
        native::bounded(&signal, Some(&session.generation), 410, async {
            let socket = open(&session).await?;
            let generation = session.generation.clone();
            let guarded_publish: crate::app_server::qoder::speech::TextCallback =
                Arc::new(move |text| {
                    let generation = generation.clone();
                    let publish = publish.clone();
                    Box::pin(async move {
                        if super::auth::is_current(&generation) {
                            publish(text).await;
                        }
                    })
                });
            let transcript = recognize(
                socket,
                Feed::Live(input),
                Some((ready, session.generation.clone())),
                Some(guarded_publish),
            )
            .await?;
            session.check()?;
            Ok(transcript.text)
        })
        .await
    })
    .await
}

fn asr_base() -> String {
    std::env::var("KLAUS_TRAE_ASR_URL")
        .ok()
        .filter(|value| {
            reqwest::Url::parse(value).is_ok_and(|url| {
                url.scheme() == "http"
                    && matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"))
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none()
                    && url.fragment().is_none()
            })
        })
        .unwrap_or_else(|| "https://api.trae.com.cn".into())
        .trim_end_matches('/')
        .to_owned()
}

async fn open(
    session: &Session,
) -> Result<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>, String> {
    let response = super::client()
        .post(format!("{}/icube/api/v2/asr/get/a", asr_base()))
        .header("content-type", "application/json")
        .header("x-icube-token", &session.credential.access_token)
        .send()
        .await
        .map_err(|_| "trae_transcription_exchange_failed")?;
    let value = native::read_json(response, 64 * 1024).await?;
    if value["success"] != true {
        return Err("trae_transcription_exchange_failed".into());
    }
    let token = value
        .pointer("/data/jwt_token")
        .and_then(Value::as_str)
        .filter(|value| {
            !value.is_empty() && value.len() <= 32768 && !value.chars().any(char::is_control)
        })
        .ok_or("trae_transcription_exchange_invalid")?;
    let app_id = value
        .pointer("/data/app_id")
        .and_then(Value::as_str)
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 64
                && value.bytes().all(|value| value.is_ascii_digit())
        })
        .ok_or("trae_transcription_exchange_invalid")?;
    let mut url =
        reqwest::Url::parse("wss://openspeech.bytedance.com/api/v3/sauc/bigmodel").unwrap();
    if let Ok(local) = std::env::var("KLAUS_TRAE_ASR_WS_URL") {
        if let Ok(candidate) = reqwest::Url::parse(&local) {
            if candidate.scheme() == "ws"
                && matches!(
                    candidate.host_str(),
                    Some("localhost" | "127.0.0.1" | "[::1]")
                )
                && candidate.username().is_empty()
                && candidate.password().is_none()
                && candidate.query().is_none()
                && candidate.fragment().is_none()
            {
                url = candidate;
            }
        }
    }
    url.query_pairs_mut()
        .append_pair("api_resource_id", "volc.bigasr.sauc.duration")
        .append_pair("api_app_key", app_id)
        .append_pair("api_access_key", &format!("Jwt; {token}"));
    session.check()?;
    let config = WebSocketConfig {
        max_message_size: Some(MAX_EVENT),
        max_frame_size: Some(MAX_EVENT),
        ..WebSocketConfig::default()
    };
    // Do not expose the request URL: its query contains the short-lived credential.
    let (socket, _) =
        tokio_tungstenite::connect_async_with_config(url.as_str(), Some(config), true)
            .await
            .map_err(|_| "trae_transcription_connection_failed")?;
    session.check()?;
    Ok(socket)
}

fn frame(kind: u8, final_audio: bool, payload: &[u8]) -> Vec<u8> {
    let mut frame = vec![0x11, (kind << 4) | if final_audio { 2 } else { 0 }, 0x10, 0];
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

struct Event {
    text: String,
    sequence: Option<i32>,
    final_result: bool,
    definite: bool,
}

fn parse_event(bytes: &[u8]) -> Result<Event, String> {
    let invalid = || "trae_transcription_response_invalid".to_owned();
    if bytes.len() < 8 || bytes.len() > MAX_EVENT || bytes[0] >> 4 != 1 {
        return Err(invalid());
    }
    let mut offset = (bytes[0] as usize & 15) * 4;
    if offset < 4 || offset > bytes.len() {
        return Err(invalid());
    }
    let kind = bytes[1] >> 4;
    let flags = bytes[1] & 15;
    let mut take_u32 = || -> Result<u32, String> {
        let value = bytes.get(offset..offset + 4).ok_or_else(invalid)?;
        offset += 4;
        Ok(u32::from_be_bytes(value.try_into().map_err(|_| invalid())?))
    };
    let sequence = match kind {
        15 => return Err(format!("trae_transcription_refused ({})", take_u32()?)),
        9 => {
            if flags & 1 != 0 {
                Some(take_u32()? as i32)
            } else {
                None
            }
        }
        _ => return Err(invalid()),
    };
    let length = take_u32()? as usize;
    if length > MAX_EVENT || offset.checked_add(length) != Some(bytes.len()) || bytes[2] >> 4 != 1 {
        return Err(invalid());
    }
    let payload = &bytes[offset..];
    let payload = match bytes[2] & 15 {
        0 => payload.to_vec(),
        1 => {
            let mut out = Vec::new();
            GzDecoder::new(payload)
                .take(MAX_EVENT as u64 + 1)
                .read_to_end(&mut out)
                .map_err(|_| invalid())?;
            if out.len() > MAX_EVENT {
                return Err(invalid());
            }
            out
        }
        _ => return Err(invalid()),
    };
    let value: Value = serde_json::from_slice(&payload).map_err(|_| invalid())?;
    if value
        .get("code")
        .and_then(Value::as_i64)
        .is_some_and(|code| !matches!(code, 0 | 1000 | 20000000))
    {
        return Err(format!("trae_transcription_refused ({})", value["code"]));
    }
    let text = value
        .pointer("/result/text")
        .and_then(Value::as_str)
        .ok_or_else(invalid)?;
    if text.len() > MAX_TEXT || text.contains('\0') {
        return Err(invalid());
    }
    let definite = value
        .pointer("/result/utterances")
        .and_then(Value::as_array)
        .is_some_and(|values| {
            !values.is_empty() && values.iter().all(|value| value["definite"] == true)
        });
    Ok(Event {
        text: text.to_owned(),
        sequence,
        final_result: flags & 2 != 0,
        definite,
    })
}

enum Feed<'a> {
    Buffered(&'a [u8]),
    Live(tokio::sync::mpsc::Receiver<Vec<u8>>),
}

async fn recognize<S>(
    socket: WebSocketStream<S>,
    feed: Feed<'_>,
    mut ready: Option<(tokio::sync::oneshot::Sender<String>, String)>,
    publish: Option<crate::app_server::qoder::speech::TextCallback>,
) -> Result<Transcript, String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut writer, mut reader) = socket.split();
    let config = json!({"user":{"uid":"klaus-dictation"},"audio":{"format":"pcm","rate":16000,"bits":16,"channel":1},
        "request":{"model_name":"bigmodel","show_utterances":true}});
    writer
        .send(Message::Binary(frame(
            1,
            false,
            config.to_string().as_bytes(),
        )))
        .await
        .map_err(|_| "trae_transcription_send_failed")?;
    let acknowledged = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match reader.next().await {
                Some(Ok(Message::Binary(bytes))) => return parse_event(&bytes),
                Some(Ok(Message::Ping(bytes))) => {
                    writer
                        .send(Message::Pong(bytes))
                        .await
                        .map_err(|_| "trae_transcription_send_failed")?;
                }
                Some(Ok(Message::Close(_))) | None => {
                    return Err("trae_transcription_incomplete".into())
                }
                Some(Err(_)) => return Err("trae_transcription_receive_failed".into()),
                _ => {}
            }
        }
    })
    .await
    .map_err(|_| "trae_transcription_ready_timed_out")??;
    if acknowledged.final_result {
        return Err("trae_transcription_response_invalid".into());
    }
    if let Some((ready, generation)) = ready.take() {
        ready
            .send(generation)
            .map_err(|_| "trae_operation_cancelled")?;
    }
    let send = async {
        match feed {
            Feed::Buffered(pcm) => {
                let chunks = pcm.chunks(FRAME_BYTES);
                let count = chunks.len();
                for (index, chunk) in chunks.enumerate() {
                    writer
                        .send(Message::Binary(frame(2, index + 1 == count, chunk)))
                        .await
                        .map_err(|_| "trae_transcription_send_failed")?;
                    if index + 1 != count {
                        tokio::time::sleep(Duration::from_millis(80)).await;
                    }
                }
            }
            Feed::Live(mut input) => {
                let mut total = 0usize;
                while let Some(pcm) = input.recv().await {
                    total = total
                        .checked_add(pcm.len())
                        .ok_or("trae_transcription_audio_too_large")?;
                    if total > MAX_PCM {
                        return Err("trae_transcription_audio_too_large".to_owned());
                    }
                    if pcm.len() % 2 != 0 {
                        return Err("trae_transcription_audio_invalid".to_owned());
                    }
                    for chunk in pcm.chunks(FRAME_BYTES) {
                        writer
                            .send(Message::Binary(frame(2, false, chunk)))
                            .await
                            .map_err(|_| "trae_transcription_send_failed")?;
                    }
                }
                if total == 0 {
                    return Err("trae_transcription_audio_invalid".into());
                }
                writer
                    .send(Message::Binary(frame(2, true, &[])))
                    .await
                    .map_err(|_| "trae_transcription_send_failed")?;
            }
        }
        Ok::<(), String>(())
    };
    let receive = async {
        let mut previous_sequence = acknowledged.sequence;
        let mut previous_text = String::new();
        loop {
            match reader.next().await {
                Some(Ok(Message::Binary(bytes))) => {
                    let event = parse_event(&bytes)?;
                    if let (Some(previous), Some(current)) = (previous_sequence, event.sequence) {
                        if current > 0 && current <= previous {
                            return Err("trae_transcription_sequence_invalid".into());
                        }
                    }
                    previous_sequence = event.sequence;
                    if event.text != previous_text {
                        if let Some(publish) = &publish {
                            publish(event.text.clone()).await;
                        }
                        previous_text = event.text.clone();
                    }
                    if event.final_result {
                        if event.text.trim().is_empty() {
                            return Err("trae_transcription_empty".into());
                        }
                        return Ok::<Transcript, String>(Transcript {
                            text: event.text,
                            sequence: event.sequence,
                            definite: event.definite,
                        });
                    }
                }
                Some(Ok(Message::Close(_))) | None => {
                    return Err("trae_transcription_incomplete".into())
                }
                Some(Err(_)) => return Err("trae_transcription_receive_failed".into()),
                _ => {}
            }
        }
    };
    let ((), transcript) = tokio::try_join!(send, receive)?;
    Ok(transcript)
}

fn decode_audio(encoded: &str, mime: Option<&str>) -> Result<Vec<u8>, String> {
    if encoded.len() > MAX_AUDIO.div_ceil(3) * 4 {
        return Err("trae_transcription_audio_too_large".into());
    }
    let bytes = STANDARD
        .decode(encoded)
        .map_err(|_| "trae_transcription_audio_invalid")?;
    if bytes.len() > MAX_AUDIO {
        return Err("trae_transcription_audio_too_large".into());
    }
    let pcm = match mime.unwrap_or("audio/wav") {
        "audio/pcm" => bytes.as_slice(),
        "audio/wav" | "audio/x-wav" => wav_pcm(&bytes)?,
        _ => return Err("trae_transcription_mime_type_unsupported".into()),
    };
    if pcm.is_empty() || pcm.len() % 2 != 0 {
        return Err("trae_transcription_audio_invalid".into());
    }
    if pcm.len() > MAX_PCM {
        return Err("trae_transcription_audio_too_large".into());
    }
    Ok(pcm.to_vec())
}

fn wav_pcm(bytes: &[u8]) -> Result<&[u8], String> {
    let invalid = || "trae_transcription_wav_format_invalid".to_string();
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
                    || u32::from_le_bytes(chunk[4..8].try_into().unwrap()) != 16000
                    || u32::from_le_bytes(chunk[8..12].try_into().unwrap()) != 32000
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

#[cfg(test)]
#[path = "audio_tests.rs"]
mod tests;
