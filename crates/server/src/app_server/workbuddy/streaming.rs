//! Continuous subscription dictation with account-bound audio and final publication.
use super::{
    media,
    native::{self, NativeSession},
};
use crate::app_server::qoder::speech::TextCallback;
use futures::{SinkExt, StreamExt};
use klaus_agent_core::types::abort::AbortSignal;
use serde_json::Value;
use std::time::Duration;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, oneshot},
};
use tokio_tungstenite::{
    tungstenite::{client::IntoClientRequest, protocol::WebSocketConfig, Message},
    WebSocketStream,
};

const STREAM_PATH: &str = "/clientcap/v2/asr/stream?source=desktop&interim_results=1";
const MAX_PCM: usize = 16_000 * 2 * 300;
const MAX_TEXT: usize = 48_000;
const MAX_MESSAGE: usize = 128 * 1024;
const WRITE_BUDGET: Duration = Duration::from_secs(5);
const FINAL_BUDGET: Duration = Duration::from_secs(30);

fn error(kind: &str) -> String {
    format!("workbuddy_transcription_{kind}")
}

fn stream_request(session: &NativeSession) -> Result<http::Request<()>, String> {
    let request = session
        .request(reqwest::Method::GET, STREAM_PATH, None)?
        .build()
        .map_err(|_| error("transport_failed"))?;
    let mut url = request.url().clone();
    let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
    url.set_scheme(scheme)
        .map_err(|_| error("transport_failed"))?;
    let mut websocket = url
        .as_str()
        .into_client_request()
        .map_err(|_| error("transport_failed"))?;
    websocket.headers_mut().extend(request.headers().clone());
    Ok(websocket)
}

pub async fn stream_dictation(
    input: mpsc::Receiver<Vec<u8>>,
    ready: oneshot::Sender<String>,
    publish: TextCallback,
    signal: AbortSignal,
) -> Result<String, String> {
    native::bounded(&signal, 360, async {
        let session = NativeSession::start().await?;
        let config = session.config().await?;
        if media::enabled(&config, "DisableVoiceInput", false) {
            return Err("workbuddy_capability_unavailable".into());
        }
        let config = WebSocketConfig {
            max_message_size: Some(MAX_MESSAGE),
            max_frame_size: Some(MAX_MESSAGE),
            ..Default::default()
        };
        let request = stream_request(&session)?;
        let connect = tokio_tungstenite::connect_async_with_config(request, Some(config), false);
        let (socket, _) = tokio::time::timeout(Duration::from_secs(15), connect)
            .await
            .map_err(|_| error("transport_failed"))?
            .map_err(|failure| match failure {
                tokio_tungstenite::tungstenite::Error::Http(response) => {
                    native::check_error(response.status().as_u16(), &Value::Null)
                        .err()
                        .unwrap_or_else(|| error("provider_rejected"))
                }
                _ => error("transport_failed"),
            })?;
        session.check()?;
        recognize(socket, input, ready, publish, &session).await
    })
    .await
}

#[derive(Debug)]
struct Event {
    voice_id: Option<String>,
    text: Option<String>,
    final_result: bool,
    noise_reject: bool,
}
fn parse_event(bytes: &str) -> Result<Event, String> {
    if bytes.len() > MAX_MESSAGE {
        return Err(error("response_invalid"));
    }
    let value: Value = serde_json::from_str(bytes).map_err(|_| error("response_invalid"))?;
    if !value.is_object() {
        return Err(error("response_invalid"));
    }
    native::check_error(200, &value)?;
    let voice_id = match value.get("voice_id") {
        Some(Value::String(id)) if !id.is_empty() && id.len() <= 256 => Some(id.clone()),
        None | Some(Value::Null) => None,
        _ => return Err(error("response_invalid")),
    };
    let text = match value.get("text") {
        Some(Value::String(text)) if text.len() <= MAX_TEXT => Some(text.clone()),
        None | Some(Value::Null) => None,
        _ => return Err(error("response_invalid")),
    };
    let final_result = match value.get("final") {
        Some(Value::Bool(value)) => *value,
        None => false,
        _ => return Err(error("response_invalid")),
    };
    Ok(Event {
        voice_id,
        text,
        final_result,
        noise_reject: value["noise_reject"].as_bool() == Some(true),
    })
}

fn bind_voice(expected: &mut Option<String>, event: &Event) -> Result<(), String> {
    if let Some(id) = &event.voice_id {
        if expected.as_ref().is_some_and(|expected| expected != id) {
            return Err(error("sequence_invalid"));
        }
        *expected = Some(id.clone());
    }
    Ok(())
}

async fn send_frame<S>(socket: &mut WebSocketStream<S>, frame: Message) -> Result<(), String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    tokio::time::timeout(WRITE_BUDGET, socket.send(frame))
        .await
        .map_err(|_| error("transport_failed"))?
        .map_err(|_| error("transport_failed"))
}

async fn recognize<S>(
    mut socket: WebSocketStream<S>,
    mut input: mpsc::Receiver<Vec<u8>>,
    ready: oneshot::Sender<String>,
    publish: TextCallback,
    session: &NativeSession,
) -> Result<String, String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut heartbeat = tokio::time::interval(Duration::from_millis(50));
    let mut voice_id = None;
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            tokio::select! {
                biased;
                _ = heartbeat.tick() => session.check()?,
                frame = socket.next() => match frame {
                    Some(Ok(Message::Text(text))) => {
                        session.check()?;
                        let event = parse_event(&text)?;
                        if event.final_result || event.text.as_ref().is_some_and(|text| !text.is_empty()) {
                            return Err(error("incomplete"));
                        }
                        bind_voice(&mut voice_id, &event)?;
                        return Ok(());
                    }
                    Some(Ok(Message::Ping(bytes))) => send_frame(&mut socket, Message::Pong(bytes)).await?,
                    Some(Ok(Message::Pong(_))) => {},
                    Some(Err(_)) => return Err(error("transport_failed")),
                    _ => return Err(error("incomplete")),
                }
            }
        }
    }).await.map_err(|_| error("ready_timed_out"))??;
    session.check()?;
    ready
        .send(session.generation())
        .map_err(|_| error("cancelled"))?;
    let mut bytes = 0usize;
    let mut closing = None;
    let mut previous = String::new();
    loop {
        let deadline =
            closing.unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(360));
        tokio::select! {
            biased;
            _ = heartbeat.tick() => session.check()?,
            _ = tokio::time::sleep_until(deadline) => return Err(error("timed_out")),
            frame = socket.next() => match frame {
                Some(Ok(Message::Text(text))) => {
                    session.check()?;
                    let event = parse_event(&text)?;
                    bind_voice(&mut voice_id, &event)?;
                    if event.final_result && closing.is_none() {
                        return Err(error("incomplete"));
                    }
                    if let Some(text) = event.text.as_ref().filter(|text| !text.is_empty()) {
                        if text != &previous {
                            publish(text.clone()).await;
                            session.check()?;
                            previous = text.clone();
                        }
                    }
                    if event.final_result {
                        if event.noise_reject { return Err(error("no_speech")); }
                        let text = event.text.filter(|text| !text.trim().is_empty())
                            .ok_or_else(|| error("no_speech"))?;
                        let _ = tokio::time::timeout(Duration::from_secs(1), socket.close(None)).await;
                        session.check()?;
                        return Ok(text);
                    }
                }
                Some(Ok(Message::Ping(bytes))) => send_frame(&mut socket, Message::Pong(bytes)).await?,
                Some(Ok(Message::Pong(_))) => {},
                Some(Err(_)) => return Err(error("transport_failed")),
                _ => return Err(error("incomplete")),
            },
            pcm = input.recv(), if closing.is_none() => {
                session.check()?;
                match pcm {
                    Some(pcm) => {
                        if pcm.is_empty() || pcm.len() > 32_768 || !pcm.len().is_multiple_of(2) {
                            return Err(error("audio_invalid"));
                        }
                        bytes = bytes.checked_add(pcm.len()).ok_or_else(|| error("audio_too_large"))?;
                        if bytes > MAX_PCM { return Err(error("audio_too_large")); }
                        send_frame(&mut socket, Message::Binary(pcm)).await?;
                    }
                    None => {
                        if bytes == 0 { return Err(error("audio_invalid")); }
                        closing = Some(tokio::time::Instant::now() + FINAL_BUDGET);
                        send_frame(&mut socket, Message::Binary(Vec::new())).await?;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "streaming_tests.rs"]
mod tests;
