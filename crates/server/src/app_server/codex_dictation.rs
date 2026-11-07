//! Subscription-native live dictation over a bounded, connection-owned PCM stream.
use super::qoder::speech::TextCallback;
use super::subscription_speech::{self, Source};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures::{SinkExt, StreamExt};
use klaus_agent_core::types::abort::AbortSignal;
use serde_json::{json, Value};
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{
    tungstenite::{client::IntoClientRequest, protocol::WebSocketConfig, Message},
    WebSocketStream,
};

const MAX_TEXT: usize = 48_000;
const MAX_AUDIO: usize = 16_000 * 2 * 300;
const MAX_MESSAGE: usize = 1024 * 1024;
const CLOSE_BUDGET: Duration = Duration::from_secs(30);

fn error(kind: &str) -> String {
    format!("codex_transcription_{kind}")
}

pub(super) async fn stream_dictation(
    root: &Path,
    endpoint: &str,
    input: mpsc::Receiver<Vec<u8>>,
    ready: oneshot::Sender<String>,
    publish: TextCallback,
    signal: AbortSignal,
) -> Result<String, String> {
    let work = async {
        let credential = subscription_speech::codex_credential(root, None).await?;
        let current = credential.current.clone();
        let changed = async {
            loop {
                if !current() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        let connect = async {
            let mut request = endpoint
                .into_client_request()
                .map_err(|_| error("transport_failed"))?;
            let headers = request.headers_mut();
            for (name, value) in [
                (
                    "authorization",
                    format!("Bearer {}", credential.token.access_token),
                ),
                ("chatgpt-account-id", credential.token.account_id),
                ("originator", "Codex Desktop".into()),
                (
                    "user-agent",
                    subscription_speech::codex_transcription_user_agent(),
                ),
                (
                    "sec-websocket-protocol",
                    "chatgpt-dictation, codex-desktop".into(),
                ),
            ] {
                headers.insert(
                    http::header::HeaderName::from_static(name),
                    value
                        .parse()
                        .map_err(|_| error("authentication_required"))?,
                );
            }
            let config = WebSocketConfig {
                max_message_size: Some(MAX_MESSAGE),
                max_frame_size: Some(MAX_MESSAGE),
                ..Default::default()
            };
            let (socket, _) =
                tokio_tungstenite::connect_async_with_config(request, Some(config), false)
                    .await
                    .map_err(|failure| match failure {
                        tokio_tungstenite::tungstenite::Error::Http(response) => {
                            if response
                                .headers()
                                .get("cf-mitigated")
                                .is_some_and(|value| value == "challenge")
                            {
                                error("network_challenge")
                            } else {
                                subscription_speech::http_error(
                                    Source::Codex,
                                    response.status().as_u16(),
                                )
                            }
                        }
                        _ => error("transport_failed"),
                    })?;
            recognize(socket, input, ready, credential.generation, publish).await
        };
        let result = tokio::select! {
            biased;
            _ = changed => Err(error("credential_changed")),
            result = connect => result,
        }?;
        if !(credential.current)() {
            return Err(error("credential_changed"));
        }
        Ok(result)
    };
    tokio::select! {
        biased;
        _ = signal.cancelled() => Err(error("cancelled")),
        result = tokio::time::timeout(Duration::from_secs(340), work) => {
            result.map_err(|_| error("timeout"))?
        }
    }
}

fn start_message() -> Value {
    json!({"type":"session.start", "config": {
        "input_audio_format":"pcm16", "sample_rate_hz":16000, "num_channels":1,
        "max_buffer_size_bytes":4194304, "max_utterance_duration_ms":30000,
        "session_ttl_ms":300000, "provider_mode":"streaming_sse", "transcript_delivery_mode":"segment",
        "vad":{"type":"server_vad", "threshold":0.5, "prefix_padding_ms":300, "silence_duration_ms":500}
    }})
}

struct Utterance {
    id: String,
    revision: u64,
    text: String,
    finalized: bool,
}
#[derive(Default)]
struct Transcript {
    utterances: Vec<Utterance>,
    sequence: Option<u64>,
}
impl Transcript {
    fn accept(&mut self, event: &Value) -> Result<Option<String>, String> {
        let sequence = event["sequence_no"]
            .as_u64()
            .ok_or_else(|| error("response_invalid"))?;
        if self.sequence.is_some_and(|previous| sequence <= previous) {
            return Err(error("sequence_invalid"));
        }
        self.sequence = Some(sequence);
        match event["type"]
            .as_str()
            .ok_or_else(|| error("response_invalid"))?
        {
            "session.error" if event["fatal"] == false => Ok(None),
            "session.error" | "transcript.failed" => {
                let code = event["error"]["code"].as_str().unwrap_or_default();
                Err(error(match code {
                    "rate_limit_exceeded" | "rate_limited" => "rate_limited",
                    "unauthorized" | "authentication_required" | "invalid_api_key" => {
                        "authentication_required"
                    }
                    "quota_exceeded" | "insufficient_quota" | "billing_required" => {
                        "billing_required"
                    }
                    _ => "provider_failed",
                }))
            }
            "transcript.segment" | "transcript.final" => {
                let id = event["utterance_id"]
                    .as_str()
                    .filter(|id| !id.is_empty() && id.len() <= 256)
                    .ok_or_else(|| error("response_invalid"))?;
                let revision = event["revision"]
                    .as_u64()
                    .ok_or_else(|| error("response_invalid"))?;
                let text = event["text"]
                    .as_str()
                    .filter(|text| text.len() <= MAX_TEXT)
                    .ok_or_else(|| error("response_invalid"))?;
                let finalized = event["type"] == "transcript.final";
                if let Some(previous) = self
                    .utterances
                    .iter_mut()
                    .find(|utterance| utterance.id == id)
                {
                    let replace = match (previous.finalized, finalized) {
                        (true, false) => false,
                        (false, true) => true,
                        _ => revision >= previous.revision,
                    };
                    if !replace {
                        return Ok(None);
                    }
                    *previous = Utterance {
                        id: id.into(),
                        revision,
                        text: text.into(),
                        finalized,
                    };
                } else {
                    if self.utterances.len() >= 3000 {
                        return Err(error("response_invalid"));
                    }
                    self.utterances.push(Utterance {
                        id: id.into(),
                        revision,
                        text: text.into(),
                        finalized,
                    });
                }
                let text = self.text()?;
                Ok(Some(text))
            }
            "session.started"
            | "session.updated"
            | "speech.started"
            | "speech.stopped"
            | "audio.frame_processed"
            | "asset.ready"
            | "asset.committed"
            | "asset.failed" => Ok(None),
            _ => Err(error("response_invalid")),
        }
    }
    fn complete_text(&self) -> Result<String, String> {
        if self.utterances.is_empty() || self.utterances.iter().any(|item| !item.finalized) {
            return Err(error("incomplete"));
        }
        self.text()
    }
    fn text(&self) -> Result<String, String> {
        let size = self
            .utterances
            .iter()
            .map(|item| item.text.len() + 1)
            .sum::<usize>();
        if size > MAX_TEXT {
            return Err(error("response_invalid"));
        }
        Ok(self
            .utterances
            .iter()
            .map(|item| item.text.trim())
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join(" "))
    }
}

async fn send_frame<S>(socket: &mut WebSocketStream<S>, frame: Message) -> Result<(), String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    tokio::time::timeout(Duration::from_secs(5), socket.send(frame))
        .await
        .map_err(|_| error("transport_failed"))?
        .map_err(|_| error("transport_failed"))
}

async fn recognize<S>(
    mut socket: WebSocketStream<S>,
    mut input: mpsc::Receiver<Vec<u8>>,
    ready: oneshot::Sender<String>,
    generation: String,
    publish: TextCallback,
) -> Result<String, String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    send_frame(&mut socket, Message::Text(start_message().to_string())).await?;
    let mut transcript = Transcript::default();
    tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Text(text))) => {
                    let event: Value =
                        serde_json::from_str(&text).map_err(|_| error("response_invalid"))?;
                    transcript.accept(&event)?;
                    if event["type"] == "session.started"
                        && event["session"]["status"] == "active"
                        && event["session"]["config"]["provider_mode"] == "streaming_sse"
                        && event["session"]["config"]["transcript_delivery_mode"] == "segment"
                    {
                        return Ok(());
                    }
                    if event["type"] != "session.error" {
                        return Err(error("response_invalid"));
                    }
                }
                Some(Ok(Message::Ping(bytes))) => {
                    send_frame(&mut socket, Message::Pong(bytes)).await?;
                }
                Some(Ok(Message::Pong(_))) => {}
                Some(Err(_)) => return Err(error("transport_failed")),
                _ => return Err(error("incomplete")),
            }
        }
    })
    .await
    .map_err(|_| error("ready_timed_out"))??;
    ready.send(generation).map_err(|_| error("cancelled"))?;
    let mut total = 0usize;
    let mut close_deadline = None;
    loop {
        let deadline = close_deadline
            .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(340));
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => return Err(error("timeout")),
            event = socket.next() => {
                match event {
                    Some(Ok(Message::Text(text))) => {
                        let event: Value = serde_json::from_str(&text).map_err(|_| error("response_invalid"))?;
                        if let Some(text) = transcript.accept(&event)? {
                            publish(text).await;
                        }
                        if event["type"] == "session.updated" && event["session"]["status"] == "closed" {
                            if close_deadline.is_none() { return Err(error("incomplete")); }
                            let text = transcript.complete_text()?;
                            if text.is_empty() { return Err(error("empty")); }
                            let _ = tokio::time::timeout(Duration::from_secs(1), socket.close(None)).await;
                            return Ok(text);
                        }
                    }
                    Some(Ok(Message::Ping(bytes))) => {
                        send_frame(&mut socket, Message::Pong(bytes)).await?;
                    }
                    Some(Ok(Message::Pong(_))) => {},
                    Some(Err(_)) => return Err(error("transport_failed")),
                    _ => return Err(error("incomplete")),
                }
            }
            pcm = input.recv(), if close_deadline.is_none() => {
                match pcm {
                    Some(pcm) => {
                        if pcm.is_empty() || !pcm.len().is_multiple_of(2) {
                            return Err(error("audio_invalid"));
                        }
                        total = total.checked_add(pcm.len()).ok_or_else(|| error("audio_too_large"))?;
                        if total > MAX_AUDIO { return Err(error("audio_too_large")); }
                        let message = json!({"type":"audio.append", "audio":STANDARD.encode(pcm)});
                        send_frame(&mut socket, Message::Text(message.to_string())).await?;
                    }
                    None => {
                        if total == 0 { return Err(error("audio_invalid")); }
                        close_deadline = Some(tokio::time::Instant::now() + CLOSE_BUDGET);
                        send_frame(&mut socket, Message::Text(json!({"type":"session.close"}).to_string())).await?;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "codex_dictation_tests.rs"]
mod tests;
