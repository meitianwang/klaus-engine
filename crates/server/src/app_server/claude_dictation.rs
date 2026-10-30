//! Continuous subscription dictation with bounded writes and finalized utterances.
use super::{
    claude_connect, claude_credential, language, while_current, ClaudeDictationOptions, Source,
    MAX_TEXT,
};
use crate::app_server::qoder::speech::TextCallback;
use futures::{SinkExt, StreamExt};
use klaus_agent_core::types::abort::AbortSignal;
use serde_json::Value;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};

pub(super) async fn stream(
    context: ClaudeDictationOptions,
    input: mpsc::Receiver<Vec<u8>>,
    ready: oneshot::Sender<String>,
    publish: TextCallback,
    signal: AbortSignal,
) -> Result<String, String> {
    let work = async {
        let language = language(context.language.as_deref())?;
        let credential =
            claude_credential(&context.root, context.store, &context.usage, None).await?;
        let current = credential.current.clone();
        let publication_check = current.clone();
        let guarded_publish: TextCallback = std::sync::Arc::new(move |text| {
            let publication_check = publication_check.clone();
            let publish = publish.clone();
            Box::pin(async move {
                if publication_check() {
                    publish(text).await;
                }
            })
        });
        let text = while_current(Source::Claude, current.clone(), async {
            let socket = claude_connect(
                &context.speech.claude_url,
                credential.headers,
                language.as_deref(),
                true,
            )
            .await?;
            if !current() {
                return Err(error("credential_changed"));
            }
            recognize(socket, input, ready, credential.generation, guarded_publish).await
        })
        .await?;
        if !current() {
            return Err(error("credential_changed"));
        }
        Ok(text)
    };
    tokio::select! {
        biased;
        _ = signal.cancelled() => Err(error("cancelled")),
        result = tokio::time::timeout(Duration::from_secs(340), work) => result.map_err(|_| error("timeout"))?,
    }
}

fn error(kind: &str) -> String {
    format!("claude_transcription_{kind}")
}

#[derive(Default)]
struct Transcript {
    committed: String,
    pending: String,
    previous: String,
}
impl Transcript {
    fn update(&mut self, value: &str) -> Result<Option<String>, String> {
        if self
            .committed
            .len()
            .saturating_add(value.len())
            .saturating_add(1)
            > MAX_TEXT
        {
            return Err(error("response_invalid"));
        }
        self.pending = value.to_owned();
        self.snapshot()
    }
    fn commit(&mut self) -> Result<Option<String>, String> {
        super::append_text(&mut self.committed, &self.pending)?;
        self.pending.clear();
        self.snapshot()
    }
    fn snapshot(&mut self) -> Result<Option<String>, String> {
        let mut text = self.committed.clone();
        super::append_text(&mut text, &self.pending)?;
        if text == self.previous {
            return Ok(None);
        }
        self.previous = text.clone();
        Ok(Some(text))
    }
    fn finish(&self) -> Result<String, String> {
        if !self.pending.trim().is_empty() {
            return Err(error("incomplete"));
        }
        if self.committed.trim().is_empty() {
            return Err(error("empty"));
        }
        Ok(self.committed.clone())
    }
}

async fn send<S>(socket: &mut WebSocketStream<S>, message: Message) -> Result<(), String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    tokio::time::timeout(Duration::from_secs(5), socket.send(message))
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
    send(&mut socket, Message::Text(r#"{"type":"KeepAlive"}"#.into())).await?;
    ready.send(generation).map_err(|_| error("cancelled"))?;
    let mut keepalive = tokio::time::interval(Duration::from_secs(5));
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    keepalive.tick().await;
    let mut transcript = Transcript::default();
    let mut close_deadline = None;
    let mut total_audio = 0usize;
    let mut response_bytes = 0usize;
    let mut response_frames = 0usize;
    loop {
        let deadline = close_deadline
            .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(340));
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => return Err(error("timeout")),
            message = socket.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        response_bytes = response_bytes.saturating_add(text.len());
                        response_frames = response_frames.saturating_add(1);
                        if response_bytes > 16 * 1024 * 1024 || response_frames > 50_000 { return Err(error("response_invalid")); }
                        let event: Value = serde_json::from_str(&text).map_err(|_| error("response_invalid"))?;
                        let snapshot = match event["type"].as_str() {
                            Some("TranscriptInterim" | "TranscriptText") => {
                                let text = event["data"].as_str().ok_or_else(|| error("response_invalid"))?;
                                transcript.update(text)?
                            }
                            Some("TranscriptEndpoint") => transcript.commit()?,
                            Some("TranscriptError" | "error") => return Err(error("provider_rejected")),
                            Some(_) => None,
                            None => return Err(error("response_invalid")),
                        };
                        if let Some(text) = snapshot { publish(text).await; }
                    }
                    Some(Ok(Message::Ping(bytes))) => send(&mut socket, Message::Pong(bytes)).await?,
                    Some(Ok(Message::Pong(_))) => {},
                    Some(Ok(Message::Close(frame))) => {
                        if close_deadline.is_none() || frame.as_ref().is_some_and(|frame| {
                            frame.code != tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal
                        }) { return Err(error("incomplete")); }
                        return transcript.finish();
                    }
                    Some(Err(_)) => return Err(error("transport_failed")),
                    _ => return Err(error("incomplete")),
                }
            }
            audio = input.recv(), if close_deadline.is_none() => {
                match audio {
                    Some(audio) => {
                        if audio.is_empty() || !audio.len().is_multiple_of(2) { return Err(error("audio_invalid")); }
                        total_audio = total_audio.saturating_add(audio.len());
                        if total_audio > 16_000 * 2 * 300 { return Err(error("audio_too_large")); }
                        send(&mut socket, Message::Binary(audio)).await?;
                    }
                    None => {
                        if total_audio == 0 { return Err(error("audio_invalid")); }
                        close_deadline = Some(tokio::time::Instant::now() + Duration::from_secs(30));
                        send(&mut socket, Message::Text(r#"{"type":"CloseStream"}"#.into())).await?;
                    }
                }
            }
            _ = keepalive.tick(), if close_deadline.is_none() => {
                send(&mut socket, Message::Text(r#"{"type":"KeepAlive"}"#.into())).await?;
            }
        }
    }
}

#[cfg(test)]
#[path = "claude_dictation_tests.rs"]
mod tests;
