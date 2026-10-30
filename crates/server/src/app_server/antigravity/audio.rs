//! Direct cloud dictation over the service's bidirectional prediction protocol.

use base64::Engine;
use bytes::{Buf, Bytes, BytesMut};
use futures::future::poll_fn;
use klaus_agent_core::types::abort::AbortSignal;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::time::{Instant, MissedTickBehavior};

const HOST: &str = "aicode.googleapis.com";
const PATH: &str = "/google.gca.aicode.v1alpha.PredictionService/BidiGenerateContent";
const MAX_AUDIO: usize = 12 * 1024 * 1024;
const MAX_PCM: usize = 16_000 * 2 * 300;
const MAX_MESSAGE: usize = 1024 * 1024;
const MAX_TEXT: usize = 256 * 1024;
const MAX_RESPONSE: usize = 4 * 1024 * 1024;
const PCM_CHUNK: usize = 3200;
const BUDGET: Duration = Duration::from_secs(450);
const SETUP_BUDGET: Duration = Duration::from_secs(30);
const DRAIN_BUDGET: Duration = Duration::from_secs(30);

pub(super) struct AudioInput<'a> {
    pub audio_base64: &'a str,
    pub mime_type: Option<&'a str>,
    pub language: Option<&'a str>,
}

pub(super) struct Credentials<'a> {
    pub access_token: &'a str,
    pub model: &'a str,
}

/// Only PCM16 mono 16 kHz WAV is accepted, avoiding implicit lossy conversion.
pub(super) async fn transcribe(
    input: AudioInput<'_>,
    credentials: Credentials<'_>,
    signal: AbortSignal,
) -> Result<Value, String> {
    if input.language.is_some_and(|value| !value.is_empty()) {
        return Err(error("language_unsupported"));
    }
    if !matches!(input.mime_type, Some("audio/wav")) {
        return Err(error("format_unsupported"));
    }
    if input.audio_base64.is_empty() || input.audio_base64.len() > MAX_AUDIO.div_ceil(3) * 4 {
        return Err(error("audio_invalid"));
    }
    let wav = base64::engine::general_purpose::STANDARD
        .decode(input.audio_base64)
        .map_err(|_| error("audio_invalid"))?;
    let pcm = pcm_from_wav(&wav)?;
    if credentials.access_token.is_empty() {
        return Err(error("authentication_required"));
    }
    if credentials.model.is_empty() || credentials.model.len() > 512 {
        return Err(error("configuration_invalid"));
    }
    let work = async {
        let io = connect().await?;
        transcribe_io(io, pcm, credentials, Duration::from_millis(100))
            .await
            .map(|text| json!({ "text": text }))
    };
    tokio::select! {
        biased;
        _ = signal.cancelled() => Err(error("cancelled")),
        result = tokio::time::timeout(BUDGET, work) => {
            result.map_err(|_| error("timeout"))?
        }
    }
}

fn error(category: &str) -> String {
    format!("antigravity_transcription_{category}")
}

fn pcm_from_wav(bytes: &[u8]) -> Result<&[u8], String> {
    if bytes.len() > MAX_AUDIO
        || bytes.len() < 12
        || &bytes[..4] != b"RIFF"
        || &bytes[8..12] != b"WAVE"
    {
        return Err(error("format_unsupported"));
    }
    let u16le = |offset| u16::from_le_bytes([bytes[offset], bytes[offset + 1]]);
    let u32le = |offset| u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    if u32le(4) as usize != bytes.len() - 8 {
        return Err(error("audio_invalid"));
    }
    let mut offset = 12;
    let mut format = false;
    let mut pcm = None;
    while offset < bytes.len() {
        let start = offset
            .checked_add(8)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| error("audio_invalid"))?;
        let length = u32le(offset + 4) as usize;
        let end = start
            .checked_add(length)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| error("audio_invalid"))?;
        match &bytes[offset..offset + 4] {
            b"fmt " => {
                if format || length < 16 {
                    return Err(error("audio_invalid"));
                }
                if u16le(start) != 1
                    || u16le(start + 2) != 1
                    || u32le(start + 4) != 16_000
                    || u32le(start + 8) != 32_000
                    || u16le(start + 12) != 2
                    || u16le(start + 14) != 16
                {
                    return Err(error("format_unsupported"));
                }
                format = true;
            }
            b"data" => {
                if pcm.is_some() || length == 0 || length > MAX_PCM || !length.is_multiple_of(2) {
                    return Err(error("audio_invalid"));
                }
                pcm = Some(&bytes[start..end]);
            }
            _ => {}
        }
        offset = end
            .checked_add(length % 2)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| error("audio_invalid"))?;
    }
    if !format {
        return Err(error("audio_invalid"));
    }
    pcm.ok_or_else(|| error("audio_invalid"))
}

async fn connect() -> Result<tokio_rustls::client::TlsStream<TcpStream>, String> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|_| error("transport_unavailable"))?
    .with_root_certificates(roots)
    .with_no_client_auth();
    tls.alpn_protocols = vec![b"h2".to_vec()];
    tokio::time::timeout(SETUP_BUDGET, async {
        let tcp = TcpStream::connect((HOST, 443))
            .await
            .map_err(|_| error("network_error"))?;
        let name = rustls::pki_types::ServerName::try_from(HOST)
            .map_err(|_| error("transport_unavailable"))?;
        let stream = tokio_rustls::TlsConnector::from(Arc::new(tls))
            .connect(name, tcp)
            .await
            .map_err(|_| error("network_error"))?;
        if stream.get_ref().1.alpn_protocol() != Some(b"h2") {
            return Err(error("transport_unavailable"));
        }
        Ok(stream)
    })
    .await
    .map_err(|_| error("timeout"))?
}

/// Cancelling the outer future must close the transport's background driver.
struct Driver(tokio::task::JoinHandle<()>);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

enum AudioFeed<'a> {
    Buffered {
        pcm: &'a [u8],
        offset: usize,
        timer: tokio::time::Interval,
    },
    Live(tokio::sync::mpsc::Receiver<Vec<u8>>),
}
impl AudioFeed<'_> {
    async fn next(&mut self) -> Option<Vec<u8>> {
        match self {
            Self::Buffered { pcm, offset, timer } => {
                timer.tick().await;
                if *offset == pcm.len() {
                    return None;
                }
                let end = (*offset + PCM_CHUNK).min(pcm.len());
                let frame = pcm[*offset..end].to_vec();
                *offset = end;
                Some(frame)
            }
            Self::Live(receiver) => receiver.recv().await,
        }
    }
}
#[derive(Default)]
pub(super) struct StreamCallbacks {
    pub ready: Option<Box<dyn FnOnce() + Send>>,
    pub partial: Option<crate::app_server::qoder::speech::TextCallback>,
    pub valid: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
}

pub(super) async fn transcribe_stream(
    input: tokio::sync::mpsc::Receiver<Vec<u8>>,
    credentials: Credentials<'_>,
    callbacks: StreamCallbacks,
    signal: AbortSignal,
) -> Result<String, String> {
    let work = async {
        let io = connect().await?;
        transcribe_feed(io, AudioFeed::Live(input), credentials, callbacks).await
    };
    tokio::select! {
        biased;
        _ = signal.cancelled() => Err(error("cancelled")),
        result = tokio::time::timeout(BUDGET, work) => result.map_err(|_| error("timeout"))?,
    }
}

async fn transcribe_io<T>(
    io: T,
    pcm: &[u8],
    credentials: Credentials<'_>,
    pacing: Duration,
) -> Result<String, String>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut timer = tokio::time::interval(pacing);
    timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    transcribe_feed(
        io,
        AudioFeed::Buffered {
            pcm,
            offset: 0,
            timer,
        },
        credentials,
        StreamCallbacks::default(),
    )
    .await
}

async fn transcribe_feed<T>(
    io: T,
    mut feed: AudioFeed<'_>,
    credentials: Credentials<'_>,
    mut callbacks: StreamCallbacks,
) -> Result<String, String>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (client, connection) = h2::client::handshake(io)
        .await
        .map_err(|_| error("network_error"))?;
    let _driver = Driver(tokio::spawn(async move {
        let _ = connection.await;
    }));
    let mut client = client.ready().await.map_err(|_| error("network_error"))?;
    let mut authorization =
        http::HeaderValue::from_str(&format!("Bearer {}", credentials.access_token))
            .map_err(|_| error("configuration_invalid"))?;
    authorization.set_sensitive(true);
    let request = http::Request::builder()
        .method("POST")
        .uri(format!("https://{HOST}{PATH}"))
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .header("grpc-timeout", "450000m")
        .header("user-agent", "klaus")
        .header("authorization", authorization)
        .body(())
        .map_err(|_| error("configuration_invalid"))?;
    let (response, mut send) = client
        .send_request(request, false)
        .map_err(|_| error("network_error"))?;
    send_frame(&mut send, setup(credentials.model), false).await?;
    let response = tokio::time::timeout(SETUP_BUDGET, response)
        .await
        .map_err(|_| error("timeout"))?
        .map_err(|_| error("network_error"))?;
    if !response.status().is_success() {
        return Err(http_error(response.status().as_u16()));
    }
    let immediate_status = grpc_status(response.headers())?;
    let mut recv = response.into_body();
    let mut decoder = FrameDecoder::default();
    let mut transcript = Transcript::default();
    let setup_deadline = Instant::now() + SETUP_BUDGET;
    while !transcript.setup_complete {
        match tokio::time::timeout_at(setup_deadline, receive(&mut recv, &mut decoder)).await {
            Ok(Ok(Some(messages))) => {
                for message in messages {
                    transcript.accept(&message)?;
                }
            }
            Ok(Ok(None)) => {
                return finish(&mut recv, decoder, transcript, immediate_status, false).await;
            }
            Ok(Err(error)) => return Err(error),
            Err(_) => return Err(error("timeout")),
        }
    }
    send_frame(&mut send, realtime_message(5, &[]), false).await?;
    if callbacks.valid.as_ref().is_some_and(|valid| !valid()) {
        return Err(error("credential_changed"));
    }
    if let Some(ready) = callbacks.ready.take() {
        ready();
    }
    let mut published = String::new();
    let mut audio_bytes = 0usize;
    let mut sending = true;
    let mut drain_deadline = None;
    loop {
        tokio::select! {
            result = receive(&mut recv, &mut decoder) => {
                match result? {
                    Some(messages) => {
                        for message in messages {
                            transcript.accept(&message)?;
                        }
                        let snapshot = transcript.snapshot();
                        if snapshot != published {
                            if let Some(partial) = &callbacks.partial { partial(snapshot.clone()).await; }
                            published = snapshot;
                        }
                    }
                    None => return finish(&mut recv, decoder, transcript, immediate_status, !sending).await,
                }
            }
            chunk = feed.next(), if sending => {
                if let Some(chunk) = chunk {
                    if callbacks.valid.as_ref().is_some_and(|valid| !valid()) { return Err(error("credential_changed")); }
                    audio_bytes = audio_bytes.saturating_add(chunk.len());
                    if chunk.is_empty() || !chunk.len().is_multiple_of(2) || audio_bytes > MAX_PCM { return Err(error("audio_invalid")); }
                    send_frame(&mut send, audio_message(&chunk), false).await?;
                } else {
                    send_frame(&mut send, realtime_message(6, &[]), true).await?;
                    sending = false;
                    drain_deadline = Some(Instant::now() + DRAIN_BUDGET);
                }
            }
            _ = async {
                if let Some(deadline) = drain_deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => return Err(error("timeout")),
        }
    }
}

async fn send_frame(
    send: &mut h2::SendStream<Bytes>,
    payload: Vec<u8>,
    end_stream: bool,
) -> Result<(), String> {
    let mut frame = BytesMut::with_capacity(payload.len() + 5);
    frame.extend_from_slice(&[0]);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);
    let mut frame = frame.freeze();
    while !frame.is_empty() {
        send.reserve_capacity(frame.len());
        let capacity = poll_fn(|cx| send.poll_capacity(cx))
            .await
            .ok_or_else(|| error("network_error"))?
            .map_err(|_| error("network_error"))?;
        if capacity == 0 {
            continue;
        }
        let part = frame.split_to(capacity.min(frame.len()));
        send.send_data(part, end_stream && frame.is_empty())
            .map_err(|_| error("network_error"))?;
    }
    Ok(())
}

async fn receive(
    recv: &mut h2::RecvStream,
    decoder: &mut FrameDecoder,
) -> Result<Option<Vec<Vec<u8>>>, String> {
    match recv.data().await {
        Some(Ok(data)) => {
            let messages = decoder.push(&data)?;
            recv.flow_control()
                .release_capacity(data.len())
                .map_err(|_| error("network_error"))?;
            Ok(Some(messages))
        }
        Some(Err(_)) => Err(error("network_error")),
        None => Ok(None),
    }
}

async fn finish(
    recv: &mut h2::RecvStream,
    decoder: FrameDecoder,
    transcript: Transcript,
    immediate_status: Option<()>,
    input_complete: bool,
) -> Result<String, String> {
    if !decoder.buffer.is_empty() {
        return Err(error("protocol_error"));
    }
    let trailers = recv.trailers().await.map_err(|_| error("network_error"))?;
    let status = trailers.as_ref().map(grpc_status).transpose()?.flatten();
    if status.or(immediate_status).is_none() || !transcript.setup_complete || !transcript.seen_text
    {
        return Err(error("protocol_error"));
    }
    if !input_complete {
        return Err(error("incomplete"));
    }
    Ok(transcript.finish())
}

fn http_error(status: u16) -> String {
    error(match status {
        401 => "authentication_required",
        402 => "billing_required",
        403 => "access_denied",
        429 => "rate_limited",
        _ => "service_error",
    })
}

fn grpc_status(headers: &http::HeaderMap) -> Result<Option<()>, String> {
    let Some(status) = headers.get("grpc-status") else {
        return Ok(None);
    };
    let status = status.to_str().map_err(|_| error("protocol_error"))?;
    let category = match status {
        "0" => return Ok(Some(())),
        "1" => "cancelled",
        "3" => "audio_invalid",
        "4" => "timeout",
        "7" | "9" => {
            let billing = headers
                .get("grpc-message")
                .and_then(|value| value.to_str().ok())
                .filter(|value| value.len() <= 8192)
                .is_some_and(|value| {
                    let value = value.to_ascii_lowercase();
                    ["billing", "paid", "subscription", "upgrade"]
                        .iter()
                        .any(|needle| value.contains(needle))
                });
            if billing {
                "billing_required"
            } else {
                "access_denied"
            }
        }
        "8" => "rate_limited",
        "12" => "unavailable",
        "16" => "authentication_required",
        _ => "service_error",
    };
    Err(error(category))
}

#[derive(Default)]
struct FrameDecoder {
    buffer: BytesMut,
    received: usize,
}
impl FrameDecoder {
    fn push(&mut self, data: &[u8]) -> Result<Vec<Vec<u8>>, String> {
        self.received = self
            .received
            .checked_add(data.len())
            .filter(|size| *size <= MAX_RESPONSE)
            .ok_or_else(|| error("response_too_large"))?;
        if self.buffer.len() + data.len() > MAX_MESSAGE + 5 {
            return Err(error("response_too_large"));
        }
        self.buffer.extend_from_slice(data);
        let mut messages = Vec::new();
        while self.buffer.len() >= 5 {
            if self.buffer[0] != 0 {
                return Err(error("protocol_error"));
            }
            let size = u32::from_be_bytes(self.buffer[1..5].try_into().unwrap()) as usize;
            if size > MAX_MESSAGE {
                return Err(error("response_too_large"));
            }
            if self.buffer.len() < size + 5 {
                break;
            }
            self.buffer.advance(5);
            messages.push(self.buffer.split_to(size).to_vec());
        }
        Ok(messages)
    }
}

#[derive(Default)]
struct Transcript {
    setup_complete: bool,
    seen_text: bool,
    committed: String,
    interim: String,
}
impl Transcript {
    fn accept(&mut self, message: &[u8]) -> Result<(), String> {
        for field in Fields::new(message) {
            match field? {
                Field::Bytes(1, _) => self.setup_complete = true,
                Field::Bytes(2, content) => {
                    if !self.setup_complete {
                        return Err(error("protocol_error"));
                    }
                    for field in Fields::new(content) {
                        if let Field::Bytes(kind @ (6 | 11), transcription) = field? {
                            if kind == 6 {
                                self.seen_text = true;
                            }
                            let mut text = "";
                            let mut finished = false;
                            for field in Fields::new(transcription) {
                                match field? {
                                    Field::Bytes(1, bytes) => {
                                        text = std::str::from_utf8(bytes)
                                            .map_err(|_| error("protocol_error"))?;
                                    }
                                    Field::Varint(2, value) if kind == 6 => finished = value != 0,
                                    _ => {}
                                }
                            }
                            if self.committed.len() + text.len() + 1 > MAX_TEXT {
                                return Err(error("response_too_large"));
                            }
                            self.interim.clear();
                            if !text.is_empty() {
                                if finished {
                                    if !self.committed.is_empty() {
                                        self.committed.push(' ');
                                    }
                                    self.committed.push_str(text);
                                } else {
                                    self.interim.push_str(text);
                                }
                            }
                        }
                    }
                }
                // Tool execution and generated output are outside dictation.
                Field::Bytes(3..=5, _) => return Err(error("protocol_error")),
                _ => {}
            }
        }
        Ok(())
    }

    fn snapshot(&self) -> String {
        if self.interim.is_empty() {
            return self.committed.clone();
        }
        if self.committed.is_empty() {
            return self.interim.clone();
        }
        format!("{} {}", self.committed, self.interim)
    }

    fn finish(mut self) -> String {
        if !self.interim.is_empty() {
            if !self.committed.is_empty() {
                self.committed.push(' ');
            }
            self.committed.push_str(&self.interim);
        }
        self.committed
    }
}

fn varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 128 {
        out.push(value as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn bytes_field(number: u32, value: &[u8], out: &mut Vec<u8>) {
    varint((number as u64) << 3 | 2, out);
    varint(value.len() as u64, out);
    out.extend_from_slice(value);
}

fn setup(model: &str) -> Vec<u8> {
    let mut setup = Vec::new();
    bytes_field(1, model.as_bytes(), &mut setup);
    // Explicit activities delimit a single dictation. No generated audio is requested.
    let mut realtime = Vec::new();
    bytes_field(6, &[8, 1], &mut realtime);
    bytes_field(5, &realtime, &mut setup);
    bytes_field(8, &[], &mut setup);
    let mut message = Vec::new();
    bytes_field(1, &setup, &mut message);
    message
}

fn realtime_message(number: u32, value: &[u8]) -> Vec<u8> {
    let mut input = Vec::new();
    bytes_field(number, value, &mut input);
    let mut message = Vec::new();
    bytes_field(3, &input, &mut message);
    message
}

fn audio_message(pcm: &[u8]) -> Vec<u8> {
    let mut blob = Vec::new();
    bytes_field(1, b"audio/pcm;rate=16000", &mut blob);
    bytes_field(2, pcm, &mut blob);
    realtime_message(1, &blob)
}

enum Field<'a> {
    Bytes(u32, &'a [u8]),
    Varint(u32, u64),
    Other,
}

struct Fields<'a> {
    input: &'a [u8],
    failed: bool,
}
impl<'a> Fields<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self {
            input,
            failed: false,
        }
    }

    fn varint(&mut self) -> Result<u64, String> {
        let mut value = 0;
        for shift in (0..70).step_by(7) {
            let (&byte, rest) = self
                .input
                .split_first()
                .ok_or_else(|| error("protocol_error"))?;
            self.input = rest;
            if shift == 63 && byte > 1 {
                return Err(error("protocol_error"));
            }
            value |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(error("protocol_error"))
    }

    fn take(&mut self, size: usize) -> Result<&'a [u8], String> {
        if size > self.input.len() {
            return Err(error("protocol_error"));
        }
        let (value, rest) = self.input.split_at(size);
        self.input = rest;
        Ok(value)
    }

    fn field(&mut self) -> Result<Field<'a>, String> {
        let tag = self.varint()?;
        let number = u32::try_from(tag >> 3).map_err(|_| error("protocol_error"))?;
        if number == 0 || number > 536_870_911 {
            return Err(error("protocol_error"));
        }
        match tag & 7 {
            0 => return self.varint().map(|value| Field::Varint(number, value)),
            1 => {
                self.take(8)?;
            }
            2 => {
                let size = usize::try_from(self.varint()?).map_err(|_| error("protocol_error"))?;
                return self.take(size).map(|value| Field::Bytes(number, value));
            }
            5 => {
                self.take(4)?;
            }
            _ => return Err(error("protocol_error")),
        }
        Ok(Field::Other)
    }
}
impl<'a> Iterator for Fields<'a> {
    type Item = Result<Field<'a>, String>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed || self.input.is_empty() {
            return None;
        }
        let result = self.field();
        self.failed = result.is_err();
        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wav(pcm: &[u8]) -> Vec<u8> {
        let mut bytes = b"RIFF".to_vec();
        bytes.extend_from_slice(&(36 + pcm.len() as u32).to_le_bytes());
        bytes.extend_from_slice(
            b"WAVEfmt \x10\0\0\0\x01\0\x01\0\x80\x3e\0\0\0\x7d\0\0\x02\0\x10\0data",
        );
        bytes.extend_from_slice(&(pcm.len() as u32).to_le_bytes());
        bytes.extend_from_slice(pcm);
        bytes
    }

    #[test]
    fn wav_requires_complete_pcm16_mono_16khz_and_bounded_duration() {
        let valid = wav(&[0, 0, 1, 0]);
        assert_eq!(pcm_from_wav(&valid).unwrap(), &[0, 0, 1, 0]);
        let mut stereo = valid.clone();
        stereo[22] = 2;
        assert_eq!(
            pcm_from_wav(&stereo).unwrap_err(),
            error("format_unsupported")
        );
        assert_eq!(
            pcm_from_wav(&valid[..valid.len() - 1]).unwrap_err(),
            error("audio_invalid")
        );
        assert_eq!(
            pcm_from_wav(&wav(&vec![0; MAX_PCM + 2])).unwrap_err(),
            error("audio_invalid")
        );
    }

    #[test]
    fn client_messages_pin_prediction_wire_fields() {
        assert_eq!(
            setup("m"),
            b"\x0a\x0b\x0a\x01m\x2a\x04\x32\x02\x08\x01\x42\x00"
        );
        assert_eq!(realtime_message(5, &[]), b"\x1a\x02\x2a\x00");
        assert_eq!(realtime_message(6, &[]), b"\x1a\x02\x32\x00");
        assert_eq!(
            audio_message(&[1, 2]),
            b"\x1a\x1c\x0a\x1a\x0a\x14audio/pcm;rate=16000\x12\x02\x01\x02"
        );
    }

    #[test]
    fn grpc_frames_allow_fragmentation_and_reject_compression_and_oversize() {
        let mut decoder = FrameDecoder::default();
        assert!(decoder.push(&[0, 0]).unwrap().is_empty());
        assert_eq!(decoder.push(&[0, 0, 2, 10, 0]).unwrap(), vec![vec![10, 0]]);
        assert_eq!(
            FrameDecoder::default().push(&[1, 0, 0, 0, 0]).unwrap_err(),
            error("protocol_error")
        );
        assert_eq!(
            FrameDecoder::default().push(&[0, 0, 16, 0, 1]).unwrap_err(),
            error("response_too_large")
        );
    }

    #[test]
    fn transcript_uses_committed_input_and_skips_undecided_or_generated_text() {
        let mut transcript = Transcript::default();
        transcript.accept(b"\x0a\x00").unwrap();
        transcript.accept(b"\x12\x08\x32\x06\x0a\x04hell").unwrap();
        transcript
            .accept(b"\x12\x0b\x32\x09\x0a\x05hello\x10\x01")
            .unwrap();
        transcript.accept(b"\x12\x09\x5a\x07\x0a\x05wrong").unwrap();
        transcript.accept(b"\x12\x09\x32\x07\x0a\x05world").unwrap();
        assert!(transcript.accept(b"\x12\x04\x32\x02\x0a\xff").is_err());
        assert_eq!(transcript.finish(), "hello world");
        assert!(Fields::new(&[0x80; 11]).next().unwrap().is_err());
    }

    #[test]
    fn grpc_errors_are_classified_without_reflecting_provider_messages() {
        for (status, expected) in [
            ("16", "authentication_required"),
            ("8", "rate_limited"),
            ("7", "access_denied"),
            ("12", "unavailable"),
        ] {
            let mut headers = http::HeaderMap::new();
            headers.insert("grpc-status", status.parse().unwrap());
            assert_eq!(grpc_status(&headers).unwrap_err(), error(expected));
        }
        let mut headers = http::HeaderMap::new();
        headers.insert("grpc-status", "9".parse().unwrap());
        headers.insert(
            "grpc-message",
            "paid%20subscription%20required".parse().unwrap(),
        );
        assert_eq!(
            grpc_status(&headers).unwrap_err(),
            error("billing_required")
        );
    }

    #[tokio::test]
    async fn cancellation_and_language_override_finish_before_network() {
        let signal = AbortSignal::new();
        signal.cancel();
        let audio = base64::engine::general_purpose::STANDARD.encode(wav(&[0, 0]));
        let credentials = || Credentials {
            access_token: "token",
            model: "model",
        };
        let result = transcribe(
            AudioInput {
                audio_base64: &audio,
                mime_type: Some("audio/wav"),
                language: None,
            },
            credentials(),
            signal,
        )
        .await;
        assert_eq!(result.unwrap_err(), error("cancelled"));
        let result = transcribe(
            AudioInput {
                audio_base64: &audio,
                mime_type: Some("audio/wav"),
                language: Some("en"),
            },
            credentials(),
            AbortSignal::new(),
        )
        .await;
        assert_eq!(result.unwrap_err(), error("language_unsupported"));
    }

    fn fixture(
        status: &'static str,
        close_after_first_audio: bool,
    ) -> (
        tokio::io::DuplexStream,
        tokio::sync::oneshot::Receiver<Vec<Vec<u8>>>,
        tokio::task::JoinHandle<()>,
    ) {
        fixture_with_interim(status, close_after_first_audio, false)
    }

    fn fixture_with_interim(
        status: &'static str,
        close_after_first_audio: bool,
        early_interim: bool,
    ) -> (
        tokio::io::DuplexStream,
        tokio::sync::oneshot::Receiver<Vec<Vec<u8>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let (client, server) = tokio::io::duplex(16 * 1024);
        let (report, received) = tokio::sync::oneshot::channel();
        let driver = tokio::spawn(async move {
            let mut connection = h2::server::handshake(server).await.unwrap();
            let (request, mut response) = connection.accept().await.unwrap().unwrap();
            assert_eq!(request.uri().path(), PATH);
            assert!(!request.headers().contains_key("x-goog-user-project"));
            assert!(!request.headers().contains_key("x-goog-request-params"));
            let mut output = response
                .send_response(
                    http::Response::builder()
                        .header("content-type", "application/grpc")
                        .body(())
                        .unwrap(),
                    false,
                )
                .unwrap();
            tokio::spawn(async move {
                let mut input = request.into_body();
                let mut decoder = FrameDecoder::default();
                let mut messages = Vec::new();
                'input: while let Some(data) = input.data().await {
                    let Ok(data) = data else {
                        break;
                    };
                    let parsed = decoder.push(&data).unwrap();
                    input.flow_control().release_capacity(data.len()).unwrap();
                    for message in parsed {
                        if messages.is_empty() {
                            assert_eq!(message, setup("fixture-model"));
                            // A fragmented setup acknowledgement exercises the read boundary.
                            output
                                .send_data(Bytes::from_static(b"\0\0\0"), false)
                                .unwrap();
                            output
                                .send_data(Bytes::from_static(b"\0\x02\x0a\0"), false)
                                .unwrap();
                        }
                        messages.push(message);
                        if early_interim && messages.len() == 3 {
                            let payload = b"\x12\x09\x5a\x07\x0a\x05hello";
                            let mut frame = vec![0];
                            frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
                            frame.extend_from_slice(payload);
                            if output.send_data(Bytes::from(frame), false).is_err() {
                                return;
                            }
                        }
                        if close_after_first_audio && messages.len() == 3 {
                            break 'input;
                        }
                    }
                }
                let _ = report.send(messages);
                if output
                    .send_data(
                        Bytes::from_static(b"\0\0\0\0\x0a\x12\x08\x32\x06\x0a\x04hell"),
                        false,
                    )
                    .is_err()
                {
                    return;
                }
                if output
                    .send_data(
                        Bytes::from_static(b"\0\0\0\0\x0d\x12\x0b\x32\x09\x0a\x05hello\x10\x01"),
                        false,
                    )
                    .is_err()
                {
                    return;
                }
                let mut trailers = http::HeaderMap::new();
                trailers.insert("grpc-status", status.parse().unwrap());
                if output.send_trailers(trailers).is_err() {
                    return;
                }
                // Keep the request half alive until the client observes the
                // response trailers and closes its connection.
                if close_after_first_audio {
                    while let Some(Ok(data)) = input.data().await {
                        input.flow_control().release_capacity(data.len()).unwrap();
                    }
                }
            });
            while connection.accept().await.is_some() {}
        });
        (client, received, driver)
    }

    #[tokio::test]
    async fn live_feed_publishes_text_before_input_closes_and_preserves_tail() {
        let (io, received, driver) = fixture_with_interim("0", false, true);
        let (input, receiver) = tokio::sync::mpsc::channel(4);
        let (ready_tx, ready) = tokio::sync::oneshot::channel();
        let (text_tx, mut texts) = tokio::sync::mpsc::unbounded_channel();
        let work = tokio::spawn(transcribe_feed(
            io,
            AudioFeed::Live(receiver),
            Credentials {
                access_token: "fixture-token",
                model: "fixture-model",
            },
            StreamCallbacks {
                ready: Some(Box::new(move || {
                    let _ = ready_tx.send(());
                })),
                partial: Some(Arc::new(move |text| {
                    let _ = text_tx.send(text);
                    Box::pin(std::future::ready(()))
                })),
                valid: None,
            },
        ));
        tokio::time::timeout(Duration::from_secs(1), ready)
            .await
            .unwrap()
            .unwrap();
        input.send(vec![1, 0]).await.unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), texts.recv())
                .await
                .unwrap()
                .unwrap(),
            "hello"
        );
        assert!(
            !work.is_finished(),
            "interim text must arrive while capture is still open"
        );
        input.send(vec![2, 0]).await.unwrap();
        drop(input);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), work)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            "hello"
        );
        assert_eq!(
            received.await.unwrap(),
            vec![
                setup("fixture-model"),
                realtime_message(5, &[]),
                audio_message(&[1, 0]),
                audio_message(&[2, 0]),
                realtime_message(6, &[])
            ]
        );
        driver.abort();
    }

    #[tokio::test]
    async fn live_feed_checks_account_identity_before_sending_each_frame() {
        let (io, received, driver) = fixture_with_interim("0", false, false);
        let (input, receiver) = tokio::sync::mpsc::channel(4);
        let (ready_tx, ready) = tokio::sync::oneshot::channel();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let check = valid.clone();
        let work = tokio::spawn(transcribe_feed(
            io,
            AudioFeed::Live(receiver),
            Credentials {
                access_token: "fixture-token",
                model: "fixture-model",
            },
            StreamCallbacks {
                ready: Some(Box::new(move || {
                    let _ = ready_tx.send(());
                })),
                partial: None,
                valid: Some(Arc::new(move || {
                    check.load(std::sync::atomic::Ordering::SeqCst)
                })),
            },
        ));
        ready.await.unwrap();
        valid.store(false, std::sync::atomic::Ordering::SeqCst);
        input.send(vec![1, 0]).await.unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), work)
                .await
                .unwrap()
                .unwrap()
                .unwrap_err(),
            error("credential_changed")
        );
        assert_eq!(
            received.await.unwrap(),
            vec![setup("fixture-model"), realtime_message(5, &[])]
        );
        driver.abort();
    }

    #[tokio::test]
    async fn live_feed_drop_while_waiting_for_audio_closes_the_transport() {
        let (io, _received, driver) = fixture("0", false);
        let (_input, receiver) = tokio::sync::mpsc::channel(4);
        let (ready_tx, ready) = tokio::sync::oneshot::channel();
        let work = tokio::spawn(transcribe_feed(
            io,
            AudioFeed::Live(receiver),
            Credentials {
                access_token: "fixture-token",
                model: "fixture-model",
            },
            StreamCallbacks {
                ready: Some(Box::new(move || {
                    let _ = ready_tx.send(());
                })),
                ..StreamCallbacks::default()
            },
        ));
        ready.await.unwrap();
        work.abort();
        assert!(work.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(1), driver)
            .await
            .unwrap()
            .unwrap();
    }

    #[test]
    fn interim_snapshots_replace_previous_words_and_final_text_corrects_the_snapshot() {
        let mut transcript = Transcript::default();
        transcript.accept(b"\x0a\x00").unwrap();
        let message = |field: u32, text: &str| {
            let mut words = Vec::new();
            bytes_field(1, text.as_bytes(), &mut words);
            let mut content = Vec::new();
            bytes_field(field, &words, &mut content);
            let mut root = Vec::new();
            bytes_field(2, &content, &mut root);
            root
        };
        transcript
            .accept(&message(11, "The word words should appear"))
            .unwrap();
        assert!(
            !transcript.seen_text,
            "an interim snapshot is not an acknowledged final transcript"
        );
        assert_eq!(transcript.snapshot(), "The word words should appear");
        transcript
            .accept(&message(11, "The word should appear before I"))
            .unwrap();
        assert_eq!(transcript.snapshot(), "The word should appear before I");
        transcript
            .accept(&message(6, "The words should appear before I"))
            .unwrap();
        assert_eq!(transcript.snapshot(), "The words should appear before I");
        assert_eq!(transcript.finish(), "The words should appear before I");
    }

    #[test]
    fn an_empty_final_replaces_the_previous_interim_snapshot() {
        let mut transcript = Transcript::default();
        transcript.accept(b"\x0a\x00").unwrap();
        transcript.accept(b"\x12\x08\x32\x06\x0a\x04hell").unwrap();
        assert_eq!(transcript.snapshot(), "hell");
        transcript
            .accept(b"\x12\x06\x32\x04\x0a\x00\x10\x01")
            .unwrap();
        assert_eq!(transcript.snapshot(), "");
        assert_eq!(transcript.finish(), "");
    }

    #[tokio::test]
    async fn bidi_fixture_observes_setup_activity_pcm_close_and_final_transcript() {
        let (io, received, driver) = fixture("0", false);
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            transcribe_io(
                io,
                &[1, 0, 2, 0],
                Credentials {
                    access_token: "fixture-token",
                    model: "fixture-model",
                },
                Duration::from_millis(1),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result, "hello");
        let messages = received.await.unwrap();
        assert_eq!(
            messages,
            vec![
                setup("fixture-model"),
                realtime_message(5, &[]),
                audio_message(&[1, 0, 2, 0]),
                realtime_message(6, &[])
            ]
        );
        driver.abort();
    }

    #[tokio::test]
    async fn bidi_fixture_preserves_trailer_failure_after_transcript() {
        let (io, received, driver) = fixture("8", false);
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            transcribe_io(
                io,
                &[0, 0],
                Credentials {
                    access_token: "fixture-token",
                    model: "fixture-model",
                },
                Duration::from_millis(1),
            ),
        )
        .await
        .unwrap();
        assert_eq!(result.unwrap_err(), error("rate_limited"));
        received.await.unwrap();
        driver.abort();
    }

    #[tokio::test]
    async fn bidi_fixture_rejects_early_success_without_masking_provider_failure() {
        let pcm = vec![0; PCM_CHUNK * 2];
        for (status, expected) in [("0", "incomplete"), ("8", "rate_limited")] {
            let (io, received, driver) = fixture(status, true);
            let result = tokio::time::timeout(
                Duration::from_secs(3),
                transcribe_io(
                    io,
                    &pcm,
                    Credentials {
                        access_token: "fixture-token",
                        model: "fixture-model",
                    },
                    Duration::from_secs(60),
                ),
            )
            .await
            .unwrap();
            assert_eq!(result.unwrap_err(), error(expected));
            assert_eq!(
                received.await.unwrap(),
                vec![
                    setup("fixture-model"),
                    realtime_message(5, &[]),
                    audio_message(&pcm[..PCM_CHUNK]),
                ]
            );
            driver.abort();
        }
    }
}
