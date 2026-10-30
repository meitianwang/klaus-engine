//! Framing for the host connection: exact JSON payloads behind a four-byte
//! little-endian length prefix.
//!
//! The length prefix (rather than a delimiter) is what lets a payload contain
//! any byte sequence, including the lone surrogates the runtime preserves. A
//! frame larger than [`MAX_FRAME_BYTES`] is refused at BOTH ends, so one
//! oversized message fails its own operation instead of exhausting the peer.

use std::io;
use std::mem::size_of;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::runtime_values::{runtime_codec, RuntimeDecode, RuntimeEncode, StrictScalarUtf8};

/// Maximum JSON payload accepted for one frame.
pub const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

/// A serialized frame that has already passed the payload size limit.
///
/// Encoding before a frame enters an I/O queue keeps the writer's failure modes
/// separate: an unencodable message is rejected at its own call site rather
/// than while draining the queue.
#[derive(Clone, Debug)]
pub struct EncodedFrame {
    payload: Vec<u8>,
}

impl EncodedFrame {
    pub fn encode<T: RuntimeEncode + ?Sized>(message: &T) -> io::Result<Self> {
        let payload = runtime_codec::encode_bytes(message)
            .map_err(|error| invalid_data(format!("failed to encode host frame: {error}")))?;
        if payload.len() > MAX_FRAME_BYTES {
            return Err(invalid_data(format!(
                "host frame length {} exceeds {MAX_FRAME_BYTES} bytes",
                payload.len()
            )));
        }
        Ok(Self { payload })
    }

    /// The complete length-prefixed representation of this frame.
    pub fn into_framed_bytes(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(size_of::<u32>() + self.payload.len());
        bytes.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&self.payload);
        bytes
    }

    /// Decode exactly one complete length-prefixed frame: trailing bytes are a
    /// framing error, not a second message.
    pub fn decode_framed<T: RuntimeDecode>(bytes: &[u8]) -> io::Result<T> {
        let length_bytes: [u8; size_of::<u32>()] = bytes
            .get(..size_of::<u32>())
            .and_then(|length_bytes| length_bytes.try_into().ok())
            .ok_or_else(|| invalid_data("host frame is missing its length prefix"))?;
        let length = u32::from_le_bytes(length_bytes) as usize;
        if length > MAX_FRAME_BYTES {
            return Err(invalid_data(format!(
                "host frame length {length} exceeds {MAX_FRAME_BYTES} bytes"
            )));
        }

        let payload = &bytes[size_of::<u32>()..];
        if payload.len() != length {
            return Err(invalid_data(format!(
                "host frame declares {length} payload bytes but contains {}",
                payload.len()
            )));
        }

        decode_payload(payload)
    }
}

fn decode_payload<T: RuntimeDecode>(payload: &[u8]) -> io::Result<T> {
    let text = StrictScalarUtf8::decode(payload, crate::runtime_values::Utf8BomPolicy::Forbid)
        .map_err(|error| invalid_data(format!("failed to decode host frame UTF-8: {error}")))?;
    runtime_codec::decode_text(&text)
        .map_err(|error| invalid_data(format!("failed to decode host frame: {error}")))
}

/// Reads length-prefixed frames off a byte stream.
pub struct FramedReader<R> {
    reader: R,
}

impl<R> FramedReader<R>
where
    R: AsyncRead + Unpin,
{
    pub fn new(reader: R) -> Self {
        Self { reader }
    }

    /// Read the next frame. `None` means a clean EOF AT a frame boundary; an
    /// EOF anywhere inside a frame is an error, so a truncated stream is never
    /// mistaken for an orderly close.
    pub async fn read<T: RuntimeDecode>(&mut self) -> io::Result<Option<T>> {
        let mut length_bytes = [0_u8; size_of::<u32>()];
        if self.reader.read(&mut length_bytes[..1]).await? == 0 {
            return Ok(None);
        }
        self.reader.read_exact(&mut length_bytes[1..]).await?;

        let length = u32::from_le_bytes(length_bytes) as usize;
        if length > MAX_FRAME_BYTES {
            return Err(invalid_data(format!(
                "host frame length {length} exceeds {MAX_FRAME_BYTES} bytes"
            )));
        }

        let mut payload = vec![0; length];
        self.reader.read_exact(&mut payload).await?;
        decode_payload(&payload).map(Some)
    }
}

/// Writes length-prefixed frames onto a byte stream.
pub struct FramedWriter<W> {
    writer: W,
}

impl<W> FramedWriter<W>
where
    W: AsyncWrite + Unpin,
{
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    /// Encode, write, and flush one frame.
    pub async fn write<T: RuntimeEncode + ?Sized>(&mut self, message: &T) -> io::Result<()> {
        self.write_frame(&EncodedFrame::encode(message)?).await
    }

    /// Write and flush a frame encoded before it entered an I/O queue.
    pub async fn write_frame(&mut self, frame: &EncodedFrame) -> io::Result<()> {
        let length = u32::try_from(frame.payload.len())
            .map_err(|_| invalid_data("host frame length exceeds u32"))?;

        self.writer.write_all(&length.to_le_bytes()).await?;
        self.writer.write_all(&frame.payload).await?;
        self.writer.flush().await
    }
}
