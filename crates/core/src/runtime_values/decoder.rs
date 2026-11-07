use std::fmt;

use encoding_rs::{CoderResult, Decoder, UTF_8};

use super::parser::RuntimeJsonError;
use super::{runtime_json, RuntimeJson, RuntimeString};

/// One complete Node `Buffer.toString("utf8")` decode.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NodeBufferUtf8;

impl NodeBufferUtf8 {
    pub fn decode(bytes: &[u8]) -> RuntimeString {
        let (value, _) = UTF_8.decode_without_bom_handling(bytes);
        RuntimeString::from_utf8(&value)
    }
}

/// One independently delivered Node Buffer/data-event decode.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NodeChunkUtf8;

impl NodeChunkUtf8 {
    pub fn decode_chunk(bytes: &[u8]) -> RuntimeString {
        NodeBufferUtf8::decode(bytes)
    }
}

/// Stateful Node `StringDecoder("utf8")` profile.
#[derive(Debug, Default)]
pub struct NodeStringDecoderUtf8 {
    incomplete: Vec<u8>,
}

impl NodeStringDecoderUtf8 {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn feed(&mut self, bytes: &[u8]) -> RuntimeString {
        let mut input = std::mem::take(&mut self.incomplete);
        input.extend_from_slice(bytes);
        if let Some(start) = incomplete_utf8_start(&input) {
            self.incomplete = input.split_off(start);
        }
        NodeBufferUtf8::decode(&input)
    }

    pub fn finish(self) -> RuntimeString {
        NodeBufferUtf8::decode(&self.incomplete)
    }
}

/// Stateful WHATWG UTF-8 decoder with one initial BOM consumed.
pub struct WhatwgUtf8 {
    decoder: Decoder,
}

impl fmt::Debug for WhatwgUtf8 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("WhatwgUtf8").finish_non_exhaustive()
    }
}

impl Default for WhatwgUtf8 {
    fn default() -> Self {
        Self::new()
    }
}

impl WhatwgUtf8 {
    pub fn new() -> Self {
        Self {
            decoder: UTF_8.new_decoder_with_bom_removal(),
        }
    }

    pub fn feed(&mut self, bytes: &[u8]) -> RuntimeString {
        decode_stream(&mut self.decoder, bytes, false)
    }

    pub fn finish(mut self) -> RuntimeString {
        decode_stream(&mut self.decoder, &[], true)
    }
}

/// Explicit BOM handling for a strict scalar UTF-8 boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Utf8BomPolicy {
    Forbid,
    Retain,
    Consume,
}

/// Stable strict UTF-8 failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Utf8DecodeErrorKind {
    BomForbidden,
    InvalidUtf8,
}

/// Strict scalar UTF-8 failure at a byte offset.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Utf8DecodeError {
    offset: usize,
    kind: Utf8DecodeErrorKind,
}

impl Utf8DecodeError {
    pub fn offset(self) -> usize {
        self.offset
    }

    pub fn kind(self) -> Utf8DecodeErrorKind {
        self.kind
    }
}

impl fmt::Display for Utf8DecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "strict UTF-8 {:?} at byte offset {}",
            self.kind, self.offset
        )
    }
}

impl std::error::Error for Utf8DecodeError {}

/// Strict scalar-only UTF-8 profile.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StrictScalarUtf8;

impl StrictScalarUtf8 {
    pub fn decode(
        bytes: &[u8],
        bom_policy: Utf8BomPolicy,
    ) -> Result<RuntimeString, Utf8DecodeError> {
        const UTF8_BOM: &[u8] = b"\xef\xbb\xbf";
        let (bytes, offset_base) = match bom_policy {
            Utf8BomPolicy::Forbid if bytes.starts_with(UTF8_BOM) => {
                return Err(Utf8DecodeError {
                    offset: 0,
                    kind: Utf8DecodeErrorKind::BomForbidden,
                });
            }
            Utf8BomPolicy::Consume => match bytes.strip_prefix(UTF8_BOM) {
                Some(bytes) => (bytes, UTF8_BOM.len()),
                None => (bytes, 0),
            },
            Utf8BomPolicy::Forbid | Utf8BomPolicy::Retain => (bytes, 0),
        };
        std::str::from_utf8(bytes)
            .map(RuntimeString::from_utf8)
            .map_err(|error| Utf8DecodeError {
                offset: offset_base + error.valid_up_to(),
                kind: Utf8DecodeErrorKind::InvalidUtf8,
            })
    }
}

/// A complete browser/runtime frame was not exact canonical runtime JSON.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CanonicalRuntimeJsonError {
    Utf8(Utf8DecodeError),
    Json(RuntimeJsonError),
    NonCanonical,
}

impl fmt::Display for CanonicalRuntimeJsonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Utf8(error) => error.fmt(formatter),
            Self::Json(error) => error.fmt(formatter),
            Self::NonCanonical => formatter.write_str("non-canonical runtime JSON bytes"),
        }
    }
}

impl std::error::Error for CanonicalRuntimeJsonError {}

/// Strict, BOM-free, compact and byte-canonical runtime JSON frame.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CanonicalRuntimeJsonUtf8;

impl CanonicalRuntimeJsonUtf8 {
    pub fn decode(bytes: &[u8]) -> Result<RuntimeJson, CanonicalRuntimeJsonError> {
        let text = StrictScalarUtf8::decode(bytes, Utf8BomPolicy::Forbid)
            .map_err(CanonicalRuntimeJsonError::Utf8)?;
        let value = runtime_json::parse(&text).map_err(CanonicalRuntimeJsonError::Json)?;
        if runtime_json::stringify(&value).to_node_utf8() != bytes {
            return Err(CanonicalRuntimeJsonError::NonCanonical);
        }
        Ok(value)
    }
}

fn decode_stream(decoder: &mut Decoder, bytes: &[u8], last: bool) -> RuntimeString {
    let capacity = decoder
        .max_utf8_buffer_length(bytes.len())
        .expect("UTF-8 stream output size fits in usize");
    let mut output = String::with_capacity(capacity);
    let (result, read, _) = decoder.decode_to_string(bytes, &mut output, last);
    assert_eq!(result, CoderResult::InputEmpty, "sized UTF-8 output buffer");
    assert_eq!(read, bytes.len(), "UTF-8 decoder consumed the input");
    RuntimeString::from_utf8(&output)
}

fn incomplete_utf8_start(bytes: &[u8]) -> Option<usize> {
    let first_candidate = bytes.len().saturating_sub(3);
    for start in (first_candidate..bytes.len()).rev() {
        let expected = utf8_sequence_length(bytes[start]);
        if expected == 0 {
            continue;
        }
        let available = bytes.len() - start;
        if available < expected && bytes[start + 1..].iter().all(|byte| byte & 0xc0 == 0x80) {
            return Some(start);
        }
    }
    None
}

fn utf8_sequence_length(byte: u8) -> usize {
    if byte >> 5 == 0b110 {
        2
    } else if byte >> 4 == 0b1110 {
        3
    } else if byte >> 3 == 0b11110 {
        4
    } else {
        0
    }
}
