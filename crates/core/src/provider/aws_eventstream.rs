//! AWS event-stream (`application/vnd.amazon.eventstream`) frame decoding.
//!
//! Bedrock's `invoke-with-response-stream` returns a binary framing rather than
//! SSE: each message is `[prelude][headers][payload][crc]` with CRC32 integrity
//! on both the prelude and the whole message. This decoder buffers bytes and
//! yields complete messages (headers + payload); the Bedrock transport then
//! unwraps each payload's `{"bytes": ...}` envelope into an Anthropic event.

use std::collections::HashMap;

/// A decoded event-stream message: its string headers and raw payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventStreamMessage {
    /// String-valued headers (the only value type Bedrock emits here), e.g.
    /// `:message-type`, `:event-type`, `:content-type`, `:exception-type`.
    pub headers: HashMap<String, String>,
    pub payload: Vec<u8>,
}

impl EventStreamMessage {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(String::as_str)
    }

    /// Whether this message is an exception/error frame rather than an event.
    pub fn is_exception(&self) -> bool {
        matches!(self.header(":message-type"), Some("exception" | "error"))
    }
}

/// Incremental decoder: push bytes, pull complete messages.
#[derive(Default)]
pub struct AwsEventStreamDecoder {
    buffer: Vec<u8>,
}

fn be_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

impl AwsEventStreamDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }

    /// Pull the next complete message, or `None` when the buffer holds only a
    /// partial frame. Returns an error on a CRC mismatch or malformed prelude.
    pub fn next_message(&mut self) -> Result<Option<EventStreamMessage>, String> {
        if self.buffer.len() < 12 {
            return Ok(None);
        }
        let total_len = be_u32(&self.buffer[0..4]) as usize;
        let headers_len = be_u32(&self.buffer[4..8]) as usize;
        // A message is at minimum the two length words, two CRCs, and headers.
        if total_len < 16 || total_len < 16 + headers_len {
            return Err(format!(
                "event-stream frame length {total_len} is malformed"
            ));
        }
        if self.buffer.len() < total_len {
            return Ok(None);
        }

        let prelude_crc = be_u32(&self.buffer[8..12]);
        if crc32fast::hash(&self.buffer[0..8]) != prelude_crc {
            return Err("event-stream prelude CRC mismatch".to_string());
        }
        let message_crc = be_u32(&self.buffer[total_len - 4..total_len]);
        if crc32fast::hash(&self.buffer[0..total_len - 4]) != message_crc {
            return Err("event-stream message CRC mismatch".to_string());
        }

        let headers_start = 12;
        let headers_end = headers_start + headers_len;
        let headers = parse_headers(&self.buffer[headers_start..headers_end])?;
        let payload = self.buffer[headers_end..total_len - 4].to_vec();
        self.buffer.drain(0..total_len);
        Ok(Some(EventStreamMessage { headers, payload }))
    }
}

/// Parse the header block, keeping string-valued headers and skipping other
/// value types by their fixed/prefixed widths so the cursor stays aligned.
fn parse_headers(mut bytes: &[u8]) -> Result<HashMap<String, String>, String> {
    let mut headers = HashMap::new();
    while !bytes.is_empty() {
        let name_len = *bytes.first().ok_or("truncated header name length")? as usize;
        bytes = &bytes[1..];
        if bytes.len() < name_len + 1 {
            return Err("truncated header name".to_string());
        }
        let name = String::from_utf8_lossy(&bytes[..name_len]).into_owned();
        bytes = &bytes[name_len..];
        let value_type = bytes[0];
        bytes = &bytes[1..];
        // Widths per the AWS event-stream header value spec.
        let advance = match value_type {
            0 | 1 => 0, // bool true / false
            2 => 1,     // byte
            3 => 2,     // short
            4 => 4,     // int
            5 | 8 => 8, // long / timestamp
            9 => 16,    // uuid
            6 | 7 => {
                // byte array / string: 2-byte big-endian length prefix.
                if bytes.len() < 2 {
                    return Err("truncated header value length".to_string());
                }
                let len = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
                bytes = &bytes[2..];
                if bytes.len() < len {
                    return Err("truncated header value".to_string());
                }
                if value_type == 7 {
                    headers.insert(name, String::from_utf8_lossy(&bytes[..len]).into_owned());
                }
                len
            }
            other => return Err(format!("unsupported event-stream header type {other}")),
        };
        if bytes.len() < advance {
            return Err("truncated header value".to_string());
        }
        bytes = &bytes[advance..];
    }
    Ok(headers)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a well-formed frame with one `:message-type` string header and the
    /// given payload, computing both CRCs so the decoder accepts it.
    fn frame(message_type: &str, payload: &[u8]) -> Vec<u8> {
        let name = ":message-type";
        let mut headers = Vec::new();
        headers.push(name.len() as u8);
        headers.extend_from_slice(name.as_bytes());
        headers.push(7); // string
        headers.extend_from_slice(&(message_type.len() as u16).to_be_bytes());
        headers.extend_from_slice(message_type.as_bytes());

        let total_len = 4 + 4 + 4 + headers.len() + payload.len() + 4;
        let mut msg = Vec::new();
        msg.extend_from_slice(&(total_len as u32).to_be_bytes());
        msg.extend_from_slice(&(headers.len() as u32).to_be_bytes());
        msg.extend_from_slice(&crc32fast::hash(&msg[0..8]).to_be_bytes());
        msg.extend_from_slice(&headers);
        msg.extend_from_slice(payload);
        msg.extend_from_slice(&crc32fast::hash(&msg[0..total_len - 4]).to_be_bytes());
        msg
    }

    #[test]
    fn decodes_a_single_event_frame_with_headers_and_payload() {
        let mut decoder = AwsEventStreamDecoder::new();
        decoder.push(&frame("event", br#"{"bytes":"abc"}"#));
        let msg = decoder.next_message().unwrap().expect("one message");
        assert_eq!(msg.header(":message-type"), Some("event"));
        assert!(!msg.is_exception());
        assert_eq!(msg.payload, br#"{"bytes":"abc"}"#);
        assert!(decoder.next_message().unwrap().is_none());
    }

    #[test]
    fn buffers_partial_frames_until_complete() {
        let full = frame("event", br#"{"x":1}"#);
        let (head, tail) = full.split_at(10);
        let mut decoder = AwsEventStreamDecoder::new();
        decoder.push(head);
        assert!(decoder.next_message().unwrap().is_none());
        decoder.push(tail);
        let msg = decoder.next_message().unwrap().expect("message after tail");
        assert_eq!(msg.payload, br#"{"x":1}"#);
    }

    #[test]
    fn decodes_two_concatenated_frames_in_order() {
        let mut decoder = AwsEventStreamDecoder::new();
        let mut stream = frame("event", b"one");
        stream.extend_from_slice(&frame("event", b"two"));
        decoder.push(&stream);
        assert_eq!(decoder.next_message().unwrap().unwrap().payload, b"one");
        assert_eq!(decoder.next_message().unwrap().unwrap().payload, b"two");
        assert!(decoder.next_message().unwrap().is_none());
    }

    #[test]
    fn flags_exception_frames() {
        let mut decoder = AwsEventStreamDecoder::new();
        decoder.push(&frame("exception", br#"{"message":"throttled"}"#));
        let msg = decoder.next_message().unwrap().unwrap();
        assert!(msg.is_exception());
    }

    #[test]
    fn rejects_a_corrupted_message_crc() {
        let mut bad = frame("event", b"payload");
        let last = bad.len() - 1;
        bad[last] ^= 0xff; // corrupt the trailing message CRC
        let mut decoder = AwsEventStreamDecoder::new();
        decoder.push(&bad);
        assert!(decoder.next_message().is_err());
    }
}
