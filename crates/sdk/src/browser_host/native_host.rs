use std::collections::VecDeque;

/// Chrome native-messaging rejects a single message larger than one MiB.
pub const MAX_CHROME_NATIVE_MESSAGE_BYTES: usize = 1024 * 1024;

/// Stable native-host framing failures. The process-facing binary maps these
/// to the frozen browser error messages and shutdown status during cutover.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChromeNativeFramingError {
    MessageTooLarge,
    ReaderClosed,
}

/// One physical input chunk may contain valid complete messages followed by a
/// terminal framing error. Callers must handle `frames` in order before acting
/// on `terminal_error`, matching the locked JavaScript drain loop.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChromeNativeReadBatch {
    pub frames: Vec<Vec<u8>>,
    pub terminal_error: Option<ChromeNativeFramingError>,
}

/// Incremental little-endian Chrome native-message reader. It never consumes
/// bytes following a complete frame into that frame and permanently closes on
/// an oversized length, matching the native-host shutdown boundary.
#[derive(Debug, Default)]
pub struct ChromeNativeMessageReader {
    buffered: Vec<u8>,
    closed: bool,
}

impl ChromeNativeMessageReader {
    /// Append one transport chunk and return every complete payload in physical
    /// arrival order. Incomplete header/payload bytes remain owned by the
    /// reader for the next call.
    pub fn feed(&mut self, input: &[u8]) -> ChromeNativeReadBatch {
        if self.closed {
            return ChromeNativeReadBatch {
                frames: Vec::new(),
                terminal_error: Some(ChromeNativeFramingError::ReaderClosed),
            };
        }
        self.buffered.extend_from_slice(input);
        let mut frames = Vec::new();
        loop {
            if self.buffered.len() < 4 {
                break;
            }
            let payload_len = u32::from_le_bytes(
                self.buffered[..4]
                    .try_into()
                    .expect("native-message header has four bytes"),
            ) as usize;
            if payload_len > MAX_CHROME_NATIVE_MESSAGE_BYTES {
                self.closed = true;
                self.buffered.clear();
                return ChromeNativeReadBatch {
                    frames,
                    terminal_error: Some(ChromeNativeFramingError::MessageTooLarge),
                };
            }
            let frame_len = 4 + payload_len;
            if self.buffered.len() < frame_len {
                break;
            }
            frames.push(self.buffered[4..frame_len].to_vec());
            self.buffered.drain(..frame_len);
        }
        ChromeNativeReadBatch {
            frames,
            terminal_error: None,
        }
    }

    pub fn buffered_len(&self) -> usize {
        self.buffered.len()
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn close(&mut self) {
        self.closed = true;
        self.buffered.clear();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeHostWriterPhase {
    Open,
    Exiting,
}

/// Result of one nonblocking OS write attempt. `Interrupted` retries without
/// advancing; `WouldBlock` arms exactly one writable notification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeHostWriteResult {
    Written(usize),
    Interrupted,
    WouldBlock,
    Fatal,
}

/// What the host loop must do after applying a write result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeHostWriteAction {
    Continue,
    Retry,
    WaitWritable,
    Exit,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NativeHostWriteRequest {
    bytes: Vec<u8>,
    offset: usize,
}

impl NativeHostWriteRequest {
    fn new(bytes: Vec<u8>) -> Self {
        Self { bytes, offset: 0 }
    }

    fn remaining(&self) -> &[u8] {
        &self.bytes[self.offset..]
    }
}

/// Closed `Open -> Exiting` stdout writer used by the native-host binary.
/// Every Chrome message admits exactly two queue records (header then payload),
/// and neither partial writes nor backpressure coalesce them.
#[derive(Debug)]
pub struct NativeHostStdoutWriter {
    phase: NativeHostWriterPhase,
    queue: VecDeque<NativeHostWriteRequest>,
    writable_notification_armed: bool,
}

impl Default for NativeHostStdoutWriter {
    fn default() -> Self {
        Self {
            phase: NativeHostWriterPhase::Open,
            queue: VecDeque::new(),
            writable_notification_armed: false,
        }
    }
}

impl NativeHostStdoutWriter {
    /// Queue immutable little-endian header and payload records without waiting
    /// for drain. Outbound payload size follows the u32 Chrome wire limit.
    pub fn enqueue_message(&mut self, payload: &[u8]) -> Result<(), ChromeNativeFramingError> {
        if self.phase != NativeHostWriterPhase::Open {
            return Err(ChromeNativeFramingError::ReaderClosed);
        }
        let length =
            u32::try_from(payload.len()).map_err(|_| ChromeNativeFramingError::MessageTooLarge)?;
        self.queue
            .push_back(NativeHostWriteRequest::new(length.to_le_bytes().to_vec()));
        self.queue
            .push_back(NativeHostWriteRequest::new(payload.to_vec()));
        Ok(())
    }

    pub fn phase(&self) -> NativeHostWriterPhase {
        self.phase
    }

    pub fn queued_requests(&self) -> usize {
        self.queue.len()
    }

    pub fn current_bytes(&self) -> Option<&[u8]> {
        self.queue.front().map(NativeHostWriteRequest::remaining)
    }

    /// Apply exactly one OS write result to the current immutable request.
    pub fn apply_write_result(&mut self, result: NativeHostWriteResult) -> NativeHostWriteAction {
        if self.phase != NativeHostWriterPhase::Open {
            return NativeHostWriteAction::Exit;
        }
        self.discard_completed_empty_requests();
        let Some(current) = self.queue.front_mut() else {
            return NativeHostWriteAction::Continue;
        };
        match result {
            NativeHostWriteResult::Written(written)
                if written > 0 && written <= current.remaining().len() =>
            {
                current.offset += written;
                if current.remaining().is_empty() {
                    self.queue.pop_front();
                    self.discard_completed_empty_requests();
                }
                NativeHostWriteAction::Continue
            }
            NativeHostWriteResult::Interrupted => NativeHostWriteAction::Retry,
            NativeHostWriteResult::WouldBlock => {
                self.writable_notification_armed = true;
                NativeHostWriteAction::WaitWritable
            }
            NativeHostWriteResult::Fatal | NativeHostWriteResult::Written(_) => {
                self.shutdown();
                NativeHostWriteAction::Exit
            }
        }
    }

    /// Consume the one armed readiness notification. Spurious notifications do
    /// not create a second writer or alter the current request.
    pub fn writable_notification(&mut self) -> bool {
        std::mem::take(&mut self.writable_notification_armed)
    }

    /// Exit immediately: cancel readiness and discard all unwritten bytes.
    pub fn shutdown(&mut self) {
        self.phase = NativeHostWriterPhase::Exiting;
        self.writable_notification_armed = false;
        self.queue.clear();
    }

    fn discard_completed_empty_requests(&mut self) {
        while self
            .queue
            .front()
            .is_some_and(|request| request.remaining().is_empty())
        {
            self.queue.pop_front();
        }
    }
}

/// Connection-local bridge write queue. A fatal write drops only this
/// connection's unsent records and lets the owner enter its reconnect path.
#[derive(Debug, Default)]
pub struct BridgeWriteQueue {
    queue: VecDeque<NativeHostWriteRequest>,
    writable_notification_armed: bool,
    failed: bool,
}

impl BridgeWriteQueue {
    pub fn enqueue(&mut self, bytes: &[u8]) -> bool {
        if self.failed {
            return false;
        }
        self.queue
            .push_back(NativeHostWriteRequest::new(bytes.to_vec()));
        true
    }

    pub fn current_bytes(&self) -> Option<&[u8]> {
        self.queue.front().map(NativeHostWriteRequest::remaining)
    }

    pub fn queued_requests(&self) -> usize {
        self.queue.len()
    }

    pub fn apply_write_result(&mut self, result: NativeHostWriteResult) -> NativeHostWriteAction {
        if self.failed {
            return NativeHostWriteAction::Exit;
        }
        let Some(current) = self.queue.front_mut() else {
            return NativeHostWriteAction::Continue;
        };
        match result {
            NativeHostWriteResult::Written(written)
                if written > 0 && written <= current.remaining().len() =>
            {
                current.offset += written;
                if current.remaining().is_empty() {
                    self.queue.pop_front();
                }
                NativeHostWriteAction::Continue
            }
            NativeHostWriteResult::Interrupted => NativeHostWriteAction::Retry,
            NativeHostWriteResult::WouldBlock => {
                self.writable_notification_armed = true;
                NativeHostWriteAction::WaitWritable
            }
            NativeHostWriteResult::Fatal | NativeHostWriteResult::Written(_) => {
                self.fail();
                NativeHostWriteAction::Exit
            }
        }
    }

    pub fn writable_notification(&mut self) -> bool {
        std::mem::take(&mut self.writable_notification_armed)
    }

    pub fn fail(&mut self) {
        self.failed = true;
        self.writable_notification_armed = false;
        self.queue.clear();
    }

    pub fn is_failed(&self) -> bool {
        self.failed
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WebSocketFrame {
    pub opcode: u8,
    pub payload: Vec<u8>,
    pub consumed: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WebSocketFrameError {
    LengthOverflow,
}

/// Parse the first complete WebSocket frame using the legacy native-host's
/// permissive masked/unmasked behavior. `Ok(None)` means more bytes are needed.
pub fn read_websocket_frame(bytes: &[u8]) -> Result<Option<WebSocketFrame>, WebSocketFrameError> {
    if bytes.len() < 2 {
        return Ok(None);
    }
    let opcode = bytes[0] & 0x0f;
    let masked = bytes[1] & 0x80 != 0;
    let mut length = u64::from(bytes[1] & 0x7f);
    let mut offset = 2_usize;
    if length == 126 {
        if bytes.len() < offset + 2 {
            return Ok(None);
        }
        length = u64::from(u16::from_be_bytes(
            bytes[offset..offset + 2]
                .try_into()
                .expect("extended websocket length has two bytes"),
        ));
        offset += 2;
    } else if length == 127 {
        if bytes.len() < offset + 8 {
            return Ok(None);
        }
        length = u64::from_be_bytes(
            bytes[offset..offset + 8]
                .try_into()
                .expect("extended websocket length has eight bytes"),
        );
        offset += 8;
    }
    let mask = if masked {
        if bytes.len() < offset + 4 {
            return Ok(None);
        }
        let mask: [u8; 4] = bytes[offset..offset + 4]
            .try_into()
            .expect("websocket mask has four bytes");
        offset += 4;
        Some(mask)
    } else {
        None
    };
    let Ok(length) = usize::try_from(length) else {
        return Ok(None);
    };
    let Some(end) = offset.checked_add(length) else {
        return Ok(None);
    };
    if bytes.len() < end {
        return Ok(None);
    }
    let mut payload = bytes[offset..end].to_vec();
    if let Some(mask) = mask {
        for (index, byte) in payload.iter_mut().enumerate() {
            *byte ^= mask[index % 4];
        }
    }
    Ok(Some(WebSocketFrame {
        opcode,
        payload,
        consumed: end,
    }))
}

/// Encode one final masked text frame with caller-supplied CSPRNG mask bytes.
pub fn write_masked_websocket_text(payload: &[u8], mask: [u8; 4]) -> Vec<u8> {
    let length = payload.len() as u64;
    let mut frame = Vec::with_capacity(payload.len().saturating_add(14));
    frame.push(0x81);
    if length < 126 {
        frame.push(0x80 | length as u8);
    } else if length < 65_536 {
        frame.push(0x80 | 126);
        frame.extend_from_slice(&(length as u16).to_be_bytes());
    } else {
        frame.push(0x80 | 127);
        frame.extend_from_slice(&length.to_be_bytes());
    }
    frame.extend_from_slice(&mask);
    frame.extend(
        payload
            .iter()
            .enumerate()
            .map(|(index, byte)| byte ^ mask[index % 4]),
    );
    frame
}

/// Exact `Math.min(Math.round(delay * 1.8), 10000)` reconnect transition.
pub fn next_native_host_reconnect_delay_ms(current_ms: u64) -> u64 {
    ((current_ms as f64 * 1.8).round() as u64).min(10_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chrome_reader_handles_one_byte_chunks_and_multiple_frames() {
        let mut wire = Vec::new();
        for payload in [b"one".as_slice(), b"".as_slice(), b"three".as_slice()] {
            wire.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            wire.extend_from_slice(payload);
        }
        let mut reader = ChromeNativeMessageReader::default();
        let mut frames = Vec::new();
        for byte in wire {
            let batch = reader.feed(&[byte]);
            assert_eq!(batch.terminal_error, None);
            frames.extend(batch.frames);
        }
        assert_eq!(frames, [b"one".to_vec(), Vec::new(), b"three".to_vec()]);
        assert_eq!(reader.buffered_len(), 0);
    }

    #[test]
    fn oversized_chrome_frame_closes_reader_without_payload_allocation() {
        let mut reader = ChromeNativeMessageReader::default();
        let header = ((MAX_CHROME_NATIVE_MESSAGE_BYTES + 1) as u32).to_le_bytes();
        assert_eq!(
            reader.feed(&header),
            ChromeNativeReadBatch {
                frames: Vec::new(),
                terminal_error: Some(ChromeNativeFramingError::MessageTooLarge),
            }
        );
        assert!(reader.is_closed());
        assert_eq!(reader.buffered_len(), 0);
        assert_eq!(
            reader.feed(b"later"),
            ChromeNativeReadBatch {
                frames: Vec::new(),
                terminal_error: Some(ChromeNativeFramingError::ReaderClosed),
            }
        );
    }

    #[test]
    fn chrome_reader_delivers_valid_prefix_before_oversize_shutdown() {
        let mut input = 1_u32.to_le_bytes().to_vec();
        input.push(b'x');
        input.extend_from_slice(&((MAX_CHROME_NATIVE_MESSAGE_BYTES + 1) as u32).to_le_bytes());
        let mut reader = ChromeNativeMessageReader::default();
        assert_eq!(
            reader.feed(&input),
            ChromeNativeReadBatch {
                frames: vec![vec![b'x']],
                terminal_error: Some(ChromeNativeFramingError::MessageTooLarge),
            }
        );
    }

    #[test]
    fn stdout_writer_preserves_header_payload_and_partial_write_boundaries() {
        let mut writer = NativeHostStdoutWriter::default();
        writer.enqueue_message(b"hello").expect("queue message");
        assert_eq!(writer.queued_requests(), 2);
        assert_eq!(writer.current_bytes(), Some(&5_u32.to_le_bytes()[..]));
        assert_eq!(
            writer.apply_write_result(NativeHostWriteResult::Written(1)),
            NativeHostWriteAction::Continue
        );
        assert_eq!(writer.current_bytes(), Some(&5_u32.to_le_bytes()[1..]));
        assert_eq!(
            writer.apply_write_result(NativeHostWriteResult::Interrupted),
            NativeHostWriteAction::Retry
        );
        assert_eq!(
            writer.apply_write_result(NativeHostWriteResult::WouldBlock),
            NativeHostWriteAction::WaitWritable
        );
        assert!(writer.writable_notification());
        assert!(!writer.writable_notification());
        assert_eq!(
            writer.apply_write_result(NativeHostWriteResult::Written(3)),
            NativeHostWriteAction::Continue
        );
        assert_eq!(writer.current_bytes(), Some(b"hello".as_slice()));
        assert_eq!(
            writer.apply_write_result(NativeHostWriteResult::Written(5)),
            NativeHostWriteAction::Continue
        );
        assert_eq!(writer.current_bytes(), None);
    }

    #[test]
    fn stdout_shutdown_discards_unwritten_suffix_and_rejects_late_messages() {
        let mut writer = NativeHostStdoutWriter::default();
        writer.enqueue_message(b"payload").expect("queue message");
        writer.apply_write_result(NativeHostWriteResult::Written(4));
        assert_eq!(writer.current_bytes(), Some(b"payload".as_slice()));
        writer.shutdown();
        assert_eq!(writer.phase(), NativeHostWriterPhase::Exiting);
        assert_eq!(writer.queued_requests(), 0);
        assert_eq!(
            writer.enqueue_message(b"late"),
            Err(ChromeNativeFramingError::ReaderClosed)
        );
    }

    #[test]
    fn bridge_fatal_write_drops_only_connection_queue() {
        let mut queue = BridgeWriteQueue::default();
        assert!(queue.enqueue(b"first"));
        assert!(queue.enqueue(b"second"));
        queue.apply_write_result(NativeHostWriteResult::Written(2));
        assert_eq!(queue.current_bytes(), Some(b"rst".as_slice()));
        assert_eq!(
            queue.apply_write_result(NativeHostWriteResult::Fatal),
            NativeHostWriteAction::Exit
        );
        assert!(queue.is_failed());
        assert_eq!(queue.queued_requests(), 0);
        assert!(!queue.enqueue(b"late"));
    }

    #[test]
    fn websocket_round_trips_all_three_length_encodings() {
        for length in [5_usize, 126, 65_536] {
            let payload = (0..length).map(|index| index as u8).collect::<Vec<_>>();
            let encoded = write_masked_websocket_text(&payload, [1, 2, 3, 4]);
            let parsed = read_websocket_frame(&encoded)
                .expect("length is supported")
                .expect("frame is complete");
            assert_eq!(parsed.opcode, 1);
            assert_eq!(parsed.payload, payload);
            assert_eq!(parsed.consumed, encoded.len());
        }
    }

    #[test]
    fn websocket_reader_waits_for_each_partial_boundary() {
        let encoded = write_masked_websocket_text(&[7; 126], [9, 8, 7, 6]);
        for end in 0..encoded.len() {
            assert_eq!(
                read_websocket_frame(&encoded[..end]).expect("partial frame is valid"),
                None
            );
        }
        assert!(read_websocket_frame(&encoded)
            .expect("complete frame is valid")
            .is_some());
    }

    #[test]
    fn websocket_unrepresentable_127_length_waits_like_node() {
        let mut frame = vec![0x81, 0x7f];
        frame.extend_from_slice(&u64::MAX.to_be_bytes());
        assert_eq!(
            read_websocket_frame(&frame).expect("extreme length is an incomplete frame"),
            None
        );
    }

    #[test]
    fn reconnect_delay_matches_javascript_rounding_and_cap() {
        let mut delay = 250;
        let mut values = Vec::new();
        for _ in 0..8 {
            delay = next_native_host_reconnect_delay_ms(delay);
            values.push(delay);
        }
        assert_eq!(values, [450, 810, 1458, 2624, 4723, 8501, 10_000, 10_000]);
    }
}
