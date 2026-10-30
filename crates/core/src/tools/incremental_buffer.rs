//! A bounded byte buffer that also tracks a per-reader cursor so callers can
//! fetch only the bytes appended since their last read. It caps memory by
//! collapsing the middle: the head is pinned and the tail rolls — readers consume
//! from a logical, monotonically increasing offset so they never see the same
//! bytes twice. Used by background shells and persistent interactive sessions.

use std::collections::VecDeque;

/// The result of [`IncrementalBuffer::read_from`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadResult {
    pub text: String,
    pub next_cursor: usize,
    pub dropped_bytes: usize,
}

pub struct IncrementalBuffer {
    head: Vec<Vec<u8>>,
    head_bytes: usize,
    tail: VecDeque<Vec<u8>>,
    tail_bytes: usize,
    /// Total bytes ever appended (the logical end-of-stream offset).
    produced: usize,
    /// Logical offset of the first byte still retained in `tail`.
    tail_start: usize,
    head_limit: usize,
    tail_limit: usize,
}

impl IncrementalBuffer {
    pub fn new(cap: usize) -> Self {
        let head_limit = (cap as f64 * 0.5).floor() as usize;
        let tail_limit = std::cmp::max(1, cap.saturating_sub(head_limit));
        IncrementalBuffer {
            head: Vec::new(),
            head_bytes: 0,
            tail: VecDeque::new(),
            tail_bytes: 0,
            produced: 0,
            tail_start: 0,
            head_limit,
            tail_limit,
        }
    }

    /// Append a chunk, evicting the oldest tail bytes once the cap is exceeded.
    pub fn push(&mut self, chunk: &[u8]) {
        self.produced += chunk.len();
        let mut buf = chunk;
        if self.head_bytes < self.head_limit {
            let room = self.head_limit - self.head_bytes;
            if buf.len() <= room {
                self.head.push(buf.to_vec());
                self.head_bytes += buf.len();
                return;
            }
            self.head.push(buf[..room].to_vec());
            self.head_bytes += room;
            buf = &buf[room..];
        }
        self.tail.push_back(buf.to_vec());
        self.tail_bytes += buf.len();
        if self.tail_start == 0 {
            self.tail_start = self.head_bytes;
        }
        while self.tail_bytes > self.tail_limit && !self.tail.is_empty() {
            let front_len = self.tail.front().map(Vec::len).unwrap_or(0);
            if self.tail.len() > 1 {
                self.tail.pop_front();
                self.tail_bytes -= front_len;
                self.tail_start += front_len;
            } else {
                let overflow = self.tail_bytes - self.tail_limit;
                let front = self.tail.front_mut().unwrap();
                *front = front[overflow..].to_vec();
                self.tail_bytes -= overflow;
                self.tail_start += overflow;
            }
        }
    }

    /// The logical offset of the end of the stream (use as the next cursor).
    pub fn end(&self) -> usize {
        self.produced
    }

    /// Return the bytes in `[from, end)` that are still retained, decoded as
    /// UTF-8. If `from` points into an evicted region, an elision note is
    /// prepended.
    pub fn read_from(&self, from: usize) -> ReadResult {
        let end = self.produced;
        if from >= end {
            return ReadResult {
                text: String::new(),
                next_cursor: end,
                dropped_bytes: 0,
            };
        }

        let mut parts: Vec<u8> = Vec::new();
        let mut cursor = from;
        let mut dropped = 0;

        // Head region: bytes [0, head_bytes) are always retained.
        if cursor < self.head_bytes {
            let head_buf: Vec<u8> = self.head.concat();
            parts.extend_from_slice(&head_buf[cursor..]);
            cursor = self.head_bytes;
        }

        // Gap between head and the retained tail was evicted.
        if cursor < self.tail_start {
            dropped = self.tail_start - cursor;
            cursor = self.tail_start;
        }

        // Tail region: bytes [tail_start, produced).
        if !self.tail.is_empty() {
            let tail_buf: Vec<u8> = self.tail.iter().flatten().copied().collect();
            let offset_in_tail = cursor - self.tail_start;
            if offset_in_tail < tail_buf.len() {
                parts.extend_from_slice(&tail_buf[offset_in_tail..]);
            }
        }

        let mut text = String::from_utf8_lossy(&parts).into_owned();
        if dropped > 0 {
            text = format!("… [{dropped} bytes elided] …\n{text}");
        }
        ReadResult {
            text,
            next_cursor: end,
            dropped_bytes: dropped,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_appended_bytes_from_a_cursor() {
        let mut b = IncrementalBuffer::new(100);
        b.push(b"hello ");
        let r1 = b.read_from(0);
        assert_eq!(r1.text, "hello ");
        assert_eq!(r1.dropped_bytes, 0);
        b.push(b"world");
        // Read only what was appended since the last cursor.
        let r2 = b.read_from(r1.next_cursor);
        assert_eq!(r2.text, "world");
        assert_eq!(r2.next_cursor, b.end());
    }

    #[test]
    fn empty_when_cursor_at_or_past_end() {
        let mut b = IncrementalBuffer::new(100);
        b.push(b"abc");
        let r = b.read_from(3);
        assert_eq!(r.text, "");
        assert_eq!(r.dropped_bytes, 0);
        assert_eq!(r.next_cursor, 3);
    }

    #[test]
    fn evicts_middle_and_reports_elision() {
        // cap 10 → head_limit 5, tail_limit 5.
        let mut b = IncrementalBuffer::new(10);
        for _ in 0..4 {
            b.push(b"12345"); // 20 bytes total produced
        }
        assert_eq!(b.end(), 20);
        let r = b.read_from(0);
        // Head keeps the first 5, tail keeps the last 5; the middle 10 elided.
        assert!(r.dropped_bytes > 0);
        assert!(r.text.contains("bytes elided"));
        // The elision note is prepended, then the retained head + tail.
        assert!(r.text.contains("12345"));
    }

    #[test]
    fn head_is_pinned_tail_rolls() {
        let mut b = IncrementalBuffer::new(10);
        b.push(b"AAAAA"); // fills head
        b.push(b"BBBBB"); // fills tail
        b.push(b"CCCCC"); // evicts oldest tail bytes
        let r = b.read_from(0);
        // The retained bytes are the pinned head + the rolled tail, with the
        // evicted middle replaced by an elision note.
        assert!(r.text.contains("AAAAA"));
        assert!(r.text.ends_with("CCCCC"));
        assert_eq!(r.dropped_bytes, 5);
    }
}
