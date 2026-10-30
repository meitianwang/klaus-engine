//! Byte-stream adapters that bridge a channel of newline-delimited JSON lines to
//! the `AsyncRead`/`AsyncWrite` pair a `ProtocolServer`/`ProtocolClient` is built
//! from. A transport (WebSocket frames, SSE data events, HTTP POST bodies) still
//! does its framing with `String` channels; these adapters wrap each channel end
//! into the byte stream the protocol reads and writes, without changing the
//! one-line-per-message contract.

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;

/// Presents a channel of newline-terminated lines as a byte `AsyncRead`. Each
/// received line is emitted as its UTF-8 bytes (a line already carries its `\n`,
/// so the protocol's line splitter yields one message per line); the reader
/// reports EOF once the channel closes.
pub(crate) struct ChannelReader {
    rx: mpsc::UnboundedReceiver<String>,
    /// The current line's bytes, not yet fully copied to the consumer.
    buf: Vec<u8>,
    /// Read cursor into `buf`.
    pos: usize,
}

impl ChannelReader {
    pub(crate) fn new(rx: mpsc::UnboundedReceiver<String>) -> Self {
        ChannelReader {
            rx,
            buf: Vec::new(),
            pos: 0,
        }
    }
}

impl AsyncRead for ChannelReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let me = &mut *self;
        loop {
            // Drain any buffered bytes first (a large line spans multiple reads).
            if me.pos < me.buf.len() {
                if out.remaining() == 0 {
                    return Poll::Ready(Ok(()));
                }
                let avail = &me.buf[me.pos..];
                let n = avail.len().min(out.remaining());
                out.put_slice(&avail[..n]);
                me.pos += n;
                return Poll::Ready(Ok(()));
            }
            // Buffer exhausted — pull the next line (or reach end of stream).
            me.buf.clear();
            me.pos = 0;
            match me.rx.poll_recv(cx) {
                Poll::Ready(Some(line)) => {
                    me.buf.extend_from_slice(line.as_bytes());
                    // An empty line carries nothing; loop to fetch the next.
                }
                Poll::Ready(None) => return Poll::Ready(Ok(())), // channel closed → EOF
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Presents a channel of lines as a byte `AsyncWrite`. Bytes are split on `\n`;
/// each complete line (including its terminator) is forwarded to the channel as
/// one `String`, and a trailing partial line is buffered until its newline
/// arrives. Writing to a closed channel drops the line (a dead peer) rather than
/// failing the protocol's write path.
pub(crate) struct ChannelWriter {
    tx: mpsc::UnboundedSender<String>,
    /// Bytes accumulated for the line currently being assembled.
    buf: Vec<u8>,
}

impl ChannelWriter {
    pub(crate) fn new(tx: mpsc::UnboundedSender<String>) -> Self {
        ChannelWriter {
            tx,
            buf: Vec::new(),
        }
    }
}

impl AsyncWrite for ChannelWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let me = &mut *self;
        me.buf.extend_from_slice(data);
        while let Some(idx) = me.buf.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = me.buf.drain(..=idx).collect();
            // The protocol writes compact JSON, so a line is always valid UTF-8.
            let s = String::from_utf8_lossy(&line).into_owned();
            let _ = me.tx.send(s); // dropped line on a closed channel — like a dead socket
        }
        Poll::Ready(Ok(data.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    #[tokio::test]
    async fn reader_yields_one_message_per_line_then_eof() {
        let (tx, rx) = mpsc::unbounded_channel::<String>();
        tx.send("{\"a\":1}\n".to_string()).unwrap();
        tx.send("{\"b\":2}\n".to_string()).unwrap();
        drop(tx); // EOF
        let mut lines = BufReader::new(ChannelReader::new(rx)).lines();
        assert_eq!(
            lines.next_line().await.unwrap().as_deref(),
            Some("{\"a\":1}")
        );
        assert_eq!(
            lines.next_line().await.unwrap().as_deref(),
            Some("{\"b\":2}")
        );
        assert_eq!(lines.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn writer_forwards_each_line_including_newline() {
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        let mut w = ChannelWriter::new(tx);
        // A message written in one call, and one split across two calls.
        w.write_all(b"{\"a\":1}\n").await.unwrap();
        w.write_all(b"{\"b\"").await.unwrap();
        w.write_all(b":2}\n").await.unwrap();
        assert_eq!(rx.recv().await, Some("{\"a\":1}\n".to_string()));
        assert_eq!(rx.recv().await, Some("{\"b\":2}\n".to_string()));
    }
}
