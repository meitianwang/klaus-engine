//! HTTP + SSE client (cloud-friendly): opens `GET {base}/sessions/{id}/events`
//! (the SSE event stream) and sends ops via `POST {base}/sessions/{id}/ops`,
//! then hands back a ready `ProtocolClient`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures::StreamExt;
use rand::RngCore;
use tokio::sync::{mpsc, Notify};

use klaus_agent_protocol::{ProtocolClient, ProtocolClientOptions};

use crate::connect::with_lifecycle_query;
use crate::line_stream::{ChannelReader, ChannelWriter};

#[derive(Default, Clone)]
pub struct ConnectHttpOptions {
    pub token: Option<String>,
    pub origin: Option<String>,
    pub resume: Option<String>,
    pub fork: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    #[error("{0}")]
    Message(String),
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
}

/// A live HTTP+SSE client connection.
pub struct HttpClientConnection {
    /// Drive the session and iterate events.
    pub client: ProtocolClient,
    /// The client-chosen session id correlating the SSE stream and the ops POSTs.
    pub session_id: String,
    aborted: Arc<AtomicBool>,
    cancel: Arc<Notify>,
}

impl HttpClientConnection {
    /// Close the SSE stream and stop sending.
    pub fn close(&self) {
        self.aborted.store(true, Ordering::SeqCst);
        self.cancel.notify_waiters();
    }
}

/// List persisted threads via `GET {base}/threads`. Returns the parsed JSON
/// page. Rejects on a non-2xx (e.g. 401 bad token, 501 not supported).
pub async fn list_threads(
    base_url: &str,
    token: Option<&str>,
) -> Result<serde_json::Value, HttpError> {
    let base = base_url.trim_end_matches('/');
    let client = reqwest::Client::new();
    let mut rb = client.get(format!("{base}/threads"));
    if let Some(t) = token {
        rb = rb.header("authorization", format!("Bearer {t}"));
    }
    let res = rb.send().await?;
    if !res.status().is_success() {
        let st = res.status();
        return Err(HttpError::Message(format!(
            "listThreads failed: {} {}",
            st.as_u16(),
            st.canonical_reason().unwrap_or("")
        )));
    }
    Ok(res.json().await?)
}

/// Connect over HTTP + Server-Sent Events. `base_url` is the HTTP origin, e.g.
/// `http://127.0.0.1:8787` (NOT the `ws://` URL). Resolves once the SSE stream
/// is open (rejects on a failed handshake, e.g. 401 on a bad token).
pub async fn connect_http(
    base_url: &str,
    opts: ConnectHttpOptions,
) -> Result<HttpClientConnection, HttpError> {
    let session_id = random_uuid();
    let base = base_url.trim_end_matches('/').to_string();
    let client = reqwest::Client::new();
    let aborted = Arc::new(AtomicBool::new(false));
    let cancel = Arc::new(Notify::new());

    // input: events → ProtocolClient. output: ops ← ProtocolClient.
    let (input_tx, input_rx) = mpsc::unbounded_channel::<String>();
    let (output_tx, output_rx) = mpsc::unbounded_channel::<String>();

    let auth_token = opts.token.clone();
    let auth_origin = opts.origin.clone();

    // Outbound ops: one POST per op line, drained serially so ops stay strictly
    // ordered (matters for e.g. run-then-interrupt). A failed POST surfaces to
    // the caller as a notification event rather than silently dropping.
    {
        let client = client.clone();
        let base = base.clone();
        let session_id = session_id.clone();
        let aborted = aborted.clone();
        let cancel = cancel.clone();
        let input_tx = input_tx.clone();
        let auth_token = auth_token.clone();
        let auth_origin = auth_origin.clone();
        let mut output_rx = output_rx;
        tokio::spawn(async move {
            loop {
                let line = tokio::select! {
                    _ = cancel.notified() => break,
                    l = output_rx.recv() => match l { Some(l) => l, None => break },
                };
                if aborted.load(Ordering::SeqCst) {
                    continue; // connection closed — don't fire a doomed request
                }
                let body = line.trim().to_string();
                let url = format!("{base}/sessions/{session_id}/ops");
                let mut rb = client
                    .post(url)
                    .header("content-type", "application/json")
                    .body(body);
                if let Some(t) = &auth_token {
                    rb = rb.header("authorization", format!("Bearer {t}"));
                }
                if let Some(o) = &auth_origin {
                    rb = rb.header("origin", o.clone());
                }
                tokio::select! {
                    _ = cancel.notified() => break,
                    res = rb.send() => match res {
                        Ok(resp) => {
                            if !resp.status().is_success() && !aborted.load(Ordering::SeqCst) {
                                report_op_failure(&input_tx, &format!("op POST failed: {}", resp.status().as_u16()));
                            }
                        }
                        Err(e) => {
                            if !aborted.load(Ordering::SeqCst) {
                                report_op_failure(&input_tx, &format!("op POST error: {e}"));
                            }
                        }
                    }
                }
            }
        });
    }

    // SSE handshake.
    let events_url = with_lifecycle_query(
        &format!("{base}/sessions/{session_id}/events"),
        opts.resume.as_deref(),
        opts.fork.as_deref(),
    );
    let mut rb = client
        .get(&events_url)
        .header("accept", "text/event-stream");
    if let Some(t) = &auth_token {
        rb = rb.header("authorization", format!("Bearer {t}"));
    }
    if let Some(o) = &auth_origin {
        rb = rb.header("origin", o.clone());
    }
    let res = rb.send().await?;
    if !res.status().is_success() {
        aborted.store(true, Ordering::SeqCst);
        cancel.notify_waiters();
        let st = res.status();
        return Err(HttpError::Message(format!(
            "HTTP+SSE connect failed: {} {}",
            st.as_u16(),
            st.canonical_reason().unwrap_or("")
        )));
    }

    // SSE pump: unwrap each `data:` event and push its JSON as one NDJSON line.
    {
        let cancel = cancel.clone();
        let input_tx = input_tx.clone();
        tokio::spawn(async move {
            let mut stream = res.bytes_stream();
            let mut buf: Vec<u8> = Vec::new();
            loop {
                let chunk = tokio::select! {
                    _ = cancel.notified() => break,
                    c = stream.next() => match c { Some(Ok(c)) => c, _ => break },
                };
                buf.extend_from_slice(&chunk);
                while let Some(idx) = find_double_newline(&buf) {
                    let frame: Vec<u8> = buf[..idx].to_vec();
                    buf.drain(..idx + 2);
                    let frame_str = String::from_utf8_lossy(&frame);
                    let data = parse_sse_frame(&frame_str);
                    if !data.is_empty() {
                        let _ = input_tx.send(format!("{data}\n"));
                    }
                }
            }
            // EOF: the task's input_tx clone drops here.
        });
    }

    let client_proto = ProtocolClient::new(ProtocolClientOptions {
        input: Box::new(ChannelReader::new(input_rx)),
        output: Box::new(ChannelWriter::new(output_tx)),
    });
    Ok(HttpClientConnection {
        client: client_proto,
        session_id,
        aborted,
        cancel,
    })
}

/// Surface a failed op POST to the caller as a `notification` event pushed into
/// the same input stream the caller reads.
fn report_op_failure(input_tx: &mpsc::UnboundedSender<String>, message: &str) {
    let payload = serde_json::json!({
        "submissionId": "",
        "event": { "type": "notification", "level": "warn", "message": message }
    });
    let _ = input_tx.send(format!("{payload}\n"));
}

/// Extract the joined `data:` payload from one SSE frame.
fn parse_sse_frame(frame: &str) -> String {
    frame
        .split('\n')
        .filter(|l| l.starts_with("data:"))
        .map(|l| {
            let s = &l[5..];
            s.strip_prefix(' ').unwrap_or(s)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn find_double_newline(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\n\n")
}

fn random_uuid() -> String {
    let mut b = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut b);
    b[6] = (b[6] & 0x0f) | 0x40; // version 4
    b[8] = (b[8] & 0x3f) | 0x80; // variant
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13],
        b[14], b[15]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sse_frames() {
        assert_eq!(parse_sse_frame("data: {\"a\":1}"), "{\"a\":1}");
        assert_eq!(parse_sse_frame(": ok"), "");
        assert_eq!(parse_sse_frame("data: a\ndata: b"), "a\nb");
        assert_eq!(parse_sse_frame("data:x"), "x");
    }

    #[test]
    fn finds_frame_boundary() {
        assert_eq!(find_double_newline(b"abc\n\ndef"), Some(3));
        assert_eq!(find_double_newline(b"abc\ndef"), None);
    }

    #[test]
    fn uuid_shape() {
        let u = random_uuid();
        assert_eq!(u.len(), 36);
        assert_eq!(u.as_bytes()[14], b'4'); // version nibble
    }
}
