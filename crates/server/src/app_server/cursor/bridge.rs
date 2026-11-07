//! A loopback provider adapter. A request performs one model invocation; tool
//! execution and subsequent invocations belong to the Klaus session loop.

use super::{changes, credential};
use bytes::{Bytes, BytesMut};
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full, Limited, StreamBody};
use hyper::{
    body::{Frame, Incoming},
    Request, Response,
};
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use std::convert::Infallible;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex};

type Body = UnsyncBoxBody<Bytes, Infallible>;
const MAX_REQUEST: usize = 32 * 1024 * 1024;
const MAX_FRAME: usize = 32 * 1024 * 1024;
// Reserve bounded transport overhead around the retained history. Individual
// operations may select a smaller stream ceiling independently of frame size.
const MAX_STREAM_BYTES: usize = super::step_history::MAX_HISTORY * 2;

struct Running {
    base: String,
    token: String,
    task: tokio::task::JoinHandle<()>,
}
fn running() -> &'static Mutex<Option<Running>> {
    static RUNNING: OnceLock<Mutex<Option<Running>>> = OnceLock::new();
    RUNNING.get_or_init(|| Mutex::new(None))
}

pub(super) async fn ensure_started() -> Result<(String, String), String> {
    let mut running = running().lock().await;
    if let Some(endpoint) = running.as_ref().filter(|r| !r.task.is_finished()) {
        return Ok((endpoint.base.clone(), endpoint.token.clone()));
    }
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|_| "cursor_bridge_bind_failed")?;
    let base = format!(
        "http://{}",
        listener
            .local_addr()
            .map_err(|_| "cursor_bridge_bind_failed")?
    );
    let token = uuid::Uuid::new_v4().to_string();
    let secret = Arc::new(token.clone());
    let task = tokio::spawn(async move {
        loop {
            let (socket, _) = match listener.accept().await {
                Ok(value) => value,
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::Interrupted | std::io::ErrorKind::ConnectionAborted
                    ) =>
                {
                    continue
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::InvalidInput
                            | std::io::ErrorKind::NotConnected
                            | std::io::ErrorKind::PermissionDenied
                    ) =>
                {
                    break
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let secret = secret.clone();
            tokio::spawn(async move {
                let service =
                    hyper::service::service_fn(move |request| handle(request, secret.clone()));
                let _ = hyper::server::conn::http1::Builder::new()
                    .header_read_timeout(Duration::from_secs(15))
                    .timer(hyper_util::rt::TokioTimer::new())
                    .serve_connection(TokioIo::new(socket), service)
                    .await;
            });
        }
    });
    *running = Some(Running {
        base: base.clone(),
        token: token.clone(),
        task,
    });
    Ok((base, token))
}

fn json_response(status: u16, value: Value) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(value.to_string())).boxed_unsync())
        .expect("valid response")
}

async fn handle(
    request: Request<Incoming>,
    secret: Arc<String>,
) -> Result<Response<Body>, Infallible> {
    let host = request
        .headers()
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    let authorized = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v == format!("Bearer {secret}"));
    if !(host.starts_with("127.0.0.1:") || host.starts_with("localhost:")) || !authorized {
        return Ok(json_response(
            401,
            json!({"error":{"message":"Invalid Cursor bridge authorization","type":"authentication_error"}}),
        ));
    }
    if request.method() != "POST" || !matches!(request.uri().path(), "/responses" | "/v1/responses")
    {
        return Ok(json_response(
            404,
            json!({"error":{"message":"Unknown provider endpoint"}}),
        ));
    }
    let collected = match tokio::time::timeout(
        Duration::from_secs(30),
        Limited::new(request.into_body(), MAX_REQUEST).collect(),
    )
    .await
    {
        Ok(Ok(value)) => value.to_bytes(),
        _ => {
            return Ok(json_response(
                413,
                json!({"error":{"message":"Cursor request exceeds the body limit or read deadline"}}),
            ))
        }
    };
    let body: Value = match serde_json::from_slice(&collected) {
        Ok(value) => value,
        Err(_) => {
            return Ok(json_response(
                400,
                json!({"error":{"message":"Invalid provider JSON"}}),
            ))
        }
    };
    if body["stream"] != true {
        return Ok(json_response(
            400,
            json!({"error":{"message":"Cursor requires a streaming provider request"}}),
        ));
    }
    let (tx, rx) = mpsc::channel::<Bytes>(16);
    tokio::spawn(async move {
        // Closing the response body drops the receiver. Cancel upstream even
        // while waiting for run admission or a silent backend.
        tokio::select! {
            _ = tx.closed() => {},
            result = tokio::time::timeout(Duration::from_secs(600), run(body, &tx)) => {
                let error = match result { Ok(Ok(())) => return, Ok(Err(error)) => error, Err(_) => "cursor_inference_timeout".into() };
                let _ = emit(&tx, super::responses::public_error(&error)).await;
            }
        }
    });
    let stream = futures::stream::unfold(rx, |mut rx| async move {
        rx.recv()
            .await
            .map(|bytes| (Ok::<_, Infallible>(Frame::data(bytes)), rx))
    });
    Ok(Response::builder()
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .body(StreamBody::new(stream).boxed_unsync())
        .expect("valid response"))
}

pub(super) async fn emit(sender: &mpsc::Sender<Bytes>, value: Value) -> Result<(), String> {
    sender
        .send(Bytes::from(format!("data: {value}\n\n")))
        .await
        .map_err(|_| "cursor_request_cancelled".into())
}

pub(super) fn encode_frame(value: Value) -> Result<Bytes, String> {
    let data = serde_json::to_vec(&value).map_err(|_| "cursor_request_invalid")?;
    if data.len() > MAX_REQUEST {
        return Err("cursor_request_too_large".into());
    }
    let mut bytes = Vec::with_capacity(data.len() + 5);
    bytes.push(0);
    bytes.extend_from_slice(&(data.len() as u32).to_be_bytes());
    bytes.extend(data);
    Ok(bytes.into())
}

pub(super) struct Encoder {
    sent: usize,
    max_bytes: usize,
}

impl Encoder {
    pub fn with_max_bytes(max_bytes: usize) -> Result<Self, String> {
        if max_bytes == 0 || max_bytes > MAX_STREAM_BYTES {
            return Err("cursor_request_too_large".into());
        }
        Ok(Self { sent: 0, max_bytes })
    }

    pub fn frame(&mut self, value: Value) -> Result<Bytes, String> {
        let frame = encode_frame(value)?;
        let sent = self.sent.saturating_add(frame.len());
        if sent > self.max_bytes {
            return Err("cursor_request_too_large".into());
        }
        self.sent = sent;
        Ok(frame)
    }
}

pub(super) struct Decoder {
    buffer: BytesMut,
    max_frame: usize,
    max_response: usize,
    received: usize,
    frames: usize,
}
impl Default for Decoder {
    fn default() -> Self {
        Self {
            buffer: BytesMut::new(),
            max_frame: MAX_FRAME,
            max_response: MAX_REQUEST,
            received: 0,
            frames: 0,
        }
    }
}
impl Decoder {
    pub fn with_max_frame(max_frame: usize) -> Result<Self, String> {
        Self::with_limits(max_frame, MAX_REQUEST)
    }
    pub fn with_max_response(max_response: usize) -> Result<Self, String> {
        Self::with_limits(MAX_FRAME, max_response)
    }
    pub fn with_limits(max_frame: usize, max_response: usize) -> Result<Self, String> {
        if max_frame == 0 || max_frame > 32 * 1024 * 1024 {
            return Err("cursor_frame_too_large".into());
        }
        if max_response == 0 || max_response > MAX_STREAM_BYTES {
            return Err("cursor_response_too_large".into());
        }
        Ok(Self {
            buffer: BytesMut::new(),
            max_frame,
            max_response,
            received: 0,
            frames: 0,
        })
    }
    pub fn push(&mut self, mut chunk: &[u8]) -> Result<Vec<(bool, Value)>, String> {
        // Bound the entire stream independently of HTTP chunk boundaries. The
        // callers also enforce their operation-specific response byte limits.
        self.received = self.received.saturating_add(chunk.len());
        if self.received > self.max_response {
            return Err("cursor_response_too_large".into());
        }
        let mut values = Vec::new();
        while !chunk.is_empty() {
            if self.buffer.len() < 5 {
                let count = (5 - self.buffer.len()).min(chunk.len());
                self.buffer.extend_from_slice(&chunk[..count]);
                chunk = &chunk[count..];
                if self.buffer.len() < 5 {
                    break;
                }
            }
            let flags = self.buffer[0];
            if flags & !2 != 0 {
                return Err("cursor_frame_encoding_unsupported".into());
            }
            let size =
                u32::from_be_bytes(self.buffer[1..5].try_into().expect("four bytes")) as usize;
            if size > self.max_frame {
                return Err("cursor_frame_too_large".into());
            }
            let count = (size + 5 - self.buffer.len()).min(chunk.len());
            self.buffer.extend_from_slice(&chunk[..count]);
            chunk = &chunk[count..];
            if self.buffer.len() < size + 5 {
                break;
            }
            // One terminal frame may follow the maximum 32,768 model events.
            // A cumulative limit keeps both the output vector and repeated
            // pushes bounded without rejecting otherwise valid coalesced frames.
            self.frames += 1;
            if self.frames > 32_769 {
                return Err("cursor_response_too_large".into());
            }
            let frame = self.buffer.split_to(size + 5);
            let value = serde_json::from_slice(&frame[5..]).map_err(|_| "cursor_frame_invalid")?;
            values.push((flags & 2 != 0, value));
        }
        Ok(values)
    }
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

pub(super) async fn run(body: Value, output: &mpsc::Sender<Bytes>) -> Result<(), String> {
    let body = super::responses::normalize(&body)?;
    super::auth::ensure_valid().await?;
    let (credential, generation) = credential::snapshot()?;
    let selection = body["model"].as_str().ok_or("cursor_model_required")?;
    let key = super::resolved_model(selection).await?;
    let model = super::lookup_model(&key)
        .await
        .ok_or("cursor_model_unavailable")?;
    if let Some(effort) = body
        .get("reasoning_effort")
        .filter(|value| !value.is_null())
    {
        if model.get("defaultEffort") != Some(effort) {
            return Err("cursor_reasoning_variant_conflict".into());
        }
    }
    let mut stream = super::responses::Output::new(&key);
    stream.start(output).await?;
    let certified =
        super::step::invoke(&body, &model, &credential, generation, &mut stream, output).await?;
    finish_output(stream, certified, generation, output).await
}

pub(super) async fn finish_output(
    stream: super::responses::Output,
    certified: super::step::Certified,
    generation: u64,
    output: &mpsc::Sender<Bytes>,
) -> Result<(), String> {
    let mut change = changes().subscribe();
    credential::check_generation(generation)?;
    let result = tokio::select! {
        biased;
        _ = change.changed() => Err("cursor_credential_changed".into()),
        result=stream.finish(certified,output) => result,
    };
    credential::check_generation(generation)?;
    result
}
