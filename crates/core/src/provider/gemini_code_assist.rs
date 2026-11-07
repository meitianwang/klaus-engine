//! Code Assist (OAuth-subscription) transport for the Gemini adapter.
//!
//! The API-key / Vertex path talks to the public Gemini surface directly. The
//! Code Assist subscription instead hits a DIFFERENT endpoint
//! (`cloudcode-pa.googleapis.com/v1internal`) that wraps every request in a
//! `{ model, project, request }` envelope and answers with a Server-Sent-Events
//! stream. This module owns that wire contract: it converts a generate-content
//! request into the envelope, establishes the SSE stream (retrying only
//! establishment), and unwraps each SSE frame back into the plain
//! generate-content response shape the shared Gemini chunk mapper consumes.
//!
//! Requests and response chunks are carried as exact runtime JSON so the
//! transport can hoist envelope fields without a full typed model of the Gemini
//! surface; the caller builds the request JSON and reads the chunk JSON with the
//! same shapes it uses for the direct path.
//!
//! Project onboarding, credit accounting, and telemetry are out of scope; the
//! project id is host-supplied.

use crate::provider::auth::AuthSource;
use crate::provider::classify::{classify_error, RawProviderError};
use crate::provider::retry::{with_retry, RetryError, RetryOptions};
use crate::runtime_values::{
    runtime_json, RuntimeJson as Value, RuntimeObject as Map, RuntimeString, WhatwgUtf8,
};
use crate::types::abort::AbortSignal;
use crate::types::errors::ProviderError;
use futures::stream::{Stream, StreamExt};
use rand::RngCore;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

/// Default Code Assist backend host.
pub const CODE_ASSIST_ENDPOINT: &str = "https://cloudcode-pa.googleapis.com";
/// Code Assist API version segment used in the request path.
pub const CODE_ASSIST_API_VERSION: &str = "v1internal";

/// Establishment inputs for the Code Assist SSE stream.
pub struct CodeAssistStreamOptions {
    /// Backend host (defaults to [`CODE_ASSIST_ENDPOINT`] upstream).
    pub endpoint: String,
    /// Request-establishment retry policy.
    pub retry: RetryOptions,
    /// Cooperative cancellation for establishment and the streamed body.
    pub signal: AbortSignal,
    /// Credential source supplying the per-request bearer.
    pub auth_source: Option<Arc<dyn AuthSource>>,
    /// GCP project id the subscription bills/routes through.
    pub project: Option<String>,
    /// Stable session id echoed into the request envelope.
    pub session_id: Option<String>,
}

/// A stream of unwrapped generate-content response chunks. An `Err` item carries
/// a mid-stream failure the shared mapper turns into an error event; malformed
/// frames are skipped rather than surfaced.
pub type CodeAssistChunkStream = Pin<Box<dyn Stream<Item = Result<Value, ProviderError>> + Send>>;

/// Establish the Code Assist SSE stream (retrying establishment only) and return
/// its chunks, each unwrapped to the plain generate-content response shape so the
/// shared Gemini chunk mapper can consume them identically to the direct path.
///
/// The POST plus non-2xx check IS the establishment seam: a 2xx means the stream
/// is open, so only that is retried, never mid-stream. The failure carries
/// status/headers so the classifier buckets 429/5xx/401 correctly.
pub async fn open_code_assist_stream(
    params: &Value,
    opts: CodeAssistStreamOptions,
) -> Result<CodeAssistChunkStream, ProviderError> {
    let CodeAssistStreamOptions {
        endpoint,
        retry,
        signal,
        auth_source,
        project,
        session_id,
    } = opts;

    let url = format!("{endpoint}/{CODE_ASSIST_API_VERSION}:streamGenerateContent?alt=sse");
    let body = runtime_json::stringify(&to_code_assist_envelope(
        params,
        project.as_deref(),
        session_id.as_deref(),
        Some(&random_uuid()),
    ))
    .to_node_utf8();

    let client = reqwest::Client::new();
    let signal_stream = signal.clone();

    let response = with_retry(retry, signal.clone(), move |_attempt: u32| {
        let client = client.clone();
        let url = url.clone();
        let body = body.clone();
        let signal = signal.clone();
        let auth_source = auth_source.clone();
        async move {
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                reqwest::header::CONTENT_TYPE,
                reqwest::header::HeaderValue::from_static("application/json"),
            );
            if let Some(auth) = &auth_source {
                let attached = auth
                    .get_headers(&signal)
                    .await
                    .map_err(raw_from_provider_error)?;
                for (name, value) in attached {
                    if let (Ok(hn), Ok(hv)) = (
                        reqwest::header::HeaderName::from_bytes(name.as_bytes()),
                        reqwest::header::HeaderValue::from_str(&value),
                    ) {
                        headers.insert(hn, hv);
                    }
                }
            }
            let request = client.post(&url).headers(headers).body(body);
            let res = tokio::select! {
                _ = signal.cancelled() => {
                    return Err(RawProviderError {
                        message: "aborted".to_string(),
                        ..Default::default()
                    });
                }
                sent = request.send() => sent.map_err(|e| RawProviderError {
                    message: e.to_string(),
                    status: e.status().map(|s| s.as_u16()),
                    ..Default::default()
                })?,
            };
            if !res.status().is_success() {
                let status = res.status().as_u16();
                return Err(RawProviderError {
                    status: Some(status),
                    message: format!("Code Assist request failed ({status})"),
                    headers: header_hashmap(res.headers()),
                    ..Default::default()
                });
            }
            Ok(res)
        }
    })
    .await
    .map_err(retry_err_to_provider)?;

    Ok(Box::pin(sse_chunk_stream(response, signal_stream)))
}

/// Build the Code Assist envelope from a generate-content request. `contents`,
/// `systemInstruction`, `tools`, `toolConfig`, and `cachedContent` are hoisted to
/// the top of `request`; every remaining generation parameter
/// (maxOutputTokens / responseMimeType / thinkingConfig …) stays under
/// `generationConfig`. A string `systemInstruction` is rendered as a content
/// object; a stable per-request prompt id is stamped on the envelope.
pub fn to_code_assist_envelope(
    params: &Value,
    project: Option<&str>,
    session_id: Option<&str>,
    user_prompt_id: Option<&str>,
) -> Value {
    // Everything left in `config` after extracting the hoisted keys is the
    // generation config.
    let mut generation_config = params
        .get("config")
        .and_then(|c| c.as_object())
        .cloned()
        .unwrap_or_default();
    let system_instruction = generation_config.remove("systemInstruction");
    let tools = generation_config.remove("tools");
    let tool_config = generation_config.remove("toolConfig");
    let cached_content = generation_config.remove("cachedContent");

    let mut request = Map::new();
    if let Some(contents) = params.get("contents") {
        request.insert("contents", contents.clone());
    }
    if let Some(si) = system_instruction {
        if !si.is_null() {
            request.insert("systemInstruction", as_content(si));
        }
    }
    if let Some(t) = tools {
        if js_truthy(&t) {
            request.insert("tools", t);
        }
    }
    if let Some(tc) = tool_config {
        if js_truthy(&tc) {
            request.insert("toolConfig", tc);
        }
    }
    if let Some(cached_content_value) = cached_content {
        if js_truthy(&cached_content_value) {
            request.insert("cachedContent", cached_content_value);
        }
    }
    request.insert("generationConfig", Value::Object(generation_config));
    if let Some(sid) = session_id {
        if !sid.is_empty() {
            request.insert("session_id", Value::String(sid.into()));
        }
    }

    let mut envelope = Map::new();
    if let Some(model) = params.get("model") {
        envelope.insert("model", model.clone());
    }
    if let Some(p) = project {
        if !p.is_empty() {
            envelope.insert("project", Value::String(p.into()));
        }
    }
    // A per-prompt id is stamped on every request.
    if let Some(pid) = user_prompt_id {
        if !pid.is_empty() {
            envelope.insert("user_prompt_id", Value::String(pid.into()));
        }
    }
    envelope.insert("request", Value::Object(request));
    Value::Object(envelope)
}

/// A joined-string `systemInstruction` renders as a content object; anything else
/// passes through unchanged.
fn as_content(si: Value) -> Value {
    if !si.is_string() {
        return si;
    }
    let string = si.into_string().expect("string checked above");
    let mut part = Map::new();
    part.insert("text", Value::String(string));
    let mut content = Map::new();
    content.insert("parts", Value::Array(vec![Value::Object(part)]));
    Value::Object(content)
}

/// Unwrap a Code Assist response frame into the plain generate-content response
/// shape the shared mapper reads. `candidates` defaults to an empty list;
/// `usageMetadata`, `modelVersion`, and the trace id (surfaced as `responseId`)
/// are carried only when present.
fn unwrap_code_assist(res: &Value) -> Value {
    let inner = res.get("response");
    let mut out = Map::new();
    let candidates = inner
        .and_then(|i| i.get("candidates"))
        .filter(|v| !v.is_null())
        .cloned()
        .unwrap_or_else(|| Value::Array(Vec::new()));
    out.insert("candidates", candidates);
    if let Some(usage) = inner.and_then(|i| i.get("usageMetadata")) {
        if js_truthy(usage) {
            out.insert("usageMetadata", usage.clone());
        }
    }
    if let Some(model_version) = inner.and_then(|i| i.get("modelVersion")) {
        if js_truthy(model_version) {
            out.insert("modelVersion", model_version.clone());
        }
    }
    if let Some(trace_id) = res.get("traceId") {
        if js_truthy(trace_id) {
            out.insert("responseId", trace_id.clone());
        }
    }
    Value::Object(out)
}

/// Truthiness of a JSON value under the same rules the envelope hoist uses:
/// null / false / 0 / empty-string are falsy; arrays and objects (even empty)
/// are truthy.
fn js_truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => {
            let value = n.as_f64();
            value != 0.0 && !value.is_nan()
        }
        Value::String(s) => !s.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn decode_code_assist_frame(frame: &RuntimeString) -> Option<Value> {
    runtime_json::parse(frame).ok()
}

/// Line-oriented SSE accumulator. Frames are `data:`-prefixed lines that
/// accumulate until a blank line flushes the buffered JSON (multi-line `data:`
/// joined with `\n`). A malformed JSON frame is skipped rather than aborting the
/// stream.
struct CodeAssistSseParser {
    data_lines: Vec<RuntimeString>,
}

impl CodeAssistSseParser {
    fn new() -> Self {
        Self {
            data_lines: Vec::new(),
        }
    }

    /// Consume one line (without its trailing newline). Returns a chunk when a
    /// blank line flushes buffered data.
    fn consume_line(&mut self, raw: &RuntimeString) -> Option<Value> {
        let line = if raw.ends_with("\r") {
            raw.slice_utf16(..raw.len_utf16().saturating_sub(1))
        } else {
            raw.clone()
        };
        const DATA_PREFIX: [u16; 5] = [
            b'd' as u16,
            b'a' as u16,
            b't' as u16,
            b'a' as u16,
            b':' as u16,
        ];
        if let Some(rest) = line.as_utf16().strip_prefix(&DATA_PREFIX) {
            self.data_lines
                .push(trim_ecmascript(&RuntimeString::from_utf16(rest.to_vec())));
            return None;
        }
        if line.is_empty() {
            self.flush()
        } else {
            None
        }
    }

    /// Flush buffered `data:` lines into a chunk (or `None` on empty/malformed).
    fn flush(&mut self) -> Option<Value> {
        if self.data_lines.is_empty() {
            return None;
        }
        let chunk = join_runtime_strings(&self.data_lines, "\n");
        self.data_lines.clear();
        decode_code_assist_frame(&chunk).map(|value| unwrap_code_assist(&value))
    }
}

fn join_runtime_strings(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let mut output = RuntimeString::default();
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            output = output.concat(&RuntimeString::from(separator));
        }
        output = output.concat(value);
    }
    output
}

fn is_ecmascript_whitespace(unit: u16) -> bool {
    matches!(
        unit,
        0x0009 | 0x000a | 0x000b | 0x000c | 0x000d | 0x0020 | 0x00a0 | 0x1680 | 0x2000
            ..=0x200a | 0x2028 | 0x2029 | 0x202f | 0x205f | 0x3000 | 0xfeff
    )
}

fn trim_ecmascript(value: &RuntimeString) -> RuntimeString {
    let units = value.as_utf16();
    let start = units
        .iter()
        .position(|unit| !is_ecmascript_whitespace(*unit))
        .unwrap_or(units.len());
    let end = units
        .iter()
        .rposition(|unit| !is_ecmascript_whitespace(*unit))
        .map_or(start, |index| index + 1);
    value.slice_utf16(start..end)
}

/// Read the response body as an SSE stream, splitting complete lines on `\n` and
/// unwrapping each flushed frame. A trailing partial line plus any buffered data
/// with no closing blank line are flushed at end-of-stream.
fn sse_chunk_stream(
    response: reqwest::Response,
    signal: AbortSignal,
) -> impl Stream<Item = Result<Value, ProviderError>> + Send {
    async_stream::stream! {
        let mut parser = CodeAssistSseParser::new();
        let mut byte_buf: Vec<u8> = Vec::new();
        let mut decoder = WhatwgUtf8::new();
        let mut body = Box::pin(response.bytes_stream());
        loop {
            let next = tokio::select! {
                _ = signal.cancelled() => return,
                chunk = body.next() => chunk,
            };
            match next {
                Some(Ok(bytes)) => {
                    byte_buf.extend_from_slice(&bytes);
                    // A newline byte can never be part of a multi-byte UTF-8
                    // sequence, so every complete line decodes on its own; only
                    // the trailing partial line is held back.
                    while let Some(pos) = byte_buf.iter().position(|&b| b == b'\n') {
                        let line: Vec<u8> = byte_buf.drain(..=pos).collect();
                        let decoded = decoder.feed(&line);
                        let text = decoded.slice_utf16(..decoded.len_utf16().saturating_sub(1));
                        if let Some(v) = parser.consume_line(&text) {
                            yield Ok(v);
                        }
                    }
                }
                Some(Err(err)) => {
                    yield Err(classify_error(&RawProviderError {
                        message: err.to_string(),
                        status: err.status().map(|s| s.as_u16()),
                        ..Default::default()
                    }));
                    return;
                }
                None => break,
            }
        }
        if !byte_buf.is_empty() {
            let text = decoder.feed(&byte_buf).concat(&decoder.finish());
            let _ = parser.consume_line(&text);
        } else {
            let _ = decoder.finish();
        }
        if let Some(v) = parser.flush() {
            yield Ok(v);
        }
    }
}

/// A random version-4 UUID (used as the per-request prompt id).
fn random_uuid() -> String {
    let mut b = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut b);
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]
    )
}

/// Flatten a response header map into the string map the classifier reads.
fn header_hashmap(headers: &reqwest::header::HeaderMap) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for (k, v) in headers.iter() {
        if let Ok(s) = v.to_str() {
            map.insert(k.as_str().to_string(), s.to_string());
        }
    }
    map
}

/// Fold an auth-source failure back into raw signals so the retry classifier
/// re-reads it as any other establishment failure (only its status and message
/// are load-bearing signals; the classifier re-derives the rest).
fn raw_from_provider_error(err: ProviderError) -> RawProviderError {
    RawProviderError {
        status: err.status,
        message: runtime_diagnostic(&err.message),
        ..Default::default()
    }
}

fn runtime_diagnostic(value: &RuntimeString) -> String {
    let mut output = String::new();
    for point in value.code_points() {
        match char::from_u32(point) {
            Some(character) => output.push(character),
            None => output.push_str(&format!("\\u{point:04x}")),
        }
    }
    output
}

/// Collapse the retry outcome onto a single classified error. An abort never
/// reaches a surfaced error event (the caller checks the signal first), so it
/// maps to a benign placeholder.
fn retry_err_to_provider(err: RetryError) -> ProviderError {
    match err {
        RetryError::Provider(pe) => pe,
        RetryError::Aborted => classify_error(&RawProviderError {
            message: "aborted".to_string(),
            ..Default::default()
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    macro_rules! exact_json {
        ($($json:tt)+) => {{
            runtime_json::parse(&RuntimeString::from(stringify!($($json)+)))
                .expect("test JSON literal must parse as exact runtime JSON")
        }};
    }

    fn decode_sse_test_line(bytes: &[u8]) -> RuntimeString {
        let mut decoder = WhatwgUtf8::new();
        decoder.feed(bytes).concat(&decoder.finish())
    }

    /// Buffer-oriented mirror of [`sse_chunk_stream`] for unit tests: feed a
    /// complete SSE byte buffer, get the unwrapped chunks in order.
    fn parse_code_assist_sse_bytes(body: &[u8]) -> Vec<Value> {
        let mut parser = CodeAssistSseParser::new();
        let mut out = Vec::new();
        let mut start = 0usize;
        for (i, &byte) in body.iter().enumerate() {
            if byte == b'\n' {
                let line = decode_sse_test_line(&body[start..i]);
                if let Some(v) = parser.consume_line(&line) {
                    out.push(v);
                }
                start = i + 1;
            }
        }
        if start < body.len() {
            let line = decode_sse_test_line(&body[start..]);
            let _ = parser.consume_line(&line);
        }
        if let Some(v) = parser.flush() {
            out.push(v);
        }
        out
    }

    fn ca_chunk(text: &str, finish: Option<&str>, usage: Option<Value>) -> Value {
        let mut part = Map::new();
        part.insert("text", Value::String(RuntimeString::from(text)));
        let mut content = Map::new();
        content.insert("parts", Value::Array(vec![Value::Object(part)]));
        let mut candidate = Map::new();
        candidate.insert("content", Value::Object(content));
        if let Some(f) = finish {
            candidate.insert("finishReason", Value::String(RuntimeString::from(f)));
        }
        let mut response = Map::new();
        response.insert("candidates", Value::Array(vec![Value::Object(candidate)]));
        response.insert(
            "modelVersion",
            Value::String(RuntimeString::from("gemini-2.5-pro")),
        );
        if let Some(u) = usage {
            response.insert("usageMetadata", u);
        }
        let mut chunk = Map::new();
        chunk.insert("response", Value::Object(response));
        chunk.insert("traceId", Value::String(RuntimeString::from("trace-1")));
        Value::Object(chunk)
    }

    fn sse_body(chunks: &[Value]) -> Vec<u8> {
        let mut body = Vec::new();
        for c in chunks {
            body.extend_from_slice(b"data: ");
            body.extend_from_slice(&runtime_json::stringify(c).to_node_utf8());
            body.extend_from_slice(b"\n\n");
        }
        body
    }

    async fn capture_one_http_request(listener: TcpListener) -> (String, String) {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut header_end = None;
        let mut content_length = None;
        loop {
            let mut chunk = [0_u8; 4096];
            let read = socket.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "client closed before completing the request");
            request.extend_from_slice(&chunk[..read]);

            if header_end.is_none() {
                header_end = request
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|position| position + 4);
                if let Some(end) = header_end {
                    let headers = std::str::from_utf8(&request[..end]).unwrap();
                    content_length = headers.lines().find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    });
                }
            }

            if let (Some(end), Some(length)) = (header_end, content_length) {
                if request.len() >= end + length {
                    break;
                }
            }
        }

        let end = header_end.unwrap();
        let length = content_length.unwrap();
        let headers = String::from_utf8(request[..end].to_vec()).unwrap();
        let body = String::from_utf8(request[end..end + length].to_vec()).unwrap();

        let event = "data: {\"response\":{\"candidates\":[]}}\n\n";
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{event}",
            event.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
        (headers, body)
    }

    #[tokio::test]
    async fn request_body_uses_surrogate_aware_javascript_stringify() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(capture_one_http_request(listener));
        let params = runtime_json::parse(&RuntimeString::from(
            r#"{"model":"gemini-test","contents":[{"role":"model","parts":[{"functionCall":{"name":"echo","args":{"\uD800":"high"}}}]}],"config":{}}"#,
        ))
        .unwrap();

        let stream = open_code_assist_stream(
            &params,
            CodeAssistStreamOptions {
                endpoint: format!("http://{address}"),
                retry: RetryOptions {
                    max_attempts: Some(1),
                    ..Default::default()
                },
                signal: AbortSignal::new(),
                auth_source: None,
                project: Some("project-test".into()),
                session_id: Some("session-test".into()),
            },
        )
        .await
        .unwrap();
        drop(stream);
        let (headers, body) = tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .expect("Code Assist request reached the local server")
            .unwrap();

        assert!(headers
            .lines()
            .any(|line| line.eq_ignore_ascii_case("content-type: application/json")));
        assert!(
            body.contains(r#""args":{"\ud800":"high"}"#),
            "raw Code Assist body did not carry the canonical surrogate escape: {body}"
        );
        assert!(
            !body.contains('\u{fdd0}'),
            "the internal surrogate sentinel leaked onto the Code Assist wire: {body}"
        );
    }

    #[test]
    fn envelope_hoists_and_wraps_system_instruction() {
        let params = exact_json!({
            "model": "gemini-2.5-pro",
            "contents": [{ "role": "user", "parts": [{ "text": "hi" }] }],
            "config": { "maxOutputTokens": 1024, "systemInstruction": "You are helpful." }
        });
        let env = to_code_assist_envelope(&params, Some("proj-1"), None, Some("uuid-x"));
        assert_eq!(env["model"], exact_json!("gemini-2.5-pro"));
        assert_eq!(env["project"], exact_json!("proj-1"));
        assert_eq!(env["user_prompt_id"], exact_json!("uuid-x"));
        assert_eq!(
            env["request"]["contents"],
            exact_json!([{ "role": "user", "parts": [{ "text": "hi" }] }])
        );
        assert_eq!(
            env["request"]["systemInstruction"],
            exact_json!({ "parts": [{ "text": "You are helpful." }] })
        );
        assert_eq!(
            env["request"]["generationConfig"]["maxOutputTokens"],
            exact_json!(1024)
        );
        assert!(env["request"]["generationConfig"]
            .get("systemInstruction")
            .is_none());
    }

    #[test]
    fn envelope_omits_absent_project_and_carries_session_id() {
        let params = exact_json!({ "model": "m", "contents": [], "config": {} });
        let env = to_code_assist_envelope(&params, None, Some("sess-9"), Some("pid"));
        assert!(env.get("project").is_none());
        assert_eq!(env["request"]["session_id"], exact_json!("sess-9"));
        assert_eq!(env["request"]["generationConfig"], exact_json!({}));
    }

    #[test]
    fn envelope_hoists_truthy_tools_toolconfig_cachedcontent() {
        let params = exact_json!({
            "model": "m",
            "contents": [],
            "config": { "tools": [], "toolConfig": { "x": 1 }, "cachedContent": "cache/1", "maxOutputTokens": 5 }
        });
        let env = to_code_assist_envelope(&params, None, None, None);
        // An empty array is truthy, so `tools` is still hoisted.
        assert_eq!(env["request"]["tools"], exact_json!([]));
        assert_eq!(env["request"]["toolConfig"], exact_json!({ "x": 1 }));
        assert_eq!(env["request"]["cachedContent"], exact_json!("cache/1"));
        assert!(env["request"]["generationConfig"].get("tools").is_none());
        assert_eq!(
            env["request"]["generationConfig"]["maxOutputTokens"],
            exact_json!(5)
        );
        assert!(env.get("user_prompt_id").is_none());
    }

    #[test]
    fn envelope_passes_object_system_instruction_through() {
        let params = exact_json!({
            "model": "m",
            "contents": [],
            "config": { "systemInstruction": { "parts": [{ "text": "x" }] } }
        });
        let env = to_code_assist_envelope(&params, None, None, None);
        assert_eq!(
            env["request"]["systemInstruction"],
            exact_json!({ "parts": [{ "text": "x" }] })
        );
    }

    #[test]
    fn sse_parses_a_single_frame_unwrapped() {
        let chunk = ca_chunk(
            "hello",
            Some("STOP"),
            Some(
                exact_json!({ "promptTokenCount": 10, "candidatesTokenCount": 5, "totalTokenCount": 15 }),
            ),
        );
        let out = parse_code_assist_sse_bytes(&sse_body(&[chunk]));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["responseId"], exact_json!("trace-1"));
        assert_eq!(out[0]["modelVersion"], exact_json!("gemini-2.5-pro"));
        assert_eq!(
            out[0]["candidates"][0]["content"]["parts"][0]["text"],
            exact_json!("hello")
        );
        assert_eq!(out[0]["usageMetadata"]["totalTokenCount"], exact_json!(15));
    }

    #[test]
    fn sse_function_call_args_round_trip_lone_surrogate_without_marker_leak() {
        let frame = runtime_json::parse(&RuntimeString::from(
            r#"{"response":{"candidates":[{"content":{"parts":[{"functionCall":{"name":"echo","args":{"\uD800":"high","low":"\uDC00"}}}]}}]}}"#,
        ))
        .unwrap();
        let mut body = b"data: ".to_vec();
        body.extend_from_slice(&runtime_json::stringify(&frame).to_node_utf8());
        body.extend_from_slice(b"\n\n");

        let out = parse_code_assist_sse_bytes(&body);
        assert_eq!(out.len(), 1, "valid JavaScript JSON frame was dropped");
        let args = &out[0]["candidates"][0]["content"]["parts"][0]["functionCall"]["args"];
        let replay_wire = runtime_json::stringify(args);
        assert_eq!(replay_wire, r#"{"\ud800":"high","low":"\udc00"}"#);
        assert!(
            !replay_wire.contains("\u{fdd0}"),
            "the internal surrogate sentinel leaked from the decoded SSE frame: {replay_wire:?}"
        );
    }

    #[test]
    fn sse_parses_multiple_frames_in_order() {
        let out = parse_code_assist_sse_bytes(&sse_body(&[
            ca_chunk("a", None, None),
            ca_chunk("b", Some("STOP"), None),
        ]));
        assert_eq!(out.len(), 2);
        assert_eq!(
            out[0]["candidates"][0]["content"]["parts"][0]["text"],
            exact_json!("a")
        );
        assert_eq!(
            out[1]["candidates"][0]["content"]["parts"][0]["text"],
            exact_json!("b")
        );
    }

    #[test]
    fn sse_skips_a_malformed_frame() {
        let bad = b"data: {not json}\n\ndata: {\"response\":{\"candidates\":[]}}\n\n";
        let out = parse_code_assist_sse_bytes(bad);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["candidates"], exact_json!([]));
    }

    #[test]
    fn sse_joins_multi_line_data() {
        let ml = b"data: {\"response\":{\"candidates\":[]},\ndata: \"traceId\":\"t2\"}\n\n";
        let out = parse_code_assist_sse_bytes(ml);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["responseId"], exact_json!("t2"));
    }

    #[test]
    fn sse_handles_crlf_and_trailing_frame_without_blank_line() {
        let crlf = b"data: {\"response\":{\"candidates\":[]}}\r\n\r\n";
        let out = parse_code_assist_sse_bytes(crlf);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["candidates"], exact_json!([]));

        let trailing = b"data: {\"response\":{\"candidates\":[],\"modelVersion\":\"mv\"}}";
        let out = parse_code_assist_sse_bytes(trailing);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["modelVersion"], exact_json!("mv"));
    }

    #[test]
    fn unwrap_defaults_candidates_and_carries_present_fields() {
        let u = unwrap_code_assist(&exact_json!({
            "response": { "candidates": null, "usageMetadata": {} }
        }));
        assert_eq!(u["candidates"], exact_json!([]));
        assert_eq!(u["usageMetadata"], exact_json!({}));
        assert!(u.get("responseId").is_none());
        assert!(u.get("modelVersion").is_none());
    }
}
