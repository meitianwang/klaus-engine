//! Loopback-only translation of the source's model wire, without another agent runtime.

use super::{auth, catalog, cloud_base, cloud_headers, stream_client};
use bytes::Bytes;
use futures::StreamExt;
use http_body_util::{BodyExt, Full, Limited, StreamBody};
use hyper::{
    body::{Frame, Incoming},
    service::service_fn,
    Request, Response, StatusCode,
};
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use std::convert::Infallible;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Semaphore};

type Body = http_body_util::combinators::UnsyncBoxBody<Bytes, Infallible>;
struct Running {
    generation: String,
    base: String,
    token: String,
    shutdown: tokio::sync::watch::Sender<bool>,
}
fn running() -> &'static Mutex<Option<Running>> {
    static RUNNING: OnceLock<Mutex<Option<Running>>> = OnceLock::new();
    RUNNING.get_or_init(|| Mutex::new(None))
}

pub(super) async fn ensure_started(generation: &str) -> Result<(String, String), String> {
    let mut state = running().lock().await;
    if let Some(running) = state
        .as_ref()
        .filter(|running| running.generation == generation)
    {
        return Ok((running.base.clone(), running.token.clone()));
    }
    if let Some(previous) = state.take() {
        previous.shutdown.send_replace(true);
    }
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|_| "trae_transport_unavailable")?;
    let port = listener
        .local_addr()
        .map_err(|_| "trae_transport_unavailable")?
        .port();
    let token = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let base = format!("http://127.0.0.1:{port}/v1");
    let generation = generation.to_owned();
    let private_token = token.clone();
    let owner = generation.clone();
    let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        let slots = Arc::new(Semaphore::new(32));
        loop {
            let accepted = tokio::select! {
                result=listener.accept()=>result,
                _=shutdown_rx.changed()=>return,
            };
            let (socket, address) = match accepted {
                Ok(value) => value,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            if !address.ip().is_loopback() {
                continue;
            }
            let Ok(permit) = slots.clone().try_acquire_owned() else {
                continue;
            };
            let generation = owner.clone();
            let token = private_token.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let service = service_fn(move |request| {
                    handle(request, port, token.clone(), generation.clone())
                });
                let connection = hyper::server::conn::http1::Builder::new()
                    .keep_alive(false)
                    .serve_connection(TokioIo::new(socket), service);
                // No adapter-side connection deadline: a served inference
                // stream lives as long as the engine-level stream budgets
                // allow, and keep_alive(false) closes the socket after each
                // response so nothing lingers idle.
                let _ = connection.await;
            });
        }
    });
    *state = Some(Running {
        generation,
        base: base.clone(),
        token: token.clone(),
        shutdown,
    });
    Ok((base, token))
}

fn response(status: StatusCode, kind: &'static str, text: impl Into<Bytes>) -> Response<Body> {
    let mut response = Response::new(
        Full::new(text.into())
            .map_err(|error| match error {})
            .boxed_unsync(),
    );
    *response.status_mut() = status;
    response.headers_mut().insert(
        "content-type",
        hyper::header::HeaderValue::from_static(kind),
    );
    response
}
fn error(status: StatusCode, code: &str) -> Response<Body> {
    response(
        status,
        "application/json",
        json!({"error":{"message":code,"type":"trae_error","code":code}}).to_string(),
    )
}

fn service_error(
    record: super::errors::ServiceError,
    model: &str,
    account: &str,
) -> Response<Body> {
    let envelope = record.envelope(model, account);
    let chunk = json!({"id":"trae-error","object":"chat.completion.chunk","model":model,"choices":[{"index":0,
        "delta":{"reasoning_content":format!("{}{envelope}",super::provider::marker())},"finish_reason":null}]});
    response(
        StatusCode::OK,
        "text/event-stream",
        format!("data: {chunk}\n\ndata: [DONE]\n\n"),
    )
}

async fn handle(
    request: Request<Incoming>,
    port: u16,
    token: String,
    generation: String,
) -> Result<Response<Body>, Infallible> {
    if request.method() != hyper::Method::POST
        || request.uri().path() != "/v1/chat/completions"
        || request.uri().query().is_some()
    {
        return Ok(error(StatusCode::NOT_FOUND, "trae_transport_route_invalid"));
    }
    if request
        .headers()
        .get("host")
        .and_then(|value| value.to_str().ok())
        != Some(&format!("127.0.0.1:{port}"))
        || request
            .headers()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            != Some(&format!("Bearer {token}"))
    {
        return Ok(error(
            StatusCode::UNAUTHORIZED,
            "trae_transport_unauthorized",
        ));
    }
    let bytes = match tokio::time::timeout(
        Duration::from_secs(15),
        Limited::new(request.into_body(), 32 * 1024 * 1024).collect(),
    )
    .await
    {
        Ok(Ok(value)) => value.to_bytes(),
        _ => {
            return Ok(error(
                StatusCode::BAD_REQUEST,
                "trae_transport_request_invalid",
            ))
        }
    };
    let request: Value = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(_) => {
            return Ok(error(
                StatusCode::BAD_REQUEST,
                "trae_transport_request_invalid",
            ))
        }
    };
    let model = match request["model"]
        .as_str()
        .filter(|model| !model.is_empty() && model.len() <= 2048)
    {
        Some(model) => model.to_owned(),
        None => return Ok(error(StatusCode::BAD_REQUEST, "trae_model_invalid")),
    };
    let credential = match auth::credential(Some(&generation)).await {
        Ok(value) => value,
        Err(message) => return Ok(error(StatusCode::UNAUTHORIZED, &message)),
    };
    let (config, detail, effort) = match catalog::selected(&model).await {
        Ok(value) => value,
        Err(message) => return Ok(error(StatusCode::BAD_REQUEST, &message)),
    };
    let body = match encode(&request, &config, &detail, effort.as_deref()) {
        Ok(body) => body,
        Err(message) => return Ok(error(StatusCode::BAD_REQUEST, &message)),
    };
    if !auth::is_current(&generation) {
        return Ok(error(StatusCode::UNAUTHORIZED, "trae_credential_changed"));
    }
    let mut headers = match cloud_headers(&credential) {
        Ok(value) => value,
        Err(message) => return Ok(error(StatusCode::UNAUTHORIZED, &message)),
    };
    headers.insert(
        "x-app-function",
        hyper::header::HeaderValue::from_static("chat_v3"),
    );
    headers.insert(
        "x-ide-function",
        hyper::header::HeaderValue::from_static("chat_v3"),
    );
    for (field, header) in [
        ("_requestPin", "x-request-pin"),
        ("_requestedAt", "x-requested-at"),
    ] {
        if let Some(value) = config[field].as_str() {
            match hyper::header::HeaderValue::from_str(value) {
                Ok(value) => {
                    headers.insert(header, value);
                }
                Err(_) => {
                    return Ok(error(
                        StatusCode::BAD_GATEWAY,
                        "trae_catalog_header_invalid",
                    ))
                }
            }
        }
    }
    let mut changes = auth::changes();
    if !auth::is_current(&generation) {
        return Ok(error(StatusCode::UNAUTHORIZED, "trae_credential_changed"));
    }
    let outgoing = stream_client()
        .post(format!("{}/api/ide/v2/llm_raw_chat", cloud_base()))
        .headers(headers)
        .json(&body);
    let upstream = match tokio::select! {
        result=outgoing.send()=>result,
        _=changes.changed()=>return Ok(error(StatusCode::UNAUTHORIZED,"trae_credential_changed")),
    } {
        Ok(response) => response,
        Err(_) => {
            return Ok(error(
                StatusCode::BAD_GATEWAY,
                "trae_request_transport_failed",
            ))
        }
    };
    if !upstream.status().is_success() {
        let error = super::errors::from_response(upstream).await;
        return Ok(service_error(
            error,
            &model,
            &auth::account_scope().unwrap_or_default(),
        ));
    }
    if !upstream
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("text/event-stream"))
    {
        return Ok(error(
            StatusCode::BAD_GATEWAY,
            "trae_stream_content_type_invalid",
        ));
    }
    let stream = async_stream::stream! {
        let mut source=upstream.bytes_stream();
        let mut pending=Vec::<u8>::new();
        let mut state=StreamState::new(&model);
        let mut total=0usize;
        let initial=json!({"id":state.id,"object":"chat.completion.chunk","model":model,"choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]});
        yield Ok::<Frame<Bytes>,Infallible>(Frame::data(Bytes::from(format!("data: {initial}\n\n"))));
        loop {
            // No adapter-side frame deadline: the engine-level idle stream
            // budget owns a stalled upstream and cancels it by dropping this
            // request, so a per-frame cap here must not fire first and turn a
            // recoverable stall into a terminal transport error.
            let next=tokio::select! {
                result=source.next()=>result,
                _=changes.changed()=>{
                    yield Ok(Frame::data(Bytes::from(stream_error("trae_credential_changed"))));
                    return;
                }
            };
            let chunk=match next {Some(Ok(value))=>value,None=>break,Some(Err(_))=>{yield Ok(Frame::data(Bytes::from(stream_interrupted("trae_stream_transport_failed",&state.model,&state.account))));return;}};
            if !auth::is_current(&generation){yield Ok(Frame::data(Bytes::from(stream_error("trae_credential_changed"))));return;}
            total=total.saturating_add(chunk.len());
            if total>64*1024*1024||pending.len().saturating_add(chunk.len())>2*1024*1024{
                yield Ok(Frame::data(Bytes::from(stream_error("trae_stream_too_large"))));return;
            }
            pending.extend_from_slice(&chunk);
            while let Some((position,length))=boundary(&pending){
                let event=pending.drain(..position+length).collect::<Vec<_>>();
                match state.event(&event){
                    Ok(values)=>for value in values{yield Ok(Frame::data(Bytes::from(format!("data: {value}\n\n"))));},
                    Err(message)=>{yield Ok(Frame::data(Bytes::from(stream_error(&message))));return;},
                }
                if state.finished {yield Ok(Frame::data(Bytes::from_static(b"data: [DONE]\n\n")));return;}
            }
        }
        if !pending.iter().all(u8::is_ascii_whitespace){
            match state.event(&pending){
                Ok(values)=>for value in values{yield Ok(Frame::data(Bytes::from(format!("data: {value}\n\n"))));},
                Err(message)=>{yield Ok(Frame::data(Bytes::from(stream_error(&message))));return;},
            }
        }
        if !state.finished {yield Ok(Frame::data(Bytes::from(stream_interrupted("trae_stream_incomplete",&state.model,&state.account))));}
        else {yield Ok(Frame::data(Bytes::from_static(b"data: [DONE]\n\n")));}
    };
    let mut response = Response::new(StreamBody::new(stream).boxed_unsync());
    response.headers_mut().insert(
        "content-type",
        hyper::header::HeaderValue::from_static("text/event-stream"),
    );
    response.headers_mut().insert(
        "cache-control",
        hyper::header::HeaderValue::from_static("no-store"),
    );
    Ok(response)
}

fn native_message(message: &Value, model: &str, account: &str) -> Result<Value, String> {
    let role = message["role"].as_str().ok_or("trae_message_invalid")?;
    if !["system", "user", "assistant", "tool"].contains(&role) {
        return Err("trae_message_role_unsupported".into());
    }
    let content = match &message["content"] {
        Value::String(text) => json!([{"type":"text","text":text}]),
        Value::Array(parts) => {
            if parts
                .iter()
                .any(|part| !matches!(part["type"].as_str(), Some("text" | "image_url")))
            {
                return Err("trae_media_input_unsupported".into());
            }
            json!(parts)
        }
        Value::Null => json!([]),
        _ => return Err("trae_message_invalid".into()),
    };
    let mut encoded = json!({"role":role,"content":content});
    let context = message["reasoning_content"]
        .as_str()
        .and_then(|value| super::provider::marker_context(value, model, account));
    if let Some(context) = &context {
        encoded["extra_info"] = context["extraInfo"].clone();
    }
    for field in ["name", "tool_call_id"] {
        if let Some(value) = message.get(field) {
            encoded[field] = value.clone();
        }
    }
    if let Some(calls) = message["tool_calls"].as_array() {
        let mut mapped = Vec::new();
        for (index, call) in calls.iter().enumerate() {
            let id = call["id"]
                .as_str()
                .filter(|id| !id.is_empty())
                .ok_or("trae_tool_call_invalid")?;
            let function = &call["function"];
            if !function.is_object() {
                return Err("trae_tool_call_invalid".into());
            }
            mapped.push(json!({"index":index,"id":id,"type":"function","function_call":function}));
        }
        encoded["tool_calls"] = json!(mapped);
        if let Some(original) = context
            .as_ref()
            .and_then(|context| context["toolCalls"].as_array())
        {
            let mapped = encoded["tool_calls"]
                .as_array()
                .ok_or("trae_tool_call_invalid")?;
            if original.len() == mapped.len()
                && mapped.iter().all(|call| {
                    original.iter().any(|original| {
                        original["id"] == call["id"]
                            && original["function_call"]["name"] == call["function_call"]["name"]
                            && original["function_call"]["arguments"]
                                == call["function_call"]["arguments"]
                    })
                })
            {
                encoded["tool_calls"] = json!(original);
            }
        }
    }
    Ok(encoded)
}

pub(super) fn encode(
    request: &Value,
    config: &Value,
    detail: &Value,
    effort: Option<&str>,
) -> Result<Value, String> {
    let model = request["model"].as_str().unwrap_or_default();
    let account = auth::account_scope().unwrap_or_default();
    let mut messages = request["messages"]
        .as_array()
        .ok_or("trae_messages_invalid")?
        .iter()
        .map(|message| native_message(message, model, &account))
        .collect::<Result<Vec<_>, _>>()?;
    let mut names = std::collections::HashMap::new();
    for message in &messages {
        for call in message["tool_calls"].as_array().into_iter().flatten() {
            if let (Some(id), Some(name)) =
                (call["id"].as_str(), call["function_call"]["name"].as_str())
            {
                names.insert(id.to_owned(), name.to_owned());
            }
        }
    }
    for message in &mut messages {
        if message["role"] == "tool" && message.get("name").is_none() {
            if let Some(name) = message["tool_call_id"]
                .as_str()
                .and_then(|id| names.get(id))
            {
                message["name"] = json!(name);
            }
        }
    }
    // The service normalizer rejects a `tool` message whose content is an empty
    // part array (4001) or a lone empty text part, which it drops before pairing
    // and then reports the assistant's tool_call unanswered (4027). The native
    // wire never carries empty text parts: tool output text stays non-empty and
    // media travels in its own parts. Match that invariant here — drop empty
    // text parts, keep media parts, and when nothing remains let an explanatory
    // text part hold the pairing, the way the native client substitutes
    // explanatory text when media cannot ride the request.
    for message in &mut messages {
        if message["role"] != "tool" {
            continue;
        }
        if let Some(parts) = message["content"].as_array_mut() {
            parts.retain(|part| {
                !(part["type"].as_str() == Some("text")
                    && part["text"].as_str().unwrap_or_default().trim().is_empty())
            });
            if parts.is_empty() {
                parts.push(
                    json!({"type":"text","text":"(This tool result contained no textual output.)"}),
                );
            }
        } else {
            message["content"] = json!([
                {"type":"text","text":"(This tool result contained no textual output.)"}
            ]);
        }
    }
    let mut body = json!({"config_name":config["config_name"],"model_name":detail["model_name"],"encrypted_model_params":detail["encrypted_model_params"],
        "messages":messages,"session_id":uuid::Uuid::new_v4().to_string(),"conversation_id":uuid::Uuid::new_v4().to_string(),"stream":true,
        "config_source":config["config_source"]});
    if let Some(tools) = request.get("tools") {
        let mut tools = tools.as_array().ok_or("trae_tools_invalid")?.clone();
        for tool in &mut tools {
            let function = tool
                .get_mut("function")
                .and_then(Value::as_object_mut)
                .ok_or("trae_tools_invalid")?;
            let parameters = function
                .get("parameters")
                .filter(|value| value.is_object())
                .ok_or("trae_tool_schema_invalid")?;
            let serialized =
                serde_json::to_string(parameters).map_err(|_| "trae_tool_schema_invalid")?;
            function.insert("parameters".into(), json!(serialized));
        }
        body["tools"] = json!(tools);
    }
    if let Some(choice) = request.get("tool_choice") {
        body["tool_choice"] = choice.clone();
    }
    if let Some(effort) = effort {
        body["reasoning_effort_level"] = json!(effort);
    }
    Ok(body)
}

fn boundary(bytes: &[u8]) -> Option<(usize, usize)> {
    let lf = bytes
        .windows(2)
        .position(|part| part == b"\n\n")
        .map(|position| (position, 2));
    let crlf = bytes
        .windows(4)
        .position(|part| part == b"\r\n\r\n")
        .map(|position| (position, 4));
    match (lf, crlf) {
        (Some(a), Some(b)) => Some(if a.0 < b.0 { a } else { b }),
        (Some(value), None) | (None, Some(value)) => Some(value),
        _ => None,
    }
}
fn stream_error(message: &str) -> String {
    format!(
        "data: {}\n\n",
        json!({"error":{"message":message,"type":"trae_error","code":message}})
    )
}
/// Emit a source-owned interruption envelope instead of a bare error frame so
/// the shared retry policy sees a classified network failure when the upstream
/// event stream stalls or ends without a terminal frame.
pub(super) fn stream_interrupted(message: &str, model: &str, account: &str) -> String {
    let envelope =
        super::errors::ServiceError::stream_interrupted(message).envelope(model, account);
    format!(
        "data: {}\n\n",
        json!({"id":"trae-error","object":"chat.completion.chunk","model":model,"choices":[{"index":0,
            "delta":{"reasoning_content":format!("{}{envelope}",super::provider::marker())},"finish_reason":null}]})
    )
}

pub(super) struct StreamState {
    id: String,
    model: String,
    account: String,
    calls: std::collections::BTreeMap<u64, Call>,
    has_text: bool,
    pub(super) finished: bool,
}
#[derive(Default)]
struct Call {
    id: String,
    name: String,
    arguments: String,
    emitted: usize,
    uses_partial_arguments: bool,
    raw: Value,
}
impl Call {
    fn append_arguments(&mut self, function: &Value) -> Result<(), String> {
        let arguments = function["arguments"]
            .as_str()
            .filter(|value| !value.is_empty());
        if let Some(partial) = function["partial_arguments"]
            .as_str()
            .filter(|value| !value.is_empty())
        {
            self.uses_partial_arguments = true;
            self.arguments.push_str(partial);
            if let Some(snapshot) = arguments {
                if !snapshot.starts_with(&self.arguments) {
                    return Err("trae_stream_tool_arguments_changed".into());
                }
                self.arguments = snapshot.into();
            }
        } else if let Some(arguments) = arguments {
            if self.uses_partial_arguments {
                if !arguments.starts_with(&self.arguments) {
                    return Err("trae_stream_tool_arguments_changed".into());
                }
                self.arguments = arguments.into();
            } else {
                self.arguments.push_str(arguments);
            }
        }
        Ok(())
    }

    fn merge_raw(&mut self, call: &Value) {
        let mut raw = self.raw.as_object().cloned().unwrap_or_default();
        for (key, value) in call.as_object().into_iter().flatten() {
            if key == "function_call" {
                let mut function = raw
                    .get(key)
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_default();
                for (key, value) in value.as_object().into_iter().flatten() {
                    if !value.is_null() && value.as_str() != Some("") {
                        function.insert(key.clone(), value.clone());
                    }
                }
                raw.insert(key.clone(), Value::Object(function));
            } else if !value.is_null() && value.as_str() != Some("") {
                raw.insert(key.clone(), value.clone());
            }
        }
        self.raw = Value::Object(raw);
        self.raw["id"] = json!(self.id);
        self.raw["function_call"]["name"] = json!(self.name);
        self.raw["function_call"]["arguments"] = json!(self.arguments);
        self.raw["function_call"]["partial_arguments"] = Value::Null;
    }
}
impl StreamState {
    pub(super) fn new(model: &str) -> Self {
        Self {
            id: format!("chatcmpl-{}", uuid::Uuid::new_v4()),
            model: model.into(),
            account: auth::account_scope().unwrap_or_default(),
            calls: std::collections::BTreeMap::new(),
            has_text: false,
            finished: false,
        }
    }
    pub(super) fn event(&mut self, bytes: &[u8]) -> Result<Vec<Value>, String> {
        let text = std::str::from_utf8(bytes).map_err(|_| "trae_stream_encoding_invalid")?;
        let mut event = "";
        let mut data = Vec::new();
        for line in text.lines() {
            if let Some(value) = line.strip_prefix("event:") {
                event = value.trim();
            }
            if let Some(value) = line.strip_prefix("data:") {
                data.push(value.trim_start());
            }
        }
        if data.is_empty() {
            return Ok(Vec::new());
        }
        if matches!(
            event,
            "metadata" | "timing_cost" | "heartbeat" | "progress_notice"
        ) {
            return Ok(Vec::new());
        }
        let body: Value =
            serde_json::from_str(&data.join("\n")).map_err(|_| "trae_stream_event_invalid")?;
        let code = body["code"].as_i64().unwrap_or(0);
        if event == "error" || code != 0 {
            self.finished = true;
            let envelope = super::errors::ServiceError::from_native(&body, None, None)
                .envelope(&self.model, &self.account);
            return Ok(vec![
                json!({"id":self.id,"object":"chat.completion.chunk","model":self.model,
                "choices":[{"index":0,"delta":{"reasoning_content":format!("{}{envelope}",super::provider::marker())},"finish_reason":null}]}),
            ]);
        }
        let mut delta = json!({});
        if event == "extra_info" {
            let mut envelope = super::provider::envelope(&self.model, &self.account, body.clone());
            envelope["toolCalls"] = json!(self
                .calls
                .values()
                .map(|call| call.raw.clone())
                .collect::<Vec<_>>());
            delta["reasoning_content"] = json!(format!("{}{envelope}", super::provider::marker()));
        } else if event == "token_usage" {
            let envelope =
                super::provider::usage_envelope(&self.model, &self.account, body.clone());
            delta["reasoning_content"] = json!(format!("{}{envelope}", super::provider::marker()));
        } else if event == "output" {
            if let Some(text) = body["response"].as_str().filter(|text| !text.is_empty()) {
                delta["content"] = json!(text);
                self.has_text = true;
            }
            if let Some(text) = body["reasoning_content"]
                .as_str()
                .filter(|text| !text.is_empty())
            {
                delta["reasoning_content"] = json!(text);
            }
        } else if event != "done" {
            return Err("trae_stream_event_unsupported".into());
        }
        if event == "output" {
            if let Some(calls) = body["tool_calls"].as_array() {
                let mut mapped = Vec::new();
                for (index, call) in calls.iter().enumerate() {
                    let function = &call["function_call"];
                    if !function.is_object() {
                        return Err("trae_stream_tool_call_invalid".into());
                    }
                    let index = call["index"].as_u64().unwrap_or(index as u64);
                    if index > 1024 {
                        return Err("trae_stream_tool_index_invalid".into());
                    }
                    let state = self.calls.entry(index).or_default();
                    if let Some(id) = call["id"].as_str().filter(|id| !id.is_empty()) {
                        if !state.id.is_empty() && state.id != id {
                            return Err("trae_stream_tool_identity_changed".into());
                        }
                        state.id = id.into();
                    }
                    if let Some(name) = function["name"].as_str().filter(|name| !name.is_empty()) {
                        if !state.name.is_empty() && state.name != name {
                            return Err("trae_stream_tool_identity_changed".into());
                        }
                        state.name = name.into();
                    }
                    state.append_arguments(function)?;
                    state.merge_raw(call);
                    if !state.id.is_empty() && !state.name.is_empty() {
                        let suffix = &state.arguments[state.emitted..];
                        mapped.push(json!({"index":index,"id":state.id,"type":"function","function":{"name":state.name,"arguments":suffix}}));
                        state.emitted = state.arguments.len();
                    }
                }
                delta["tool_calls"] = json!(mapped);
            }
        }
        // This model leaves its explicit terminal reason empty; EOF is still incomplete.
        let empty_terminal = event == "done"
            && self.model.split("::effort=").next() == Some("DeepSeek-V4-Flash-Official");
        let complete_calls = !self.calls.is_empty()
            && self.calls.values().all(|call| {
                !call.id.is_empty()
                    && !call.name.is_empty()
                    && serde_json::from_str::<Value>(&call.arguments)
                        .is_ok_and(|args| args.is_object())
            });
        let finish = match body["finish_reason"].as_str() {
            Some("") if empty_terminal && complete_calls => Some("tool_calls"),
            Some("") if empty_terminal && self.calls.is_empty() && self.has_text => Some("stop"),
            None | Some("") => None,
            Some("stop" | "end_turn" | "stop_sequence") => Some("stop"),
            Some("tool_calls" | "tool_use") => Some("tool_calls"),
            Some("length" | "max_tokens") => Some("length"),
            Some(_) => return Err("trae_stream_stop_reason_unsupported".into()),
        };
        if event == "done" && finish.is_none() {
            return Err("trae_stream_stop_reason_missing".into());
        }
        self.finished = finish.is_some();
        let mut output = json!({"id":self.id,"object":"chat.completion.chunk","model":self.model,"choices":[{"index":0,"delta":delta,"finish_reason":finish}]});
        let mut usage = json!({});
        for key in ["prompt_tokens", "completion_tokens", "total_tokens"] {
            if let Some(value) = body[key].as_u64() {
                usage[key] = json!(value);
            }
        }
        if let Some(read) = body["cache_read_input_tokens"].as_u64() {
            usage["prompt_tokens_details"] = json!({"cached_tokens":read});
        }
        if usage.as_object().is_some_and(|usage| !usage.is_empty()) {
            output["usage"] = usage;
        }
        Ok(vec![output])
    }
}
