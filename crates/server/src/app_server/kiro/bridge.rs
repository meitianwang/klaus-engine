//! Loopback transport for source-scoped request and replay policies.
use super::{auth, catalog, client, headers, runtime_base};
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
use std::{
    convert::Infallible,
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{Mutex, Semaphore},
};
type Body = http_body_util::combinators::UnsyncBoxBody<Bytes, Infallible>;
struct Running {
    generation: String,
    base: String,
    token: String,
    shutdown: tokio::sync::watch::Sender<bool>,
}
fn running() -> &'static Mutex<Option<Running>> {
    static STATE: OnceLock<Mutex<Option<Running>>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(None))
}
pub(super) async fn ensure_started(generation: &str) -> Result<(String, String), String> {
    let mut state = running().lock().await;
    if let Some(r) = state.as_ref().filter(|r| r.generation == generation) {
        return Ok((r.base.clone(), r.token.clone()));
    }
    if let Some(old) = state.take() {
        old.shutdown.send_replace(true);
    }
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|_| "kiro_transport_unavailable")?;
    let port = listener
        .local_addr()
        .map_err(|_| "kiro_transport_unavailable")?
        .port();
    let token = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let base = format!("http://127.0.0.1:{port}/v1");
    let owner = generation.to_owned();
    let private_token = token.clone();
    let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        let slots = Arc::new(Semaphore::new(32));
        loop {
            let accepted = tokio::select! {v=listener.accept()=>v,_=shutdown_rx.changed()=>return};
            let Ok((socket, address)) = accepted else {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            };
            if !address.ip().is_loopback() {
                continue;
            }
            let Ok(permit) = slots.clone().try_acquire_owned() else {
                continue;
            };
            let token = private_token.clone();
            let generation = owner.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let service =
                    service_fn(move |r| handle(r, port, token.clone(), generation.clone()));
                let connection = hyper::server::conn::http1::Builder::new()
                    .keep_alive(false)
                    .serve_connection(TokioIo::new(socket), service);
                let _ = tokio::time::timeout(Duration::from_secs(600), connection).await;
            });
        }
    });
    *state = Some(Running {
        generation: generation.into(),
        base: base.clone(),
        token: token.clone(),
        shutdown,
    });
    Ok((base, token))
}
fn response(status: StatusCode, kind: &'static str, text: impl Into<Bytes>) -> Response<Body> {
    let mut r = Response::new(
        Full::new(text.into())
            .map_err(|e| match e {})
            .boxed_unsync(),
    );
    *r.status_mut() = status;
    r.headers_mut().insert(
        "content-type",
        hyper::header::HeaderValue::from_static(kind),
    );
    r
}
fn error(status: StatusCode, code: &str) -> Response<Body> {
    response(
        status,
        "application/json",
        json!({"error":{"message":code,"type":"kiro_error","code":code}}).to_string(),
    )
}
pub(super) fn encode(
    mut request: Value,
    c: &auth::Credential,
    generation: &str,
) -> Result<Value, String> {
    let model = request["model"]
        .as_str()
        .ok_or("kiro_model_invalid")?
        .to_owned();
    let object = request.as_object_mut().ok_or("kiro_request_invalid")?;
    let (base, _) = catalog::selection_parts(&model)?;
    object.insert("model".into(), json!(base));
    object.insert("origin".into(), json!("AI_EDITOR"));
    if let Some(profile) = &c.profile_arn {
        object.insert("profileArn".into(), json!(profile));
    }
    object.remove("include");
    // The native service owns this output limit and rejects the Responses knob.
    object.remove("max_output_tokens");
    // Automatic selection is the native default; forced selection has no wire equivalent.
    match object.remove("tool_choice") {
        None | Some(Value::Null) => {}
        Some(Value::String(choice)) if choice == "auto" => {}
        Some(Value::String(choice)) if choice == "none" => {
            object.remove("tools");
        }
        Some(_) => return Err("kiro_tool_choice_unsupported".into()),
    }
    if object.remove("parallel_tool_calls") == Some(json!(false))
        && object
            .get("tools")
            .and_then(Value::as_array)
            .is_some_and(|tools| !tools.is_empty())
    {
        return Err("kiro_parallel_tool_policy_unsupported".into());
    }
    object.insert("store".into(), json!(false));
    if let Some(tools) = object.get_mut("tools").and_then(Value::as_array_mut) {
        for tool in tools {
            if tool["type"] != "function" {
                return Err("kiro_tool_type_unsupported".into());
            }
            // The service accepts function schemas but has no strict/deferred wire flags.
            if let Some(fields) = tool.as_object_mut() {
                fields.remove("strict");
                fields.remove("defer_loading");
            }
        }
    }
    let instructions = object.remove("instructions");
    if instructions
        .as_ref()
        .is_some_and(|value| !value.is_string())
    {
        return Err("kiro_instructions_invalid".into());
    }
    if instructions.is_some() && !object.get("input").is_some_and(Value::is_array) {
        return Err("kiro_input_invalid".into());
    }
    if let Some(items) = object.get_mut("input").and_then(Value::as_array_mut) {
        // The native conversation carries host guidance in its initial message.
        // Keep it in a bounded preamble so it never becomes a new request after tool output.
        let trusted = |item: &Value| {
            matches!(
                item.get("role").and_then(Value::as_str),
                Some("system" | "developer")
            )
        };
        let mut preamble = Vec::new();
        if let Some(text) = instructions
            .as_ref()
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
        {
            preamble.push(format!(
                "<klaus_host_instructions>\n{text}\n</klaus_host_instructions>"
            ));
        }
        for item in items.iter().filter(|item| trusted(item)) {
            let text = match &item["content"] {
                Value::String(text) => text.clone(),
                Value::Array(parts) => parts
                    .iter()
                    .map(|part| {
                        part.get("text")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                            .ok_or("kiro_instructions_invalid")
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .join("\n"),
                _ => return Err("kiro_instructions_invalid".into()),
            };
            preamble.push(format!(
                "<klaus_runtime_guidance>\n{text}\n</klaus_runtime_guidance>"
            ));
        }
        // User messages and tool outputs are never parsed as trusted guidance.
        items.retain(|item| !trusted(item));
        if !preamble.is_empty() {
            items.splice(0..0, [
                json!({"role":"user","content":preamble.join("\n\n")}),
                json!({"role":"assistant","content":"I will follow the provided instructions."}),
            ]);
        }
        items.retain_mut(|item| {
            if item["type"] != "reasoning" {
                return true;
            }
            let Some(encoded) = item["encrypted_content"].as_str() else {
                return false;
            };
            let Ok(envelope) = serde_json::from_str::<Value>(encoded) else {
                return false;
            };
            if envelope["kind"] != "klaus.kiro.reasoning.v1"
                || envelope["account"].as_str() != Some(generation)
                || envelope["model"].as_str() != Some(&model)
            {
                return false;
            }
            let Some(native) = envelope["native"].as_str() else {
                return false;
            };
            if serde_json::from_str::<Value>(native).is_err() {
                return false;
            }
            item["encrypted_content"] = json!(native);
            item["summary"] = json!([]);
            true
        });
    }
    Ok(request)
}

pub(super) fn native_options(
    mut request: Value,
    row: &Value,
    selection: &str,
) -> Result<Value, String> {
    let (_, explicit) = catalog::selection_parts(selection)?;
    let schema = &row["providerMetadata"]["additionalModelRequestFieldsSchema"];
    let effort_schema = schema
        .pointer("/properties/output_config/properties/effort")
        .or_else(|| schema.pointer("/properties/reasoning/properties/effort"));
    let effort = explicit.or_else(|| {
        row["providerMetadata"]["nativeDefaultEffort"]
            .as_str()
            .map(str::to_owned)
    });
    let mut reasoning = serde_json::Map::new();
    let thinking_types = schema
        .pointer("/properties/thinking/properties/type/enum")
        .and_then(Value::as_array);
    let supports_type = |kind: &str| thinking_types.is_some_and(|v| v.iter().any(|v| v == kind));
    let selected_type = effort
        .as_deref()
        .filter(|kind| matches!(*kind, "disabled" | "adaptive") && supports_type(kind));
    let explicit_thinking_type = selected_type.map(str::to_owned);
    if let Some(kind) = &explicit_thinking_type {
        reasoning.insert("type".into(), json!(kind));
    }
    if let Some(effort) = effort.filter(|_| explicit_thinking_type.is_none()) {
        if effort == "default" {
            return Err("kiro_reasoning_effort_unsupported".into());
        }
        if !effort_schema
            .and_then(|v| v["enum"].as_array())
            .is_some_and(|values| values.iter().any(|v| v.as_str() == Some(&effort)))
        {
            return Err("kiro_reasoning_effort_unsupported".into());
        }
        reasoning.insert("effort".into(), json!(effort));
        if supports_type("adaptive") {
            reasoning.insert("type".into(), json!("adaptive"));
        }
    }
    if !reasoning.contains_key("type") {
        if let Some(kind) = schema
            .pointer("/properties/thinking/properties/type/default")
            .and_then(Value::as_str)
            .filter(|kind| supports_type(kind))
        {
            reasoning.insert("type".into(), json!(kind));
        }
    }
    if let Some(display) = schema
        .pointer("/properties/thinking/properties/display/default")
        .and_then(Value::as_str)
    {
        if matches!(display, "summarized" | "omitted") {
            reasoning.insert("summary".into(), json!(display));
        }
    }
    let object = request.as_object_mut().ok_or("kiro_request_invalid")?;
    object.remove("reasoning");
    if !reasoning.is_empty() {
        object.insert("reasoning".into(), Value::Object(reasoning));
    }
    Ok(request)
}

fn wrap_item(item: &mut Value, model: &str, generation: &str) {
    if item["type"] != "reasoning" {
        return;
    }
    if let Some(native) = item["encrypted_content"].as_str().filter(|s| !s.is_empty()) {
        item["encrypted_content"]=json!(json!({"kind":"klaus.kiro.reasoning.v1","account":generation,"model":model,"native":native}).to_string());
    }
}
pub(super) fn adapt(mut event: Value, model: &str, generation: &str) -> Value {
    if matches!(
        event["type"].as_str(),
        Some("response.reasoning_text.delta" | "response.reasoning.delta")
    ) {
        event["type"] = json!("response.reasoning_summary_text.delta");
        if event["delta"].is_object() {
            event["delta"] = event["delta"]["text"].clone();
        }
    }
    if let Some(response) = event.get_mut("response").and_then(Value::as_object_mut) {
        if response.contains_key("model") {
            response.insert("model".into(), json!(model));
        }
    }
    let kind = event["type"].as_str().unwrap_or_default().to_owned();
    if kind == "response.failed" || kind == "error" {
        let native = if kind == "response.failed" {
            &event["response"]["error"]
        } else {
            event.get("error").unwrap_or(&event)
        };
        let (code, original) = super::errors::classify(0, native);
        let normalized =
            json!({"message":format!("{code} ({original})"),"code":code,"type":original});
        if kind == "response.failed" {
            event["response"]["error"] = normalized;
        } else {
            event["error"] = normalized;
            event["code"] = json!(code);
            event["message"] = json!(format!("{code} ({original})"));
        }
    }
    if let Some(item) = event.get_mut("item") {
        wrap_item(item, model, generation)
    }
    if let Some(output) = event
        .pointer_mut("/response/output")
        .and_then(Value::as_array_mut)
    {
        for item in output {
            wrap_item(item, model, generation)
        }
    }
    event
}
fn frame_end(buffer: &[u8]) -> Option<(usize, usize)> {
    let lf = buffer
        .windows(2)
        .position(|w| w == b"\n\n")
        .map(|end| (end, 2));
    let crlf = buffer
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|end| (end, 4));
    match (lf, crlf) {
        (Some(a), Some(b)) => Some(if a.0 < b.0 { a } else { b }),
        (a, b) => a.or(b),
    }
}
async fn handle(
    request: Request<Incoming>,
    port: u16,
    token: String,
    generation: String,
) -> Result<Response<Body>, Infallible> {
    if request.method() != hyper::Method::POST
        || request.uri().path() != "/v1/responses"
        || request.uri().query().is_some()
    {
        return Ok(error(StatusCode::NOT_FOUND, "kiro_transport_route_invalid"));
    }
    if request.headers().get("host").and_then(|v| v.to_str().ok())
        != Some(&format!("127.0.0.1:{port}"))
        || request
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            != Some(&format!("Bearer {token}"))
    {
        return Ok(error(
            StatusCode::UNAUTHORIZED,
            "kiro_transport_unauthorized",
        ));
    }
    let bytes = match tokio::time::timeout(
        Duration::from_secs(15),
        Limited::new(request.into_body(), 32 * 1024 * 1024).collect(),
    )
    .await
    {
        Ok(Ok(v)) => v.to_bytes(),
        _ => {
            return Ok(error(
                StatusCode::BAD_REQUEST,
                "kiro_transport_request_invalid",
            ))
        }
    };
    let request: Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(_) => {
            return Ok(error(
                StatusCode::BAD_REQUEST,
                "kiro_transport_request_invalid",
            ))
        }
    };
    let model = match request["model"]
        .as_str()
        .filter(|s| !s.is_empty() && s.len() <= 512)
    {
        Some(v) => v.to_owned(),
        None => return Ok(error(StatusCode::BAD_REQUEST, "kiro_model_invalid")),
    };
    let c = match auth::credential(Some(&generation)).await {
        Ok(v) => v,
        Err(e) => return Ok(error(StatusCode::UNAUTHORIZED, &e)),
    };
    let rows = match catalog::rows(true)
        .await
        .map(|rows| catalog::expanded_rows(&rows))
    {
        Ok(v) => v,
        Err(e) => return Ok(error(StatusCode::BAD_REQUEST, &e)),
    };
    let Some(selected) = rows.iter().find(|r| r["key"].as_str() == Some(&model)) else {
        return Ok(error(StatusCode::BAD_REQUEST, "kiro_model_unavailable"));
    };
    let body = match encode(request, &c, &generation)
        .and_then(|body| native_options(body, selected, &model))
    {
        Ok(v) => v,
        Err(e) => return Ok(error(StatusCode::BAD_REQUEST, &e)),
    };
    let h = match headers(&c) {
        Ok(v) => v,
        Err(e) => return Ok(error(StatusCode::UNAUTHORIZED, &e)),
    };
    let mut changes = auth::changes();
    if !auth::is_current(&generation) {
        return Ok(error(StatusCode::UNAUTHORIZED, "kiro_credential_changed"));
    }
    let request = client()
        .post(format!("{}/v1/responses", runtime_base(&c)))
        .headers(h)
        .json(&body)
        .timeout(Duration::from_secs(600));
    let upstream = match tokio::select! {r=request.send()=>r,_=changes.changed()=>return Ok(error(StatusCode::UNAUTHORIZED,"kiro_credential_changed"))}
    {
        Ok(v) => v,
        Err(_) => {
            return Ok(error(
                StatusCode::BAD_GATEWAY,
                "kiro_request_transport_failed",
            ))
        }
    };
    if !upstream.status().is_success() {
        let (status, body, retry, request_id) = tokio::select! {
            value=super::errors::response(upstream)=>value,
            _=changes.changed()=>return Ok(error(StatusCode::UNAUTHORIZED,"kiro_credential_changed")),
        };
        let mut rejected = response(status, "application/json", body.to_string());
        for (key, value) in [("retry-after", retry), ("x-request-id", request_id)] {
            if let Some(value) = value.and_then(|v| hyper::header::HeaderValue::from_str(&v).ok()) {
                rejected.headers_mut().insert(key, value);
            }
        }
        return Ok(rejected);
    }
    let stream = async_stream::stream! {let mut upstream=upstream.bytes_stream();let mut buffer=Vec::new();let mut finished=false;loop{let chunk=tokio::select!{v=upstream.next()=>v,_=changes.changed()=>{yield Ok::<Frame<Bytes>,Infallible>(Frame::data(Bytes::from("data: {\"type\":\"error\",\"code\":\"kiro_credential_changed\",\"message\":\"kiro_credential_changed\"}\n\n")));return;}};let Some(chunk)=chunk else{break};let chunk=match chunk{Ok(v)=>v,Err(_)=>break};buffer.extend_from_slice(&chunk);if buffer.len()>4*1024*1024{break}while let Some((end,separator))=frame_end(&buffer){let frame:Vec<u8>=buffer.drain(..end+separator).collect();let Ok(frame)=std::str::from_utf8(&frame)else{break};let data=frame.lines().filter_map(|l|l.strip_prefix("data:").map(str::trim_start)).collect::<Vec<_>>().join("\n");if data.is_empty(){continue}
    if data=="[DONE]"{if finished{yield Ok::<Frame<Bytes>,Infallible>(Frame::data(Bytes::from_static(b"data: [DONE]\n\n")));return;}break}let Ok(event)=serde_json::from_str::<Value>(&data)else{yield Ok::<Frame<Bytes>,Infallible>(Frame::data(Bytes::from_static(b"data: {\"type\":\"error\",\"code\":\"kiro_stream_invalid\",\"message\":\"kiro_stream_invalid\"}\n\n")));return;};finished|=matches!(event["type"].as_str(),Some("response.completed"|"response.incomplete"|"response.failed"|"error"));let event=adapt(event,&model,&generation);yield Ok::<Frame<Bytes>,Infallible>(Frame::data(Bytes::from(format!("data: {event}\n\n"))));}}if !finished{yield Ok::<Frame<Bytes>,Infallible>(Frame::data(Bytes::from_static(b"data: {\"type\":\"error\",\"code\":\"kiro_stream_incomplete\",\"message\":\"kiro_stream_incomplete\"}\n\n")));}};
    let mut response = Response::new(StreamBody::new(stream).boxed_unsync());
    response.headers_mut().insert(
        "content-type",
        hyper::header::HeaderValue::from_static("text/event-stream"),
    );
    Ok(response)
}
