//! Content-free operation timing, including futures dropped by outer deadlines.

use serde_json::{json, Value};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Instant;

tokio::task_local! { static CONTEXT: Context; }
#[cfg(test)]
tokio::task_local! { static CAPTURE: Arc<Mutex<Vec<Value>>>; }

#[derive(Clone)]
struct Context {
    operation_id: String,
    span_id: String,
    parent_id: Option<String>,
    operation: &'static str,
    started: Instant,
    phase: Arc<Mutex<&'static str>>,
}

impl Context {
    fn new(operation: &'static str) -> Self {
        let parent = CONTEXT.try_with(Clone::clone).ok();
        Self {
            operation_id: parent
                .as_ref()
                .map(|p| p.operation_id.clone())
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            parent_id: parent.map(|p| p.span_id),
            span_id: uuid::Uuid::new_v4().to_string(),
            operation,
            started: Instant::now(),
            phase: Arc::new(Mutex::new("begin")),
        }
    }

    fn emit(&self, phase: &'static str, fields: Value) {
        let last_phase = *self.phase.lock().unwrap_or_else(|p| p.into_inner());
        let event = json!({"schemaVersion":1,"operationId":self.operation_id,
            "spanId":self.span_id,"parentId":self.parent_id,"operation":self.operation,
            "phase":phase,"lastPhase":last_phase,"elapsedMs":self.started.elapsed().as_millis() as u64,
            "fields":fields});
        #[cfg(test)]
        let _ = CAPTURE.try_with(|events| events.lock().unwrap().push(event.clone()));
        eprintln!("[CursorDiagnostic] {event}");
        *self.phase.lock().unwrap_or_else(|p| p.into_inner()) = phase;
    }
}

struct Span {
    context: Context,
    finished: bool,
}
impl Span {
    fn new(operation: &'static str) -> Self {
        let context = Context::new(operation);
        context.emit("begin", json!({}));
        Self {
            context,
            finished: false,
        }
    }
    fn finish(&mut self, outcome: &'static str, error: Option<&str>) {
        self.context.emit(
            "end",
            json!({"outcome":outcome,"error":error.map(error_code)}),
        );
        self.finished = true;
    }
}
impl Drop for Span {
    fn drop(&mut self) {
        if !self.finished {
            self.context.emit("end", json!({"outcome":"cancelled"}));
        }
    }
}

pub(crate) async fn rpc<T>(
    id: Option<u64>,
    method: &str,
    future: impl Future<Output = Result<T, String>>,
) -> Result<T, String> {
    result("facade", async {
        event("rpc_dispatch", json!({"rpcRequestId":id,"method":method}));
        future.await
    })
    .await
}

pub(super) fn event(phase: &'static str, fields: Value) {
    let _ = CONTEXT.try_with(|context| context.emit(phase, fields));
}

pub(super) async fn result<T>(
    operation: &'static str,
    future: impl Future<Output = Result<T, String>>,
) -> Result<T, String> {
    let mut span = Span::new(operation);
    let context = span.context.clone();
    CONTEXT
        .scope(context, async move {
            let result = future.await;
            span.finish(
                if result.is_ok() { "ok" } else { "error" },
                result.as_ref().err().map(String::as_str),
            );
            result
        })
        .await
}

pub(super) async fn value(operation: &'static str, future: impl Future<Output = Value>) -> Value {
    let mut span = Span::new(operation);
    CONTEXT
        .scope(span.context.clone(), async move {
            let value = future.await;
            let error = value["error"].as_str();
            span.finish(
                if value["pending"] == true {
                    "pending"
                } else if error.is_some() {
                    "error"
                } else {
                    "ok"
                },
                error,
            );
            value
        })
        .await
}

pub(super) fn sync_value(operation: &'static str, work: impl FnOnce() -> Value) -> Value {
    let mut span = Span::new(operation);
    CONTEXT.sync_scope(span.context.clone(), || {
        let value = work();
        let error = value["error"].as_str();
        span.finish(if error.is_some() { "error" } else { "ok" }, error);
        value
    })
}

/// Carry the parent operation into a detached cache refresh without a global ID.
pub(super) fn background(future: impl Future<Output = ()> + Send + 'static) {
    let parent = CONTEXT.try_with(Clone::clone).ok();
    tokio::spawn(async move {
        if let Some(parent) = parent {
            CONTEXT.scope(parent, future).await;
        } else {
            future.await;
        }
    });
}

pub(super) fn network_error(error: &reqwest::Error) {
    use std::error::Error;
    let mut source = error.source();
    let mut os_error = None;
    while let Some(cause) = source {
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
            os_error = io.raw_os_error();
            break;
        }
        source = cause.source();
    }
    event(
        "network_error",
        json!({"timeout":error.is_timeout(),"connect":error.is_connect(),
        "request":error.is_request(),"body":error.is_body(),"decode":error.is_decode(),"osError":os_error}),
    );
}

pub(super) fn headers(response: &reqwest::Response) {
    event(
        "response_headers",
        json!({"httpStatus":response.status().as_u16(),
        "http2":response.version() == reqwest::Version::HTTP_2,"contentLength":response.content_length()}),
    );
}

pub(super) fn error_code(error: &str) -> &'static str {
    const CODES: &[&str] = &[
        "cursor_not_signed_in",
        "cursor_network_error",
        "cursor_response_read_failed",
        "cursor_response_too_large",
        "cursor_invalid_response",
        "cursor_models_unavailable",
        "cursor_model_unavailable",
        "cursor_credential_changed",
        "cursor_credentials_changed",
        "cursor_client_credentials_invalid",
        "cursor_credential_read_failed",
        "cursor_credential_write_failed",
        "cursor_credential_delete_failed",
        "cursor_adopted_credential_needs_reimport",
        "cursor_browser_login_expired",
        "cursor_client_metadata_missing",
        "cursor_client_metadata_invalid",
        "cursor_endpoint_invalid",
        "cursor_http_client_failed",
        "cursor_login_pending",
        "cursor_login_network_error",
        "cursor_login_response_too_large",
        "cursor_login_invalid_response",
        "cursor_login_denied",
        "cursor_login_failed",
        "cursor_login_cancelled",
        "cursor_login_expired",
        "cursor_login_metadata_invalid",
        "cursor_login_random_failed",
        "cursor_permission_denied",
        "cursor_rate_limit",
        "cursor_overloaded",
        "cursor_upstream_error",
        "cursor_native_message_invalid",
    ];
    CODES
        .iter()
        .copied()
        .find(|code| *code == error)
        .unwrap_or("unclassified")
}

#[cfg(test)]
pub(super) async fn capture<T>(future: impl Future<Output = T>) -> (T, Vec<Value>) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let result = CAPTURE.scope(events.clone(), future).await;
    let captured = events.lock().unwrap().clone();
    (result, captured)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn nested_operations_share_an_identity_and_retain_parent_spans() {
        let events = Arc::new(Mutex::new(Vec::new()));
        CAPTURE
            .scope(
                events.clone(),
                result("outer", async {
                    result("inner", async { Ok::<_, String>(()) }).await
                }),
            )
            .await
            .unwrap();
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 4);
        assert_eq!(events[0]["operationId"], events[1]["operationId"]);
        assert_eq!(events[1]["parentId"], events[0]["spanId"]);
    }
    #[tokio::test]
    async fn dropping_a_request_records_the_last_phase() {
        let events = Arc::new(Mutex::new(Vec::new()));
        CAPTURE
            .scope(events.clone(), async {
                let result = tokio::time::timeout(
                    std::time::Duration::from_millis(5),
                    result("catalog", async {
                        event("request_send", json!({"timeoutMs":20_000}));
                        std::future::pending::<Result<(), String>>().await
                    }),
                )
                .await;
                assert!(result.is_err());
            })
            .await;
        let events = events.lock().unwrap();
        let last = events.last().unwrap();
        assert_eq!(last["fields"]["outcome"], "cancelled");
        assert_eq!(last["lastPhase"], "request_send");
    }
    #[test]
    fn arbitrary_error_text_is_never_recorded() {
        assert_eq!(
            error_code("request failed: https://user:secret@example.invalid?verifier=secret"),
            "unclassified"
        );
        assert_eq!(error_code("cursor_network_error"), "cursor_network_error");
    }
}
