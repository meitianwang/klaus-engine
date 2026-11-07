//! Shared response-body helpers for the HTTP surfaces (plain-text and JSON
//! responses share one boxed body type with the streaming SSE bodies).

use std::convert::Infallible;

use bytes::Bytes;
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Empty, Full};
use hyper::header::CONTENT_TYPE;
use hyper::{Response, StatusCode};

/// One boxed body type shared by every response this crate emits. Unsync because
/// the SSE streaming body is fed by a (non-`Sync`) channel receiver.
pub(crate) type ResponseBody = UnsyncBoxBody<Bytes, Infallible>;

pub(crate) fn full_body(bytes: impl Into<Bytes>) -> ResponseBody {
    Full::new(bytes.into()).boxed_unsync()
}

pub(crate) fn empty_body() -> ResponseBody {
    Empty::<Bytes>::new().boxed_unsync()
}

pub(crate) fn text_response(code: u16, body: &str) -> Response<ResponseBody> {
    Response::builder()
        .status(StatusCode::from_u16(code).unwrap())
        .header(CONTENT_TYPE, "text/plain")
        .body(full_body(body.to_string()))
        .unwrap()
}
