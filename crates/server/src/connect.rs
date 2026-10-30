//! WebSocket client: connect to a served engine and get a ready
//! `ProtocolClient` over the socket.

use std::sync::Arc;

use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;

use klaus_agent_protocol::{ProtocolClient, ProtocolClientOptions};

use crate::app_server::AppServerClient;
use crate::ws_stream::{ws_to_streams, WsStreams, WsTransportHandle};

#[derive(Default, Clone)]
pub struct ConnectOptions {
    /// Bearer token sent as `Authorization: Bearer <token>` (required if the server sets one).
    pub token: Option<String>,
    /// `Origin` header (only needed for a server configured with `allowed_origins`).
    pub origin: Option<String>,
    /// Resume a persisted thread by id instead of starting fresh.
    pub resume: Option<String>,
    /// Fork a NEW session seeded from this persisted thread's history.
    pub fork: Option<String>,
}

/// A live client connection: the driver plus a `close()`.
pub struct ClientConnection {
    /// Drive the session (`run`/`interrupt`/`approve`/`rewind`/…) and iterate events.
    pub client: ProtocolClient,
    transport: WsTransportHandle,
}

impl ClientConnection {
    /// Close the connection.
    pub fn close(&self) {
        self.transport.close();
    }

    /// Close the connection and wait until both socket pumps have exited.
    pub async fn close_and_wait(&self) {
        self.transport.close_and_wait().await;
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error(transparent)]
    Ws(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("invalid header value: {0}")]
    Header(String),
}

/// Append `?resume=`/`?fork=` connect-time lifecycle params to a base URL.
pub fn with_lifecycle_query(url: &str, resume: Option<&str>, fork: Option<&str>) -> String {
    let resume = resume.filter(|s| !s.is_empty());
    let fork = fork.filter(|s| !s.is_empty());
    if resume.is_none() && fork.is_none() {
        return url.to_string();
    }
    let sep = if url.contains('?') { '&' } else { '?' };
    let mut ser = url::form_urlencoded::Serializer::new(String::new());
    if let Some(r) = resume {
        ser.append_pair("resume", r);
    }
    if let Some(f) = fork {
        ser.append_pair("fork", f);
    }
    format!("{url}{sep}{}", ser.finish())
}

/// Connect to a served engine and get a ready `ProtocolClient` over the socket.
/// Resolves once the WebSocket is open (rejects if the handshake fails, e.g.
/// 401 on a bad token).
pub async fn connect(url: &str, opts: ConnectOptions) -> Result<ClientConnection, ConnectError> {
    let full = ensure_url_path(&with_lifecycle_query(
        url,
        opts.resume.as_deref(),
        opts.fork.as_deref(),
    ));
    let WsStreams {
        input,
        output,
        transport,
    } = open_socket(&full, &opts).await?;
    let client = ProtocolClient::new(ProtocolClientOptions { input, output });
    Ok(ClientConnection { client, transport })
}

/// A live facade connection: the app-server client plus a `close()`.
///
/// This is the same surface `klaus app-server` speaks on stdio, so a host that
/// drives a served engine calls the declared method table either way and does
/// not need a second, socket-only integration.
pub struct FacadeConnection {
    /// Call declared methods, read notifications, answer reverse requests.
    ///
    /// Shared rather than owned because the client and the socket are torn down
    /// by different parties: a host keeps the client for the whole run while
    /// holding this connection only to `close()` it. `AppServerClient::drop`
    /// aborts the reader, so handing out a clone instead of a copy is what keeps
    /// the two from racing.
    pub client: Arc<AppServerClient>,
    transport: WsTransportHandle,
}

impl FacadeConnection {
    /// Close the connection.
    pub fn close(&self) {
        self.transport.close();
    }

    /// Close the connection and wait until both socket pumps have exited.
    pub async fn close_and_wait(&self) {
        self.transport.close_and_wait().await;
    }
}

/// Connect to a served engine's JSON-RPC facade instead of the inner protocol.
///
/// `resume`/`fork` are deliberately NOT forwarded: one facade connection
/// multiplexes many sessions, so lifecycle intent belongs to each `session/start`
/// (which carries `resumeSessionId`/`forkSessionId`) and a connect-time value
/// could only silently apply to one of them.
pub async fn connect_facade(
    url: &str,
    opts: ConnectOptions,
) -> Result<FacadeConnection, ConnectError> {
    let full = ensure_url_path(&with_surface_query(url));
    let WsStreams {
        input,
        output,
        transport,
    } = open_socket(&full, &opts).await?;
    let client = Arc::new(AppServerClient::new(input, output));
    Ok(FacadeConnection { client, transport })
}

/// Open the WebSocket and split it into the byte streams both surfaces read.
async fn open_socket(url: &str, opts: &ConnectOptions) -> Result<WsStreams, ConnectError> {
    let mut request = url.into_client_request()?;
    if let Some(t) = &opts.token {
        let v = HeaderValue::from_str(&format!("Bearer {t}"))
            .map_err(|e| ConnectError::Header(e.to_string()))?;
        request.headers_mut().insert("authorization", v);
    }
    if let Some(o) = &opts.origin {
        let v = HeaderValue::from_str(o).map_err(|e| ConnectError::Header(e.to_string()))?;
        request.headers_mut().insert("origin", v);
    }

    let (ws, _resp) = connect_async(request).await?;
    Ok(ws_to_streams(ws))
}

/// Ask the server for the facade instead of the inner protocol.
fn with_surface_query(url: &str) -> String {
    let sep = if url.contains('?') { '&' } else { '?' };
    format!("{url}{sep}surface=appServer")
}

/// Insert a `/` path into a URL whose authority is immediately followed by a
/// query, e.g. `ws://host:port?resume=x` → `ws://host:port/?resume=x`. Without
/// it the handshake request line is `GET ?resume=x` (no path), which a peer
/// rejects as malformed. A URL that already has a path — or none at all — is
/// returned unchanged.
fn ensure_url_path(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let authority_start = scheme_end + 3;
    let rest = &url[authority_start..];
    match rest.find(['/', '?', '#']) {
        // Authority is immediately followed by the query/fragment — inject `/`.
        Some(i) if rest.as_bytes()[i] != b'/' => {
            format!("{}/{}", &url[..authority_start + i], &rest[i..])
        }
        // Already has a path, or authority-only (the handshake defaults to `/`).
        _ => url.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn injects_path_before_a_bare_query() {
        assert_eq!(ensure_url_path("ws://h:1?resume=a"), "ws://h:1/?resume=a");
        assert_eq!(ensure_url_path("ws://h:1#frag"), "ws://h:1/#frag");
        // Already has a path, or authority-only — unchanged.
        assert_eq!(
            ensure_url_path("ws://h:1/x?resume=a"),
            "ws://h:1/x?resume=a"
        );
        assert_eq!(ensure_url_path("ws://h:1/?resume=a"), "ws://h:1/?resume=a");
        assert_eq!(ensure_url_path("ws://h:1"), "ws://h:1");
    }

    #[test]
    fn appends_lifecycle_query() {
        assert_eq!(with_lifecycle_query("ws://h/x", None, None), "ws://h/x");
        assert_eq!(
            with_lifecycle_query("ws://h/x", Some("a"), None),
            "ws://h/x?resume=a"
        );
        assert_eq!(
            with_lifecycle_query("ws://h/x?q=1", Some("a"), Some("b")),
            "ws://h/x?q=1&resume=a&fork=b"
        );
        assert_eq!(with_lifecycle_query("ws://h/x", Some(""), None), "ws://h/x");
    }

    #[test]
    fn asks_for_the_facade_surface() {
        assert_eq!(
            ensure_url_path(&with_surface_query("ws://h:1")),
            "ws://h:1/?surface=appServer"
        );
        assert_eq!(
            with_surface_query("ws://h/x?token=t"),
            "ws://h/x?token=t&surface=appServer"
        );
    }
}
