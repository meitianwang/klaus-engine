//! An opt-in authorize handler that completes MCP OAuth on the local machine: it
//! starts a loopback HTTP server on the engine-registered `redirect_uri`, opens
//! (or prints) the authorization URL, waits for the browser redirect, validates
//! the CSRF `state` against the one embedded in the authorization URL, and
//! returns the captured `code`.
//!
//! This lives OUTSIDE the core connect path on purpose so the engine core stays
//! headless (no browser, no listening socket) while a CLI host can wire OAuth
//! with one line.

use crate::mcp::client::{McpAuthorizeHandler, McpAuthorizeRequest, McpAuthorizeResponse};
use crate::runtime_process::RuntimeProcessContext;
use crate::runtime_values::RuntimeString;
use crate::types::abort::AbortSignal;
use async_trait::async_trait;
use std::sync::Arc;
use url::Url;

#[derive(Debug, Clone, Default)]
pub struct LoopbackAuthorizeOptions {
    /// Open the URL in the system browser (default true). When false, print it to stdout.
    pub open: Option<bool>,
    /// How long to wait for the redirect before declining (default 5 min).
    pub timeout_ms: Option<u64>,
}

/// Build a loopback authorize handler.
pub fn loopback_authorize_handler(
    process_context: RuntimeProcessContext,
    opts: LoopbackAuthorizeOptions,
) -> Arc<dyn McpAuthorizeHandler> {
    Arc::new(LoopbackAuthorizeHandler {
        process_context,
        opts,
    })
}

struct LoopbackAuthorizeHandler {
    process_context: RuntimeProcessContext,
    opts: LoopbackAuthorizeOptions,
}

#[async_trait]
impl McpAuthorizeHandler for LoopbackAuthorizeHandler {
    async fn authorize(
        &self,
        request: McpAuthorizeRequest,
        signal: AbortSignal,
    ) -> McpAuthorizeResponse {
        run(&self.process_context, request, signal, self.opts.clone()).await
    }
}

async fn run(
    process_context: &RuntimeProcessContext,
    request: McpAuthorizeRequest,
    signal: AbortSignal,
    opts: LoopbackAuthorizeOptions,
) -> McpAuthorizeResponse {
    let redirect = match Url::parse(&request.redirect_uri) {
        Ok(u) => u,
        Err(_) => {
            return McpAuthorizeResponse::Decline {
                reason: Some("could not bind loopback server".to_string()),
            }
        }
    };
    let port = redirect.port().unwrap_or(0);
    let redirect_path = redirect.path().to_string();
    // The expected state is the one the engine put in the authorization URL —
    // comparing the redirect's state to it is the CSRF check.
    let expected_state = Url::parse(&request.authorization_url).ok().and_then(|u| {
        u.query_pairs()
            .find(|(k, _)| k == "state")
            .map(|(_, v)| v.into_owned())
    });

    // An already-aborted signal never re-dispatches 'abort', so settle now —
    // before binding the loopback socket or opening a browser for a cancelled op.
    if signal.is_cancelled() {
        return McpAuthorizeResponse::Decline {
            reason: Some("aborted".to_string()),
        };
    }

    let listener = match tokio::net::TcpListener::bind(("127.0.0.1", port)).await {
        Ok(l) => l,
        Err(_) => {
            return McpAuthorizeResponse::Decline {
                reason: Some("could not bind loopback server".to_string()),
            }
        }
    };

    if opts.open == Some(false) {
        print!(
            "Open this URL to authorize:\n{}\n",
            request.authorization_url
        );
    } else {
        open_browser(process_context, &request.authorization_url).await;
    }

    let timeout_ms = opts.timeout_ms.unwrap_or(300_000);
    let timeout = tokio::time::sleep(tokio::time::Duration::from_millis(timeout_ms));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            _ = signal.cancelled() => {
                return McpAuthorizeResponse::Decline { reason: Some("aborted".to_string()) };
            }
            _ = &mut timeout => {
                return McpAuthorizeResponse::Decline {
                    reason: Some("timed out waiting for authorization".to_string()),
                };
            }
            accepted = listener.accept() => {
                let (stream, _addr) = match accepted {
                    Ok(pair) => pair,
                    Err(_) => {
                        return McpAuthorizeResponse::Decline {
                            reason: Some("could not bind loopback server".to_string()),
                        };
                    }
                };
                match handle_connection(stream, &redirect_path).await {
                    ConnResult::NotMatched => continue,
                    ConnResult::Matched { code, state } => {
                        if code.is_none() {
                            return McpAuthorizeResponse::Decline {
                                reason: Some("redirect missing code".to_string()),
                            };
                        }
                        if let Some(exp) = &expected_state {
                            if state.as_deref() != Some(exp.as_str()) {
                                return McpAuthorizeResponse::Decline {
                                    reason: Some("state mismatch".to_string()),
                                };
                            }
                        }
                        return McpAuthorizeResponse::Code { code: code.unwrap(), state };
                    }
                }
            }
        }
    }
}

enum ConnResult {
    NotMatched,
    Matched {
        code: Option<String>,
        state: Option<String>,
    },
}

async fn handle_connection(mut stream: tokio::net::TcpStream, redirect_path: &str) -> ConnResult {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1024];
    loop {
        if find_crlf(&buf).is_some() || buf.len() > 8192 {
            break;
        }
        match stream.read(&mut tmp).await {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(_) => break,
        }
    }
    let line_end = find_crlf(&buf).unwrap_or(buf.len());
    let request_line = String::from_utf8_lossy(&buf[..line_end]).into_owned();
    let target = request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("/")
        .to_string();
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target, String::new()),
    };
    if path != redirect_path {
        let _ = stream
            .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await;
        let _ = stream.flush().await;
        return ConnResult::NotMatched;
    }
    let code = query_get(&query, "code");
    let state = query_get(&query, "state");
    let body = "<html><body>Authorization complete. You may close this window.</body></html>";
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.flush().await;
    ConnResult::Matched { code, state }
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

fn query_get(query: &str, key: &str) -> Option<String> {
    url::form_urlencoded::parse(query.as_bytes())
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.into_owned())
}

/// Best-effort: open `url` in the system browser. On failure the user can copy
/// it from the log.
async fn open_browser(process_context: &RuntimeProcessContext, url: &str) {
    let cmd = if cfg!(target_os = "macos") {
        "open"
    } else if cfg!(target_os = "windows") {
        "start"
    } else {
        "xdg-open"
    };
    if let Ok(mut child) = process_context
        .spawn_oauth_opener(RuntimeString::from(cmd), vec![RuntimeString::from(url)])
        .await
    {
        tokio::spawn(async move {
            let _ = child.wait().await;
        });
    }
    print!("Opening browser to authorize:\n{}\n", url);
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn free_port() -> u16 {
        let l = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        p
    }

    fn auth_url(state: &str) -> String {
        format!("https://as.test/authorize?client_id=c&state={}", state)
    }

    #[test]
    fn loopback_handler_implements_the_canonical_client_authorize_trait() {
        let _handler: Arc<dyn crate::mcp::client::McpAuthorizeHandler> = loopback_authorize_handler(
            crate::runtime_process::test_runtime_process_context(),
            LoopbackAuthorizeOptions::default(),
        );
    }

    #[tokio::test]
    async fn captures_the_redirect_code_when_the_state_matches() {
        // free_port() releases the socket before authorize rebinds it, so a
        // parallel test can steal the port in between. Retry the whole flow
        // on a fresh port instead of failing on that race.
        let mut last = None;
        for _ in 0..3 {
            let port = free_port().await;
            let redirect_uri = format!("http://127.0.0.1:{}/callback", port);
            let handler = loopback_authorize_handler(
                crate::runtime_process::test_runtime_process_context(),
                LoopbackAuthorizeOptions {
                    open: Some(false),
                    ..Default::default()
                },
            );
            let signal = AbortSignal::new();
            let pending = tokio::spawn(async move {
                handler
                    .authorize(
                        McpAuthorizeRequest {
                            server_name: "s".to_string(),
                            authorization_url: auth_url("S1"),
                            redirect_uri: redirect_uri.clone(),
                        },
                        signal,
                    )
                    .await
            });
            tokio::spawn(async move {
                // Under parallel test load the server task may not be
                // listening yet; retry the callback until the connection is
                // accepted so the pending authorize cannot hang on one
                // dropped request.
                let url = format!("http://127.0.0.1:{}/callback?code=CODE1&state=S1", port);
                for _ in 0..200 {
                    if reqwest::get(&url).await.is_ok() {
                        return;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
            });
            let resp = pending.await.unwrap();
            let expected = McpAuthorizeResponse::Code {
                code: "CODE1".to_string(),
                state: Some("S1".to_string()),
            };
            if resp == expected {
                return;
            }
            last = Some(resp);
        }
        panic!("authorize never captured the redirect code: {last:?}");
    }

    #[tokio::test]
    async fn declines_on_a_state_mismatch() {
        let port = free_port().await;
        let redirect_uri = format!("http://127.0.0.1:{}/callback", port);
        let handler = loopback_authorize_handler(
            crate::runtime_process::test_runtime_process_context(),
            LoopbackAuthorizeOptions {
                open: Some(false),
                ..Default::default()
            },
        );
        let signal = AbortSignal::new();
        let pending = tokio::spawn(async move {
            handler
                .authorize(
                    McpAuthorizeRequest {
                        server_name: "s".to_string(),
                        authorization_url: auth_url("EXPECTED"),
                        redirect_uri,
                    },
                    signal,
                )
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let _ = reqwest::get(format!(
            "http://127.0.0.1:{}/callback?code=CODE&state=WRONG",
            port
        ))
        .await;
        let resp = pending.await.unwrap();
        assert!(matches!(resp, McpAuthorizeResponse::Decline { .. }));
    }

    #[tokio::test]
    async fn declines_when_the_wait_is_aborted() {
        let port = free_port().await;
        let redirect_uri = format!("http://127.0.0.1:{}/callback", port);
        let signal = AbortSignal::new();
        let child = signal.clone();
        let handler = loopback_authorize_handler(
            crate::runtime_process::test_runtime_process_context(),
            LoopbackAuthorizeOptions {
                open: Some(false),
                ..Default::default()
            },
        );
        let pending = tokio::spawn(async move {
            handler
                .authorize(
                    McpAuthorizeRequest {
                        server_name: "s".to_string(),
                        authorization_url: auth_url("S"),
                        redirect_uri,
                    },
                    child,
                )
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        signal.cancel();
        let resp = pending.await.unwrap();
        assert_eq!(
            resp,
            McpAuthorizeResponse::Decline {
                reason: Some("aborted".to_string())
            }
        );
    }

    #[tokio::test]
    async fn declines_immediately_when_already_aborted_binding_no_socket() {
        let port = free_port().await;
        let redirect_uri = format!("http://127.0.0.1:{}/callback", port);
        let signal = AbortSignal::new();
        signal.cancel(); // already aborted BEFORE the handler runs
        let handler = loopback_authorize_handler(
            crate::runtime_process::test_runtime_process_context(),
            LoopbackAuthorizeOptions {
                open: Some(false),
                ..Default::default()
            },
        );
        let resp = handler
            .authorize(
                McpAuthorizeRequest {
                    server_name: "s".to_string(),
                    authorization_url: auth_url("S"),
                    redirect_uri,
                },
                signal,
            )
            .await;
        assert_eq!(
            resp,
            McpAuthorizeResponse::Decline {
                reason: Some("aborted".to_string())
            }
        );
        // The port was never bound — a fresh listen on it succeeds.
        let probe = tokio::net::TcpListener::bind(("127.0.0.1", port)).await;
        assert!(probe.is_ok());
    }
}
