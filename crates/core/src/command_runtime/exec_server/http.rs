use super::backend::{ExecServerError, ExecServerResult};
use super::protocol::{
    HttpHeader, HttpRedirectPolicy, HttpRequestBodyDeltaNotification, HttpRequestParams,
    HttpRequestResponse,
};
use crate::runtime_values::RuntimeString;
use reqwest::header::{HeaderName, HeaderValue};
use reqwest::{Client, Method, Url};
use std::time::Duration;

#[derive(Clone, Default)]
pub struct HttpRequestRunner;

#[derive(Debug)]
pub struct HttpRequestOutcome {
    pub response: HttpRequestResponse,
    pub body_deltas: Vec<HttpRequestBodyDeltaNotification>,
}

impl HttpRequestRunner {
    pub fn new() -> Self {
        Self
    }

    pub async fn request(&self, params: HttpRequestParams) -> ExecServerResult<HttpRequestOutcome> {
        let url_text = params
            .url
            .try_to_utf8()
            .map_err(|error| ExecServerError::invalid_params(error.to_string()))?;
        let url = Url::parse(&url_text)
            .map_err(|error| ExecServerError::invalid_params(error.to_string()))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(ExecServerError::invalid_params(
                "http/request URL must use http or https",
            ));
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(ExecServerError::invalid_params(
                "http/request URL must not include credentials",
            ));
        }

        let client = client_for_redirect(params.redirect_policy)?;
        let method_text = params
            .method
            .try_to_utf8()
            .map_err(|error| ExecServerError::invalid_params(error.to_string()))?;
        let method = Method::from_bytes(method_text.as_bytes())
            .map_err(|error| ExecServerError::invalid_params(error.to_string()))?;
        let mut request = client.request(method, url);
        if let Some(timeout_ms) = params.timeout_ms {
            request = request.timeout(Duration::from_millis(timeout_ms));
        }
        for header in &params.headers {
            let name_text = header
                .name
                .try_to_utf8()
                .map_err(|error| ExecServerError::invalid_params(error.to_string()))?;
            let value_text = header
                .value
                .try_to_utf8()
                .map_err(|error| ExecServerError::invalid_params(error.to_string()))?;
            let name = HeaderName::from_bytes(name_text.as_bytes())
                .map_err(|error| ExecServerError::invalid_params(error.to_string()))?;
            let value = HeaderValue::from_str(&value_text)
                .map_err(|error| ExecServerError::invalid_params(error.to_string()))?;
            request = request.header(name, value);
        }
        if let Some(body) = params.body.clone() {
            request = request.body(body.into_inner());
        }

        let response = request.send().await.map_err(map_reqwest_error)?;
        let status = response.status().as_u16();
        let headers = response_headers(response.headers());
        let body = response.bytes().await.map_err(map_reqwest_error)?.to_vec();

        if params.stream_response {
            let body_deltas = vec![HttpRequestBodyDeltaNotification {
                request_id: params.request_id,
                seq: 1,
                delta: body.into(),
                done: true,
                error: None,
            }];
            Ok(HttpRequestOutcome {
                response: HttpRequestResponse {
                    status,
                    headers,
                    body: Vec::new().into(),
                },
                body_deltas,
            })
        } else {
            Ok(HttpRequestOutcome {
                response: HttpRequestResponse {
                    status,
                    headers,
                    body: body.into(),
                },
                body_deltas: Vec::new(),
            })
        }
    }
}

fn client_for_redirect(policy: HttpRedirectPolicy) -> ExecServerResult<Client> {
    let builder = match policy {
        HttpRedirectPolicy::Follow => Client::builder(),
        HttpRedirectPolicy::Stop => Client::builder().redirect(reqwest::redirect::Policy::none()),
    };
    builder
        .build()
        .map_err(|error| ExecServerError::internal(error.to_string()))
}

fn response_headers(headers: &reqwest::header::HeaderMap) -> Vec<HttpHeader> {
    headers
        .iter()
        .map(|(name, value)| HttpHeader {
            name: RuntimeString::from(name.as_str()),
            value: RuntimeString::from(value.to_str().unwrap_or_default()),
        })
        .collect()
}

fn map_reqwest_error(error: reqwest::Error) -> ExecServerError {
    if error.is_timeout() {
        return ExecServerError::internal(format!("http/request timed out: {error}"));
    }
    if error.is_redirect() {
        return ExecServerError::internal(format!("http/request redirect failed: {error}"));
    }
    ExecServerError::internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command_runtime::exec_server::protocol::{HttpRedirectPolicy, HttpRequestParams};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn spawn_http_server() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut buf = vec![0; 4096];
                    let Ok(n) = socket.read(&mut buf).await else {
                        return;
                    };
                    let request = String::from_utf8_lossy(&buf[..n]);
                    let path = request
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or("/");
                    // 每个连接只应答一次就关闭；必须声明 connection: close，
                    // 否则客户端按 keep-alive 复用已关 socket 会偶发发送失败。
                    let response = match path {
                        "/ok" => "HTTP/1.1 200 OK\r\nx-test: one\r\nconnection: close\r\ncontent-length: 5\r\n\r\nhello"
                            .to_string(),
                        "/redirect" => {
                            "HTTP/1.1 302 Found\r\nlocation: /ok\r\nconnection: close\r\ncontent-length: 0\r\n\r\n"
                                .to_string()
                        }
                        "/slow" => {
                            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
                            "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-length: 4\r\n\r\nslow".to_string()
                        }
                        _ => "HTTP/1.1 404 Not Found\r\nconnection: close\r\ncontent-length: 0\r\n\r\n".to_string(),
                    };
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        format!("http://{addr}")
    }

    fn params(url: String) -> HttpRequestParams {
        HttpRequestParams {
            method: "GET".into(),
            url: url.into(),
            headers: Vec::new(),
            body: None,
            // 宽松的默认超时：高并发测试负载下回环请求可能超出 1s 窗口，
            // 超时语义由 timeout_and_invalid_url_are_reported_as_errors 自行覆盖。
            timeout_ms: Some(10_000),
            redirect_policy: HttpRedirectPolicy::Follow,
            request_id: "req-1".into(),
            stream_response: false,
        }
    }

    #[tokio::test]
    async fn buffered_request_preserves_status_headers_and_body() {
        let base = spawn_http_server().await;
        let runner = HttpRequestRunner::new();
        let outcome = runner.request(params(format!("{base}/ok"))).await.unwrap();

        assert_eq!(outcome.response.status, 200);
        assert!(outcome
            .response
            .headers
            .iter()
            .any(|header| header.name == "x-test" && header.value == "one"));
        assert_eq!(outcome.response.body.into_inner(), b"hello");
        assert!(outcome.body_deltas.is_empty());
    }

    #[tokio::test]
    async fn redirect_policy_can_follow_or_stop() {
        let base = spawn_http_server().await;
        let runner = HttpRequestRunner::new();

        let followed = runner
            .request(params(format!("{base}/redirect")))
            .await
            .unwrap();
        assert_eq!(followed.response.status, 200);
        assert_eq!(followed.response.body.into_inner(), b"hello");

        let mut stopped_params = params(format!("{base}/redirect"));
        stopped_params.redirect_policy = HttpRedirectPolicy::Stop;
        let stopped = runner.request(stopped_params).await.unwrap();
        assert_eq!(stopped.response.status, 302);
        assert!(stopped.response.body.into_inner().is_empty());
    }

    #[tokio::test]
    async fn streamed_response_returns_body_deltas() {
        let base = spawn_http_server().await;
        let runner = HttpRequestRunner::new();
        let mut request = params(format!("{base}/ok"));
        request.stream_response = true;
        request.request_id = "stream-1".into();

        let outcome = runner.request(request).await.unwrap();
        assert_eq!(outcome.response.status, 200);
        assert!(outcome.response.body.into_inner().is_empty());
        assert_eq!(outcome.body_deltas.len(), 1);
        assert_eq!(outcome.body_deltas[0].request_id, "stream-1");
        assert_eq!(outcome.body_deltas[0].seq, 1);
        assert_eq!(outcome.body_deltas[0].delta.clone().into_inner(), b"hello");
        assert!(outcome.body_deltas[0].done);
    }

    #[tokio::test]
    async fn timeout_and_invalid_url_are_reported_as_errors() {
        let base = spawn_http_server().await;
        let runner = HttpRequestRunner::new();
        let mut slow = params(format!("{base}/slow"));
        slow.timeout_ms = Some(1);
        assert!(runner
            .request(slow)
            .await
            .unwrap_err()
            .message
            .contains("timed out"));

        let mut invalid = params("file:///tmp/nope".to_string());
        invalid.timeout_ms = None;
        assert!(runner
            .request(invalid)
            .await
            .unwrap_err()
            .message
            .contains("http"));
    }
}
