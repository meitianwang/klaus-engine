use super::backend::{
    ExecBackend, ExecProcess, ExecProcessEventReceiver, ExecServerError, ExecServerResult,
    StartedExecProcess,
};
use super::handler::ExecServerHandler;
use super::protocol::{
    ExecParams, ExecResponse, InitializeParams, InitializeResponse, JsonRpcRequest,
    JsonRpcResponse, ProcessSignal, ReadParams, ReadResponse, RequestId, SignalParams,
    SignalResponse, TerminateParams, TerminateResponse, WriteParams, WriteResponse,
    INITIALIZED_METHOD, INITIALIZE_METHOD, PROCESS_READ_METHOD, PROCESS_SIGNAL_METHOD,
    PROCESS_START_METHOD, PROCESS_TERMINATE_METHOD, PROCESS_WRITE_METHOD,
};
use crate::runtime_values::{
    runtime_codec, RuntimeDecode, RuntimeEncode, RuntimeJson, RuntimeObject, RuntimeString,
};
use async_trait::async_trait;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::sync::watch;

#[async_trait]
pub trait JsonRpcTransport: Send + Sync {
    async fn send(&self, request: JsonRpcRequest) -> ExecServerResult<Option<JsonRpcResponse>>;

    fn drain_notifications(&self) -> Vec<JsonRpcRequest> {
        Vec::new()
    }
}

#[derive(Clone)]
pub struct InMemoryTransport {
    handler: Arc<ExecServerHandler>,
}

impl InMemoryTransport {
    pub fn new(handler: ExecServerHandler) -> Self {
        Self {
            handler: Arc::new(handler),
        }
    }
}

#[async_trait]
impl JsonRpcTransport for InMemoryTransport {
    async fn send(&self, request: JsonRpcRequest) -> ExecServerResult<Option<JsonRpcResponse>> {
        Ok(self.handler.handle_request(request).await)
    }

    fn drain_notifications(&self) -> Vec<JsonRpcRequest> {
        self.handler.drain_notifications()
    }
}

#[derive(Clone)]
pub struct ExecServerClient {
    inner: Arc<ExecServerClientInner>,
}

struct ExecServerClientInner {
    transport: Arc<dyn JsonRpcTransport>,
    session_id: String,
    next_id: AtomicI64,
}

impl ExecServerClient {
    pub async fn connect(
        transport: impl JsonRpcTransport + 'static,
        client_name: &str,
    ) -> ExecServerResult<Self> {
        Self::connect_with_resume(transport, client_name, None).await
    }

    pub async fn connect_with_resume(
        transport: impl JsonRpcTransport + 'static,
        client_name: &str,
        resume_session_id: Option<String>,
    ) -> ExecServerResult<Self> {
        let transport: Arc<dyn JsonRpcTransport> = Arc::new(transport);
        let response: InitializeResponse = request_on_transport(
            Arc::clone(&transport),
            RequestId::Number(1),
            INITIALIZE_METHOD,
            InitializeParams {
                client_name: RuntimeString::from(client_name),
                resume_session_id: resume_session_id.map(RuntimeString::from),
            },
        )
        .await?;
        transport
            .send(JsonRpcRequest::notification(
                INITIALIZED_METHOD,
                RuntimeJson::Object(RuntimeObject::new()),
            ))
            .await?;
        Ok(Self {
            inner: Arc::new(ExecServerClientInner {
                transport,
                session_id: response.session_id.try_to_utf8().map_err(|error| {
                    ExecServerError::invalid_request(format!(
                        "initialize sessionId is not a scalar string: {error}"
                    ))
                })?,
                next_id: AtomicI64::new(2),
            }),
        })
    }

    pub fn session_id(&self) -> &str {
        &self.inner.session_id
    }

    pub async fn call<P, R>(&self, method: &str, params: P) -> ExecServerResult<R>
    where
        P: RuntimeEncode,
        R: RuntimeDecode,
    {
        let id = RequestId::Number(self.inner.next_id.fetch_add(1, Ordering::Relaxed));
        request_on_transport(Arc::clone(&self.inner.transport), id, method, params).await
    }

    pub async fn notify<P>(&self, method: &str, params: P) -> ExecServerResult<()>
    where
        P: RuntimeEncode,
    {
        let params = runtime_codec::encode_value(&params)
            .map_err(|error| ExecServerError::internal(error.to_string()))?;
        self.inner
            .transport
            .send(JsonRpcRequest::notification(method, params))
            .await?;
        Ok(())
    }

    pub fn drain_notifications(&self) -> Vec<JsonRpcRequest> {
        self.inner.transport.drain_notifications()
    }

    pub fn process(&self, process_id: impl Into<String>) -> Arc<dyn ExecProcess> {
        Arc::new(RemoteExecProcess {
            client: self.clone(),
            process_id: process_id.into(),
        })
    }
}

async fn request_on_transport<P, R>(
    transport: Arc<dyn JsonRpcTransport>,
    id: RequestId,
    method: &str,
    params: P,
) -> ExecServerResult<R>
where
    P: RuntimeEncode,
    R: RuntimeDecode,
{
    let params = runtime_codec::encode_value(&params)
        .map_err(|error| ExecServerError::internal(error.to_string()))?;
    let response = transport
        .send(JsonRpcRequest::request(id.clone(), method, params))
        .await?
        .ok_or_else(|| ExecServerError::invalid_request("request did not return a response"))?;
    decode_response(id, response)
}

fn decode_response<R: RuntimeDecode>(
    expected_id: RequestId,
    response: JsonRpcResponse,
) -> ExecServerResult<R> {
    if response.id != expected_id {
        return Err(ExecServerError::invalid_request(
            "JSON-RPC response id mismatch",
        ));
    }
    if let Some(error) = response.error {
        let message = error
            .message
            .try_to_utf8()
            .unwrap_or_else(|_| format!("{:?}", error.message));
        return Err(ExecServerError::internal(format!(
            "JSON-RPC error {}: {}",
            error.code, message
        )));
    }
    let result = response
        .result
        .ok_or_else(|| ExecServerError::invalid_request("JSON-RPC response missing result"))?;
    runtime_codec::decode_value(&result)
        .map_err(|error| ExecServerError::internal(error.to_string()))
}

#[derive(Clone)]
pub struct RemoteExecBackend {
    client: ExecServerClient,
}

impl RemoteExecBackend {
    pub fn new(client: ExecServerClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ExecBackend for RemoteExecBackend {
    async fn start(&self, params: ExecParams) -> ExecServerResult<StartedExecProcess> {
        let response: ExecResponse = self.client.call(PROCESS_START_METHOD, params).await?;
        Ok(StartedExecProcess {
            process: Arc::new(RemoteExecProcess {
                client: self.client.clone(),
                process_id: response.process_id,
            }),
        })
    }
}

struct RemoteExecProcess {
    client: ExecServerClient,
    process_id: String,
}

#[async_trait]
impl ExecProcess for RemoteExecProcess {
    fn process_id(&self) -> &str {
        &self.process_id
    }

    fn subscribe_wake(&self) -> watch::Receiver<u64> {
        watch::channel(0).1
    }

    fn subscribe_events(&self) -> ExecProcessEventReceiver {
        ExecProcessEventReceiver::empty()
    }

    async fn read(&self, params: ReadParams) -> ExecServerResult<ReadResponse> {
        self.ensure_process_id(&params.process_id)?;
        self.client.call(PROCESS_READ_METHOD, params).await
    }

    async fn write(&self, params: WriteParams) -> ExecServerResult<WriteResponse> {
        self.ensure_process_id(&params.process_id)?;
        self.client.call(PROCESS_WRITE_METHOD, params).await
    }

    async fn signal(&self, signal: ProcessSignal) -> ExecServerResult<()> {
        let _: SignalResponse = self
            .client
            .call(
                PROCESS_SIGNAL_METHOD,
                SignalParams {
                    process_id: self.process_id.clone(),
                    signal,
                },
            )
            .await?;
        Ok(())
    }

    async fn terminate(&self) -> ExecServerResult<TerminateResponse> {
        self.client
            .call(
                PROCESS_TERMINATE_METHOD,
                TerminateParams {
                    process_id: self.process_id.clone(),
                },
            )
            .await
    }
}

impl RemoteExecProcess {
    fn ensure_process_id(&self, process_id: &str) -> ExecServerResult<()> {
        if process_id == self.process_id {
            Ok(())
        } else {
            Err(ExecServerError::invalid_params(format!(
                "process id mismatch: expected {}, got {process_id}",
                self.process_id
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command_runtime::exec_server::backend::{ExecBackend, LocalExecBackend};
    use crate::command_runtime::exec_server::handler::ExecServerHandler;
    use crate::command_runtime::exec_server::protocol::{
        runtime_path_uri_from_host_native_path, ExecParams, HttpRedirectPolicy, HttpRequestParams,
        ReadParams, WriteParams, WriteStatus, HTTP_REQUEST_METHOD,
    };
    use crate::command_runtime::process::LocalExecProcessRegistry;
    use crate::runtime_process::RuntimeEnv;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn exec_params(process_id: &str, command: &str) -> ExecParams {
        ExecParams {
            process_id: process_id.to_string(),
            argv: vec!["sh".into(), "-lc".into(), command.into()],
            cwd: runtime_path_uri_from_host_native_path(std::env::current_dir().unwrap()).unwrap(),
            env_policy: None,
            env: RuntimeEnv::new(),
            tty: false,
            pipe_stdin: true,
            arg0: None,
            sandbox: None,
            enforce_managed_network: false,
            managed_network: None,
        }
    }

    async fn client() -> ExecServerClient {
        let handler = ExecServerHandler::new(LocalExecBackend::new(
            crate::runtime_process::test_runtime_process_context(),
        ));
        ExecServerClient::connect(InMemoryTransport::new(handler), "client-test")
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn remote_backend_drives_process_over_in_memory_json_rpc_transport() {
        let client = client().await;
        let backend = RemoteExecBackend::new(client.clone());
        let started = backend
            .start(exec_params("client-cat", "cat"))
            .await
            .unwrap();
        let process = started.process;

        let written = process
            .write(WriteParams {
                process_id: "client-cat".to_string(),
                write_id: "write-1".into(),
                chunk: b"client hello\n".to_vec().into(),
            })
            .await
            .unwrap();
        assert_eq!(written.status, WriteStatus::Accepted);

        let read = process
            .read(ReadParams {
                process_id: "client-cat".to_string(),
                after_seq: Some(0),
                max_bytes: None,
                wait_ms: Some(1000),
            })
            .await
            .unwrap();
        assert_eq!(read.chunks[0].chunk.clone().into_inner(), b"client hello\n");
        process.terminate().await.unwrap();
    }

    async fn spawn_http_server() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut buf = vec![0; 1024];
            let _ = socket.read(&mut buf).await;
            let _ = socket
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\nhello")
                .await;
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn client_drains_http_body_delta_notifications() {
        let client = client().await;
        let base = spawn_http_server().await;
        let response = client
            .call::<_, crate::command_runtime::exec_server::protocol::HttpRequestResponse>(
                HTTP_REQUEST_METHOD,
                HttpRequestParams {
                    method: "GET".into(),
                    url: format!("{base}/ok").into(),
                    headers: Vec::new(),
                    body: None,
                    timeout_ms: Some(1000),
                    redirect_policy: HttpRedirectPolicy::Follow,
                    request_id: "client-http".into(),
                    stream_response: true,
                },
            )
            .await
            .unwrap();
        assert_eq!(response.status, 200);
        assert!(response.body.into_inner().is_empty());

        let notifications = client.drain_notifications();
        assert_eq!(notifications.len(), 1);
        assert_eq!(
            notifications[0]
                .params
                .as_ref()
                .unwrap()
                .get("deltaBase64")
                .and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("aGVsbG8="))
        );
    }

    #[tokio::test]
    async fn resume_options_are_sent_on_initialize() {
        let handler = ExecServerHandler::new(LocalExecBackend::new(
            crate::runtime_process::test_runtime_process_context(),
        ));
        let client = ExecServerClient::connect_with_resume(
            InMemoryTransport::new(handler),
            "client-test",
            Some("session-123".to_string()),
        )
        .await
        .unwrap();

        assert_eq!(client.session_id(), "session-123");
    }

    #[tokio::test]
    async fn resumed_client_can_reattach_and_read_retained_process_output() {
        let registry = Arc::new(LocalExecProcessRegistry::new());
        let backend = LocalExecBackend::with_registry(
            crate::runtime_process::test_runtime_process_context(),
            Arc::clone(&registry),
        );
        let handler1 = ExecServerHandler::new(backend.clone());
        let client1 = ExecServerClient::connect(InMemoryTransport::new(handler1), "client-test")
            .await
            .unwrap();
        let remote1 = RemoteExecBackend::new(client1.clone());
        let process1 = remote1
            .start(exec_params("recover-cat", "cat"))
            .await
            .unwrap()
            .process;
        process1
            .write(WriteParams {
                process_id: "recover-cat".to_string(),
                write_id: "recover-write".into(),
                chunk: b"after reconnect\n".to_vec().into(),
            })
            .await
            .unwrap();

        let handler2 = ExecServerHandler::new(backend);
        let client2 = ExecServerClient::connect_with_resume(
            InMemoryTransport::new(handler2),
            "client-test",
            Some(client1.session_id().to_string()),
        )
        .await
        .unwrap();
        let process2 = client2.process("recover-cat");
        let read = process2
            .read(ReadParams {
                process_id: "recover-cat".to_string(),
                after_seq: Some(0),
                max_bytes: None,
                wait_ms: Some(1000),
            })
            .await
            .unwrap();
        assert_eq!(
            read.chunks[0].chunk.clone().into_inner(),
            b"after reconnect\n"
        );

        process2.terminate().await.unwrap();
    }
}
