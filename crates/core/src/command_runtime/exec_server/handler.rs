use super::backend::{ExecBackend, ExecProcess, ExecServerError, ExecServerResult};
use super::filesystem::FileSystemHandler;
use super::http::HttpRequestRunner;
use super::protocol::{
    EnvironmentInfo, ExecParams, ExecResponse, FsCanonicalizeParams, FsCloseParams, FsCopyParams,
    FsCreateDirectoryParams, FsGetMetadataParams, FsOpenParams, FsReadBlockParams,
    FsReadDirectoryParams, FsReadFileParams, FsRemoveParams, FsWalkParams, FsWriteFileParams,
    HttpRequestParams, InitializeParams, InitializeResponse, JsonRpcErrorObject, JsonRpcRequest,
    JsonRpcResponse, ReadParams, SignalParams, SignalResponse, TerminateParams, WriteParams,
    FS_CANONICALIZE_METHOD, FS_CLOSE_METHOD, FS_COPY_METHOD, FS_CREATE_DIRECTORY_METHOD,
    FS_GET_METADATA_METHOD, FS_OPEN_METHOD, FS_READ_BLOCK_METHOD, FS_READ_DIRECTORY_METHOD,
    FS_READ_FILE_METHOD, FS_REMOVE_METHOD, FS_WALK_METHOD, FS_WRITE_FILE_METHOD,
    HTTP_REQUEST_BODY_DELTA_METHOD, HTTP_REQUEST_METHOD, INITIALIZED_METHOD, INITIALIZE_METHOD,
    PROCESS_READ_METHOD, PROCESS_SIGNAL_METHOD, PROCESS_START_METHOD, PROCESS_TERMINATE_METHOD,
    PROCESS_WRITE_METHOD,
};
use crate::runtime_values::{
    runtime_codec, RuntimeDecode, RuntimeEncode, RuntimeJson, RuntimeString,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

pub struct ExecServerHandler {
    backend: Arc<dyn ExecBackend>,
    file_system: FileSystemHandler,
    http_runner: HttpRequestRunner,
    notifications: Mutex<Vec<JsonRpcRequest>>,
    initialized: Mutex<InitializationState>,
    processes: Mutex<HashMap<String, Arc<dyn ExecProcess>>>,
}

#[derive(Debug, Clone, Default)]
struct InitializationState {
    initialize_requested: bool,
    initialized: bool,
    session_id: Option<RuntimeString>,
}

impl ExecServerHandler {
    pub fn new(backend: impl ExecBackend + 'static) -> Self {
        Self::new_shared(Arc::new(backend))
    }

    pub fn new_shared(backend: Arc<dyn ExecBackend>) -> Self {
        Self {
            backend,
            file_system: FileSystemHandler::new(),
            http_runner: HttpRequestRunner::new(),
            notifications: Mutex::new(Vec::new()),
            initialized: Mutex::new(InitializationState::default()),
            processes: Mutex::new(HashMap::new()),
        }
    }

    pub async fn handle_request(&self, request: JsonRpcRequest) -> Option<JsonRpcResponse> {
        let id = request.id.clone();
        let response = self.dispatch(request).await;
        let id = id?;
        Some(match response {
            Ok(result) => JsonRpcResponse::success(id, result),
            Err(error) => JsonRpcResponse::error(
                id,
                JsonRpcErrorObject {
                    code: error.json_rpc_code(),
                    message: RuntimeString::from(error.message),
                    data: None,
                },
            ),
        })
    }

    pub fn drain_notifications(&self) -> Vec<JsonRpcRequest> {
        std::mem::take(&mut *self.notifications())
    }

    async fn dispatch(&self, request: JsonRpcRequest) -> ExecServerResult<RuntimeJson> {
        let method = request.method.try_to_utf8().map_err(|error| {
            ExecServerError::method_not_found(format!(
                "exec-server method must be a scalar string: {error}"
            ))
        })?;
        match method.as_str() {
            INITIALIZE_METHOD => self.initialize(parse_params(request.params)?).await,
            INITIALIZED_METHOD => {
                self.initialized()?;
                Ok(RuntimeJson::Null)
            }
            "environment/info" => self.environment_info(),
            PROCESS_START_METHOD => self.process_start(parse_params(request.params)?).await,
            PROCESS_READ_METHOD => self.process_read(parse_params(request.params)?).await,
            PROCESS_WRITE_METHOD => self.process_write(parse_params(request.params)?).await,
            PROCESS_SIGNAL_METHOD => self.process_signal(parse_params(request.params)?).await,
            PROCESS_TERMINATE_METHOD => self.process_terminate(parse_params(request.params)?).await,
            FS_READ_FILE_METHOD => self.fs_read_file(parse_params(request.params)?).await,
            FS_OPEN_METHOD => self.fs_open(parse_params(request.params)?).await,
            FS_READ_BLOCK_METHOD => self.fs_read_block(parse_params(request.params)?).await,
            FS_CLOSE_METHOD => self.fs_close(parse_params(request.params)?).await,
            FS_WRITE_FILE_METHOD => self.fs_write_file(parse_params(request.params)?).await,
            FS_CREATE_DIRECTORY_METHOD => {
                self.fs_create_directory(parse_params(request.params)?)
                    .await
            }
            FS_GET_METADATA_METHOD => self.fs_get_metadata(parse_params(request.params)?).await,
            FS_CANONICALIZE_METHOD => self.fs_canonicalize(parse_params(request.params)?).await,
            FS_READ_DIRECTORY_METHOD => self.fs_read_directory(parse_params(request.params)?).await,
            FS_WALK_METHOD => self.fs_walk(parse_params(request.params)?).await,
            FS_REMOVE_METHOD => self.fs_remove(parse_params(request.params)?).await,
            FS_COPY_METHOD => self.fs_copy(parse_params(request.params)?).await,
            HTTP_REQUEST_METHOD => self.http_request(parse_params(request.params)?).await,
            method => Err(ExecServerError::method_not_found(format!(
                "unknown exec-server method {method}"
            ))),
        }
    }

    async fn initialize(&self, params: InitializeParams) -> ExecServerResult<RuntimeJson> {
        if runtime_trim_is_empty(&params.client_name) {
            return Err(ExecServerError::invalid_params(
                "initialize clientName must not be empty",
            ));
        }
        let response = {
            let mut state = self.state();
            if state.initialize_requested {
                return Err(ExecServerError::invalid_request(
                    "initialize may only be sent once per connection",
                ));
            }
            state.initialize_requested = true;
            let session_id = params.resume_session_id.unwrap_or_else(|| {
                RuntimeString::from(format!(
                    "exec-{}",
                    NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed)
                ))
            });
            state.session_id = Some(session_id.clone());
            InitializeResponse { session_id }
        };
        to_value(response)
    }

    fn initialized(&self) -> ExecServerResult<()> {
        let mut state = self.state();
        if !state.initialize_requested {
            return Err(ExecServerError::invalid_request(
                "received initialized before initialize",
            ));
        }
        state.initialized = true;
        Ok(())
    }

    fn environment_info(&self) -> ExecServerResult<RuntimeJson> {
        self.require_initialized()?;
        to_value(EnvironmentInfo::local())
    }

    async fn process_start(&self, params: ExecParams) -> ExecServerResult<RuntimeJson> {
        self.require_initialized()?;
        let process_id = params.process_id.clone();
        let started = self.backend.start(params).await?;
        self.processes()
            .insert(process_id.clone(), Arc::clone(&started.process));
        to_value(ExecResponse { process_id })
    }

    async fn process_read(&self, params: ReadParams) -> ExecServerResult<RuntimeJson> {
        self.require_initialized()?;
        let process = self.require_process(&params.process_id)?;
        to_value(process.read(params).await?)
    }

    async fn process_write(&self, params: WriteParams) -> ExecServerResult<RuntimeJson> {
        self.require_initialized()?;
        let process = self.require_process(&params.process_id)?;
        to_value(process.write(params).await?)
    }

    async fn process_signal(&self, params: SignalParams) -> ExecServerResult<RuntimeJson> {
        self.require_initialized()?;
        let process = self.require_process(&params.process_id)?;
        process.signal(params.signal).await?;
        to_value(SignalResponse {})
    }

    async fn process_terminate(&self, params: TerminateParams) -> ExecServerResult<RuntimeJson> {
        self.require_initialized()?;
        let process = self.require_process(&params.process_id)?;
        to_value(process.terminate().await?)
    }

    async fn fs_read_file(&self, params: FsReadFileParams) -> ExecServerResult<RuntimeJson> {
        self.require_initialized()?;
        to_value(self.file_system.read_file(params).await?)
    }

    async fn fs_open(&self, params: FsOpenParams) -> ExecServerResult<RuntimeJson> {
        self.require_initialized()?;
        to_value(self.file_system.open(params).await?)
    }

    async fn fs_read_block(&self, params: FsReadBlockParams) -> ExecServerResult<RuntimeJson> {
        self.require_initialized()?;
        to_value(self.file_system.read_block(params).await?)
    }

    async fn fs_close(&self, params: FsCloseParams) -> ExecServerResult<RuntimeJson> {
        self.require_initialized()?;
        to_value(self.file_system.close(params).await?)
    }

    async fn fs_write_file(&self, params: FsWriteFileParams) -> ExecServerResult<RuntimeJson> {
        self.require_initialized()?;
        to_value(self.file_system.write_file(params).await?)
    }

    async fn fs_create_directory(
        &self,
        params: FsCreateDirectoryParams,
    ) -> ExecServerResult<RuntimeJson> {
        self.require_initialized()?;
        to_value(self.file_system.create_directory(params).await?)
    }

    async fn fs_get_metadata(&self, params: FsGetMetadataParams) -> ExecServerResult<RuntimeJson> {
        self.require_initialized()?;
        to_value(self.file_system.get_metadata(params).await?)
    }

    async fn fs_canonicalize(&self, params: FsCanonicalizeParams) -> ExecServerResult<RuntimeJson> {
        self.require_initialized()?;
        to_value(self.file_system.canonicalize(params).await?)
    }

    async fn fs_read_directory(
        &self,
        params: FsReadDirectoryParams,
    ) -> ExecServerResult<RuntimeJson> {
        self.require_initialized()?;
        to_value(self.file_system.read_directory(params).await?)
    }

    async fn fs_walk(&self, params: FsWalkParams) -> ExecServerResult<RuntimeJson> {
        self.require_initialized()?;
        to_value(self.file_system.walk(params).await?)
    }

    async fn fs_remove(&self, params: FsRemoveParams) -> ExecServerResult<RuntimeJson> {
        self.require_initialized()?;
        to_value(self.file_system.remove(params).await?)
    }

    async fn fs_copy(&self, params: FsCopyParams) -> ExecServerResult<RuntimeJson> {
        self.require_initialized()?;
        to_value(self.file_system.copy(params).await?)
    }

    async fn http_request(&self, params: HttpRequestParams) -> ExecServerResult<RuntimeJson> {
        self.require_initialized()?;
        let outcome = self.http_runner.request(params).await?;
        if !outcome.body_deltas.is_empty() {
            let mut notifications = self.notifications();
            for delta in outcome.body_deltas {
                notifications.push(JsonRpcRequest::notification(
                    HTTP_REQUEST_BODY_DELTA_METHOD,
                    to_value(delta)?,
                ));
            }
        }
        to_value(outcome.response)
    }

    fn require_initialized(&self) -> ExecServerResult<()> {
        let state = self.state();
        if !state.initialize_requested {
            return Err(ExecServerError::invalid_request(
                "initialize must be sent before using exec-server methods",
            ));
        }
        if !state.initialized {
            return Err(ExecServerError::invalid_request(
                "initialized notification must be sent before using exec-server methods",
            ));
        }
        Ok(())
    }

    fn require_process(&self, process_id: &str) -> ExecServerResult<Arc<dyn ExecProcess>> {
        if let Some(process) = self.processes().get(process_id).cloned() {
            return Ok(process);
        }
        if let Some(process) = self.backend.get_process(process_id) {
            self.processes()
                .insert(process_id.to_string(), Arc::clone(&process));
            return Ok(process);
        }
        Err(ExecServerError::invalid_params(format!(
            "unknown process {process_id}"
        )))
    }

    fn state(&self) -> std::sync::MutexGuard<'_, InitializationState> {
        self.initialized
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn processes(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<dyn ExecProcess>>> {
        self.processes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn notifications(&self) -> std::sync::MutexGuard<'_, Vec<JsonRpcRequest>> {
        self.notifications
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn parse_params<T: RuntimeDecode>(params: Option<RuntimeJson>) -> ExecServerResult<T> {
    runtime_codec::decode_value(&params.unwrap_or(RuntimeJson::Null))
        .map_err(|error| ExecServerError::invalid_params(error.to_string()))
}

fn to_value(value: impl RuntimeEncode) -> ExecServerResult<RuntimeJson> {
    runtime_codec::encode_value(&value)
        .map_err(|error| ExecServerError::internal(error.to_string()))
}

fn runtime_trim_is_empty(value: &RuntimeString) -> bool {
    value.as_utf16().iter().all(|unit| {
        matches!(
            *unit,
            0x0009..=0x000d
                | 0x0020
                | 0x00a0
                | 0x1680
                | 0x2000..=0x200a
                | 0x2028
                | 0x2029
                | 0x202f
                | 0x205f
                | 0x3000
                | 0xfeff
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command_runtime::exec_server::backend::LocalExecBackend;
    use crate::command_runtime::exec_server::protocol::{
        runtime_path_uri_from_host_native_path, JsonRpcRequest, RequestId, FS_READ_FILE_METHOD,
        FS_WRITE_FILE_METHOD, HTTP_REQUEST_BODY_DELTA_METHOD, HTTP_REQUEST_METHOD,
        INITIALIZED_METHOD, INITIALIZE_METHOD, PROCESS_READ_METHOD, PROCESS_START_METHOD,
        PROCESS_TERMINATE_METHOD, PROCESS_WRITE_METHOD,
    };
    use crate::runtime_process::RuntimeEnv;
    use crate::runtime_values::RuntimeObject;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn request(id: i64, method: &str, params: impl RuntimeEncode) -> JsonRpcRequest {
        JsonRpcRequest::request(
            RequestId::Number(id),
            method,
            runtime_codec::encode_value(&params).unwrap(),
        )
    }

    fn initialize_params() -> InitializeParams {
        InitializeParams {
            client_name: "test-client".into(),
            resume_session_id: None,
        }
    }

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

    fn initialized_notification() -> JsonRpcRequest {
        JsonRpcRequest::notification(
            INITIALIZED_METHOD,
            RuntimeJson::Object(RuntimeObject::new()),
        )
    }

    fn handler() -> ExecServerHandler {
        ExecServerHandler::new(LocalExecBackend::new(
            crate::runtime_process::test_runtime_process_context(),
        ))
    }

    #[tokio::test]
    async fn initialize_is_required_and_only_allowed_once() {
        let handler = handler();
        let start_before_initialized = handler
            .handle_request(request(
                1,
                PROCESS_START_METHOD,
                exec_params("too-soon", "true"),
            ))
            .await
            .unwrap();
        assert_eq!(start_before_initialized.error.unwrap().code, -32600);

        let first = handler
            .handle_request(request(2, INITIALIZE_METHOD, initialize_params()))
            .await
            .unwrap();
        assert!(first.error.is_none());
        assert!(first.result.unwrap()["sessionId"]
            .as_str()
            .unwrap()
            .starts_with("exec-"));

        let second = handler
            .handle_request(request(3, INITIALIZE_METHOD, initialize_params()))
            .await
            .unwrap();
        assert_eq!(second.error.unwrap().code, -32600);
    }

    #[tokio::test]
    async fn json_rpc_process_methods_drive_a_real_local_process() {
        let handler = handler();
        handler
            .handle_request(request(1, INITIALIZE_METHOD, initialize_params()))
            .await
            .unwrap();
        handler.handle_request(initialized_notification()).await;

        let started = handler
            .handle_request(request(
                2,
                PROCESS_START_METHOD,
                exec_params("rpc-cat", "cat"),
            ))
            .await
            .unwrap();
        assert_eq!(
            started.result.unwrap()["processId"].as_str(),
            Some(&RuntimeString::from("rpc-cat"))
        );

        let written = handler
            .handle_request(request(
                3,
                PROCESS_WRITE_METHOD,
                WriteParams {
                    process_id: "rpc-cat".to_string(),
                    write_id: "write-1".into(),
                    chunk: b"rpc hello\n".to_vec().into(),
                },
            ))
            .await
            .unwrap();
        assert_eq!(
            written.result.unwrap()["status"].as_str(),
            Some(&RuntimeString::from("accepted"))
        );

        let read = handler
            .handle_request(request(
                4,
                PROCESS_READ_METHOD,
                ReadParams {
                    process_id: "rpc-cat".to_string(),
                    after_seq: Some(0),
                    max_bytes: None,
                    wait_ms: Some(1000),
                },
            ))
            .await
            .unwrap();
        assert_eq!(
            read.result
                .unwrap()
                .pointer("/chunks/0/chunk")
                .and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("cnBjIGhlbGxvCg=="))
        );

        let terminated = handler
            .handle_request(request(
                5,
                PROCESS_TERMINATE_METHOD,
                TerminateParams {
                    process_id: "rpc-cat".to_string(),
                },
            ))
            .await
            .unwrap();
        assert_eq!(terminated.result.unwrap()["running"].as_bool(), Some(true));
    }

    #[tokio::test]
    async fn json_rpc_filesystem_methods_drive_local_filesystem_helper() {
        let handler = handler();
        handler
            .handle_request(request(1, INITIALIZE_METHOD, initialize_params()))
            .await
            .unwrap();
        handler.handle_request(initialized_notification()).await;

        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("klaus-exec-handler-fs-{stamp}"));
        let file = root.join("data.bin");
        let path = runtime_path_uri_from_host_native_path(&file).unwrap();

        let written = handler
            .handle_request(request(
                10,
                FS_WRITE_FILE_METHOD,
                FsWriteFileParams {
                    path,
                    data_base64: "ZmY=".into(),
                    sandbox: None,
                },
            ))
            .await
            .unwrap();
        assert!(written.error.is_none());

        let path = runtime_path_uri_from_host_native_path(&file).unwrap();
        let read = handler
            .handle_request(request(
                11,
                FS_READ_FILE_METHOD,
                FsReadFileParams {
                    path,
                    sandbox: None,
                },
            ))
            .await
            .unwrap();
        assert_eq!(
            read.result.unwrap()["dataBase64"].as_str(),
            Some(&RuntimeString::from("ZmY="))
        );

        let _ = tokio::fs::remove_dir_all(root).await;
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
    async fn json_rpc_http_request_streams_body_delta_notifications() {
        let handler = handler();
        handler
            .handle_request(request(1, INITIALIZE_METHOD, initialize_params()))
            .await
            .unwrap();
        handler.handle_request(initialized_notification()).await;

        let base = spawn_http_server().await;
        let response = handler
            .handle_request(request(
                20,
                HTTP_REQUEST_METHOD,
                HttpRequestParams {
                    method: "GET".into(),
                    url: format!("{base}/ok").into(),
                    headers: Vec::new(),
                    body: None,
                    timeout_ms: Some(1000),
                    redirect_policy: Default::default(),
                    request_id: "http-1".into(),
                    stream_response: true,
                },
            ))
            .await
            .unwrap();
        let result = response.result.unwrap();
        assert_eq!(result["status"].as_u64(), Some(200));
        assert_eq!(
            result["bodyBase64"].as_str(),
            Some(&RuntimeString::from(""))
        );

        let notifications = handler.drain_notifications();
        assert_eq!(notifications.len(), 1);
        assert_eq!(notifications[0].method, HTTP_REQUEST_BODY_DELTA_METHOD);
        assert_eq!(
            notifications[0].params.as_ref().unwrap()["requestId"].as_str(),
            Some(&RuntimeString::from("http-1"))
        );
        assert_eq!(
            notifications[0].params.as_ref().unwrap()["deltaBase64"].as_str(),
            Some(&RuntimeString::from("aGVsbG8="))
        );
        assert_eq!(
            notifications[0].params.as_ref().unwrap()["done"].as_bool(),
            Some(true)
        );
    }
}
