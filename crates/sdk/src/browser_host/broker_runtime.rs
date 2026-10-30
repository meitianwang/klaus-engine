use klaus_agent_core::runtime_process::{
    RuntimeBrowserBrokerProcessRequest, RuntimeBrowserBrokerRead, RuntimeBrowserBrokerWrite,
    RuntimeBrowserFfmpegEncodeRequest, RuntimeBrowserLinuxLddRequest,
    RuntimeBrowserPlaywrightLaunchMode, RuntimeBrowserPlaywrightLaunchRequest,
    RuntimeBrowserProcessBinding, RuntimeBrowserWindowsWinlddRequest, RuntimeEnv,
    RuntimeExecFileOutput, RuntimeProcessContext,
};
use klaus_agent_core::runtime_values::{
    NodeBufferUtf8, RuntimeJson, RuntimeObject, RuntimePath, RuntimeString,
};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;

use super::broker_wire::{
    decode_sync_request_payload, BrowserFfmpegEncodeRequest, BrowserPlaywrightLaunchRequest,
    BrowserSyncExchange, BrowserSyncFrame, BrowserSyncRequestPayload,
};
use super::mux_runtime::{BrowserMuxChildKind, BrowserMuxHandle, BrowserMuxRuntime};
use super::winldd_distribution::verified_winldd_from_distribution;
use super::BrowserGeneration;

const SYNC_HEADER_LEN: usize = 20;
const MAX_SYNC_REMAINING: usize = 16 * 1024 * 1024;

pub(crate) struct BrowserBrokerRuntime {
    sync_task: JoinHandle<()>,
    mux: BrowserMuxRuntime,
    generation: std::sync::Arc<Mutex<BrowserGeneration>>,
    generation_id: u64,
    fatal_rx: watch::Receiver<Option<RuntimeString>>,
}

impl BrowserBrokerRuntime {
    pub(crate) fn start(
        process_context: RuntimeProcessContext,
        binding: RuntimeBrowserProcessBinding,
        generation: std::sync::Arc<Mutex<BrowserGeneration>>,
        sync_inbound: RuntimeBrowserBrokerRead,
        sync_outbound: RuntimeBrowserBrokerWrite,
        mux_inbound: RuntimeBrowserBrokerRead,
        mux_outbound: RuntimeBrowserBrokerWrite,
    ) -> Self {
        let generation_id = generation
            .try_lock()
            .expect("browser generation is uncontended during broker startup")
            .generation();
        let (fatal_tx, fatal_rx) = watch::channel(None);
        let mux =
            BrowserMuxRuntime::start(generation_id, mux_inbound, mux_outbound, fatal_tx.clone());
        let executor = BrowserSyncExecutor {
            process_context,
            binding,
            generation: generation.clone(),
            generation_id,
            next_object_id: AtomicU64::new(1),
            mux: mux.handle(),
            pending_activation: Mutex::new(None),
        };
        let sync_task = tokio::spawn(async move {
            if let Err(message) =
                run_sync_responder(generation_id, sync_inbound, sync_outbound, executor).await
            {
                fatal_tx.send_replace(Some(message));
            }
        });
        Self {
            sync_task,
            mux,
            generation,
            generation_id,
            fatal_rx,
        }
    }

    pub(crate) fn fatal_error(&self) -> Option<RuntimeString> {
        self.fatal_rx.borrow().clone()
    }

    pub(crate) async fn begin_orderly_shutdown(&self) -> Result<(), RuntimeString> {
        self.generation
            .lock()
            .await
            .trigger_orderly_shutdown(self.generation_id)
            .map_err(|error| broker_error("trigger orderly shutdown", error))?;
        self.mux.begin_orderly_shutdown().await?;
        let mut generation = self.generation.lock().await;
        generation
            .advance_orderly_children(self.generation_id)
            .map_err(|error| broker_error("advance orderly browser children", error))?;
        generation
            .start_sidecar_action(self.generation_id)
            .map_err(|error| broker_error("start orderly sidecar action", error))?;
        Ok(())
    }

    pub(crate) async fn begin_immediate_shutdown(
        &self,
        public_error: RuntimeString,
    ) -> Result<(), RuntimeString> {
        self.generation
            .lock()
            .await
            .trigger_immediate(self.generation_id, public_error)
            .map_err(|error| broker_error("trigger immediate shutdown", error))?;
        self.mux.begin_immediate_shutdown();
        self.generation
            .lock()
            .await
            .start_sidecar_action(self.generation_id)
            .map_err(|error| broker_error("start immediate sidecar action", error))?;
        Ok(())
    }
}

impl Drop for BrowserBrokerRuntime {
    fn drop(&mut self) {
        self.sync_task.abort();
    }
}

struct BrowserSyncExecutor {
    process_context: RuntimeProcessContext,
    binding: RuntimeBrowserProcessBinding,
    generation: std::sync::Arc<Mutex<BrowserGeneration>>,
    generation_id: u64,
    next_object_id: AtomicU64,
    mux: BrowserMuxHandle,
    pending_activation: Mutex<Option<u64>>,
}

impl BrowserSyncExecutor {
    async fn execute(
        &self,
        payload: BrowserSyncRequestPayload,
    ) -> Result<RuntimeJson, RuntimeString> {
        match payload {
            BrowserSyncRequestPayload::PlaywrightLaunch(request) => {
                self.launch_playwright(request).await
            }
            BrowserSyncRequestPayload::BrowserFfmpegEncode(request) => {
                self.launch_ffmpeg(request).await
            }
            BrowserSyncRequestPayload::PlaywrightTreeKill(request) => {
                let control = self.mux.control_for_pid(request.pid).await?;
                let output = self
                    .execute_closed_process(RuntimeBrowserBrokerProcessRequest::PlaywrightTreeKill(
                        control,
                    ))
                    .await?;
                let value = if cfg!(windows) {
                    tree_kill_windows_value(&output)
                } else {
                    RuntimeJson::Bool(true)
                };
                Ok(value_success(value))
            }
            BrowserSyncRequestPayload::PlaywrightMacOsSwVers => {
                let output = self
                    .execute_closed_process(
                        RuntimeBrowserBrokerProcessRequest::PlaywrightMacOsSwVers,
                    )
                    .await?;
                Ok(value_success(RuntimeJson::String(NodeBufferUtf8::decode(
                    &output.stdout,
                ))))
            }
            BrowserSyncRequestPayload::PlaywrightLinuxLdd(request) => {
                let operation = RuntimeBrowserBrokerProcessRequest::PlaywrightLinuxLdd(
                    RuntimeBrowserLinuxLddRequest::new(
                        RuntimePath::from_runtime_string(request.file_path),
                        RuntimePath::from_runtime_string(request.cwd),
                        request.ld_library_path,
                    ),
                );
                Ok(value_success(self.spawn_async_value(operation).await))
            }
            BrowserSyncRequestPayload::PlaywrightLinuxLdconfig => Ok(value_success(
                self.spawn_async_value(RuntimeBrowserBrokerProcessRequest::PlaywrightLinuxLdconfig)
                    .await,
            )),
            BrowserSyncRequestPayload::PlaywrightWindowsWinldd(request) => {
                let executable = verified_winldd_from_distribution(request.executable)?;
                let operation = RuntimeBrowserBrokerProcessRequest::PlaywrightWindowsWinldd(
                    RuntimeBrowserWindowsWinlddRequest::new(
                        executable,
                        RuntimePath::from_runtime_string(request.file_path),
                        RuntimePath::from_runtime_string(request.cwd),
                        request.ld_library_path,
                    ),
                );
                Ok(value_success(self.spawn_async_value(operation).await))
            }
        }
    }

    async fn execute_closed_process(
        &self,
        operation: RuntimeBrowserBrokerProcessRequest,
    ) -> Result<RuntimeExecFileOutput, RuntimeString> {
        let (request_token, process_token) = self.admit_process().await?;
        let result = self
            .process_context
            .execute_browser_broker_process(&self.binding, operation)
            .await;
        self.settle_process(request_token, process_token).await?;
        result.map_err(|error| RuntimeString::from(error.message()))
    }

    async fn spawn_async_value(
        &self,
        operation: RuntimeBrowserBrokerProcessRequest,
    ) -> RuntimeJson {
        match self.execute_closed_process(operation).await {
            Ok(output) => spawn_async_output_value(&output),
            Err(error) => spawn_async_error_value(&error),
        }
    }

    async fn launch_playwright(
        &self,
        request: BrowserPlaywrightLaunchRequest,
    ) -> Result<RuntimeJson, RuntimeString> {
        let object_id = self.reserve_object_id()?;
        let request = match core_playwright_request(request) {
            Ok(request) => request,
            Err(message) => {
                self.mux.register_spawn_failure(object_id).await?;
                self.queue_activation(object_id).await?;
                return Ok(proxy_failure_runtime(object_id, &message));
            }
        };
        let (request_token, process_token) = self.admit_process().await?;
        match self
            .process_context
            .spawn_browser_playwright_child(&self.binding, request)
            .await
        {
            Ok(child) => {
                let pid = child.pid();
                let completion = self
                    .mux
                    .register_child(object_id, child, BrowserMuxChildKind::Playwright)
                    .await?;
                self.queue_activation(object_id).await?;
                let generation = self.generation.clone();
                let generation_id = self.generation_id;
                let mux = self.mux.clone();
                tokio::spawn(async move {
                    if completion.await.is_err() {
                        mux.fail(RuntimeString::from(
                            "Browser mux child completion owner closed.",
                        ));
                    }
                    if let Err(message) = settle_browser_process(
                        generation,
                        generation_id,
                        request_token,
                        process_token,
                    )
                    .await
                    {
                        mux.fail(message);
                    }
                });
                Ok(proxy_success(object_id, pid))
            }
            Err(error) => {
                self.settle_process(request_token, process_token).await?;
                self.mux.register_spawn_failure(object_id).await?;
                self.queue_activation(object_id).await?;
                Ok(proxy_failure(object_id, error.message()))
            }
        }
    }

    async fn launch_ffmpeg(
        &self,
        request: BrowserFfmpegEncodeRequest,
    ) -> Result<RuntimeJson, RuntimeString> {
        let object_id = self.reserve_object_id()?;
        let request = RuntimeBrowserFfmpegEncodeRequest::new(
            request.list_path,
            request.file_path,
            request.fps,
        )
        .map_err(|error| RuntimeString::from(error.message()))?;
        let (request_token, process_token) = self.admit_process().await?;
        match self
            .process_context
            .spawn_browser_ffmpeg_child(&self.binding, request)
            .await
        {
            Ok(child) => {
                let pid = child.pid();
                let control = child.control();
                let mut completion = self
                    .mux
                    .register_child(object_id, child, BrowserMuxChildKind::Ffmpeg)
                    .await?;
                self.queue_activation(object_id).await?;
                let generation = self.generation.clone();
                let generation_id = self.generation_id;
                let mux = self.mux.clone();
                tokio::spawn(async move {
                    let completion_result = tokio::select! {
                        result = &mut completion => result,
                        _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                            control.hard_kill();
                            completion.await
                        }
                    };
                    if completion_result.is_err() {
                        mux.fail(RuntimeString::from(
                            "Browser mux child completion owner closed.",
                        ));
                    }
                    if let Err(message) = settle_browser_process(
                        generation,
                        generation_id,
                        request_token,
                        process_token,
                    )
                    .await
                    {
                        mux.fail(message);
                    }
                });
                Ok(proxy_success(object_id, pid))
            }
            Err(error) => {
                self.settle_process(request_token, process_token).await?;
                self.mux.register_spawn_failure(object_id).await?;
                self.queue_activation(object_id).await?;
                Ok(proxy_failure(object_id, error.message()))
            }
        }
    }

    async fn admit_process(&self) -> Result<(u64, u64), RuntimeString> {
        let mut generation = self.generation.lock().await;
        let request_token = generation
            .admit_request(self.generation_id)
            .map_err(|error| broker_error("admit browser child request", error))?;
        match generation.issue_process(self.generation_id, request_token) {
            Ok(process_token) => Ok((request_token, process_token)),
            Err(error) => {
                let _ = generation.complete_request(self.generation_id, request_token);
                Err(broker_error("issue browser child process", error))
            }
        }
    }

    async fn settle_process(
        &self,
        request_token: u64,
        process_token: u64,
    ) -> Result<(), RuntimeString> {
        settle_browser_process(
            self.generation.clone(),
            self.generation_id,
            request_token,
            process_token,
        )
        .await
    }

    async fn queue_activation(&self, object_id: u64) -> Result<(), RuntimeString> {
        let mut pending = self.pending_activation.lock().await;
        if pending.replace(object_id).is_some() {
            return Err(RuntimeString::from(
                "Browser broker activation already pending.",
            ));
        }
        Ok(())
    }

    async fn activate_pending(&self) -> Result<(), RuntimeString> {
        if let Some(object_id) = self.pending_activation.lock().await.take() {
            self.mux.activate(object_id).await?;
        }
        Ok(())
    }

    fn reserve_object_id(&self) -> Result<u64, RuntimeString> {
        self.next_object_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .map_err(|_| RuntimeString::from("Browser broker object id exhausted."))
    }
}

async fn settle_browser_process(
    generation: std::sync::Arc<Mutex<BrowserGeneration>>,
    generation_id: u64,
    request_token: u64,
    process_token: u64,
) -> Result<(), RuntimeString> {
    let mut generation = generation.lock().await;
    generation
        .settle_process(generation_id, request_token, process_token)
        .map_err(|error| broker_error("settle browser child process", error))?;
    generation
        .complete_request(generation_id, request_token)
        .map_err(|error| broker_error("complete browser child request", error))?;
    Ok(())
}

async fn run_sync_responder(
    generation: u64,
    mut inbound: RuntimeBrowserBrokerRead,
    mut outbound: RuntimeBrowserBrokerWrite,
    executor: BrowserSyncExecutor,
) -> Result<(), RuntimeString> {
    let mut exchange = BrowserSyncExchange::new(generation);
    loop {
        let bytes = read_sync_frame(&mut inbound).await?;
        let request = BrowserSyncFrame::decode_exact(&bytes)
            .map_err(|error| broker_error("decode sync request", error))?;
        exchange
            .admit_request(&request)
            .map_err(|error| broker_error("admit sync request", error))?;
        let payload = decode_sync_request_payload(request.kind, &request.payload)
            .map_err(|error| broker_error("validate sync payload", error))?;
        let response_payload = match executor.execute(payload).await {
            Ok(payload) => payload,
            Err(message) => value_failure(&message),
        };
        let response = BrowserSyncFrame::new(
            generation,
            request.request_id,
            request.kind,
            response_payload,
        )
        .map_err(|error| broker_error("construct sync response", error))?;
        exchange
            .accept_response(&response)
            .map_err(|error| broker_error("accept sync response", error))?;
        let encoded = response
            .encode()
            .map_err(|error| broker_error("encode sync response", error))?;
        outbound.write_all(&encoded).await.map_err(|error| {
            RuntimeString::from(format!("Browser broker sync write failed: {error}"))
        })?;
        outbound.flush().await.map_err(|error| {
            RuntimeString::from(format!("Browser broker sync flush failed: {error}"))
        })?;
        executor.activate_pending().await?;
    }
}

async fn read_sync_frame(inbound: &mut RuntimeBrowserBrokerRead) -> Result<Vec<u8>, RuntimeString> {
    let mut prefix = [0_u8; 4];
    inbound.read_exact(&mut prefix).await.map_err(|error| {
        RuntimeString::from(format!("Browser broker sync read failed: {error}"))
    })?;
    let remaining = u32::from_be_bytes(prefix) as usize;
    if !(SYNC_HEADER_LEN..=MAX_SYNC_REMAINING).contains(&remaining) {
        return Err(RuntimeString::from(
            "Invalid browser broker sync frame length.",
        ));
    }
    let mut bytes = vec![0_u8; 4 + remaining];
    bytes[..4].copy_from_slice(&prefix);
    inbound.read_exact(&mut bytes[4..]).await.map_err(|error| {
        RuntimeString::from(format!("Browser broker sync read failed: {error}"))
    })?;
    Ok(bytes)
}

fn core_playwright_request(
    request: BrowserPlaywrightLaunchRequest,
) -> Result<RuntimeBrowserPlaywrightLaunchRequest, RuntimeString> {
    validate_playwright_planner_evidence(&request)?;
    let RuntimeJson::Object(options) = &request.options else {
        return Err(RuntimeString::from("Invalid Playwright spawn options."));
    };
    let detached = match options.get("detached") {
        Some(RuntimeJson::Bool(value)) => *value,
        _ => return Err(RuntimeString::from("Invalid Playwright detached option.")),
    };
    match options.get("shell") {
        Some(RuntimeJson::Null | RuntimeJson::Bool(false)) => {}
        _ => return Err(RuntimeString::from("Unsupported Playwright shell option.")),
    }
    let mode = match request.mode {
        super::broker_wire::BrowserPlaywrightLaunchMode::Ephemeral => {
            RuntimeBrowserPlaywrightLaunchMode::Ephemeral
        }
        super::broker_wire::BrowserPlaywrightLaunchMode::Persistent => {
            RuntimeBrowserPlaywrightLaunchMode::Persistent
        }
    };
    let mut core = RuntimeBrowserPlaywrightLaunchRequest::new(request.command, request.args)
        .with_mode(mode)
        .with_detached(detached);
    if let Some(RuntimeJson::String(cwd)) = options.get("cwd") {
        core = core.with_cwd(RuntimePath::from_runtime_string(cwd.clone()));
    }
    if let Some(RuntimeJson::Array(entries)) = options.get("env") {
        let mut environment = RuntimeEnv::new();
        for entry in entries {
            let RuntimeJson::Array(pair) = entry else {
                return Err(RuntimeString::from("Invalid Playwright environment entry."));
            };
            let [RuntimeJson::String(key), RuntimeJson::String(value)] = pair.as_slice() else {
                return Err(RuntimeString::from("Invalid Playwright environment entry."));
            };
            environment.insert(key.clone(), value.clone());
        }
        core = core.with_environment(environment);
    }
    Ok(core)
}

fn validate_playwright_planner_evidence(
    request: &BrowserPlaywrightLaunchRequest,
) -> Result<(), RuntimeString> {
    let RuntimeJson::Object(source_options) = &request.source_options else {
        return Err(RuntimeString::from(
            "Invalid Playwright source options evidence.",
        ));
    };
    let RuntimeJson::Object(planner) = &request.planner_trace else {
        return Err(RuntimeString::from("Invalid Playwright planner evidence."));
    };
    let mut entries = planner.entries();
    let Some((executable_key, RuntimeJson::String(executable))) = entries.next() else {
        return Err(RuntimeString::from(
            "Invalid Playwright planner executable.",
        ));
    };
    let Some((browser_arguments_key, RuntimeJson::Array(browser_arguments))) = entries.next()
    else {
        return Err(RuntimeString::from(
            "Invalid Playwright planner browser arguments.",
        ));
    };
    let Some((user_data_key, RuntimeJson::String(user_data_dir))) = entries.next() else {
        return Err(RuntimeString::from(
            "Invalid Playwright planner user data directory.",
        ));
    };
    let Some((artifacts_key, RuntimeJson::String(artifacts_dir))) = entries.next() else {
        return Err(RuntimeString::from(
            "Invalid Playwright planner artifacts directory.",
        ));
    };
    let Some((temp_directories_key, RuntimeJson::Array(planner_temp_directories))) = entries.next()
    else {
        return Err(RuntimeString::from(
            "Invalid Playwright planner temporary directories.",
        ));
    };
    if executable_key != "executable"
        || browser_arguments_key != "browserArguments"
        || user_data_key != "userDataDir"
        || artifacts_key != "artifactsDir"
        || temp_directories_key != "tempDirectories"
        || entries.next().is_some()
        || executable != &request.command
        || browser_arguments.len() != request.args.len()
        || browser_arguments
            .iter()
            .zip(&request.args)
            .any(|(actual, expected)| !matches!(actual, RuntimeJson::String(value) if value == expected))
        || planner_temp_directories.len() != request.temp_directories.len()
        || planner_temp_directories
            .iter()
            .zip(&request.temp_directories)
            .any(|(actual, expected)| !matches!(actual, RuntimeJson::String(value) if value == expected))
    {
        return Err(RuntimeString::from(
            "Playwright planner evidence does not match the spawn image.",
        ));
    }

    let user_data_is_temporary = request
        .temp_directories
        .iter()
        .any(|directory| directory == user_data_dir);
    match request.mode {
        super::broker_wire::BrowserPlaywrightLaunchMode::Ephemeral if !user_data_is_temporary => {
            return Err(RuntimeString::from(
                "Ephemeral Playwright user data directory is not planner-owned.",
            ));
        }
        super::broker_wire::BrowserPlaywrightLaunchMode::Persistent if user_data_is_temporary => {
            return Err(RuntimeString::from(
                "Persistent Playwright user data directory was marked temporary.",
            ));
        }
        _ => {}
    }

    match source_options.get("executablePath") {
        Some(RuntimeJson::String(source_executable)) if source_executable == executable => {}
        Some(RuntimeJson::Null) | None => {}
        Some(_) => {
            return Err(RuntimeString::from(
                "Playwright source executable evidence does not match the planner.",
            ));
        }
    }
    match source_options.get("artifactsDir") {
        Some(RuntimeJson::String(source_artifacts)) if source_artifacts == artifacts_dir => {}
        Some(RuntimeJson::Null) | None
            if request
                .temp_directories
                .iter()
                .any(|directory| directory == artifacts_dir) => {}
        _ => {
            return Err(RuntimeString::from(
                "Playwright source artifacts evidence does not match the planner.",
            ));
        }
    }
    Ok(())
}

fn proxy_success(object_id: u64, pid: u32) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    object.insert("ok", RuntimeJson::Bool(true));
    object.insert(
        "object_id",
        RuntimeJson::String(RuntimeString::from(format!("{object_id:016x}"))),
    );
    object.insert("pid", RuntimeJson::from(pid));
    RuntimeJson::Object(object)
}

fn proxy_failure(object_id: u64, message: &str) -> RuntimeJson {
    proxy_failure_runtime(object_id, &RuntimeString::from(message))
}

fn proxy_failure_runtime(object_id: u64, message: &RuntimeString) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    object.insert("ok", RuntimeJson::Bool(false));
    object.insert(
        "object_id",
        RuntimeJson::String(RuntimeString::from(format!("{object_id:016x}"))),
    );
    object.insert("error", error_descriptor_runtime(message));
    RuntimeJson::Object(object)
}

fn value_success(value: RuntimeJson) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    object.insert("ok", RuntimeJson::Bool(true));
    object.insert("value", value);
    object.insert("error", RuntimeJson::Null);
    RuntimeJson::Object(object)
}

fn tree_kill_windows_value(output: &RuntimeExecFileOutput) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    object.insert(
        "stdout",
        RuntimeJson::String(NodeBufferUtf8::decode(&output.stdout)),
    );
    object.insert(
        "stderr",
        RuntimeJson::String(NodeBufferUtf8::decode(&output.stderr)),
    );
    RuntimeJson::Object(object)
}

fn spawn_async_output_value(output: &RuntimeExecFileOutput) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    object.insert(
        "stdout",
        RuntimeJson::String(NodeBufferUtf8::decode(&output.stdout)),
    );
    object.insert(
        "code",
        output
            .status
            .code()
            .map(RuntimeJson::from)
            .unwrap_or(RuntimeJson::Null),
    );
    object.insert("error", RuntimeJson::Null);
    RuntimeJson::Object(object)
}

fn spawn_async_error_value(error: &RuntimeString) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    object.insert("stdout", RuntimeJson::String(RuntimeString::from("")));
    object.insert("code", RuntimeJson::from(0));
    object.insert("error", error_descriptor_runtime(error));
    RuntimeJson::Object(object)
}

fn value_failure(message: &RuntimeString) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    object.insert("ok", RuntimeJson::Bool(false));
    object.insert("value", RuntimeJson::Null);
    object.insert("error", error_descriptor_runtime(message));
    RuntimeJson::Object(object)
}

fn error_descriptor(message: &str) -> RuntimeJson {
    error_descriptor_runtime(&RuntimeString::from(message))
}

fn error_descriptor_runtime(message: &RuntimeString) -> RuntimeJson {
    let mut error = RuntimeObject::new();
    error.insert("name", RuntimeJson::from("Error"));
    error.insert("message", RuntimeJson::String(message.clone()));
    error.insert("code", RuntimeJson::Null);
    error.insert("errno", RuntimeJson::Null);
    error.insert("syscall", RuntimeJson::Null);
    error.insert("path", RuntimeJson::Null);
    RuntimeJson::Object(error)
}

fn broker_error(action: &str, error: impl std::fmt::Debug) -> RuntimeString {
    RuntimeString::from(format!("Browser broker failed to {action}: {error:?}"))
}
