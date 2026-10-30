use crate::network_proxy::ManagedNetworkSandboxContext;
use crate::runtime_process::RuntimeEnv;
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimePath, RuntimeString,
};
use crate::tools::shell::{process_env, resolve_windows_shell, ShellKind};
use crate::types::sandbox::{ExecRequest, SandboxBackend, SandboxPolicy, SpawnSpec};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, VecDeque};

const POWERSHELL_UTF8_PREFIX: &str =
    "try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch {}\n";
const POWERSHELL_EXITCODE_SUFFIX: &str =
    "\n$__ae_ec = if ($null -ne $LASTEXITCODE) { $LASTEXITCODE } elseif ($?) { 0 } else { 1 }\nexit $__ae_ec";

fn decode_provider_unit_enum<T: Copy>(
    value: &RuntimeJson,
    variants: &[RuntimeEnumVariant<'_>],
    values: &[T],
) -> Result<T, RuntimeCodecError> {
    runtime_codec::decode_external(value, variants, |variant| match variant {
        RuntimeExternalVariant::Unit { variant } => values
            .get(variant)
            .copied()
            .ok_or_else(|| RuntimeCodecError::new(RuntimeCodecErrorKind::UnknownEnumVariant)),
        RuntimeExternalVariant::Payload { .. } => Err(RuntimeCodecError::new(
            RuntimeCodecErrorKind::InvalidEnumShape,
        )),
    })
}

pub mod background;
pub mod exec_server;
pub mod foreground;
pub mod interactive;
pub mod process;

/// Stable id for a command process controlled by the command runtime.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CommandProcessId(RuntimeString);

impl CommandProcessId {
    pub fn new(id: impl Into<RuntimeString>) -> Self {
        Self(id.into())
    }

    pub fn as_runtime_string(&self) -> &RuntimeString {
        &self.0
    }

    fn generated_ascii(&self) -> String {
        self.0
            .try_to_utf8()
            .expect("command process ids are generated ASCII")
    }
}

impl RuntimeEncode for CommandProcessId {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        self.0.encode_runtime(out)
    }
}

impl RuntimeDecode for CommandProcessId {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        RuntimeString::decode_runtime(value).map(Self)
    }
}

/// Small allocator for long-lived command process ids.
#[derive(Debug, Clone)]
pub struct CommandProcessIdAllocator {
    next: u64,
    deterministic: bool,
    reserved: BTreeSet<String>,
    released: BTreeSet<String>,
}

impl CommandProcessIdAllocator {
    pub fn deterministic(start: u64) -> Self {
        Self {
            next: start,
            deterministic: true,
            reserved: BTreeSet::new(),
            released: BTreeSet::new(),
        }
    }

    pub fn random() -> Self {
        Self {
            next: 0,
            deterministic: false,
            reserved: BTreeSet::new(),
            released: BTreeSet::new(),
        }
    }

    pub fn reserve(&mut self) -> CommandProcessId {
        if let Some(id) = self.released.pop_first() {
            self.reserved.insert(id.clone());
            return CommandProcessId::new(id);
        }
        if !self.deterministic {
            loop {
                let id = random_uuid();
                if self.reserved.insert(id.clone()) {
                    return CommandProcessId::new(id);
                }
            }
        }
        let id = self.next;
        self.next = self.next.saturating_add(1);
        self.reserved.insert(id.to_string());
        CommandProcessId::new(id.to_string())
    }

    pub fn release(&mut self, id: &CommandProcessId) {
        let id = id.generated_ascii();
        if self.reserved.remove(&id) && self.deterministic {
            self.released.insert(id);
        }
    }
}

fn random_uuid() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5],
        bytes[6], bytes[7],
        bytes[8], bytes[9],
        bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
    )
}

/// Shared lifecycle state for a command process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CommandProcessStatus {
    Running,
    Exited,
    Killed,
    Failed,
}

impl RuntimeEncode for CommandProcessStatus {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Running => "running",
            Self::Exited => "exited",
            Self::Killed => "killed",
            Self::Failed => "failed",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for CommandProcessStatus {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("running", &[]),
                RuntimeEnumVariant::new("exited", &[]),
                RuntimeEnumVariant::new("killed", &[]),
                RuntimeEnumVariant::new("failed", &[]),
            ],
            &[Self::Running, Self::Exited, Self::Killed, Self::Failed],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandProcessState {
    id: CommandProcessId,
    status: CommandProcessStatus,
    exit_code: Option<i32>,
    failure_message: Option<RuntimeString>,
}

impl RuntimeEncode for CommandProcessState {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("id", &self.id)?;
        object.field("status", &self.status)?;
        object.field("exit_code", &self.exit_code)?;
        object.field("failure_message", &self.failure_message)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for CommandProcessState {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                id: object.required("id", &[])?,
                status: object.required("status", &[])?,
                exit_code: object.optional("exit_code", &[])?,
                failure_message: object.optional("failure_message", &[])?,
            })
        })
    }
}

impl CommandProcessState {
    pub fn running(id: CommandProcessId) -> Self {
        Self {
            id,
            status: CommandProcessStatus::Running,
            exit_code: None,
            failure_message: None,
        }
    }

    pub fn status(&self) -> CommandProcessStatus {
        self.status
    }

    pub fn exit_code(&self) -> Option<i32> {
        self.exit_code
    }

    pub fn failure_message(&self) -> Option<&RuntimeString> {
        self.failure_message.as_ref()
    }

    pub fn mark_exited(&mut self, exit_code: Option<i32>) {
        self.status = CommandProcessStatus::Exited;
        self.exit_code = exit_code;
        self.failure_message = None;
    }

    pub fn mark_killed(&mut self) {
        self.status = CommandProcessStatus::Killed;
        self.exit_code = None;
        self.failure_message = None;
    }

    pub fn mark_failed(&mut self, message: impl Into<RuntimeString>) {
        self.status = CommandProcessStatus::Failed;
        self.failure_message = Some(message.into());
    }
}

/// Bounded output buffer for command-runtime controlled processes.
pub struct CommandOutputBuffer {
    head: Vec<OutputChunk>,
    tail: VecDeque<OutputChunk>,
    head_bytes: usize,
    tail_bytes: usize,
    produced: usize,
    tail_start: usize,
    head_limit: usize,
    tail_limit: usize,
}

#[derive(Clone)]
struct OutputChunk {
    bytes: Vec<u8>,
    text: RuntimeString,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutputRead {
    pub text: RuntimeString,
    pub next_cursor: usize,
    pub dropped_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutputBytesRead {
    pub bytes: Vec<u8>,
    pub next_cursor: usize,
    pub dropped_bytes: usize,
}

impl CommandOutputBuffer {
    pub fn new(max_bytes: usize) -> Self {
        let head_limit = (max_bytes as f64 * 0.5).floor() as usize;
        Self {
            head: Vec::new(),
            tail: VecDeque::new(),
            head_bytes: 0,
            tail_bytes: 0,
            produced: 0,
            tail_start: 0,
            head_limit,
            tail_limit: max_bytes.saturating_sub(head_limit).max(1),
        }
    }

    pub fn push(&mut self, chunk: &[u8]) {
        self.produced = self.produced.saturating_add(chunk.len());
        let mut remaining = chunk;
        if self.head_bytes < self.head_limit {
            let take = remaining.len().min(self.head_limit - self.head_bytes);
            if take > 0 {
                self.head.push(OutputChunk::new(&remaining[..take]));
                self.head_bytes += take;
                remaining = &remaining[take..];
            }
        }
        if remaining.is_empty() {
            return;
        }
        if self.tail_start == 0 {
            self.tail_start = self.head_bytes;
        }
        self.tail.push_back(OutputChunk::new(remaining));
        self.tail_bytes += remaining.len();
        while self.tail_bytes > self.tail_limit {
            let overflow = self.tail_bytes - self.tail_limit;
            let Some(front_len) = self.tail.front().map(|front| front.bytes.len()) else {
                break;
            };
            if front_len <= overflow && self.tail.len() > 1 {
                let removed = self.tail.pop_front().expect("front exists");
                self.tail_bytes -= removed.bytes.len();
                self.tail_start += removed.bytes.len();
            } else {
                let front = self.tail.front_mut().expect("front exists");
                let take = overflow.min(front_len);
                front.bytes.drain(..take);
                front.text = crate::runtime_values::NodeChunkUtf8::decode_chunk(&front.bytes);
                self.tail_bytes -= take;
                self.tail_start += take;
            }
        }
    }

    pub fn read_from(&self, cursor: usize) -> CommandOutputRead {
        if cursor >= self.produced {
            return CommandOutputRead {
                text: RuntimeString::default(),
                next_cursor: self.produced,
                dropped_bytes: 0,
            };
        }
        let mut output = RuntimeString::default();
        let mut logical = cursor;
        if logical < self.head_bytes {
            output = append_chunks_from(&output, &self.head, logical);
            logical = self.head_bytes;
        }
        let dropped_bytes = self.tail_start.saturating_sub(logical);
        if dropped_bytes > 0 {
            output = output.concat(&RuntimeString::from_utf8(&format!(
                "… [{dropped_bytes} bytes elided] …\n"
            )));
            logical = self.tail_start;
        }
        if logical < self.produced {
            let (first, second) = self.tail.as_slices();
            let mut skip = logical - self.tail_start;
            output = append_chunks_from(&output, first, skip);
            skip = skip.saturating_sub(first.iter().map(|chunk| chunk.bytes.len()).sum());
            output = append_chunks_from(&output, second, skip);
        }
        CommandOutputRead {
            text: output,
            next_cursor: self.produced,
            dropped_bytes,
        }
    }

    pub fn read_bytes_from(&self, cursor: usize, max_bytes: usize) -> CommandOutputBytesRead {
        let cursor = cursor.min(self.produced);
        let start = cursor.max(if cursor < self.head_bytes {
            cursor
        } else {
            self.tail_start
        });
        let dropped_bytes = start.saturating_sub(cursor);
        let mut retained = Vec::new();
        if start < self.head_bytes {
            append_chunk_bytes_from(&mut retained, &self.head, start, max_bytes);
        }
        if retained.len() < max_bytes && start.max(self.head_bytes) < self.produced {
            let skip = start.max(self.head_bytes).saturating_sub(self.tail_start);
            let (first, second) = self.tail.as_slices();
            append_chunk_bytes_from(&mut retained, first, skip, max_bytes);
            let first_bytes = first.iter().map(|chunk| chunk.bytes.len()).sum::<usize>();
            append_chunk_bytes_from(
                &mut retained,
                second,
                skip.saturating_sub(first_bytes),
                max_bytes,
            );
        }
        CommandOutputBytesRead {
            next_cursor: start.saturating_add(retained.len()),
            bytes: retained,
            dropped_bytes,
        }
    }

    pub fn retained_start(&self) -> usize {
        if self.produced <= self.head_bytes {
            0
        } else {
            self.tail_start
        }
    }

    pub fn end(&self) -> usize {
        self.produced
    }
}

impl OutputChunk {
    fn new(bytes: &[u8]) -> Self {
        Self {
            bytes: bytes.to_vec(),
            text: crate::runtime_values::NodeChunkUtf8::decode_chunk(bytes),
        }
    }
}

fn append_chunks_from(
    base: &RuntimeString,
    chunks: &[OutputChunk],
    mut skip: usize,
) -> RuntimeString {
    let mut output = base.clone();
    for chunk in chunks {
        if skip >= chunk.bytes.len() {
            skip -= chunk.bytes.len();
            continue;
        }
        if skip == 0 {
            output = output.concat(&chunk.text);
        } else {
            output = output.concat(&crate::runtime_values::NodeChunkUtf8::decode_chunk(
                &chunk.bytes[skip..],
            ));
            skip = 0;
        }
    }
    output
}

fn append_chunk_bytes_from(
    output: &mut Vec<u8>,
    chunks: &[OutputChunk],
    mut skip: usize,
    max_bytes: usize,
) {
    for chunk in chunks {
        if output.len() >= max_bytes {
            break;
        }
        if skip >= chunk.bytes.len() {
            skip -= chunk.bytes.len();
            continue;
        }
        let available = &chunk.bytes[skip..];
        let take = available.len().min(max_bytes - output.len());
        output.extend_from_slice(&available[..take]);
        skip = 0;
    }
}

/// One spawn request before sandbox transformation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpawnRequest {
    pub command: RuntimeString,
    pub cwd: RuntimePath,
    pub env: Option<RuntimeEnv>,
    pub writable_roots: Vec<RuntimeString>,
    pub network_allowed: bool,
    pub managed_network: Option<ManagedNetworkSandboxContext>,
    /// Waive the sandbox's write confinement (see
    /// [`SandboxPolicy::unconfined_writes`]). Set under `bypassPermissions`.
    pub unconfined_writes: bool,
}

impl CommandSpawnRequest {
    pub fn to_spawn_spec(&self, sandbox: &dyn SandboxBackend) -> SpawnSpec {
        let policy = SandboxPolicy {
            writable_roots: self.writable_roots.clone(),
            unconfined_writes: self.unconfined_writes.then_some(true),
            network_allowed: if self.managed_network.is_some() {
                false
            } else {
                self.network_allowed
            },
            allow_loopback_ports: self
                .managed_network
                .as_ref()
                .map(|network| network.loopback_ports.clone()),
            allow_unix_sockets: self.managed_network.as_ref().map(|network| {
                network
                    .allow_unix_sockets
                    .iter()
                    .map(RuntimeString::from)
                    .collect()
            }),
            allow_all_unix_sockets: self
                .managed_network
                .as_ref()
                .map(|network| network.allow_all_unix_sockets),
            windows_network_proxy_restricting_sid: self
                .managed_network
                .as_ref()
                .and_then(|network| network.network_proxy_restricting_sid.clone()),
            windows_network_allow_local_binding: self
                .managed_network
                .as_ref()
                .map(|network| network.allow_local_binding),
            ..Default::default()
        };
        let req = ExecRequest {
            argv: build_runtime_shell_invocation(&self.command),
            cwd: self.cwd.clone(),
            env: self.env.clone(),
            timeout_ms: None,
        };
        sandbox.transform(&req, &policy)
    }
}

pub(crate) fn build_runtime_shell_invocation(command: &RuntimeString) -> Vec<RuntimeString> {
    if !cfg!(windows) {
        return vec![
            RuntimeString::from("bash"),
            RuntimeString::from("-c"),
            command.clone(),
        ];
    }
    let shell = resolve_windows_shell(&process_env());
    let file = RuntimeString::from(shell.path);
    match shell.kind {
        ShellKind::Bash => vec![file, RuntimeString::from("-c"), command.clone()],
        ShellKind::Cmd => vec![file, RuntimeString::from("/c"), command.clone()],
        ShellKind::Pwsh | ShellKind::Powershell => {
            let script = RuntimeString::from(POWERSHELL_UTF8_PREFIX)
                .concat(command)
                .concat(&RuntimeString::from(POWERSHELL_EXITCODE_SUFFIX));
            vec![
                file,
                RuntimeString::from("-NoProfile"),
                RuntimeString::from("-NonInteractive"),
                RuntimeString::from("-Command"),
                script,
            ]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_process::RuntimeEnv;
    use crate::runtime_values::runtime_json;
    use crate::runtime_values::RuntimePath;
    use crate::types::sandbox::{
        ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
    };
    use std::sync::{Arc, Mutex};

    struct CaptureSandbox {
        captured: Arc<Mutex<Option<(ExecRequest, SandboxPolicy)>>>,
    }

    impl SandboxBackend for CaptureSandbox {
        fn kind(&self) -> SandboxKind {
            SandboxKind::Process
        }

        fn is_available(&self) -> bool {
            true
        }

        fn transform(&self, req: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
            *self.captured.lock().unwrap() = Some((req.clone(), policy.clone()));
            SpawnSpec {
                file: req.argv[0].clone(),
                args: req.argv[1..].to_vec(),
                cwd: req.cwd.clone(),
                env: req.env.clone().unwrap_or_default(),
            }
        }
    }

    #[test]
    fn process_id_allocator_reserves_unique_ids_and_reuses_released_ids() {
        let mut allocator = CommandProcessIdAllocator::deterministic(1000);

        let first = allocator.reserve();
        let second = allocator.reserve();
        allocator.release(&first);
        let reused = allocator.reserve();

        assert_eq!(first.as_runtime_string(), "1000");
        assert_eq!(second.as_runtime_string(), "1001");
        assert_eq!(reused.as_runtime_string(), "1000");
    }

    #[test]
    fn process_id_allocator_ignores_ids_that_were_never_reserved() {
        let mut allocator = CommandProcessIdAllocator::deterministic(1000);

        allocator.release(&CommandProcessId::new("1005"));

        assert_eq!(allocator.reserve().as_runtime_string(), "1000");
        assert_eq!(allocator.reserve().as_runtime_string(), "1001");
    }

    #[test]
    fn random_process_id_allocator_does_not_reuse_released_ids() {
        let mut allocator = CommandProcessIdAllocator::random();

        let first = allocator.reserve();
        allocator.release(&first);
        let second = allocator.reserve();

        assert_ne!(first, second);
    }

    #[test]
    fn process_state_records_terminal_exit_kill_and_failure() {
        let mut exited = CommandProcessState::running(CommandProcessId::new("p1"));
        exited.mark_exited(Some(3));
        assert_eq!(exited.status(), CommandProcessStatus::Exited);
        assert_eq!(exited.exit_code(), Some(3));
        assert_eq!(exited.failure_message(), None);

        let mut killed = CommandProcessState::running(CommandProcessId::new("p2"));
        killed.mark_killed();
        assert_eq!(killed.status(), CommandProcessStatus::Killed);
        assert_eq!(killed.exit_code(), None);

        let mut failed = CommandProcessState::running(CommandProcessId::new("p3"));
        failed.mark_failed("spawn failed");
        assert_eq!(failed.status(), CommandProcessStatus::Failed);
        assert_eq!(
            failed.failure_message(),
            Some(&RuntimeString::from("spawn failed"))
        );
    }

    #[test]
    fn output_buffer_reads_from_cursor_and_reports_dropped_bytes() {
        let mut buffer = CommandOutputBuffer::new(64);
        buffer.push(b"hello");
        let first = buffer.read_from(0);
        assert_eq!(first.text, "hello");
        assert_eq!(first.dropped_bytes, 0);

        buffer.push(b" world");
        let second = buffer.read_from(first.next_cursor);
        assert_eq!(second.text, " world");

        let mut small = CommandOutputBuffer::new(10);
        small.push(b"hello");
        small.push(b" world 1234567890");
        let full = small.read_from(0);
        assert!(full.text.contains("bytes elided"));
        assert!(full.dropped_bytes > 0);
    }

    #[test]
    fn spawn_request_uses_existing_shell_and_sandbox_contract() {
        let captured = Arc::new(Mutex::new(None));
        let sandbox = CaptureSandbox {
            captured: Arc::clone(&captured),
        };
        let mut env = RuntimeEnv::new();
        env.insert(RuntimeString::from("K"), RuntimeString::from("V"));

        let request = CommandSpawnRequest {
            command: RuntimeString::from("echo hello"),
            cwd: RuntimePath::from("/tmp"),
            env: Some(env.clone()),
            writable_roots: vec![RuntimeString::from("/tmp/project")],
            network_allowed: true,
            managed_network: None,
            unconfined_writes: false,
        };

        let spec = request.to_spawn_spec(&sandbox);

        assert!(!spec.file.is_empty());
        assert_eq!(spec.cwd, RuntimePath::from("/tmp"));
        assert_eq!(spec.env, env);

        let (exec_request, policy) = captured.lock().unwrap().clone().unwrap();
        assert_eq!(exec_request.cwd, RuntimePath::from("/tmp"));
        assert_eq!(exec_request.env, Some(env));
        assert_eq!(
            policy.writable_roots,
            vec![RuntimeString::from("/tmp/project")]
        );
        assert!(policy.network_allowed);
        assert!(!exec_request.argv.is_empty());
    }

    #[test]
    fn the_sandbox_and_the_spawn_spec_both_keep_a_lone_surrogate_exact() {
        let captured = Arc::new(Mutex::new(None));
        let sandbox = CaptureSandbox {
            captured: Arc::clone(&captured),
        };
        let parsed = runtime_json::parse(&RuntimeString::from(r#""\ud800""#)).unwrap();
        let lone = parsed.as_str().unwrap();
        let mut env = RuntimeEnv::new();
        env.insert(RuntimeString::from("VALUE"), lone.clone());
        let request = CommandSpawnRequest {
            command: RuntimeString::from("printf ").concat(lone),
            cwd: RuntimePath::from_runtime_string(RuntimeString::from("/tmp/").concat(lone)),
            env: Some(env),
            writable_roots: vec![RuntimeString::from("/tmp/").concat(lone)],
            network_allowed: false,
            managed_network: None,
            unconfined_writes: false,
        };

        let logical_spec = request.to_spawn_spec(&sandbox);
        let (logical_request, logical_policy) = captured.lock().unwrap().clone().unwrap();
        assert!(runtime_json::stringify(
            &runtime_codec::encode_value(&logical_request.argv).unwrap()
        )
        .contains(r"\ud800"));
        assert!(runtime_json::stringify(
            &runtime_codec::encode_value(&logical_policy.writable_roots).unwrap(),
        )
        .contains(r"\ud800"));

        // The spec itself must stay exact: `to_spawn_spec` hands the surrogate onward
        // untouched, and the Node/V8 UTF-8 projection that turns it into U+FFFD happens
        // later, at the spawn boundary. Asserting that here is what would catch a
        // regression that made this conversion lossy again — it used to be, back when
        // `SpawnSpec` was `String`-backed. The projection itself is proven end to end by
        // `tools::builtins::bash::tests::parsed_lone_surrogate_command_projects_at_spawn`,
        // which runs a real command and reads the U+FFFD filename back off disk.
        assert!(logical_spec
            .args
            .iter()
            .any(|arg| arg == &RuntimeString::from("printf ").concat(lone)));
        assert_eq!(
            logical_spec.cwd.as_runtime_string(),
            &RuntimeString::from("/tmp/").concat(lone)
        );
        assert_eq!(
            logical_spec.env.get(&RuntimeString::from("VALUE")),
            Some(lone)
        );
    }

    #[test]
    fn spawn_request_threads_managed_network_loopback_ports() {
        let captured = Arc::new(Mutex::new(None));
        let sandbox = CaptureSandbox {
            captured: Arc::clone(&captured),
        };

        let request = CommandSpawnRequest {
            command: RuntimeString::from("curl https://example.com"),
            cwd: RuntimePath::from("/tmp"),
            env: None,
            writable_roots: vec![RuntimeString::from("/tmp/project")],
            network_allowed: false,
            managed_network: Some(crate::network_proxy::ManagedNetworkSandboxContext {
                loopback_ports: vec![43123],
                allow_local_binding: false,
                allow_unix_sockets: vec![RuntimeString::from("/tmp/docker.sock")],
                allow_all_unix_sockets: false,
                managed_ca_bundle_path: None,
                network_proxy_restricting_sid: None,
                windows_broker_generation: None,
            }),
            unconfined_writes: false,
        };

        let _ = request.to_spawn_spec(&sandbox);

        let (_exec_request, policy) = captured.lock().unwrap().clone().unwrap();
        assert!(!policy.network_allowed);
        assert_eq!(policy.allow_loopback_ports, Some(vec![43123]));
        assert_eq!(
            policy.allow_unix_sockets,
            Some(vec![RuntimeString::from("/tmp/docker.sock")])
        );
    }
}
