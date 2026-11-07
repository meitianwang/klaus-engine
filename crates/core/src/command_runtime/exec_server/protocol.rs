use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use std::path::{Path, PathBuf};
use url::Url;

pub use crate::network_proxy::ManagedNetworkSandboxContext;
use crate::runtime_process::RuntimeEnv;
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimePath, RuntimeString,
};

pub const INITIALIZE_METHOD: &str = "initialize";
pub const INITIALIZED_METHOD: &str = "initialized";
pub const ENVIRONMENT_INFO_METHOD: &str = "environment/info";
pub const PROCESS_START_METHOD: &str = "process/start";
pub const PROCESS_READ_METHOD: &str = "process/read";
pub const PROCESS_WRITE_METHOD: &str = "process/write";
pub const PROCESS_SIGNAL_METHOD: &str = "process/signal";
pub const PROCESS_TERMINATE_METHOD: &str = "process/terminate";
pub const PROCESS_OUTPUT_METHOD: &str = "process/output";
pub const PROCESS_EXITED_METHOD: &str = "process/exited";
pub const PROCESS_CLOSED_METHOD: &str = "process/closed";
pub const FS_READ_FILE_METHOD: &str = "fs/readFile";
pub const FS_OPEN_METHOD: &str = "fs/open";
pub const FS_READ_BLOCK_METHOD: &str = "fs/readBlock";
pub const FS_CLOSE_METHOD: &str = "fs/close";
pub const FS_WRITE_FILE_METHOD: &str = "fs/writeFile";
pub const FS_CREATE_DIRECTORY_METHOD: &str = "fs/createDirectory";
pub const FS_GET_METADATA_METHOD: &str = "fs/getMetadata";
pub const FS_CANONICALIZE_METHOD: &str = "fs/canonicalize";
pub const FS_READ_DIRECTORY_METHOD: &str = "fs/readDirectory";
pub const FS_WALK_METHOD: &str = "fs/walk";
pub const FS_REMOVE_METHOD: &str = "fs/remove";
pub const FS_COPY_METHOD: &str = "fs/copy";
pub const HTTP_REQUEST_METHOD: &str = "http/request";
pub const HTTP_REQUEST_BODY_DELTA_METHOD: &str = "http/request/bodyDelta";

pub type ProcessId = String;

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

fn runtime_custom_error(message: impl Into<String>) -> RuntimeCodecError {
    RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(RuntimeString::from(
        message.into(),
    )))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PathUri(RuntimeString);

impl PathUri {
    pub fn parse(value: impl Into<RuntimeString>) -> Result<Self, String> {
        let value = value.into();
        let scalar = value
            .try_to_utf8()
            .map_err(|error| format!("invalid file URI: {error}"))?;
        let url = Url::parse(&scalar).map_err(|err| format!("invalid file URI: {err}"))?;
        if url.scheme() != "file" {
            return Err("path must be a file URI".to_string());
        }
        Ok(Self(value))
    }

    pub fn from_host_native_path(path: impl AsRef<Path>) -> Result<Self, String> {
        Url::from_file_path(path.as_ref())
            .map(|url| Self(RuntimeString::from(url.to_string())))
            .map_err(|_| "path cannot be represented as a file URI".to_string())
    }

    pub fn to_host_path(&self) -> Result<PathBuf, String> {
        let scalar = self
            .0
            .try_to_utf8()
            .map_err(|error| format!("invalid file URI: {error}"))?;
        Url::parse(&scalar)
            .map_err(|err| format!("invalid file URI: {err}"))?
            .to_file_path()
            .map_err(|_| "file URI cannot be converted to a native path".to_string())
    }

    pub fn as_runtime_string(&self) -> &RuntimeString {
        &self.0
    }

    pub fn into_runtime_path(self) -> RuntimePath {
        RuntimePath::from_runtime_string(self.0)
    }
}

impl RuntimeEncode for PathUri {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        self.0.encode_runtime(out)
    }
}

impl RuntimeDecode for PathUri {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        let value = RuntimeString::decode_runtime(value)?;
        Self::parse(value).map_err(runtime_custom_error)
    }
}

pub fn runtime_path_uri_from_host_native_path(
    path: impl AsRef<Path>,
) -> Result<RuntimePath, String> {
    PathUri::from_host_native_path(path).map(PathUri::into_runtime_path)
}

pub fn runtime_path_uri_to_host_path(path: &RuntimePath) -> Result<PathBuf, String> {
    PathUri::parse(path.as_runtime_string().clone())?.to_host_path()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ByteChunk(pub Vec<u8>);

impl ByteChunk {
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }
}

impl From<Vec<u8>> for ByteChunk {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl RuntimeEncode for ByteChunk {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        RuntimeString::from(BASE64_STANDARD.encode(&self.0)).encode_runtime(out)
    }
}

impl RuntimeDecode for ByteChunk {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        let encoded = RuntimeString::decode_runtime(value)?;
        let bytes = encoded
            .as_utf16()
            .iter()
            .copied()
            .map(|unit| {
                u8::try_from(unit)
                    .map_err(|_| runtime_custom_error("base64 byte chunk must contain ASCII only"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        BASE64_STANDARD
            .decode(bytes)
            .map(Self)
            .map_err(|error| runtime_custom_error(format!("invalid base64 byte chunk: {error}")))
    }
}

#[derive(Debug)]
struct ScalarRuntimeString(String);

impl RuntimeDecode for ScalarRuntimeString {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        RuntimeString::decode_runtime(value)?
            .try_to_utf8()
            .map(Self)
            .map_err(|error| runtime_custom_error(format!("scalar string required: {error}")))
    }
}

fn validate_runtime_path_uri(path: &RuntimePath) -> Result<(), RuntimeCodecError> {
    PathUri::parse(path.as_runtime_string().clone())
        .map(|_| ())
        .map_err(runtime_custom_error)
}

fn validate_runtime_path_uri_list(paths: &[RuntimePath]) -> Result<(), RuntimeCodecError> {
    for (index, path) in paths.iter().enumerate() {
        validate_runtime_path_uri(path).map_err(|error| error.prepend(index.to_string()))?;
    }
    Ok(())
}

macro_rules! encode_runtime_object_field {
    ($object:ident, $source:ident, $field:ident, $wire:literal, omit_optional) => {
        if let Some(value) = &$source.$field {
            $object.field($wire, value)?;
        }
    };
    ($object:ident, $source:ident, $field:ident, $wire:literal, path_required) => {
        validate_runtime_path_uri(&$source.$field).map_err(|error| error.prepend($wire))?;
        $object.field($wire, &$source.$field)?;
    };
    ($object:ident, $source:ident, $field:ident, $wire:literal, path_optional) => {
        if let Some(value) = &$source.$field {
            validate_runtime_path_uri(value).map_err(|error| error.prepend($wire))?;
        }
        $object.field($wire, &$source.$field)?;
    };
    ($object:ident, $source:ident, $field:ident, $wire:literal, path_vec_defaulted) => {
        validate_runtime_path_uri_list(&$source.$field).map_err(|error| error.prepend($wire))?;
        $object.field($wire, &$source.$field)?;
    };
    ($object:ident, $source:ident, $field:ident, $wire:literal, $mode:ident) => {
        $object.field($wire, &$source.$field)?;
    };
}

macro_rules! decode_runtime_object_field {
    ($object:ident, $wire:literal, required) => {
        $object.required($wire, &[])
    };
    ($object:ident, $wire:literal, scalar_required) => {
        $object
            .required::<ScalarRuntimeString>($wire, &[])
            .map(|value| value.0)
    };
    ($object:ident, $wire:literal, defaulted) => {
        $object.defaulted($wire, &[], Default::default)
    };
    ($object:ident, $wire:literal, optional) => {
        $object.optional($wire, &[])
    };
    ($object:ident, $wire:literal, omit_optional) => {
        $object.optional($wire, &[])
    };
    ($object:ident, $wire:literal, path_required) => {
        $object
            .required::<RuntimePath>($wire, &[])
            .and_then(|value| {
                validate_runtime_path_uri(&value)
                    .map_err(|error| error.prepend($wire))
                    .map(|_| value)
            })
    };
    ($object:ident, $wire:literal, path_optional) => {
        $object
            .optional::<RuntimePath>($wire, &[])
            .and_then(|value| {
                if let Some(path) = value.as_ref() {
                    validate_runtime_path_uri(path).map_err(|error| error.prepend($wire))?;
                }
                Ok(value)
            })
    };
    ($object:ident, $wire:literal, path_vec_defaulted) => {
        $object
            .defaulted::<Vec<RuntimePath>>($wire, &[], Vec::new)
            .and_then(|value| {
                validate_runtime_path_uri_list(&value)
                    .map_err(|error| error.prepend($wire))
                    .map(|_| value)
            })
    };
}

macro_rules! impl_runtime_object_codec {
    ($type:ident { $( $field:ident => ($wire:literal, $mode:ident) ),* $(,)? }) => {
        impl RuntimeEncode for $type {
            fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
                let mut object = RuntimeObjectEncoder::new();
                $(encode_runtime_object_field!(object, self, $field, $wire, $mode);)*
                out.emit(RuntimeJson::Object(object.finish()))
            }
        }

        impl RuntimeDecode for $type {
            fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
                runtime_codec::decode_object(value, false, |object| {
                    Ok(Self {
                        $($field: decode_runtime_object_field!(object, $wire, $mode)?,)*
                    })
                })
            }
        }
    };
}

impl RuntimeEncode for ManagedNetworkSandboxContext {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("loopbackPorts", &self.loopback_ports)?;
        object.field("allowLocalBinding", &self.allow_local_binding)?;
        if !self.allow_unix_sockets.is_empty() {
            object.field("allowUnixSockets", &self.allow_unix_sockets)?;
        }
        if self.allow_all_unix_sockets {
            object.field("allowAllUnixSockets", &self.allow_all_unix_sockets)?;
        }
        if let Some(value) = &self.managed_ca_bundle_path {
            object.field("managedCaBundlePath", value)?;
        }
        if let Some(value) = &self.network_proxy_restricting_sid {
            object.field("networkProxyRestrictingSid", value)?;
        }
        if let Some(value) = &self.windows_broker_generation {
            object.field("windowsBrokerGeneration", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ManagedNetworkSandboxContext {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                loopback_ports: object.defaulted("loopbackPorts", &[], Vec::new)?,
                allow_local_binding: object.defaulted("allowLocalBinding", &[], || false)?,
                allow_unix_sockets: object.defaulted("allowUnixSockets", &[], Vec::new)?,
                allow_all_unix_sockets: object.defaulted("allowAllUnixSockets", &[], || false)?,
                managed_ca_bundle_path: object.optional("managedCaBundlePath", &[])?,
                network_proxy_restricting_sid: object
                    .optional("networkProxyRestrictingSid", &[])?,
                windows_broker_generation: object.optional("windowsBrokerGeneration", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitializeParams {
    pub client_name: RuntimeString,
    pub resume_session_id: Option<RuntimeString>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitializeResponse {
    pub session_id: RuntimeString,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShellInfo {
    pub name: RuntimeString,
    pub path: RuntimeString,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvironmentInfo {
    pub shell: ShellInfo,
    pub cwd: Option<RuntimePath>,
}

impl EnvironmentInfo {
    pub fn local() -> Self {
        let shell_path = std::env::var("SHELL").unwrap_or_else(|_| {
            #[cfg(windows)]
            {
                "cmd.exe".to_string()
            }
            #[cfg(not(windows))]
            {
                "/bin/sh".to_string()
            }
        });
        let name = Path::new(&shell_path)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(shell_path.as_str())
            .to_string();
        Self {
            shell: ShellInfo {
                name: RuntimeString::from(name),
                path: RuntimeString::from(shell_path),
            },
            cwd: std::env::current_dir()
                .ok()
                .and_then(|path| runtime_path_uri_from_host_native_path(path).ok()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecParams {
    pub process_id: ProcessId,
    pub argv: Vec<RuntimeString>,
    pub cwd: RuntimePath,
    pub env_policy: Option<ExecEnvPolicy>,
    pub env: RuntimeEnv,
    pub tty: bool,
    pub pipe_stdin: bool,
    pub arg0: Option<RuntimeString>,
    pub sandbox: Option<FileSystemSandboxContext>,
    pub enforce_managed_network: bool,
    pub managed_network: Option<ManagedNetworkSandboxContext>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecEnvPolicy {
    pub inherit: ShellEnvironmentPolicyInherit,
    pub ignore_default_excludes: bool,
    pub exclude: Vec<RuntimeString>,
    pub set_env: RuntimeEnv,
    pub include_only: Vec<RuntimeString>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShellEnvironmentPolicyInherit {
    All,
    Core,
    None,
}

impl RuntimeEncode for ShellEnvironmentPolicyInherit {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::All => "all",
            Self::Core => "core",
            Self::None => "none",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ShellEnvironmentPolicyInherit {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("all", &[]),
                RuntimeEnumVariant::new("core", &[]),
                RuntimeEnumVariant::new("none", &[]),
            ],
            &[Self::All, Self::Core, Self::None],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSystemSandboxContext {
    pub cwd: RuntimePath,
    pub writable_roots: Vec<RuntimePath>,
    pub network_allowed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecResponse {
    pub process_id: ProcessId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadParams {
    pub process_id: ProcessId,
    pub after_seq: Option<u64>,
    pub max_bytes: Option<usize>,
    pub wait_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecOutputStream {
    Stdout,
    Stderr,
    Pty,
}

impl RuntimeEncode for ExecOutputStream {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
            Self::Pty => "pty",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ExecOutputStream {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("stdout", &[]),
                RuntimeEnumVariant::new("stderr", &[]),
                RuntimeEnumVariant::new("pty", &[]),
            ],
            &[Self::Stdout, Self::Stderr, Self::Pty],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessOutputChunk {
    pub seq: u64,
    pub stream: ExecOutputStream,
    pub chunk: ByteChunk,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadResponse {
    pub chunks: Vec<ProcessOutputChunk>,
    pub next_seq: u64,
    pub exited: bool,
    pub exit_code: Option<i32>,
    pub closed: bool,
    pub failure: Option<RuntimeString>,
    pub sandbox_denied: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteParams {
    pub process_id: ProcessId,
    pub chunk: ByteChunk,
    pub write_id: RuntimeString,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteStatus {
    Accepted,
    UnknownProcess,
    StdinClosed,
    Starting,
}

impl RuntimeEncode for WriteStatus {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Accepted => "accepted",
            Self::UnknownProcess => "unknownProcess",
            Self::StdinClosed => "stdinClosed",
            Self::Starting => "starting",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for WriteStatus {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("accepted", &[]),
                RuntimeEnumVariant::new("unknownProcess", &[]),
                RuntimeEnumVariant::new("stdinClosed", &[]),
                RuntimeEnumVariant::new("starting", &[]),
            ],
            &[
                Self::Accepted,
                Self::UnknownProcess,
                Self::StdinClosed,
                Self::Starting,
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteResponse {
    pub status: WriteStatus,
}

impl RuntimeEncode for WriteResponse {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self { status } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("status", status)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for WriteResponse {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                status: object.required("status", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessSignal {
    Interrupt,
}

impl RuntimeEncode for ProcessSignal {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Interrupt => "interrupt",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ProcessSignal {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[RuntimeEnumVariant::new("interrupt", &[])],
            &[Self::Interrupt],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalParams {
    pub process_id: ProcessId,
    pub signal: ProcessSignal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalResponse {}

impl RuntimeEncode for SignalResponse {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {} = self;
        let object = RuntimeObjectEncoder::new();
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SignalResponse {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |_object| Ok(Self {}))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminateParams {
    pub process_id: ProcessId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminateResponse {
    pub running: bool,
}

impl RuntimeEncode for TerminateResponse {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self { running } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("running", running)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for TerminateResponse {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                running: object.required("running", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsReadFileParams {
    pub path: RuntimePath,
    pub sandbox: Option<FileSystemSandboxContext>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsReadFileResponse {
    pub data_base64: RuntimeString,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsOpenParams {
    pub handle_id: RuntimeString,
    pub path: RuntimePath,
    pub sandbox: Option<FileSystemSandboxContext>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsOpenResponse {
    pub handle_id: RuntimeString,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsReadBlockParams {
    pub handle_id: RuntimeString,
    pub offset: u64,
    pub len: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsReadBlockResponse {
    pub chunk: ByteChunk,
    pub eof: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsCloseParams {
    pub handle_id: RuntimeString,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsCloseResponse {}

impl RuntimeEncode for FsCloseResponse {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {} = self;
        let object = RuntimeObjectEncoder::new();
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for FsCloseResponse {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |_object| Ok(Self {}))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsWriteFileParams {
    pub path: RuntimePath,
    pub data_base64: RuntimeString,
    pub sandbox: Option<FileSystemSandboxContext>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsWriteFileResponse {}

impl RuntimeEncode for FsWriteFileResponse {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {} = self;
        let object = RuntimeObjectEncoder::new();
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for FsWriteFileResponse {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |_object| Ok(Self {}))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsCreateDirectoryParams {
    pub path: RuntimePath,
    pub recursive: Option<bool>,
    pub sandbox: Option<FileSystemSandboxContext>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsCreateDirectoryResponse {}

impl RuntimeEncode for FsCreateDirectoryResponse {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {} = self;
        let object = RuntimeObjectEncoder::new();
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for FsCreateDirectoryResponse {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |_object| Ok(Self {}))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsGetMetadataParams {
    pub path: RuntimePath,
    pub sandbox: Option<FileSystemSandboxContext>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsGetMetadataResponse {
    pub is_directory: bool,
    pub is_file: bool,
    pub is_symlink: bool,
    pub size: u64,
    pub created_at_ms: i64,
    pub modified_at_ms: i64,
}

impl RuntimeEncode for FsGetMetadataResponse {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {
            is_directory,
            is_file,
            is_symlink,
            size,
            created_at_ms,
            modified_at_ms,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("isDirectory", is_directory)?;
        object.field("isFile", is_file)?;
        object.field("isSymlink", is_symlink)?;
        object.field("size", size)?;
        object.field("createdAtMs", created_at_ms)?;
        object.field("modifiedAtMs", modified_at_ms)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for FsGetMetadataResponse {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                is_directory: object.required("isDirectory", &[])?,
                is_file: object.required("isFile", &[])?,
                is_symlink: object.required("isSymlink", &[])?,
                size: object.required("size", &[])?,
                created_at_ms: object.required("createdAtMs", &[])?,
                modified_at_ms: object.required("modifiedAtMs", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsCanonicalizeParams {
    pub path: RuntimePath,
    pub sandbox: Option<FileSystemSandboxContext>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsCanonicalizeResponse {
    pub path: RuntimePath,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsReadDirectoryParams {
    pub path: RuntimePath,
    pub sandbox: Option<FileSystemSandboxContext>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsReadDirectoryEntry {
    pub file_name: RuntimeString,
    pub is_directory: bool,
    pub is_file: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsReadDirectoryResponse {
    pub entries: Vec<FsReadDirectoryEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalkOptions {
    pub max_depth: Option<usize>,
    pub max_entries: Option<usize>,
}

impl RuntimeEncode for WalkOptions {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {
            max_depth,
            max_entries,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("maxDepth", max_depth)?;
        object.field("maxEntries", max_entries)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for WalkOptions {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                max_depth: object.optional("maxDepth", &[])?,
                max_entries: object.optional("maxEntries", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsWalkParams {
    pub path: RuntimePath,
    pub options: WalkOptions,
    pub sandbox: Option<FileSystemSandboxContext>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsWalkEntry {
    pub path: RuntimePath,
    pub is_directory: bool,
    pub is_file: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsWalkResponse {
    pub entries: Vec<FsWalkEntry>,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsRemoveParams {
    pub path: RuntimePath,
    pub recursive: Option<bool>,
    pub force: Option<bool>,
    pub sandbox: Option<FileSystemSandboxContext>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsRemoveResponse {}

impl RuntimeEncode for FsRemoveResponse {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {} = self;
        let object = RuntimeObjectEncoder::new();
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for FsRemoveResponse {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |_object| Ok(Self {}))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsCopyParams {
    pub source_path: RuntimePath,
    pub destination_path: RuntimePath,
    pub recursive: bool,
    pub sandbox: Option<FileSystemSandboxContext>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsCopyResponse {}

impl RuntimeEncode for FsCopyResponse {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {} = self;
        let object = RuntimeObjectEncoder::new();
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for FsCopyResponse {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |_object| Ok(Self {}))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpHeader {
    pub name: RuntimeString,
    pub value: RuntimeString,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum HttpRedirectPolicy {
    #[default]
    Follow,
    Stop,
}

impl RuntimeEncode for HttpRedirectPolicy {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Follow => "follow",
            Self::Stop => "stop",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for HttpRedirectPolicy {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("follow", &[]),
                RuntimeEnumVariant::new("stop", &[]),
            ],
            &[Self::Follow, Self::Stop],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequestParams {
    pub method: RuntimeString,
    pub url: RuntimeString,
    pub headers: Vec<HttpHeader>,
    pub body: Option<ByteChunk>,
    pub timeout_ms: Option<u64>,
    pub redirect_policy: HttpRedirectPolicy,
    pub request_id: RuntimeString,
    pub stream_response: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequestResponse {
    pub status: u16,
    pub headers: Vec<HttpHeader>,
    pub body: ByteChunk,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequestBodyDeltaNotification {
    pub request_id: RuntimeString,
    pub seq: u64,
    pub delta: ByteChunk,
    pub done: bool,
    pub error: Option<RuntimeString>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecOutputDeltaNotification {
    pub process_id: ProcessId,
    pub seq: u64,
    pub stream: ExecOutputStream,
    pub chunk: ByteChunk,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecExitedNotification {
    pub process_id: ProcessId,
    pub seq: u64,
    pub exit_code: i32,
    pub sandbox_denied: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecClosedNotification {
    pub process_id: ProcessId,
    pub seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestId {
    String(RuntimeString),
    Number(i64),
}

#[derive(Debug, Clone, PartialEq)]
pub struct JsonRpcRequest {
    pub jsonrpc: RuntimeString,
    pub id: Option<RequestId>,
    pub method: RuntimeString,
    pub params: Option<RuntimeJson>,
}

impl JsonRpcRequest {
    pub fn request(id: RequestId, method: impl Into<RuntimeString>, params: RuntimeJson) -> Self {
        Self {
            jsonrpc: RuntimeString::from("2.0"),
            id: Some(id),
            method: method.into(),
            params: Some(params),
        }
    }

    pub fn notification(method: impl Into<RuntimeString>, params: RuntimeJson) -> Self {
        Self {
            jsonrpc: RuntimeString::from("2.0"),
            id: None,
            method: method.into(),
            params: Some(params),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct JsonRpcResponse {
    pub jsonrpc: RuntimeString,
    pub id: RequestId,
    pub result: Option<RuntimeJson>,
    pub error: Option<JsonRpcErrorObject>,
}

impl JsonRpcResponse {
    pub fn success(id: RequestId, result: RuntimeJson) -> Self {
        Self {
            jsonrpc: RuntimeString::from("2.0"),
            id,
            result: Some(result),
            error: None,
        }
    }

    pub fn error(id: RequestId, error: JsonRpcErrorObject) -> Self {
        Self {
            jsonrpc: RuntimeString::from("2.0"),
            id,
            result: None,
            error: Some(error),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct JsonRpcErrorObject {
    pub code: i64,
    pub message: RuntimeString,
    pub data: Option<RuntimeJson>,
}

impl JsonRpcErrorObject {
    pub fn new(code: i64, message: impl Into<RuntimeString>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }
}

impl RuntimeEncode for RequestId {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        match self {
            Self::String(value) => value.encode_runtime(out),
            Self::Number(value) => value.encode_runtime(out),
        }
    }
}

impl RuntimeDecode for RequestId {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        match value {
            RuntimeJson::String(value) => Ok(Self::String(value.clone())),
            RuntimeJson::Number(_) => i64::decode_runtime(value).map(Self::Number),
            _ => Err(RuntimeCodecError::new(
                RuntimeCodecErrorKind::NoMatchingUntaggedVariant,
            )),
        }
    }
}

impl_runtime_object_codec!(InitializeParams {
    client_name => ("clientName", required),
    resume_session_id => ("resumeSessionId", optional),
});
impl_runtime_object_codec!(InitializeResponse {
    session_id => ("sessionId", required),
});
impl_runtime_object_codec!(ShellInfo {
    name => ("name", required),
    path => ("path", required),
});
impl_runtime_object_codec!(EnvironmentInfo {
    shell => ("shell", required),
    cwd => ("cwd", path_optional),
});
impl_runtime_object_codec!(ExecParams {
    process_id => ("processId", scalar_required),
    argv => ("argv", required),
    cwd => ("cwd", path_required),
    env_policy => ("envPolicy", optional),
    env => ("env", defaulted),
    tty => ("tty", required),
    pipe_stdin => ("pipeStdin", defaulted),
    arg0 => ("arg0", optional),
    sandbox => ("sandbox", optional),
    enforce_managed_network => ("enforceManagedNetwork", defaulted),
    managed_network => ("managedNetwork", optional),
});
impl_runtime_object_codec!(ExecEnvPolicy {
    inherit => ("inherit", required),
    ignore_default_excludes => ("ignoreDefaultExcludes", defaulted),
    exclude => ("exclude", defaulted),
    set_env => ("set", defaulted),
    include_only => ("includeOnly", defaulted),
});
impl_runtime_object_codec!(FileSystemSandboxContext {
    cwd => ("cwd", path_required),
    writable_roots => ("writableRoots", path_vec_defaulted),
    network_allowed => ("networkAllowed", defaulted),
});
impl_runtime_object_codec!(ExecResponse {
    process_id => ("processId", scalar_required),
});
impl_runtime_object_codec!(ReadParams {
    process_id => ("processId", scalar_required),
    after_seq => ("afterSeq", optional),
    max_bytes => ("maxBytes", optional),
    wait_ms => ("waitMs", optional),
});
impl_runtime_object_codec!(ProcessOutputChunk {
    seq => ("seq", required),
    stream => ("stream", required),
    chunk => ("chunk", required),
});
impl_runtime_object_codec!(ReadResponse {
    chunks => ("chunks", required),
    next_seq => ("nextSeq", required),
    exited => ("exited", required),
    exit_code => ("exitCode", optional),
    closed => ("closed", required),
    failure => ("failure", optional),
    sandbox_denied => ("sandboxDenied", defaulted),
});
impl_runtime_object_codec!(WriteParams {
    process_id => ("processId", scalar_required),
    chunk => ("chunk", required),
    write_id => ("writeId", required),
});
impl_runtime_object_codec!(SignalParams {
    process_id => ("processId", scalar_required),
    signal => ("signal", required),
});
impl_runtime_object_codec!(TerminateParams {
    process_id => ("processId", scalar_required),
});
impl_runtime_object_codec!(FsReadFileParams {
    path => ("path", path_required),
    sandbox => ("sandbox", optional),
});
impl_runtime_object_codec!(FsReadFileResponse {
    data_base64 => ("dataBase64", required),
});
impl_runtime_object_codec!(FsOpenParams {
    handle_id => ("handleId", required),
    path => ("path", path_required),
    sandbox => ("sandbox", optional),
});
impl_runtime_object_codec!(FsOpenResponse {
    handle_id => ("handleId", required),
});
impl_runtime_object_codec!(FsReadBlockParams {
    handle_id => ("handleId", required),
    offset => ("offset", required),
    len => ("len", required),
});
impl_runtime_object_codec!(FsReadBlockResponse {
    chunk => ("chunk", required),
    eof => ("eof", required),
});
impl_runtime_object_codec!(FsCloseParams {
    handle_id => ("handleId", required),
});
impl_runtime_object_codec!(FsWriteFileParams {
    path => ("path", path_required),
    data_base64 => ("dataBase64", required),
    sandbox => ("sandbox", optional),
});
impl_runtime_object_codec!(FsCreateDirectoryParams {
    path => ("path", path_required),
    recursive => ("recursive", optional),
    sandbox => ("sandbox", optional),
});
impl_runtime_object_codec!(FsGetMetadataParams {
    path => ("path", path_required),
    sandbox => ("sandbox", optional),
});
impl_runtime_object_codec!(FsCanonicalizeParams {
    path => ("path", path_required),
    sandbox => ("sandbox", optional),
});
impl_runtime_object_codec!(FsCanonicalizeResponse {
    path => ("path", path_required),
});
impl_runtime_object_codec!(FsReadDirectoryParams {
    path => ("path", path_required),
    sandbox => ("sandbox", optional),
});
impl_runtime_object_codec!(FsReadDirectoryEntry {
    file_name => ("fileName", required),
    is_directory => ("isDirectory", required),
    is_file => ("isFile", required),
});
impl_runtime_object_codec!(FsReadDirectoryResponse {
    entries => ("entries", required),
});
impl_runtime_object_codec!(FsWalkParams {
    path => ("path", path_required),
    options => ("options", required),
    sandbox => ("sandbox", optional),
});
impl_runtime_object_codec!(FsWalkEntry {
    path => ("path", path_required),
    is_directory => ("isDirectory", required),
    is_file => ("isFile", required),
});
impl_runtime_object_codec!(FsWalkResponse {
    entries => ("entries", required),
    truncated => ("truncated", required),
});
impl_runtime_object_codec!(FsRemoveParams {
    path => ("path", path_required),
    recursive => ("recursive", optional),
    force => ("force", optional),
    sandbox => ("sandbox", optional),
});
impl_runtime_object_codec!(FsCopyParams {
    source_path => ("sourcePath", path_required),
    destination_path => ("destinationPath", path_required),
    recursive => ("recursive", required),
    sandbox => ("sandbox", optional),
});
impl_runtime_object_codec!(HttpHeader {
    name => ("name", required),
    value => ("value", required),
});
impl_runtime_object_codec!(HttpRequestParams {
    method => ("method", required),
    url => ("url", required),
    headers => ("headers", defaulted),
    body => ("bodyBase64", optional),
    timeout_ms => ("timeoutMs", omit_optional),
    redirect_policy => ("redirectPolicy", defaulted),
    request_id => ("requestId", required),
    stream_response => ("streamResponse", defaulted),
});
impl_runtime_object_codec!(HttpRequestResponse {
    status => ("status", required),
    headers => ("headers", required),
    body => ("bodyBase64", required),
});
impl_runtime_object_codec!(HttpRequestBodyDeltaNotification {
    request_id => ("requestId", required),
    seq => ("seq", required),
    delta => ("deltaBase64", required),
    done => ("done", defaulted),
    error => ("error", optional),
});
impl_runtime_object_codec!(ExecOutputDeltaNotification {
    process_id => ("processId", scalar_required),
    seq => ("seq", required),
    stream => ("stream", required),
    chunk => ("chunk", required),
});
impl_runtime_object_codec!(ExecExitedNotification {
    process_id => ("processId", scalar_required),
    seq => ("seq", required),
    exit_code => ("exitCode", required),
    sandbox_denied => ("sandboxDenied", optional),
});
impl_runtime_object_codec!(ExecClosedNotification {
    process_id => ("processId", scalar_required),
    seq => ("seq", required),
});
impl_runtime_object_codec!(JsonRpcRequest {
    jsonrpc => ("jsonrpc", required),
    id => ("id", optional),
    method => ("method", required),
    params => ("params", optional),
});
impl_runtime_object_codec!(JsonRpcResponse {
    jsonrpc => ("jsonrpc", required),
    id => ("id", required),
    result => ("result", omit_optional),
    error => ("error", omit_optional),
});
impl_runtime_object_codec!(JsonRpcErrorObject {
    code => ("code", required),
    message => ("message", required),
    data => ("data", omit_optional),
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::{runtime_json, RuntimeObject};

    fn json(source: &str) -> RuntimeJson {
        runtime_json::parse(&RuntimeString::from(source)).unwrap()
    }

    #[test]
    fn method_names_match_exec_server_wire_contract() {
        assert_eq!(INITIALIZE_METHOD, "initialize");
        assert_eq!(INITIALIZED_METHOD, "initialized");
        assert_eq!(ENVIRONMENT_INFO_METHOD, "environment/info");
        assert_eq!(PROCESS_START_METHOD, "process/start");
        assert_eq!(PROCESS_READ_METHOD, "process/read");
        assert_eq!(PROCESS_WRITE_METHOD, "process/write");
        assert_eq!(PROCESS_SIGNAL_METHOD, "process/signal");
        assert_eq!(PROCESS_TERMINATE_METHOD, "process/terminate");
        assert_eq!(PROCESS_OUTPUT_METHOD, "process/output");
        assert_eq!(PROCESS_EXITED_METHOD, "process/exited");
        assert_eq!(PROCESS_CLOSED_METHOD, "process/closed");
        assert_eq!(FS_READ_FILE_METHOD, "fs/readFile");
        assert_eq!(FS_OPEN_METHOD, "fs/open");
        assert_eq!(FS_READ_BLOCK_METHOD, "fs/readBlock");
        assert_eq!(FS_CLOSE_METHOD, "fs/close");
        assert_eq!(FS_WRITE_FILE_METHOD, "fs/writeFile");
        assert_eq!(FS_CREATE_DIRECTORY_METHOD, "fs/createDirectory");
        assert_eq!(FS_GET_METADATA_METHOD, "fs/getMetadata");
        assert_eq!(FS_CANONICALIZE_METHOD, "fs/canonicalize");
        assert_eq!(FS_READ_DIRECTORY_METHOD, "fs/readDirectory");
        assert_eq!(FS_WALK_METHOD, "fs/walk");
        assert_eq!(FS_REMOVE_METHOD, "fs/remove");
        assert_eq!(FS_COPY_METHOD, "fs/copy");
        assert_eq!(HTTP_REQUEST_METHOD, "http/request");
        assert_eq!(HTTP_REQUEST_BODY_DELTA_METHOD, "http/request/bodyDelta");
    }

    #[test]
    fn byte_chunks_are_base64_on_the_wire() {
        let value = runtime_codec::encode_value(&ByteChunk::from(b"hello".to_vec())).unwrap();
        assert_eq!(value, RuntimeJson::String(RuntimeString::from("aGVsbG8=")));

        let decoded: ByteChunk =
            runtime_codec::decode_value(&RuntimeJson::String(RuntimeString::from("AAEC"))).unwrap();
        assert_eq!(decoded.into_inner(), vec![0, 1, 2]);
    }

    #[test]
    fn exec_params_use_camel_case_and_default_legacy_fields() {
        let cwd = PathUri::from_host_native_path(std::env::current_dir().unwrap()).unwrap();
        let params = ExecParams {
            process_id: "proc-1".to_string(),
            argv: vec!["sh".into(), "-lc".into(), "echo ok".into()],
            cwd: cwd.into_runtime_path(),
            env_policy: None,
            env: RuntimeEnv::new(),
            tty: true,
            pipe_stdin: false,
            arg0: None,
            sandbox: None,
            enforce_managed_network: true,
            managed_network: Some(ManagedNetworkSandboxContext {
                loopback_ports: vec![43123],
                allow_local_binding: false,
                allow_unix_sockets: Vec::new(),
                allow_all_unix_sockets: false,
                managed_ca_bundle_path: None,
                network_proxy_restricting_sid: None,
                windows_broker_generation: None,
            }),
        };

        let mut value = runtime_codec::encode_value(&params).unwrap();
        assert_eq!(
            value.get("processId").and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("proc-1"))
        );
        assert_eq!(
            value.get("pipeStdin").and_then(RuntimeJson::as_bool),
            Some(false)
        );
        assert_eq!(
            value
                .get("enforceManagedNetwork")
                .and_then(RuntimeJson::as_bool),
            Some(true)
        );
        assert_eq!(
            value.get("managedNetwork"),
            Some(&json(
                r#"{"loopbackPorts":[43123],"allowLocalBinding":false}"#
            ))
        );

        value
            .as_object_mut()
            .unwrap()
            .remove(RuntimeString::from("managedNetwork"));
        let legacy: ExecParams = runtime_codec::decode_value(&value).unwrap();
        assert!(legacy.enforce_managed_network);
        assert_eq!(legacy.managed_network, None);
    }

    #[test]
    fn filesystem_protocol_rejects_native_paths() {
        let native = std::env::current_dir().unwrap().join("native.txt");
        let mut object = RuntimeObject::new();
        object.insert(
            "path",
            RuntimeJson::String(RuntimeString::from(native.to_string_lossy())),
        );
        object.insert("sandbox", RuntimeJson::Null);
        let err = runtime_codec::decode_value::<FsReadFileParams>(&RuntimeJson::Object(object))
            .expect_err("native paths are not valid exec-server path URIs");
        assert!(err.to_string().contains("file URI"));
    }

    #[test]
    fn json_rpc_envelopes_round_trip_string_and_numeric_ids() {
        let request: JsonRpcRequest = runtime_codec::decode_value(&json(
            r#"{"jsonrpc":"2.0","id":"abc","method":"process/read","params":{"processId":"p1"}}"#,
        ))
        .unwrap();
        assert_eq!(request.id, Some(RequestId::String("abc".into())));
        assert_eq!(request.method, PROCESS_READ_METHOD);

        let response = JsonRpcResponse::success(RequestId::Number(7), json(r#"{"ok":true}"#));
        assert_eq!(
            runtime_codec::encode_value(&response).unwrap(),
            json(r#"{"jsonrpc":"2.0","id":7,"result":{"ok":true}}"#)
        );
    }
}
