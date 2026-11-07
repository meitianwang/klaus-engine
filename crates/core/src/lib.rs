//! `klaus-agent-core` — headless Rust agent engine core.
//!
//! Public surface of the agent engine core.

/// Package version.
pub const VERSION: &str = "0.1.0";

// 编译期守护：本引擎以 panic + catch_unwind 作为正式的错误回收通道（turn/工具边界
// 统一回收 panic 并转换为结构化错误）。若嵌入方或发布 profile 配置为
// `panic = "abort"`，所有业务 panic 都会直接终止进程，引擎的错误处理契约随之失效。
// 因此在非测试构建下强制要求 unwind 策略，配置为 abort 时直接拒绝编译。
#[cfg(all(not(test), not(panic = "unwind")))]
compile_error!(
    "klaus-agent-core 以 panic 展开（unwind）作为错误回收通道：业务错误通过 panic 抛出，在 turn/工具边界经 catch_unwind 回收为结构化错误。禁止以 panic = \"abort\" 编译本引擎，请在对应 profile 中设置 panic = \"unwind\"。"
);

pub mod sync_ext;

/// Shared test-only synchronization. Several modules exercise behavior that
/// depends on the process-global `KLAUS_CONFIG_DIR` and `KLAUS_TASK_LIST_ID`
/// environment variables; since the test binary runs cases in parallel, they
/// must all take the SAME lock around reads/writes of them — a per-module lock
/// would not serialize across modules.
#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Mutex;

    /// Serializes every test that reads or writes `KLAUS_CONFIG_DIR` or
    /// `KLAUS_TASK_LIST_ID`.
    pub(crate) static CONFIG_ENV_LOCK: Mutex<()> = Mutex::new(());

    /// Serializes tests that spawn short-timeout external programs (git, rg,
    /// node). The default Rust test runner can otherwise start enough child
    /// processes at once that unrelated timeout-based assertions become flaky.
    pub(crate) static EXTERNAL_PROCESS_LOCK: tokio::sync::Mutex<()> =
        tokio::sync::Mutex::const_new(());

    pub(crate) async fn external_process_lock() -> tokio::sync::MutexGuard<'static, ()> {
        EXTERNAL_PROCESS_LOCK.lock().await
    }

    /// Serializes tests that mutate the process-global ripgrep resolution — the
    /// `KLAUS_RIPGREP_PATH` env var and the memoized command mode — or that
    /// invoke rg and therefore READ that state. It must live here rather than in
    /// `fs::ripgrep`'s test module: `tools::builtins::grep` installs a fake rg
    /// through the same globals, and a per-module lock would not serialize the
    /// two. Without it, one case's reset lands between another's "install fake
    /// rg" and "run rg", so the fake never runs and its capture file is missing.
    pub(crate) static RIPGREP_GLOBAL_LOCK: tokio::sync::Mutex<()> =
        tokio::sync::Mutex::const_new(());

    /// Both locks a ripgrep-global test needs, always in this order to preclude
    /// a deadlock against `external_process_lock` callers.
    pub(crate) async fn ripgrep_global_guard() -> (
        tokio::sync::MutexGuard<'static, ()>,
        tokio::sync::MutexGuard<'static, ()>,
    ) {
        let external = external_process_lock().await;
        let ripgrep = RIPGREP_GLOBAL_LOCK.lock().await;
        (external, ripgrep)
    }

    #[cfg(test)]
    mod tests {
        #[tokio::test]
        async fn external_process_lock_is_exclusive() {
            let _guard = super::external_process_lock().await;
            assert!(super::EXTERNAL_PROCESS_LOCK.try_lock().is_err());
        }
    }
}

// ---- Contract: schemas ----
pub mod schemas;

pub mod config_governance;
pub mod config_store;
pub mod credential_store;

/// Exact logical process carriers.
///
/// Native host atoms are intentionally opaque to downstream crates: neither
/// their host representation nor the native environment-overlay representation
/// may be constructed or projected outside the generated runtime binding.
///
/// ```compile_fail,E0451
/// use klaus_agent_core::runtime_process::NativeOsString;
///
/// #[cfg(unix)]
/// fn construct() -> NativeOsString {
///     NativeOsString { bytes: Vec::new() }
/// }
///
/// #[cfg(windows)]
/// fn construct() -> NativeOsString {
///     NativeOsString { wide: Vec::new() }
/// }
/// ```
///
/// ```compile_fail,E0451
/// use klaus_agent_core::runtime_process::NativeOsString;
///
/// #[cfg(unix)]
/// fn project(value: NativeOsString) {
///     let NativeOsString { bytes: _ } = value;
/// }
///
/// #[cfg(windows)]
/// fn project(value: NativeOsString) {
///     let NativeOsString { wide: _ } = value;
/// }
/// ```
///
/// ```compile_fail,E0451
/// use klaus_agent_core::runtime_process::{NativeHostEnvOverlay, NativeOsString};
/// use klaus_agent_core::runtime_values::RuntimeString;
///
/// fn construct(key: RuntimeString, value: NativeOsString) -> NativeHostEnvOverlay {
///     NativeHostEnvOverlay { key, value }
/// }
/// ```
///
/// ```compile_fail,E0451
/// use klaus_agent_core::runtime_process::NativeHostEnvOverlay;
///
/// fn project(overlay: NativeHostEnvOverlay) {
///     let NativeHostEnvOverlay { key: _, value: _ } = overlay;
/// }
/// ```
///
/// Lifecycle policy selection is generated inside the crate, so downstream
/// code cannot obtain its policy constructor.
///
/// ```compile_fail,E0603
/// use klaus_agent_core::runtime_process::PreSpawnPolicy;
///
/// fn downstream_policy_constructor_is_not_public() {
///     let _ = PreSpawnPolicy::new;
/// }
/// ```
///
/// Windows verbatim-argument mode is likewise reserved for named surface
/// transforms instead of general logical-request callers.
///
/// ```compile_fail,E0624
/// use klaus_agent_core::runtime_process::{LogicalSpawnRequest, ProcessAtom};
/// use klaus_agent_core::runtime_values::RuntimeString;
///
/// let request = LogicalSpawnRequest::new(ProcessAtom::Runtime(
///     RuntimeString::from_utf8("node"),
/// ));
/// let _ = request.with_windows_verbatim_arguments(true);
/// ```
///
/// Terminal filesystem projection is sealed to the crate-owned actual-host
/// syscall adapter. Downstream crates cannot select a target or read projected
/// native bytes early:
///
/// ```compile_fail,E0603
/// use klaus_agent_core::runtime_process::fs_path_projection::Node20FsPathProjection;
/// ```
pub mod runtime_process;
pub mod runtime_values;

/// Typed JSON protocol boundaries over the exact runtime carrier.
#[doc(hidden)]
pub mod json_wire {
    use std::fmt;

    use crate::runtime_values::{
        runtime_codec, runtime_json, RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeString,
    };

    #[derive(Debug)]
    pub enum JsonWireError {
        Parse(runtime_json::RuntimeJsonError),
        Codec(RuntimeCodecError),
        InvalidUtf8(std::str::Utf8Error),
        NonScalar(crate::runtime_values::NonScalarStringError),
    }

    impl fmt::Display for JsonWireError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Parse(error) => write!(formatter, "runtime JSON parse error: {error:?}"),
                Self::Codec(error) => error.fmt(formatter),
                Self::InvalidUtf8(error) => error.fmt(formatter),
                Self::NonScalar(error) => error.fmt(formatter),
            }
        }
    }

    impl std::error::Error for JsonWireError {}

    /// Parse JSON with JavaScript runtime semantics, then deserialize the
    /// projected value into the requested wire type.
    pub fn parse<T>(input: &str) -> Result<T, JsonWireError>
    where
        T: RuntimeDecode,
    {
        let value =
            runtime_json::parse(&RuntimeString::from(input)).map_err(JsonWireError::Parse)?;
        runtime_codec::decode_value(&value).map_err(JsonWireError::Codec)
    }

    /// Parse strict UTF-8 JSON bytes with JavaScript runtime semantics, then
    /// deserialize the projected value into the requested wire type.
    pub fn parse_slice<T>(input: &[u8]) -> Result<T, JsonWireError>
    where
        T: RuntimeDecode,
    {
        let input = std::str::from_utf8(input).map_err(JsonWireError::InvalidUtf8)?;
        parse(input)
    }

    /// Serialize a typed wire value with JavaScript `JSON.stringify`
    /// semantics, including reversible lone-surrogate escapes.
    pub fn stringify<T>(value: &T) -> Result<String, JsonWireError>
    where
        T: RuntimeEncode + ?Sized,
    {
        let value = runtime_codec::encode_value(value).map_err(JsonWireError::Codec)?;
        runtime_json::stringify(&value)
            .try_to_utf8()
            .map_err(JsonWireError::NonScalar)
    }

    /// Serialize a typed wire value as canonical two-space pretty JSON with
    /// JavaScript number and string semantics.
    pub fn stringify_pretty<T>(value: &T) -> Result<String, JsonWireError>
    where
        T: RuntimeEncode + ?Sized,
    {
        let value = runtime_codec::encode_value(value).map_err(JsonWireError::Codec)?;
        runtime_json::stringify_pretty(&value)
            .try_to_utf8()
            .map_err(JsonWireError::NonScalar)
    }

    /// JavaScript `String.length` for runtime strings, measured in UTF-16 code
    /// units while preserving the internal lone-surrogate representation.
    pub fn string_utf16_len(value: &str) -> usize {
        value.encode_utf16().count()
    }
}

// ---- Contract: pure types ----
pub mod types;

// ---- Subsystems ----
pub mod browser;
pub mod capabilities;
pub mod checkpoint;
pub mod command_runtime;
pub mod context;
pub mod engine;
pub mod extensions;
pub mod fs;
pub mod git;
pub mod goal;
pub mod hooks;
pub mod lsp;
pub mod mcp;
pub mod memory;
pub mod network_proxy;
pub mod observability;
pub mod permission;
pub mod provider;
pub mod sandbox;
pub mod secrets;
pub mod store;
pub mod tasks;
// Test-support surface: excluded from production builds; enabled by this
// crate's own tests and by downstream crates via the `testing` feature.
#[cfg(any(test, feature = "testing"))]
pub mod testing;
pub mod tools;
pub(crate) mod v8_platform;
pub mod worktree;

pub mod planning;
