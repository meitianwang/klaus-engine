//! Exact logical process carriers and the crate-owned runtime spawn boundary.
//!
//! # Compile-fail process boundary guards
//!
//! `NativeOsString` has no downstream scalar-string conversion:
//!
//! ```compile_fail,E0277
//! use klaus_agent_core::runtime_process::NativeOsString;
//!
//! fn requires_conversion<T: Into<String>>() {}
//!
//! requires_conversion::<NativeOsString>();
//! ```
//!
//! Nor can a scalar `String` construct it:
//!
//! ```compile_fail,E0277
//! use klaus_agent_core::runtime_process::NativeOsString;
//!
//! fn requires_conversion<T: Into<NativeOsString>>() {}
//!
//! requires_conversion::<String>();
//! ```
//!
//! Logical runtime strings cannot be projected from native host identity:
//!
//! ```compile_fail,E0277
//! use klaus_agent_core::runtime_process::NativeOsString;
//! use klaus_agent_core::runtime_values::RuntimeString;
//!
//! fn requires_conversion<T: Into<RuntimeString>>() {}
//!
//! requires_conversion::<NativeOsString>();
//! ```
//!
//! And runtime-derived strings cannot construct native host identity:
//!
//! ```compile_fail,E0277
//! use klaus_agent_core::runtime_process::NativeOsString;
//! use klaus_agent_core::runtime_values::RuntimeString;
//!
//! fn requires_conversion<T: Into<NativeOsString>>() {}
//!
//! requires_conversion::<RuntimeString>();
//! ```
//!
//! Native process atoms are not a public filesystem-path projection:
//!
//! ```compile_fail,E0277
//! use std::path::PathBuf;
//!
//! use klaus_agent_core::runtime_process::NativeOsString;
//!
//! fn requires_conversion<T: Into<PathBuf>>() {}
//!
//! requires_conversion::<NativeOsString>();
//! ```
//!
//! Nor is `PathBuf` a public native-atom constructor:
//!
//! ```compile_fail,E0277
//! use std::path::PathBuf;
//!
//! use klaus_agent_core::runtime_process::NativeOsString;
//!
//! fn requires_conversion<T: Into<NativeOsString>>() {}
//!
//! requires_conversion::<PathBuf>();
//! ```
//!
//! The same boundary excludes downstream `OsString` projection:
//!
//! ```compile_fail,E0277
//! use std::ffi::OsString;
//!
//! use klaus_agent_core::runtime_process::NativeOsString;
//!
//! fn requires_conversion<T: Into<OsString>>() {}
//!
//! requires_conversion::<NativeOsString>();
//! ```
//!
//! And excludes public construction from an `OsString`:
//!
//! ```compile_fail,E0277
//! use std::ffi::OsString;
//!
//! use klaus_agent_core::runtime_process::NativeOsString;
//!
//! fn requires_conversion<T: Into<NativeOsString>>() {}
//!
//! requires_conversion::<OsString>();
//! ```
//!
//! A generic runtime object is not an environment constructor; callers must
//! cross the checked typed-codec boundary:
//!
//! ```compile_fail,E0277
//! use klaus_agent_core::runtime_process::RuntimeEnv;
//! use klaus_agent_core::runtime_values::RuntimeObject;
//!
//! fn requires_object_conversion<T: From<RuntimeObject>>() {}
//!
//! requires_object_conversion::<RuntimeEnv>();
//! ```
//!
//! Environment mutation accepts exact string values, never arbitrary runtime
//! JSON:
//!
//! ```compile_fail,E0308
//! use klaus_agent_core::runtime_process::RuntimeEnv;
//! use klaus_agent_core::runtime_values::{RuntimeJson, RuntimeString};
//!
//! let _: fn(&mut RuntimeEnv, RuntimeString, RuntimeJson) -> Option<RuntimeString> =
//!     RuntimeEnv::insert;
//! ```
//!
//! Neither backing map is observable to downstream crates:
//!
//! ```compile_fail,E0616
//! use klaus_agent_core::runtime_process::RuntimeEnv;
//!
//! fn inspect_array_indices(environment: &RuntimeEnv) {
//!     let _ = &environment.array_indices;
//! }
//! ```
//!
//! ```compile_fail,E0616
//! use klaus_agent_core::runtime_process::RuntimeEnv;
//!
//! fn inspect_string_properties(environment: &RuntimeEnv) {
//!     let _ = &environment.string_properties;
//! }
//! ```
//!
//! Raw parent-environment capture is not a downstream API. An external crate
//! cannot share this crate's mutex with arbitrary `std::env` mutation, so the
//! shadow capture remains crate-private until the atomic process cutover:
//!
//! ```compile_fail,E0603
//! use klaus_agent_core::runtime_process::ParentEnvSnapshot;
//! ```

mod env;
#[allow(dead_code)]
pub(crate) mod fs_path_projection;
#[allow(dead_code)]
mod lifecycle;
#[cfg(any(target_os = "linux", all(test, target_os = "macos")))]
#[allow(dead_code)]
mod linux_notifier;
#[cfg(any(target_os = "macos", all(test, target_os = "linux")))]
#[allow(dead_code)]
mod macos_notifier;
#[cfg(any(target_os = "macos", all(test, target_os = "linux")))]
#[allow(dead_code)]
mod macos_posix_spawn;
#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod macos_posix_spawn_tests;
mod node20_path_case_data;
#[allow(dead_code)]
mod parent_env_snapshot;
mod path;
mod plan;
mod process_environment;
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[allow(dead_code)]
mod reaper;
#[cfg(any(target_os = "linux", target_os = "macos", windows))]
mod runtime_spawner;
#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod shared_reaper_tests;
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[allow(dead_code)]
mod spawn;
#[cfg(unix)]
#[allow(dead_code)]
mod unix;
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[allow(dead_code)]
mod unix_spawn;
#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod unix_spawn_tests;
#[cfg(all(test, unix))]
mod unix_tests;
#[allow(dead_code)]
mod windows;
mod windows_browser_broker;
#[cfg(windows)]
mod windows_platform;
#[allow(dead_code)]
mod windows_spawn;
#[cfg(test)]
mod windows_spawn_tests;
#[cfg(test)]
mod windows_tests;

pub use env::{normalize_child_env, RuntimeEnv, RuntimeEnvEntries};
#[allow(unused_imports)]
pub(crate) use lifecycle::{
    EnvironmentImageDialect, KillTarget, PreSpawnPolicy, PreSpawnValidationError,
    ProcessEnvironmentGuard, ProcessEnvironmentKind, ProcessImageDialect, ProcessTarget,
    ProcessTopology, WindowsJobPolicy,
};
#[allow(unused_imports)]
pub(crate) use parent_env_snapshot::ParentEnvSnapshot;
pub(crate) use path::path_lowercase;
pub use path::{
    path_basename, path_dirname, path_extname, path_format, path_is_absolute, path_join,
    path_normalize, path_parse, path_relative, path_resolve, path_to_namespaced_path,
    NodePathDialect, NodePathHostPlatform, RuntimePathCwdError, RuntimePathCwdRead,
    RuntimePathCwdSnapshot, RuntimePathFormatInput, RuntimePathParts,
};
pub use plan::{
    LogicalSpawnRequest, NativeHostEnvOverlay, NativeOsString, NodeSpawnPlan, ProcessAtom,
    ProcessEnvironmentPlan, ProcessPlanError, ProcessPlanErrorKind, ProcessPlanField,
    Rust193EnvMutation,
};
#[doc(hidden)]
pub use process_environment::{remove_process_environment_var, set_process_environment_var};
#[cfg(all(test, unix))]
pub(crate) use runtime_spawner::pty_allocatable;
#[cfg(all(test, any(target_os = "linux", target_os = "macos", windows)))]
pub(crate) use runtime_spawner::test_runtime_process_context;
#[cfg(any(target_os = "linux", target_os = "macos", windows))]
#[allow(unused_imports)]
pub(crate) use runtime_spawner::{
    parent_runtime_env_value, RuntimeChildPtyReader, RuntimeChildPtyWriter,
    RuntimeCompletedProcessRequest, RuntimeCompletedProcessSurface, RuntimeProcessController,
    RuntimeProcessSurface, RuntimeSignal, RuntimeSpawnError, RuntimeSpawnRequest, RuntimeStdio,
    SignalDelivery, SpawnedRuntimeProcess, SHELL_ENV_SNAPSHOT_MAX_STDOUT_BYTES,
};
#[cfg(any(target_os = "linux", target_os = "macos", windows))]
pub use runtime_spawner::{
    RuntimeBrowserBrokerProcessRequest, RuntimeBrowserBrokerRead, RuntimeBrowserBrokerWrite,
    RuntimeBrowserChildControl, RuntimeBrowserChildDuplex, RuntimeBrowserChildProcess,
    RuntimeBrowserFfmpegEncodeRequest, RuntimeBrowserLinuxLddRequest,
    RuntimeBrowserOpenChromeRequest, RuntimeBrowserPlaywrightLaunchMode,
    RuntimeBrowserPlaywrightLaunchRequest, RuntimeBrowserProcessBinding,
    RuntimeBrowserSidecarRequest, RuntimeBrowserUtilityProcess, RuntimeBrowserWindowsWinlddRequest,
    RuntimeBrowserWinlddExecutable, RuntimeChildStderr, RuntimeChildStdin, RuntimeChildStdout,
    RuntimeExecFileError, RuntimeExecFileErrorStage, RuntimeExecFileOutput, RuntimeExecFileRequest,
    RuntimeExecFileStatus, RuntimeExecFileSurface, RuntimePipeProcess,
    RuntimeProcessBootstrapError, RuntimeProcessContext, RuntimeProcessNamespaceId,
    RuntimeProcessPipeError, RuntimeProcessShutdownError, RuntimeProcessSupervisor,
};
