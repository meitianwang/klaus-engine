//! Shadow-only exact browser utility contracts.
//!
//! This module deliberately contains no host discovery, filesystem mutation,
//! registry access, or process creation. It freezes the CLI, result, and closed
//! process-plan boundary that the atomic browser cutover will consume.

mod broker_runtime;
mod broker_wire;
mod cli;
mod distribution_manifest;
mod distribution_source;
mod generation;
pub(crate) mod host_io;
mod mux_runtime;
mod native_host;
mod native_host_io;
mod plan;
mod result;
mod transport;
mod windows_bootstrap;
#[cfg(windows)]
mod windows_runtime;
mod winldd_distribution;

pub(crate) use broker_runtime::BrowserBrokerRuntime;
use broker_wire::{
    decode_sync_request_payload, BrowserBootstrapRecord, BrowserBootstrapWireError,
    BrowserBrokerWireError, BrowserFfmpegEncodeRequest, BrowserMuxDirection, BrowserMuxFrame,
    BrowserMuxKind, BrowserMuxPayload, BrowserMuxSequenceClass, BrowserMuxState,
    BrowserPlaywrightLaunchMode, BrowserPlaywrightLaunchRequest, BrowserSyncExchange,
    BrowserSyncFrame, BrowserSyncKind, BrowserSyncRequestPayload, ExactBootstrapRecordReader,
    ExactMuxFrameReader, ExactSyncFrameReader, UnixPackageRecordType, WindowsBootstrapPhase,
    WindowsBrokerLanePair,
};
pub use cli::{
    identify_subcommand, parse_browser_utility, BrowserUtilityInvocation, BrowserUtilityParse,
    BrowserUtilitySubcommand,
};
pub use generation::{
    BrowserGeneration, BrowserGenerationError, BrowserGenerationPhase, BrowserGenerationTarget,
    BrowserRequestBoundary, BrowserStartingSubphase, SidecarTerminationActionPhase,
    SidecarTerminationMachine, SidecarTerminationPrelude, SidecarTerminationState,
};
pub use host_io::run_browser_cli_main;
pub(crate) use host_io::{run_browser_cli, BrowserUtilityProcessExecutor};
pub use native_host::{
    next_native_host_reconnect_delay_ms, read_websocket_frame, write_masked_websocket_text,
    BridgeWriteQueue, ChromeNativeFramingError, ChromeNativeMessageReader, ChromeNativeReadBatch,
    NativeHostStdoutWriter, NativeHostWriteAction, NativeHostWriteResult, NativeHostWriterPhase,
    WebSocketFrame, WebSocketFrameError, MAX_CHROME_NATIVE_MESSAGE_BYTES,
};
pub use native_host_io::run_browser_native_host_main;
pub use plan::{
    browser_process_plan, open_chrome_execution_plan, BrowserProcessPlan, BrowserQuietProbe,
    ChromeRunningProcessPlan, InstallNativeHostAction, InstallNativeHostManifestPlan,
    InstallNativeHostPlanError, InstalledBrowsersProcessPlan, OpenChromeEffect,
    OpenChromeExecutionPlan, OpenChromeLaunchResolutionPlan, OpenChromePlanError,
    OpenChromeProcessPlan, OpenChromeProfilePlan, VerifiedChromeLaunchPathError,
    VerifiedChromeLaunchTarget,
};
pub(crate) use plan::{
    install_native_host_manifest_plan, VerifiedNativeHostBinary, VerifiedNativeHostPathError,
};
pub use result::{
    render_check_extension_error, render_check_extension_status, render_check_native_host_error,
    render_check_native_host_status, render_chrome_running_status,
    render_install_native_host_error, render_install_native_host_result,
    render_installed_browsers_result, render_open_chrome_error, BrowserCliOutput, BrowserPlatform,
    ChromeProcessStatus, ChromeRunningStatus, ExtensionInstallStatus, ExtensionProfileStatus,
    InstallNativeHostResult, InstalledBrowser, InstalledBrowsersResult, NativeHostManifest,
    NativeHostManifestStatus,
};
use transport::{
    BrowserHostAdmission, BrowserHostCallback, BrowserHostCallbackKind, BrowserHostConfiguration,
    BrowserHostEndpoint, BrowserHostEndpointPhase, BrowserHostError, BrowserHostPermissionSet,
    BrowserHostPolicy, BrowserHostSendResult,
};
pub use windows_bootstrap::{
    build_windows_browser_bootstrap, parse_windows_browser_bootstrap, WindowsBootstrapKeyAttempt,
    WindowsBrowserBootstrapError, WindowsBrowserBrokerBootstrap,
};
#[cfg(windows)]
pub(crate) use windows_runtime::start_windows_browser_broker;

/// Extension identity fixed by the bundled browser contract.
pub const EXTENSION_ID: &str = "gjamccelebgpogpbiceajehcajghphnb";

/// Native-messaging host identity fixed by the bundled browser contract.
pub const NATIVE_HOST_NAME: &str = "com.klaus.browser";

pub(crate) const UNIX_BROWSER_BOOTSTRAP_RECORD_LEN: usize = 48;

pub(crate) fn unix_package_load_permit(
    generation: u64,
    challenge: [u8; 32],
) -> [u8; UNIX_BROWSER_BOOTSTRAP_RECORD_LEN] {
    BrowserBootstrapRecord::encode_unix(
        UnixPackageRecordType::PackageLoadPermit,
        generation,
        challenge,
    )
}

pub(crate) fn validate_unix_package_ready(
    bytes: &[u8; UNIX_BROWSER_BOOTSTRAP_RECORD_LEN],
    generation: u64,
    challenge: &[u8; 32],
) -> bool {
    BrowserBootstrapRecord::decode_unix(
        bytes,
        UnixPackageRecordType::PackageReady,
        generation,
        challenge,
    )
    .is_ok()
}

#[cfg(test)]
mod tests;
