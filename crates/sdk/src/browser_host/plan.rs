use klaus_agent_core::runtime_values::{RuntimePath, RuntimeString};

use super::cli::BrowserUtilityInvocation;
use super::result::{
    render_install_native_host_result, render_open_chrome_success, BrowserCliOutput,
    BrowserPlatform, InstallNativeHostResult, NativeHostManifest, OpenChromeSuccess,
};

const WINDOWS_REGISTRY_KEY: &str =
    "HKCU\\Software\\Google\\Chrome\\NativeMessagingHosts\\com.klaus.browser";

/// A Rust native-host binary admitted by the signed distribution verifier.
/// Its constructor remains crate-private so browser callers cannot claim that
/// an arbitrary path was hash/mode verified.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VerifiedNativeHostBinary {
    platform: BrowserPlatform,
    absolute_path: RuntimePath,
}

impl VerifiedNativeHostBinary {
    // Shadow-only until the signed distribution verifier is connected in the
    // atomic cutover; tests exercise the fail-closed constructor now.
    #[allow(dead_code)]
    pub(crate) fn from_attested_distribution_path(
        platform: BrowserPlatform,
        absolute_path: RuntimePath,
    ) -> Result<Self, VerifiedNativeHostPathError> {
        let units = absolute_path.as_runtime_string().as_utf16();
        if units.contains(&0) {
            return Err(VerifiedNativeHostPathError::ContainsNul);
        }
        if !is_absolute(platform, units) {
            return Err(VerifiedNativeHostPathError::NotAbsolute);
        }
        let basename = basename(platform, units);
        let has_expected_name = match platform {
            BrowserPlatform::Windows => {
                equals_ascii_case_insensitive(basename, "klaus-browser-native-host.exe")
            }
            BrowserPlatform::MacOs | BrowserPlatform::Linux => {
                equals_ascii(basename, "klaus-browser-native-host")
            }
        };
        if !has_expected_name {
            return Err(VerifiedNativeHostPathError::WrongBinaryName);
        }
        Ok(Self {
            platform,
            absolute_path,
        })
    }

    /// Absolute exact path selected by the signed distribution manifest.
    pub(crate) fn absolute_path(&self) -> &RuntimePath {
        &self.absolute_path
    }
}

/// Fail-closed validation before a path may carry the verified native-host
/// brand.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VerifiedNativeHostPathError {
    ContainsNul,
    NotAbsolute,
    WrongBinaryName,
}

/// Absolute Chrome application/executable path branded only after the
/// crate-owned target discovery and identity checks have succeeded.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedChromeLaunchTarget {
    platform: BrowserPlatform,
    absolute_path: RuntimePath,
}

impl VerifiedChromeLaunchTarget {
    pub(super) fn from_verified_discovery_path(
        platform: BrowserPlatform,
        absolute_path: RuntimePath,
    ) -> Result<Self, VerifiedChromeLaunchPathError> {
        let units = absolute_path.as_runtime_string().as_utf16();
        if units.contains(&0) {
            return Err(VerifiedChromeLaunchPathError::ContainsNul);
        }
        if !is_absolute(platform, units) {
            return Err(VerifiedChromeLaunchPathError::NotAbsolute);
        }
        Ok(Self {
            platform,
            absolute_path,
        })
    }

    fn absolute_path(&self) -> &RuntimePath {
        &self.absolute_path
    }
}

/// Fail-closed validation before a discovered Chrome target may carry the
/// crate-owned launch brand.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VerifiedChromeLaunchPathError {
    ContainsNul,
    NotAbsolute,
}

/// Closed process policy for a parsed browser utility before host discovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrowserProcessPlan {
    CheckExtensionInstalledFilesystemOnly,
    CheckNativeHostManifestFilesystemOnly,
    InstallNativeHost {
        platform: BrowserPlatform,
        dry_run: bool,
    },
    ChromeIsRunning(ChromeRunningProcessPlan),
    InstalledBrowsers(InstalledBrowsersProcessPlan),
    OpenChromeWindow {
        platform: BrowserPlatform,
        dry_run: bool,
    },
}

/// Target-specific closed running probe. Commands and arguments are fixed in
/// the platform backend and are not fields on this API.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChromeRunningProcessPlan {
    WindowsTasklist,
    MacOsSingletonThenPs,
    LinuxPs,
}

/// One exact quiet probe from the locked installed-browser source order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrowserQuietProbe {
    MacOsSpotlightChrome,
    MacOsPlistPerExistingCandidate,
    WindowsAppPathCurrentUser,
    WindowsAppPathLocalMachine,
    WindowsAppPathWow6432,
    WindowsWhereChrome,
    WindowsVersionCurrentUser,
    WindowsVersionLocalMachine,
    LinuxWhichGoogleChrome,
    LinuxWhichChromium,
    LinuxWhichChromiumBrowser,
}

const MACOS_BROWSER_PROBES: &[BrowserQuietProbe] = &[
    BrowserQuietProbe::MacOsSpotlightChrome,
    BrowserQuietProbe::MacOsPlistPerExistingCandidate,
];
const WINDOWS_BROWSER_PROBES: &[BrowserQuietProbe] = &[
    BrowserQuietProbe::WindowsAppPathCurrentUser,
    BrowserQuietProbe::WindowsAppPathLocalMachine,
    BrowserQuietProbe::WindowsAppPathWow6432,
    BrowserQuietProbe::WindowsWhereChrome,
    BrowserQuietProbe::WindowsVersionCurrentUser,
    BrowserQuietProbe::WindowsVersionLocalMachine,
];
const LINUX_BROWSER_PROBES: &[BrowserQuietProbe] = &[
    BrowserQuietProbe::LinuxWhichGoogleChrome,
    BrowserQuietProbe::LinuxWhichChromium,
    BrowserQuietProbe::LinuxWhichChromiumBrowser,
];

/// Target-specific immutable installed-browser probe order. Filesystem fallback
/// candidates are host-discovery work and therefore absent from this process
/// plan rather than represented as fake processes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InstalledBrowsersProcessPlan {
    platform: BrowserPlatform,
}

impl InstalledBrowsersProcessPlan {
    /// Exact process probes in source order.
    pub fn probes(self) -> &'static [BrowserQuietProbe] {
        match self.platform {
            BrowserPlatform::MacOs => MACOS_BROWSER_PROBES,
            BrowserPlatform::Windows => WINDOWS_BROWSER_PROBES,
            BrowserPlatform::Linux => LINUX_BROWSER_PROBES,
        }
    }
}

/// Derive the closed process policy from an exact parsed invocation. No host
/// action is performed.
pub fn browser_process_plan(
    invocation: &BrowserUtilityInvocation,
    platform: BrowserPlatform,
) -> BrowserProcessPlan {
    match invocation {
        BrowserUtilityInvocation::CheckExtensionInstalled { .. } => {
            BrowserProcessPlan::CheckExtensionInstalledFilesystemOnly
        }
        BrowserUtilityInvocation::CheckNativeHostManifest { .. } => {
            BrowserProcessPlan::CheckNativeHostManifestFilesystemOnly
        }
        BrowserUtilityInvocation::InstallNativeHostManifest { dry_run, .. } => {
            BrowserProcessPlan::InstallNativeHost {
                platform,
                dry_run: *dry_run,
            }
        }
        BrowserUtilityInvocation::ChromeIsRunning { .. } => {
            BrowserProcessPlan::ChromeIsRunning(match platform {
                BrowserPlatform::MacOs => ChromeRunningProcessPlan::MacOsSingletonThenPs,
                BrowserPlatform::Windows => ChromeRunningProcessPlan::WindowsTasklist,
                BrowserPlatform::Linux => ChromeRunningProcessPlan::LinuxPs,
            })
        }
        BrowserUtilityInvocation::InstalledBrowsers { .. } => {
            BrowserProcessPlan::InstalledBrowsers(InstalledBrowsersProcessPlan { platform })
        }
        BrowserUtilityInvocation::OpenChromeWindow { dry_run, .. } => {
            BrowserProcessPlan::OpenChromeWindow {
                platform,
                dry_run: *dry_run,
            }
        }
    }
}

/// Ordered installation action. The only process-bearing variant is the fixed
/// Windows registry registration operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InstallNativeHostAction {
    DryRun,
    UnixWriteManifestThenChmodHost0755,
    WindowsWriteManifestThenRegister { manifest_path: RuntimePath },
}

/// Complete shadow installation plan and exact user-visible result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstallNativeHostManifestPlan {
    action: InstallNativeHostAction,
    result: InstallNativeHostResult,
}

impl InstallNativeHostManifestPlan {
    /// Ordered host action; execution is introduced only at the atomic cutover.
    pub fn action(&self) -> &InstallNativeHostAction {
        &self.action
    }

    /// Exact result rendered after the action succeeds.
    pub fn result(&self) -> &InstallNativeHostResult {
        &self.result
    }

    /// Render success without performing the plan.
    pub fn render_success(&self, dry_run: bool, json: bool) -> BrowserCliOutput {
        render_install_native_host_result(&self.result, dry_run, json)
    }
}

/// Build an installation plan only from a branded, absolute Rust native-host
/// binary. The legacy JavaScript native-host path cannot enter this API.
pub(crate) fn install_native_host_manifest_plan(
    platform: BrowserPlatform,
    manifest_path: RuntimePath,
    native_host: &VerifiedNativeHostBinary,
    dry_run: bool,
) -> Result<InstallNativeHostManifestPlan, InstallNativeHostPlanError> {
    if native_host.platform != platform {
        return Err(InstallNativeHostPlanError::PlatformMismatch);
    }
    let registry_key = matches!(platform, BrowserPlatform::Windows)
        .then(|| RuntimeString::from(WINDOWS_REGISTRY_KEY));
    let result = InstallNativeHostResult {
        manifest_path: manifest_path.clone(),
        host_path: native_host.absolute_path.clone(),
        manifest: NativeHostManifest {
            path: native_host.absolute_path.clone(),
        },
        registry_key,
    };
    let action = if dry_run {
        InstallNativeHostAction::DryRun
    } else {
        match platform {
            BrowserPlatform::Windows => {
                InstallNativeHostAction::WindowsWriteManifestThenRegister { manifest_path }
            }
            BrowserPlatform::MacOs | BrowserPlatform::Linux => {
                InstallNativeHostAction::UnixWriteManifestThenChmodHost0755
            }
        }
    };
    Ok(InstallNativeHostManifestPlan { action, result })
}

/// Invalid attempt to combine independently attested target inputs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstallNativeHostPlanError {
    PlatformMismatch,
}

/// Resolved Chrome profile inputs shared by all target launch variants.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenChromeProfilePlan {
    pub user_data_directory: RuntimePath,
    pub profile_directory: RuntimeString,
    pub preferences_path: RuntimePath,
    pub include_user_data_argument: bool,
}

/// Closed target-specific launch plan. The backend derives its fixed command,
/// argv, detached/null-stdio topology, parent cwd/env, and policy ID from the
/// crate-owned verified target; no generic process image is accepted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenChromeProcessPlan {
    target: OpenChromeTarget,
    profile: OpenChromeProfilePlan,
    url: RuntimeString,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum OpenChromeTarget {
    MacOs(VerifiedChromeLaunchTarget),
    Windows(VerifiedChromeLaunchTarget),
    Linux(VerifiedChromeLaunchTarget),
}

impl OpenChromeProcessPlan {
    pub(super) fn macos(
        application: VerifiedChromeLaunchTarget,
        profile: OpenChromeProfilePlan,
        url: RuntimeString,
    ) -> Result<Self, OpenChromePlanError> {
        Self::new(BrowserPlatform::MacOs, application, profile, url)
    }

    pub(super) fn windows(
        executable: VerifiedChromeLaunchTarget,
        profile: OpenChromeProfilePlan,
        url: RuntimeString,
    ) -> Result<Self, OpenChromePlanError> {
        Self::new(BrowserPlatform::Windows, executable, profile, url)
    }

    pub(super) fn linux(
        executable: VerifiedChromeLaunchTarget,
        profile: OpenChromeProfilePlan,
        url: RuntimeString,
    ) -> Result<Self, OpenChromePlanError> {
        Self::new(BrowserPlatform::Linux, executable, profile, url)
    }

    fn new(
        platform: BrowserPlatform,
        target: VerifiedChromeLaunchTarget,
        profile: OpenChromeProfilePlan,
        url: RuntimeString,
    ) -> Result<Self, OpenChromePlanError> {
        if target.platform != platform {
            return Err(OpenChromePlanError::PlatformMismatch);
        }
        let target = match platform {
            BrowserPlatform::MacOs => OpenChromeTarget::MacOs(target),
            BrowserPlatform::Windows => OpenChromeTarget::Windows(target),
            BrowserPlatform::Linux => OpenChromeTarget::Linux(target),
        };
        Ok(Self {
            target,
            profile,
            url,
        })
    }

    fn success(&self, launched: bool) -> OpenChromeSuccess {
        let (command, mut args, browser_path) = match &self.target {
            OpenChromeTarget::MacOs(application) => (
                RuntimeString::from("open"),
                vec![
                    RuntimeString::from("-a"),
                    application.absolute_path().as_runtime_string().clone(),
                    RuntimeString::from("--args"),
                ],
                application.absolute_path(),
            ),
            OpenChromeTarget::Windows(executable) | OpenChromeTarget::Linux(executable) => (
                executable.absolute_path().as_runtime_string().clone(),
                Vec::new(),
                executable.absolute_path(),
            ),
        };
        if self.profile.include_user_data_argument {
            let mut argument = RuntimeString::from("--user-data-dir=").as_utf16().to_vec();
            argument.extend_from_slice(
                self.profile
                    .user_data_directory
                    .as_runtime_string()
                    .as_utf16(),
            );
            args.push(RuntimeString::from_utf16(argument));
        }
        let mut profile_argument = RuntimeString::from("--profile-directory=")
            .as_utf16()
            .to_vec();
        profile_argument.extend_from_slice(self.profile.profile_directory.as_utf16());
        args.push(RuntimeString::from_utf16(profile_argument));
        args.push(self.url.clone());

        OpenChromeSuccess {
            launched,
            command,
            args,
            browser_path: browser_path.clone(),
            user_data_directory: self.profile.user_data_directory.clone(),
            profile_directory: self.profile.profile_directory.clone(),
            preferences_path: self.profile.preferences_path.clone(),
        }
    }

    fn platform(&self) -> BrowserPlatform {
        match &self.target {
            OpenChromeTarget::MacOs(_) => BrowserPlatform::MacOs,
            OpenChromeTarget::Windows(_) => BrowserPlatform::Windows,
            OpenChromeTarget::Linux(_) => BrowserPlatform::Linux,
        }
    }
}

/// A target brand was combined with the wrong target-specific launch recipe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpenChromePlanError {
    PlatformMismatch,
}

/// A dry-run has a complete immediate output. A real launch owns one deferred
/// resolution task whose effect order is fixed below.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OpenChromeExecutionPlan {
    DryRun(BrowserCliOutput),
    Launch(OpenChromeLaunchResolutionPlan),
}

/// One finite deferred launch task. Its only outcomes retain the already-
/// rendered success output before target-specific create failure bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenChromeLaunchResolutionPlan {
    process: OpenChromeProcessPlan,
    success_stdout: RuntimeString,
    error_domain: LaunchErrorDomain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LaunchErrorDomain {
    Errno,
    Win32,
}

/// Ordered effects owned by the CLI and its one launch-resolution task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OpenChromeEffect {
    AttemptDetachedLaunch(OpenChromeProcessPlan),
    WriteStdout(RuntimeString),
    FlushStdout,
    WriteStderr(RuntimeString),
    Exit(u8),
}

impl OpenChromeLaunchResolutionPlan {
    /// OS creation succeeded and was already committed to the detached root.
    pub fn created_effects(&self) -> Vec<OpenChromeEffect> {
        vec![
            OpenChromeEffect::AttemptDetachedLaunch(self.process.clone()),
            OpenChromeEffect::WriteStdout(self.success_stdout.clone()),
            OpenChromeEffect::FlushStdout,
            OpenChromeEffect::Exit(0),
        ]
    }

    /// OS creation failed. Success is still written and flushed first, then the
    /// stable target-specific line is emitted and the task exits one.
    pub fn failed_effects(&self, unsigned_error: u32) -> Vec<OpenChromeEffect> {
        let domain = match self.error_domain {
            LaunchErrorDomain::Errno => "errno",
            LaunchErrorDomain::Win32 => "win32",
        };
        vec![
            OpenChromeEffect::AttemptDetachedLaunch(self.process.clone()),
            OpenChromeEffect::WriteStdout(self.success_stdout.clone()),
            OpenChromeEffect::FlushStdout,
            OpenChromeEffect::WriteStderr(RuntimeString::from(format!(
                "Failed to launch Chrome: os-error {domain}:{unsigned_error}\n"
            ))),
            OpenChromeEffect::Exit(1),
        ]
    }
}

/// Convert a resolved target plan into either an immediate dry-run result or
/// the one success-before-deferred-error launch task. No process is created.
pub fn open_chrome_execution_plan(
    process: OpenChromeProcessPlan,
    dry_run: bool,
    json: bool,
) -> OpenChromeExecutionPlan {
    let success = process.success(!dry_run);
    let output = render_open_chrome_success(&success, dry_run, json);
    if dry_run {
        OpenChromeExecutionPlan::DryRun(output)
    } else {
        let error_domain = match process.platform() {
            BrowserPlatform::Windows => LaunchErrorDomain::Win32,
            BrowserPlatform::MacOs | BrowserPlatform::Linux => LaunchErrorDomain::Errno,
        };
        OpenChromeExecutionPlan::Launch(OpenChromeLaunchResolutionPlan {
            process,
            success_stdout: output.stdout().clone(),
            error_domain,
        })
    }
}

#[allow(dead_code)]
fn is_absolute(platform: BrowserPlatform, units: &[u16]) -> bool {
    match platform {
        BrowserPlatform::MacOs | BrowserPlatform::Linux => units.first() == Some(&u16::from(b'/')),
        BrowserPlatform::Windows => {
            let drive = units.len() >= 3
                && is_ascii_alphabetic(units[0])
                && units[1] == u16::from(b':')
                && matches!(units[2], unit if unit == u16::from(b'\\') || unit == u16::from(b'/'));
            let unc = units.len() >= 2 && is_separator(units[0]) && is_separator(units[1]);
            drive || unc
        }
    }
}

#[allow(dead_code)]
fn is_separator(unit: u16) -> bool {
    unit == u16::from(b'\\') || unit == u16::from(b'/')
}

#[allow(dead_code)]
fn is_ascii_alphabetic(unit: u16) -> bool {
    (u16::from(b'a')..=u16::from(b'z')).contains(&unit)
        || (u16::from(b'A')..=u16::from(b'Z')).contains(&unit)
}

fn basename(platform: BrowserPlatform, units: &[u16]) -> &[u16] {
    let separator = |unit: &u16| match platform {
        BrowserPlatform::MacOs | BrowserPlatform::Linux => *unit == u16::from(b'/'),
        BrowserPlatform::Windows => is_separator(*unit),
    };
    units
        .iter()
        .rposition(separator)
        .map_or(units, |index| &units[index + 1..])
}

fn equals_ascii(units: &[u16], expected: &str) -> bool {
    units.iter().copied().eq(expected.bytes().map(u16::from))
}

fn equals_ascii_case_insensitive(units: &[u16], expected: &str) -> bool {
    let expected = expected.as_bytes();
    units.len() == expected.len()
        && units.iter().zip(expected).all(|(unit, byte)| {
            u8::try_from(*unit).is_ok_and(|value| value.eq_ignore_ascii_case(byte))
        })
}
