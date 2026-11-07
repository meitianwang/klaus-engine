//! Production I/O boundary for the closed `klaus-browser` utility surface.
//!
//! Filesystem commands are executed here. Every process-bearing command is
//! delegated to the sealed executor below; the fallback used before the Core
//! policy wiring is installed fails closed and never reports a fake success.

use std::ffi::{OsStr, OsString};
use std::io::Write;
use std::path::{Path, PathBuf};

use klaus_agent_core::runtime_process::{
    RuntimeBrowserOpenChromeRequest, RuntimeBrowserUtilityProcess, RuntimeExecFileOutput,
    RuntimeProcessContext, RuntimeProcessPipeError, RuntimeProcessSupervisor,
};
use klaus_agent_core::runtime_values::{
    runtime_json, NodeBufferUtf8, RuntimeJson, RuntimeObject, RuntimePath, RuntimeString,
};

use super::cli::{
    identify_subcommand, parse_browser_utility, BrowserUtilityInvocation, BrowserUtilityParse,
};
use super::distribution_manifest::verified_native_host_from_distribution;
use super::plan::{
    browser_process_plan, install_native_host_manifest_plan, open_chrome_execution_plan,
    BrowserProcessPlan, BrowserQuietProbe, ChromeRunningProcessPlan, InstallNativeHostAction,
    OpenChromeEffect, OpenChromeExecutionPlan, OpenChromeProcessPlan, OpenChromeProfilePlan,
    VerifiedChromeLaunchTarget, VerifiedNativeHostBinary,
};
use super::result::{
    render_check_extension_error, render_check_extension_status, render_check_native_host_error,
    render_check_native_host_status, render_chrome_running_status,
    render_install_native_host_error, render_installed_browsers_result, render_open_chrome_error,
    BrowserCliOutput, BrowserPlatform, ChromeProcessStatus, ChromeRunningStatus,
    ExtensionInstallStatus, ExtensionProfileStatus, InstalledBrowser, InstalledBrowsersResult,
    NativeHostManifestStatus,
};
use super::{EXTENSION_ID, NATIVE_HOST_NAME};

const TOP_LEVEL_USAGE: &str = "Usage: klaus-browser <browser-utility> [arguments]\n";

pub(crate) mod sealed {
    pub trait Sealed {}
}

/// Closed production executor. It accepts only the already-validated browser
/// plans and cannot be widened into a generic file/argv/env process API.
#[async_trait::async_trait]
pub(crate) trait BrowserUtilityProcessExecutor: sealed::Sealed + Send {
    fn verified_native_host(
        &self,
        platform: BrowserPlatform,
    ) -> Result<VerifiedNativeHostBinary, RuntimeString>;

    async fn register_native_host(
        &mut self,
        manifest_path: &RuntimePath,
    ) -> Result<(), RuntimeString>;

    async fn execute_process_plan(
        &mut self,
        plan: BrowserProcessPlan,
        invocation: &BrowserUtilityInvocation,
    ) -> Result<BrowserCliOutput, RuntimeString>;
}

struct RuntimeBrowserUtilityProcessExecutor {
    context: RuntimeProcessContext,
}

impl sealed::Sealed for RuntimeBrowserUtilityProcessExecutor {}

#[async_trait::async_trait]
impl BrowserUtilityProcessExecutor for RuntimeBrowserUtilityProcessExecutor {
    fn verified_native_host(
        &self,
        platform: BrowserPlatform,
    ) -> Result<VerifiedNativeHostBinary, RuntimeString> {
        verified_native_host_from_distribution(platform)
    }

    async fn register_native_host(
        &mut self,
        manifest_path: &RuntimePath,
    ) -> Result<(), RuntimeString> {
        let output = self
            .context
            .execute_browser_utility_process(
                RuntimeBrowserUtilityProcess::WindowsRegisterNativeHost {
                    manifest_path: manifest_path.clone(),
                },
            )
            .await
            .map_err(process_error)?;
        if output.status.success() {
            Ok(())
        } else {
            Err(process_failure(
                &output,
                "Native host registry registration failed.",
            ))
        }
    }

    async fn execute_process_plan(
        &mut self,
        plan: BrowserProcessPlan,
        invocation: &BrowserUtilityInvocation,
    ) -> Result<BrowserCliOutput, RuntimeString> {
        match plan {
            BrowserProcessPlan::ChromeIsRunning(plan) => {
                execute_chrome_running(&self.context, plan, invocation).await
            }
            BrowserProcessPlan::InstalledBrowsers(_) => {
                execute_installed_browsers(&self.context, invocation).await
            }
            BrowserProcessPlan::OpenChromeWindow { dry_run, .. } => {
                execute_open_chrome(&self.context, dry_run, invocation).await
            }
            BrowserProcessPlan::CheckExtensionInstalledFilesystemOnly
            | BrowserProcessPlan::CheckNativeHostManifestFilesystemOnly
            | BrowserProcessPlan::InstallNativeHost { .. } => Err(RuntimeString::from(
                "Filesystem-only browser operation reached the process executor.",
            )),
        }
    }
}

pub fn run_browser_cli_main() -> u8 {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = writeln!(std::io::stderr(), "klaus-browser: {error}");
            return 70;
        }
    };
    let arguments = std::env::args_os()
        .skip(1)
        .map(|value| runtime_string_from_os(&value))
        .collect::<Vec<_>>();
    let output = runtime.block_on(async move {
        let supervisor = RuntimeProcessSupervisor::initialize_current()
            .map_err(|error| RuntimeString::from(error.message()))?;
        let context = RuntimeProcessContext::initialize(supervisor.clone())
            .map_err(|error| RuntimeString::from(error.message()))?;
        let mut executor = RuntimeBrowserUtilityProcessExecutor {
            context: context.clone(),
        };
        let output = run_browser_cli(arguments, &mut executor).await;
        drop(executor);
        context
            .shutdown()
            .await
            .map_err(|error| RuntimeString::from(error.message()))?;
        drop(context);
        supervisor
            .shutdown()
            .await
            .map_err(|error| RuntimeString::from(error.message()))?;
        Ok::<_, RuntimeString>(output)
    });
    match output {
        Ok(output) => write_cli_output(output),
        Err(error) => write_cli_output(BrowserCliOutput::new(
            RuntimeString::default(),
            error.concat(&RuntimeString::from("\n")),
            70,
        )),
    }
}

pub(crate) async fn run_browser_cli(
    mut arguments: Vec<RuntimeString>,
    executor: &mut dyn BrowserUtilityProcessExecutor,
) -> BrowserCliOutput {
    if arguments.is_empty() {
        return BrowserCliOutput::from_stderr(TOP_LEVEL_USAGE, 64);
    }
    let command = arguments.remove(0);
    let Some(subcommand) = identify_subcommand(&command) else {
        return BrowserCliOutput::from_stderr(TOP_LEVEL_USAGE, 64);
    };
    let invocation = match parse_browser_utility(subcommand, &arguments) {
        BrowserUtilityParse::Immediate(output) => return output,
        BrowserUtilityParse::Invoke(invocation) => invocation,
    };
    let platform = current_platform();
    match &invocation {
        BrowserUtilityInvocation::CheckExtensionInstalled { json } => {
            match check_extension_installed() {
                Ok(status) => render_check_extension_status(&status, *json),
                Err(error) => render_check_extension_error(error, *json),
            }
        }
        BrowserUtilityInvocation::CheckNativeHostManifest { json } => {
            match check_native_host_manifest() {
                Ok(status) => render_check_native_host_status(&status, *json),
                Err(error) => render_check_native_host_error(error, *json),
            }
        }
        BrowserUtilityInvocation::InstallNativeHostManifest { dry_run, json } => {
            match install_native_host_manifest(platform, *dry_run, *json, executor).await {
                Ok(output) => output,
                Err(error) => render_install_native_host_error(error, *json),
            }
        }
        BrowserUtilityInvocation::ChromeIsRunning { .. }
        | BrowserUtilityInvocation::InstalledBrowsers { .. }
        | BrowserUtilityInvocation::OpenChromeWindow { .. } => {
            let plan = browser_process_plan(&invocation, platform);
            executor
                .execute_process_plan(plan, &invocation)
                .await
                .unwrap_or_else(|error| {
                    BrowserCliOutput::new(
                        RuntimeString::default(),
                        error.concat(&RuntimeString::from("\n")),
                        70,
                    )
                })
        }
    }
}

async fn execute_chrome_running(
    context: &RuntimeProcessContext,
    plan: ChromeRunningProcessPlan,
    invocation: &BrowserUtilityInvocation,
) -> Result<BrowserCliOutput, RuntimeString> {
    // The source probes the macOS singleton before invoking `ps`, but uses it
    // only as a fallback when the application-process list is empty.
    let macos_singleton = (plan == ChromeRunningProcessPlan::MacOsSingletonThenPs)
        .then(macos_singleton_process)
        .flatten();
    let operation = match plan {
        ChromeRunningProcessPlan::WindowsTasklist => {
            RuntimeBrowserUtilityProcess::WindowsTasklistChrome
        }
        ChromeRunningProcessPlan::MacOsSingletonThenPs => {
            RuntimeBrowserUtilityProcess::MacOsPsChrome
        }
        ChromeRunningProcessPlan::LinuxPs => RuntimeBrowserUtilityProcess::LinuxPsChrome,
    };
    let output = process_text(context, operation).await;
    let processes = match plan {
        ChromeRunningProcessPlan::WindowsTasklist => output
            .as_ref()
            .map(parse_windows_tasklist)
            .unwrap_or_default(),
        ChromeRunningProcessPlan::MacOsSingletonThenPs => {
            let processes = output
                .as_ref()
                .map(|value| parse_process_list(value, true))
                .unwrap_or_default();
            if processes.is_empty() {
                macos_singleton.into_iter().collect()
            } else {
                processes
            }
        }
        ChromeRunningProcessPlan::LinuxPs => output
            .as_ref()
            .map(|value| parse_process_list(value, false))
            .unwrap_or_default(),
    };
    let (check, json) = match invocation {
        BrowserUtilityInvocation::ChromeIsRunning { check, json } => (*check, *json),
        _ => return Err(RuntimeString::from("Invalid chrome-is-running invocation.")),
    };
    Ok(render_chrome_running_status(
        &ChromeRunningStatus {
            processes,
            platform: current_platform(),
        },
        check,
        json,
    ))
}

async fn execute_installed_browsers(
    context: &RuntimeProcessContext,
    invocation: &BrowserUtilityInvocation,
) -> Result<BrowserCliOutput, RuntimeString> {
    let json = match invocation {
        BrowserUtilityInvocation::InstalledBrowsers { json } => *json,
        _ => {
            return Err(RuntimeString::from(
                "Invalid installed-browsers invocation.",
            ))
        }
    };
    let browsers = match current_platform() {
        BrowserPlatform::MacOs => installed_macos_browsers(context).await,
        BrowserPlatform::Windows => installed_windows_browsers(context).await,
        BrowserPlatform::Linux => installed_linux_browsers(context).await,
    };
    Ok(render_installed_browsers_result(
        &InstalledBrowsersResult {
            browsers,
            platform: current_platform(),
        },
        json,
    ))
}

async fn execute_open_chrome(
    context: &RuntimeProcessContext,
    dry_run: bool,
    invocation: &BrowserUtilityInvocation,
) -> Result<BrowserCliOutput, RuntimeString> {
    let (json, url) = match invocation {
        BrowserUtilityInvocation::OpenChromeWindow { json, url, .. } => (*json, url.clone()),
        _ => {
            return Err(RuntimeString::from(
                "Invalid open-chrome-window invocation.",
            ))
        }
    };
    let profile = resolve_open_chrome_profile()?;
    let target_path =
        discover_chrome_target(context)
            .await
            .ok_or_else(|| match current_platform() {
                BrowserPlatform::MacOs => RuntimeString::from("Google Chrome.app was not found."),
                BrowserPlatform::Windows => RuntimeString::from("chrome.exe was not found."),
                BrowserPlatform::Linux => {
                    RuntimeString::from("google-chrome/chromium was not found.")
                }
            })?;
    let target = VerifiedChromeLaunchTarget::from_verified_discovery_path(
        current_platform(),
        target_path.clone(),
    )
    .map_err(|_| RuntimeString::from("Discovered Chrome path was not absolute."))?;
    let process = match current_platform() {
        BrowserPlatform::MacOs => {
            OpenChromeProcessPlan::macos(target, profile.clone(), url.clone())
        }
        BrowserPlatform::Windows => {
            OpenChromeProcessPlan::windows(target, profile.clone(), url.clone())
        }
        BrowserPlatform::Linux => {
            OpenChromeProcessPlan::linux(target, profile.clone(), url.clone())
        }
    }
    .map_err(|_| RuntimeString::from("Chrome launch target platform mismatch."))?;
    let core_user_data = profile
        .include_user_data_argument
        .then(|| profile.user_data_directory.clone());
    let core_request = match current_platform() {
        BrowserPlatform::MacOs => RuntimeBrowserOpenChromeRequest::macos(
            target_path,
            core_user_data,
            profile.profile_directory.clone(),
            url,
        ),
        BrowserPlatform::Windows => RuntimeBrowserOpenChromeRequest::windows(
            target_path,
            core_user_data,
            profile.profile_directory.clone(),
            url,
        ),
        BrowserPlatform::Linux => RuntimeBrowserOpenChromeRequest::linux(
            target_path,
            core_user_data,
            profile.profile_directory.clone(),
            url,
        ),
    };
    match open_chrome_execution_plan(process, dry_run, json) {
        OpenChromeExecutionPlan::DryRun(output) => Ok(output),
        OpenChromeExecutionPlan::Launch(resolution) => {
            let effects = match context.spawn_browser_open_chrome(core_request).await {
                Ok(()) => resolution.created_effects(),
                Err(error) => resolution.failed_effects(browser_launch_os_error_code(&error)),
            };
            Ok(render_open_chrome_effects(effects))
        }
    }
}

fn browser_launch_os_error_code(error: &RuntimeProcessPipeError) -> u32 {
    stable_os_error_code(error.message()).unwrap_or(0)
}

fn stable_os_error_code(message: &str) -> Option<u32> {
    ["win32:", "errno:", "errno=", "errno "]
        .into_iter()
        .filter_map(|marker| {
            let tail = message.rsplit_once(marker)?.1.trim_start();
            let digits = tail
                .bytes()
                .take_while(u8::is_ascii_digit)
                .collect::<Vec<_>>();
            (!digits.is_empty())
                .then(|| std::str::from_utf8(&digits).ok()?.parse::<u32>().ok())
                .flatten()
        })
        .next()
}

async fn process_text(
    context: &RuntimeProcessContext,
    operation: RuntimeBrowserUtilityProcess,
) -> Option<RuntimeString> {
    let output = context
        .execute_browser_utility_process(operation)
        .await
        .ok()?;
    output
        .status
        .success()
        .then(|| trim_runtime(&NodeBufferUtf8::decode(&output.stdout)))
}

fn process_error(error: impl std::fmt::Display) -> RuntimeString {
    RuntimeString::from(error.to_string())
}

fn process_failure(output: &RuntimeExecFileOutput, fallback: &str) -> RuntimeString {
    let stderr = trim_runtime(&NodeBufferUtf8::decode(&output.stderr));
    if stderr.is_empty() {
        RuntimeString::from(fallback)
    } else {
        stderr
    }
}

fn macos_singleton_process() -> Option<ChromeProcessStatus> {
    #[cfg(target_os = "macos")]
    {
        let home = PathBuf::from(nonempty_env_os("HOME")?);
        macos_singleton_process_from_home(&home)
    }
    #[cfg(not(target_os = "macos"))]
    None
}

#[cfg(target_os = "macos")]
fn macos_singleton_process_from_home(home: &Path) -> Option<ChromeProcessStatus> {
    let target = std::fs::read_link(
        home.join("Library")
            .join("Application Support")
            .join("Google")
            .join("Chrome")
            .join("SingletonLock"),
    )
    .ok()?;
    let pid = parse_macos_singleton_pid(target.as_os_str())?;
    // SAFETY: signal zero performs only the existence/permission probe used by
    // Node's `process.kill(pid, 0)`; `pid` is a checked positive pid_t.
    if unsafe { libc::kill(pid, 0) } != 0 {
        return None;
    }
    Some(ChromeProcessStatus {
        pid: pid as u32,
        process_name: RuntimeString::from("Google Chrome"),
        command: RuntimeString::from("Google Chrome"),
    })
}

#[cfg(any(target_os = "macos", test))]
fn parse_macos_singleton_pid(target: &OsStr) -> Option<i32> {
    let target = scalar_boundary(&runtime_string_from_os(target));
    let digits = target.rsplit_once('-')?.1;
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let pid = digits.parse::<i32>().ok()?;
    (pid > 0).then_some(pid)
}

fn parse_process_list(output: &RuntimeString, macos: bool) -> Vec<ChromeProcessStatus> {
    let accepted = if macos {
        &["Google Chrome", "Google Chrome Helper"][..]
    } else {
        &["google-chrome", "chrome", "chromium", "chromium-browser"][..]
    };
    scalar_boundary(output)
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            let split = line.find(char::is_whitespace)?;
            let pid = line[..split].parse::<u32>().ok()?;
            let mut command = line[split..].trim();
            if let Some(index) = command.find(" --") {
                command = &command[..index];
            }
            let name = command.rsplit('/').next().unwrap_or(command);
            if !accepted.contains(&name)
                || (macos && !command.contains("/Google Chrome.app/Contents/"))
            {
                return None;
            }
            Some(ChromeProcessStatus {
                pid,
                process_name: RuntimeString::from(name),
                command: RuntimeString::from(command),
            })
        })
        .collect()
}

fn parse_windows_tasklist(output: &RuntimeString) -> Vec<ChromeProcessStatus> {
    scalar_boundary(output)
        .lines()
        .filter_map(|line| {
            let fields = line.split("\",\"").collect::<Vec<_>>();
            if fields.len() < 2 {
                return None;
            }
            let name = fields[0].trim_start_matches('"');
            if !name.eq_ignore_ascii_case("chrome.exe") {
                return None;
            }
            let pid = fields[1].trim_matches('"').parse::<u32>().ok()?;
            Some(ChromeProcessStatus {
                pid,
                process_name: RuntimeString::from(name),
                command: RuntimeString::from(name),
            })
        })
        .collect()
}

async fn installed_linux_browsers(context: &RuntimeProcessContext) -> Vec<InstalledBrowser> {
    let probes = [
        (
            RuntimeBrowserUtilityProcess::LinuxWhichGoogleChrome,
            "Google Chrome",
        ),
        (RuntimeBrowserUtilityProcess::LinuxWhichChromium, "Chromium"),
        (
            RuntimeBrowserUtilityProcess::LinuxWhichChromiumBrowser,
            "Chromium",
        ),
    ];
    let mut browsers = Vec::new();
    for (probe, name) in probes {
        if let Some(path) = process_text(context, probe)
            .await
            .filter(|value| !value.is_empty())
        {
            browsers.push(InstalledBrowser::Desktop {
                name: RuntimeString::from(name),
                path: RuntimePath::from_runtime_string(first_runtime_line(&path)),
                version: None,
            });
        }
    }
    browsers
}

async fn installed_macos_browsers(context: &RuntimeProcessContext) -> Vec<InstalledBrowser> {
    let mut candidates = vec![
        PathBuf::from("/Applications/Google Chrome.app"),
        PathBuf::from("/System/Applications/Google Chrome.app"),
    ];
    if let Ok(home) = home_directory() {
        candidates.push(home.join("Applications/Google Chrome.app"));
    }
    if let Some(spotlight) =
        process_text(context, RuntimeBrowserUtilityProcess::MacOsMdfindChrome).await
    {
        for line in scalar_boundary(&spotlight)
            .lines()
            .filter(|line| line.ends_with(".app"))
        {
            candidates.push(PathBuf::from(line));
        }
    }
    let mut browsers = Vec::new();
    for candidate in candidates.into_iter().filter(|path| path.exists()) {
        let plist = runtime_path_from_native(&candidate.join("Contents/Info.plist"));
        let info = process_text(
            context,
            RuntimeBrowserUtilityProcess::MacOsPlutilInfo { plist_path: plist },
        )
        .await
        .and_then(|text| runtime_json::parse(&text).ok());
        let bundle_id = info
            .as_ref()
            .and_then(|value| value.get("CFBundleIdentifier"))
            .and_then(RuntimeJson::as_string)
            .cloned()
            .unwrap_or_else(|| RuntimeString::from("com.google.Chrome"));
        let version = info.as_ref().and_then(|value| {
            value
                .get("CFBundleShortVersionString")
                .or_else(|| value.get("CFBundleVersion"))
                .and_then(RuntimeJson::as_string)
                .cloned()
        });
        let browser = InstalledBrowser::MacOs {
            name: RuntimeString::from("Google Chrome"),
            bundle_id: bundle_id.clone(),
            path: runtime_path_from_native(&candidate),
            version,
        };
        if let Some(index) = browsers.iter().position(|existing| {
            matches!(existing, InstalledBrowser::MacOs { bundle_id: value, .. } if value == &bundle_id)
        }) {
            browsers[index] = browser;
        } else {
            browsers.push(browser);
        }
    }
    browsers
}

async fn installed_windows_browsers(context: &RuntimeProcessContext) -> Vec<InstalledBrowser> {
    let mut paths = Vec::new();
    for probe in [
        RuntimeBrowserUtilityProcess::WindowsAppPathChromeCurrentUser,
        RuntimeBrowserUtilityProcess::WindowsAppPathChromeLocalMachine,
        RuntimeBrowserUtilityProcess::WindowsAppPathChromeWow6432,
    ] {
        if let Some(output) = process_text(context, probe).await {
            if let Some(value) = parse_registry_value(&output, "(Default)") {
                paths.push(PathBuf::from(scalar_boundary(&value)));
            }
        }
    }
    let home = home_directory().unwrap_or_default();
    let local = nonempty_env_os("LOCALAPPDATA")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join("AppData/Local"));
    paths.push(local.join("Google/Chrome/Application/chrome.exe"));
    for key in ["PROGRAMFILES", "PROGRAMFILES(X86)"] {
        if let Some(root) = nonempty_env_os(key) {
            paths.push(PathBuf::from(root).join("Google/Chrome/Application/chrome.exe"));
        }
    }
    if let Some(output) =
        process_text(context, RuntimeBrowserUtilityProcess::WindowsWhereChrome).await
    {
        paths.push(PathBuf::from(scalar_boundary(&first_runtime_line(&output))));
    }
    let version = if let Some(output) = process_text(
        context,
        RuntimeBrowserUtilityProcess::WindowsChromeVersionCurrentUser,
    )
    .await
    {
        parse_registry_value(&output, "DisplayVersion")
    } else {
        None
    }
    .or(None);
    let mut seen = Vec::<String>::new();
    paths
        .into_iter()
        .filter(|path| path.exists())
        .filter_map(|path| {
            let key =
                scalar_boundary(runtime_path_from_native(&path).as_runtime_string()).to_lowercase();
            if seen.contains(&key) {
                return None;
            }
            seen.push(key);
            Some(InstalledBrowser::Desktop {
                name: RuntimeString::from("Google Chrome"),
                path: runtime_path_from_native(&path),
                version: version.clone(),
            })
        })
        .collect()
}

async fn discover_chrome_target(context: &RuntimeProcessContext) -> Option<RuntimePath> {
    match current_platform() {
        BrowserPlatform::MacOs => installed_macos_browsers(context)
            .await
            .into_iter()
            .next()
            .map(|browser| match browser {
                InstalledBrowser::MacOs { path, .. } | InstalledBrowser::Desktop { path, .. } => {
                    path
                }
            }),
        BrowserPlatform::Windows => installed_windows_browsers(context)
            .await
            .into_iter()
            .next()
            .map(|browser| match browser {
                InstalledBrowser::MacOs { path, .. } | InstalledBrowser::Desktop { path, .. } => {
                    path
                }
            }),
        BrowserPlatform::Linux => installed_linux_browsers(context)
            .await
            .into_iter()
            .next()
            .map(|browser| match browser {
                InstalledBrowser::MacOs { path, .. } | InstalledBrowser::Desktop { path, .. } => {
                    path
                }
            }),
    }
}

fn resolve_open_chrome_profile() -> Result<OpenChromeProfilePlan, RuntimeString> {
    let override_path = nonempty_env_os("KLAUS_CHROME_PREFERENCES_PATH")
        .map(resolve_native_path)
        .transpose()?;
    let (user_data, preferences) = match override_path {
        Some(preferences) => {
            let profile = preferences.parent().ok_or_else(|| {
                RuntimeString::from("Chrome preferences path has no profile directory.")
            })?;
            let user_data = profile
                .parent()
                .ok_or_else(|| RuntimeString::from("Chrome profile has no user-data directory."))?;
            (user_data.to_path_buf(), preferences)
        }
        None => {
            let user_data = chrome_user_data_directory()?;
            let mut profiles = list_profile_names(&user_data)?;
            let profile = profiles.pop().ok_or_else(|| {
                RuntimeString::from(
                    "Could not find a Chrome profile directory with Preferences in ",
                )
                .concat(runtime_path_from_native(&user_data).as_runtime_string())
                .concat(&RuntimeString::from("."))
            })?;
            let preferences = user_data
                .join(project_runtime_filename(&profile)?)
                .join("Preferences");
            (user_data, preferences)
        }
    };
    let profile_path = preferences
        .parent()
        .ok_or_else(|| RuntimeString::from("Chrome preferences path has no profile directory."))?;
    Ok(OpenChromeProfilePlan {
        user_data_directory: runtime_path_from_native(&user_data),
        profile_directory: runtime_string_from_os(profile_path.file_name().unwrap_or_default()),
        preferences_path: runtime_path_from_native(&preferences),
        include_user_data_argument: nonempty_env_os("KLAUS_CHROME_USER_DATA_DIR").is_some(),
    })
}

fn list_profile_names(user_data: &Path) -> Result<Vec<RuntimeString>, RuntimeString> {
    if !user_data.exists() {
        return Err(
            RuntimeString::from("Chrome user data directory does not exist: ")
                .concat(runtime_path_from_native(user_data).as_runtime_string()),
        );
    }
    let mut values = Vec::new();
    for entry in std::fs::read_dir(user_data).map_err(io_error)? {
        let entry = entry.map_err(io_error)?;
        let name = runtime_string_from_os(&entry.file_name());
        if entry.file_type().map_err(io_error)?.is_dir()
            && is_profile_directory(&name)
            && entry.path().join("Preferences").exists()
        {
            values.push(name);
        }
    }
    values.sort_by_key(profile_sort_key);
    Ok(values)
}

fn parse_registry_value(output: &RuntimeString, label: &str) -> Option<RuntimeString> {
    for line in scalar_boundary(output).lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with(label) {
            continue;
        }
        let marker = trimmed.find("REG_")?;
        let value = trimmed[marker..].find(char::is_whitespace)? + marker;
        return Some(RuntimeString::from(
            trimmed[value..].trim().trim_matches('"'),
        ));
    }
    None
}

fn render_open_chrome_effects(effects: Vec<OpenChromeEffect>) -> BrowserCliOutput {
    let mut stdout = RuntimeString::default();
    let mut stderr = RuntimeString::default();
    let mut exit = 1;
    for effect in effects {
        match effect {
            OpenChromeEffect::WriteStdout(value) => stdout = stdout.concat(&value),
            OpenChromeEffect::WriteStderr(value) => stderr = stderr.concat(&value),
            OpenChromeEffect::Exit(code) => exit = code,
            OpenChromeEffect::AttemptDetachedLaunch(_) | OpenChromeEffect::FlushStdout => {}
        }
    }
    BrowserCliOutput::new(stdout, stderr, exit)
}

fn first_runtime_line(value: &RuntimeString) -> RuntimeString {
    let units = value.as_utf16();
    let end = units
        .iter()
        .position(|unit| *unit == u16::from(b'\n') || *unit == u16::from(b'\r'))
        .unwrap_or(units.len());
    value.slice_utf16(..end)
}

fn trim_runtime(value: &RuntimeString) -> RuntimeString {
    RuntimeString::from(scalar_boundary(value).trim().to_owned())
}

fn scalar_boundary(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8()).expect("Node UTF-8 projection is scalar")
}

fn check_extension_installed() -> Result<ExtensionInstallStatus, RuntimeString> {
    let preferences_override = nonempty_env_os("KLAUS_CHROME_PREFERENCES_PATH")
        .map(resolve_native_path)
        .transpose()?;
    let user_data_native = match &preferences_override {
        Some(preferences) => preferences
            .parent()
            .ok_or_else(|| RuntimeString::from("Chrome preferences override has no profile root."))?
            .to_path_buf(),
        None => chrome_user_data_directory()?,
    };
    let user_data_directory = runtime_path_from_native(&user_data_native);
    let profiles = match &preferences_override {
        Some(preferences) => {
            let profile_path = preferences.parent().ok_or_else(|| {
                RuntimeString::from("Chrome preferences override has no profile directory.")
            })?;
            vec![extension_profile_status(
                profile_path
                    .parent()
                    .ok_or_else(|| RuntimeString::from("Chrome profile has no user-data root."))?,
                runtime_string_from_os(profile_path.file_name().unwrap_or_default()),
                Some(preferences),
            )?]
        }
        None => {
            let mut profile_names = Vec::new();
            if !user_data_native.exists() {
                return Err(
                    RuntimeString::from("Chrome user data directory does not exist: ")
                        .concat(user_data_directory.as_runtime_string()),
                );
            }
            let entries = std::fs::read_dir(&user_data_native).map_err(io_error)?;
            for entry in entries {
                let entry = entry.map_err(io_error)?;
                let file_type = entry.file_type().map_err(io_error)?;
                if !file_type.is_dir() {
                    continue;
                }
                let name = runtime_string_from_os(&entry.file_name());
                if is_profile_directory(&name) && entry.path().join("Preferences").exists() {
                    profile_names.push(name);
                }
            }
            profile_names.sort_by_key(profile_sort_key);
            let mut profiles = Vec::with_capacity(profile_names.len());
            for name in profile_names {
                profiles.push(extension_profile_status(&user_data_native, name, None)?);
            }
            profiles
        }
    };
    let installed = profiles.iter().any(|profile| profile.installed);
    let enabled = profiles.iter().any(|profile| profile.enabled);
    Ok(ExtensionInstallStatus {
        user_data_directory,
        installed,
        enabled,
        profiles,
    })
}

fn extension_profile_status(
    user_data_directory: &Path,
    profile_directory: RuntimeString,
    preferences_override: Option<&PathBuf>,
) -> Result<ExtensionProfileStatus, RuntimeString> {
    let profile_path_native = match preferences_override {
        Some(preferences) => preferences
            .parent()
            .unwrap_or(user_data_directory)
            .to_path_buf(),
        None => user_data_directory.join(project_runtime_filename(&profile_directory)?),
    };
    let preferences_native = preferences_override
        .cloned()
        .unwrap_or_else(|| profile_path_native.join("Preferences"));
    let preferences = read_runtime_json_if_present(&preferences_native)?
        .unwrap_or_else(|| RuntimeJson::Object(RuntimeObject::new()));
    let settings = preferences
        .get("extensions")
        .and_then(|value| value.get("settings"))
        .and_then(|value| value.get(EXTENSION_ID));
    let extension_path = profile_path_native.join("Extensions").join(EXTENSION_ID);
    let mut versions = Vec::new();
    if extension_path.exists() {
        for entry in std::fs::read_dir(&extension_path).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            if entry.file_type().map_err(io_error)?.is_dir() {
                versions.push(runtime_string_from_os(&entry.file_name()));
            }
        }
        versions.sort();
    }
    let unpacked = settings
        .and_then(|value| value.get("path"))
        .and_then(RuntimeJson::as_string)
        .cloned();
    let unpacked_installed = unpacked
        .as_ref()
        .and_then(|path| project_runtime_path(path).ok())
        .is_some_and(|path| path.exists());
    let disable_reasons: Vec<RuntimeString> = settings
        .and_then(|value| value.get("disable_reasons"))
        .and_then(RuntimeJson::as_object)
        .map(|object| object.keys().collect())
        .unwrap_or_default();
    let installed = !versions.is_empty() || unpacked_installed;
    let state = settings
        .and_then(|value| value.get("state"))
        .cloned()
        .unwrap_or(RuntimeJson::Null);
    let enabled = installed
        && settings.is_some()
        && state.as_f64() != Some(0.0)
        && disable_reasons.is_empty();
    Ok(ExtensionProfileStatus {
        profile_directory,
        profile_path: runtime_path_from_native(&profile_path_native),
        preferences_path: runtime_path_from_native(&preferences_native),
        installed,
        enabled,
        registered: settings.is_some(),
        state,
        disable_reasons,
        versions,
        unpacked_path: unpacked.map(RuntimePath::from_runtime_string),
    })
}

fn check_native_host_manifest() -> Result<NativeHostManifestStatus, RuntimeString> {
    let manifest_native = native_host_manifest_path()?;
    let manifest_path = runtime_path_from_native(&manifest_native);
    let Some(manifest) = read_runtime_json_if_present(&manifest_native)? else {
        return Ok(NativeHostManifestStatus {
            correct: false,
            manifest_path: manifest_path.clone(),
            exists: false,
            manifest: None,
            problem: Some(
                RuntimeString::from("Native host manifest does not exist: ")
                    .concat(manifest_path.as_runtime_string()),
            ),
        });
    };
    let expected_origin = RuntimeString::from(format!("chrome-extension://{EXTENSION_ID}/"));
    let name_matches = manifest
        .get("name")
        .and_then(RuntimeJson::as_string)
        .is_some_and(|value| runtime_eq_ascii(value, NATIVE_HOST_NAME));
    let origin_allowed = manifest
        .get("allowed_origins")
        .and_then(RuntimeJson::as_array)
        .is_some_and(|origins| {
            origins
                .iter()
                .any(|origin| origin.as_string() == Some(&expected_origin))
        });
    let path_exists = manifest
        .get("path")
        .and_then(RuntimeJson::as_string)
        .and_then(|path| project_runtime_path(path).ok())
        .is_some_and(|path| path.exists());
    let type_matches = manifest
        .get("type")
        .and_then(RuntimeJson::as_string)
        .is_some_and(|value| runtime_eq_ascii(value, "stdio"));
    let correct = name_matches && origin_allowed && path_exists && type_matches;
    let mut problems = Vec::new();
    if !name_matches {
        problems.push(
            RuntimeString::from("name is ")
                .concat(&manifest_value_or_missing(manifest.get("name")))
                .concat(&RuntimeString::from(format!(
                    ", expected {NATIVE_HOST_NAME}"
                ))),
        );
    }
    if !origin_allowed {
        problems.push(RuntimeString::from(format!(
            "allowed_origins missing chrome-extension://{EXTENSION_ID}/"
        )));
    }
    if !path_exists {
        problems.push(
            RuntimeString::from("native host path missing: ")
                .concat(&manifest_value_or_missing(manifest.get("path"))),
        );
    }
    if !type_matches {
        problems.push(
            RuntimeString::from("type is ")
                .concat(&manifest_value_or_missing(manifest.get("type")))
                .concat(&RuntimeString::from(", expected stdio")),
        );
    }
    Ok(NativeHostManifestStatus {
        correct,
        manifest_path,
        exists: true,
        manifest: Some(manifest),
        problem: join_runtime_strings(&problems, "; "),
    })
}

async fn install_native_host_manifest(
    platform: BrowserPlatform,
    dry_run: bool,
    json: bool,
    executor: &mut dyn BrowserUtilityProcessExecutor,
) -> Result<BrowserCliOutput, RuntimeString> {
    let manifest_native = native_host_manifest_path()?;
    let manifest_path = runtime_path_from_native(&manifest_native);
    let native_host = executor.verified_native_host(platform)?;
    let plan =
        install_native_host_manifest_plan(platform, manifest_path.clone(), &native_host, dry_run)
            .map_err(|_| RuntimeString::from("Native host distribution platform mismatch."))?;
    match plan.action() {
        InstallNativeHostAction::DryRun => {}
        InstallNativeHostAction::UnixWriteManifestThenChmodHost0755 => {
            write_native_host_manifest(&manifest_native, plan.result().manifest.to_runtime_json())?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let host_path = project_runtime_path(plan.result().host_path.as_runtime_string())?;
                std::fs::set_permissions(host_path, std::fs::Permissions::from_mode(0o755))
                    .map_err(io_error)?;
            }
        }
        InstallNativeHostAction::WindowsWriteManifestThenRegister { .. } => {
            write_native_host_manifest(&manifest_native, plan.result().manifest.to_runtime_json())?;
            executor.register_native_host(&manifest_path).await?;
        }
    }
    Ok(plan.render_success(dry_run, json))
}

fn write_native_host_manifest(path: &Path, manifest: RuntimeJson) -> Result<(), RuntimeString> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(io_error)?;
    }
    let bytes = runtime_json::stringify_pretty(&manifest)
        .concat(&RuntimeString::from("\n"))
        .to_node_utf8();
    std::fs::write(path, bytes).map_err(io_error)
}

fn read_runtime_json_if_present(path: &Path) -> Result<Option<RuntimeJson>, RuntimeString> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(path).map_err(io_error)?;
    runtime_json::parse(&NodeBufferUtf8::decode(&bytes))
        .map(Some)
        .map_err(|error| RuntimeString::from(error.to_string()))
}

fn chrome_user_data_directory() -> Result<PathBuf, RuntimeString> {
    if let Some(path) = nonempty_env_os("KLAUS_CHROME_USER_DATA_DIR") {
        return resolve_native_path(path);
    }
    let home = home_directory()?;
    Ok(match current_platform() {
        BrowserPlatform::MacOs => home.join("Library/Application Support/Google/Chrome"),
        BrowserPlatform::Windows => nonempty_env_os("LOCALAPPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join("AppData/Local"))
            .join("Google/Chrome/User Data"),
        BrowserPlatform::Linux => home.join(".config/google-chrome"),
    })
}

fn native_host_manifest_path() -> Result<PathBuf, RuntimeString> {
    if let Some(path) = nonempty_env_os("KLAUS_CHROME_NATIVE_HOST_MANIFEST_PATH") {
        return resolve_native_path(path);
    }
    let home = home_directory()?;
    Ok(match current_platform() {
        BrowserPlatform::MacOs => home
            .join("Library/Application Support/Google/Chrome/NativeMessagingHosts")
            .join(format!("{NATIVE_HOST_NAME}.json")),
        BrowserPlatform::Windows => nonempty_env_os("LOCALAPPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join("AppData/Local"))
            .join("Klaus/ChromeNativeHosts")
            .join(format!("{NATIVE_HOST_NAME}.json")),
        BrowserPlatform::Linux => home
            .join(".config/google-chrome/NativeMessagingHosts")
            .join(format!("{NATIVE_HOST_NAME}.json")),
    })
}

fn home_directory() -> Result<PathBuf, RuntimeString> {
    #[cfg(windows)]
    let value = nonempty_env_os("USERPROFILE").or_else(|| nonempty_env_os("HOME"));
    #[cfg(not(windows))]
    let value = nonempty_env_os("HOME");
    value
        .map(PathBuf::from)
        .ok_or_else(|| RuntimeString::from("Could not resolve the current home directory."))
}

fn resolve_native_path(value: OsString) -> Result<PathBuf, RuntimeString> {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        return Ok(path);
    }
    std::env::current_dir()
        .map(|cwd| cwd.join(path))
        .map_err(io_error)
}

fn nonempty_env_os(name: &str) -> Option<OsString> {
    std::env::var_os(name).filter(|value| !value.is_empty())
}

fn is_profile_directory(value: &RuntimeString) -> bool {
    if runtime_eq_ascii(value, "Default") {
        return true;
    }
    let prefix = "Profile ".encode_utf16().collect::<Vec<_>>();
    let Some(rest) = value.as_utf16().strip_prefix(prefix.as_slice()) else {
        return false;
    };
    !rest.is_empty()
        && rest
            .iter()
            .all(|unit| (u16::from(b'0')..=u16::from(b'9')).contains(unit))
}

fn profile_sort_key(value: &RuntimeString) -> u64 {
    if runtime_eq_ascii(value, "Default") {
        return 0;
    }
    value
        .as_utf16()
        .iter()
        .skip(8)
        .try_fold(0_u64, |value, unit| {
            value
                .checked_mul(10)?
                .checked_add(u64::from(*unit - u16::from(b'0')))
        })
        .unwrap_or(u64::MAX)
}

fn project_runtime_filename(value: &RuntimeString) -> Result<OsString, RuntimeString> {
    project_runtime_path(value).map(PathBuf::into_os_string)
}

fn project_runtime_path(value: &RuntimeString) -> Result<PathBuf, RuntimeString> {
    if value.as_utf16().contains(&0) {
        return Err(RuntimeString::from("Path contains a null byte."));
    }
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        Ok(PathBuf::from(OsString::from_vec(value.to_node_utf8())))
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStringExt;
        Ok(PathBuf::from(OsString::from_wide(value.as_utf16())))
    }
}

fn runtime_path_from_native(value: &Path) -> RuntimePath {
    RuntimePath::from_runtime_string(runtime_string_from_os(value.as_os_str()))
}

fn runtime_string_from_os(value: &OsStr) -> RuntimeString {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        NodeBufferUtf8::decode(value.as_bytes())
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        RuntimeString::from_utf16(value.encode_wide().collect::<Vec<_>>())
    }
}

fn runtime_eq_ascii(value: &RuntimeString, expected: &str) -> bool {
    value
        .as_utf16()
        .iter()
        .copied()
        .eq(expected.bytes().map(u16::from))
}

fn manifest_value_or_missing(value: Option<&RuntimeJson>) -> RuntimeString {
    match value {
        None | Some(RuntimeJson::Null) | Some(RuntimeJson::Bool(false)) => {
            RuntimeString::from("<missing>")
        }
        Some(RuntimeJson::String(value)) if value.is_empty() => RuntimeString::from("<missing>"),
        Some(RuntimeJson::String(value)) => value.clone(),
        Some(RuntimeJson::Bool(true)) => RuntimeString::from("true"),
        Some(RuntimeJson::Number(value)) if value.as_f64() == 0.0 || value.as_f64().is_nan() => {
            RuntimeString::from("<missing>")
        }
        Some(RuntimeJson::Number(value)) => RuntimeString::from(value.as_f64().to_string()),
        Some(RuntimeJson::Array(values)) => {
            let parts = values
                .iter()
                .map(|value| manifest_value_or_missing(Some(value)))
                .collect::<Vec<_>>();
            join_runtime_strings(&parts, ",").unwrap_or_default()
        }
        Some(RuntimeJson::Object(_)) => RuntimeString::from("[object Object]"),
    }
}

fn join_runtime_strings(values: &[RuntimeString], separator: &str) -> Option<RuntimeString> {
    let mut values = values.iter();
    let mut output = values.next()?.clone();
    let separator = RuntimeString::from(separator);
    for value in values {
        output = output.concat(&separator).concat(value);
    }
    Some(output)
}

fn current_platform() -> BrowserPlatform {
    if cfg!(target_os = "macos") {
        BrowserPlatform::MacOs
    } else if cfg!(windows) {
        BrowserPlatform::Windows
    } else {
        BrowserPlatform::Linux
    }
}

fn io_error(error: std::io::Error) -> RuntimeString {
    RuntimeString::from(error.to_string())
}

fn write_cli_output(output: BrowserCliOutput) -> u8 {
    let mut stdout = std::io::stdout().lock();
    let mut stderr = std::io::stderr().lock();
    if stdout.write_all(&output.stdout().to_node_utf8()).is_err()
        || stdout.flush().is_err()
        || stderr.write_all(&output.stderr().to_node_utf8()).is_err()
        || stderr.flush().is_err()
    {
        return 74;
    }
    output.exit_code()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_filter_and_order_match_the_frozen_javascript_rules() {
        let mut values = ["Profile 10", "Other", "Profile 2", "Default"]
            .map(RuntimeString::from)
            .into_iter()
            .filter(is_profile_directory)
            .collect::<Vec<_>>();
        values.sort_by_key(profile_sort_key);
        assert_eq!(
            values,
            ["Default", "Profile 2", "Profile 10"].map(RuntimeString::from)
        );
    }

    #[test]
    fn macos_singleton_pid_parser_accepts_only_a_positive_decimal_suffix() {
        assert_eq!(parse_macos_singleton_pid(OsStr::new("host-42")), Some(42));
        assert_eq!(
            parse_macos_singleton_pid(OsStr::new("host-with-dashes--7")),
            Some(7)
        );
        assert_eq!(parse_macos_singleton_pid(OsStr::new("host-0")), None);
        assert_eq!(parse_macos_singleton_pid(OsStr::new("host--")), None);
        assert_eq!(parse_macos_singleton_pid(OsStr::new("host-12x")), None);
        assert_eq!(
            parse_macos_singleton_pid(OsStr::new("host-999999999999999999999")),
            None
        );
    }

    #[test]
    fn detached_launch_error_parser_preserves_stable_unsigned_os_codes() {
        assert_eq!(
            stable_os_error_code("Unix spawn failed with errno 2"),
            Some(2)
        );
        assert_eq!(
            stable_os_error_code("Syscall { operation: Spawn, errno: 13 }"),
            Some(13)
        );
        assert_eq!(
            stable_os_error_code("CreateProcessW failed with win32:5"),
            Some(5)
        );
        assert_eq!(stable_os_error_code("spawn failed without a code"), None);
        assert_eq!(stable_os_error_code("spawn failed with errno -2"), None);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_singleton_signal_zero_probe_accepts_the_live_test_process() {
        use std::os::unix::fs::symlink;
        use std::time::{SystemTime, UNIX_EPOCH};

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock must be after epoch")
            .as_nanos();
        let home = std::env::temp_dir().join(format!(
            "klaus-browser-singleton-{}-{nonce}",
            std::process::id()
        ));
        let chrome = home
            .join("Library")
            .join("Application Support")
            .join("Google")
            .join("Chrome");
        std::fs::create_dir_all(&chrome).expect("create singleton fixture");
        symlink(
            format!("fixture-host-{}", std::process::id()),
            chrome.join("SingletonLock"),
        )
        .expect("create singleton symlink");

        let process = macos_singleton_process_from_home(&home).expect("live pid must probe");
        assert_eq!(process.pid, std::process::id());
        assert_eq!(process.process_name, RuntimeString::from("Google Chrome"));
        assert_eq!(process.command, RuntimeString::from("Google Chrome"));
        std::fs::remove_dir_all(home).expect("remove singleton fixture");
    }
}
