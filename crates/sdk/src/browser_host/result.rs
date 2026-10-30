use klaus_agent_core::runtime_values::{
    runtime_json, RuntimeJson, RuntimeObject, RuntimePath, RuntimeString,
};

use super::{EXTENSION_ID, NATIVE_HOST_NAME};

const CHECK_EXTENSION_HINT: &str = concat!(
    "Set KLAUS_CHROME_USER_DATA_DIR or KLAUS_CHROME_PREFERENCES_PATH ",
    "to inspect a non-default Chrome profile."
);
const CHECK_MANIFEST_HINT: &str = concat!(
    "Set KLAUS_CHROME_NATIVE_HOST_MANIFEST_PATH ",
    "to inspect a non-default manifest path."
);
const INSTALL_MANIFEST_HINT: &str =
    "Set KLAUS_CHROME_NATIVE_HOST_MANIFEST_PATH to override the manifest path.";

/// Target tag serialized with the source's `process.platform` spellings.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum BrowserPlatform {
    MacOs,
    Windows,
    Linux,
}

impl BrowserPlatform {
    pub(crate) fn node_name(self) -> &'static str {
        match self {
            Self::MacOs => "darwin",
            Self::Windows => "win32",
            Self::Linux => "linux",
        }
    }
}

/// Fully buffered CLI outcome for commands without a deferred launch task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserCliOutput {
    stdout: RuntimeString,
    stderr: RuntimeString,
    exit_code: u8,
}

impl BrowserCliOutput {
    pub(crate) fn new(stdout: RuntimeString, stderr: RuntimeString, exit_code: u8) -> Self {
        Self {
            stdout,
            stderr,
            exit_code,
        }
    }

    pub(crate) fn from_stderr(message: &str, exit_code: u8) -> Self {
        Self::new(
            RuntimeString::default(),
            RuntimeString::from(message),
            exit_code,
        )
    }

    /// Exact stdout code units.
    pub fn stdout(&self) -> &RuntimeString {
        &self.stdout
    }

    /// Exact stderr code units.
    pub fn stderr(&self) -> &RuntimeString {
        &self.stderr
    }

    /// Process exit status selected by the utility contract.
    pub fn exit_code(&self) -> u8 {
        self.exit_code
    }
}

/// Per-profile result of `check-extension-installed`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtensionProfileStatus {
    pub profile_directory: RuntimeString,
    pub profile_path: RuntimePath,
    pub preferences_path: RuntimePath,
    pub installed: bool,
    pub enabled: bool,
    pub registered: bool,
    pub state: RuntimeJson,
    pub disable_reasons: Vec<RuntimeString>,
    pub versions: Vec<RuntimeString>,
    pub unpacked_path: Option<RuntimePath>,
}

/// Successful filesystem result of `check-extension-installed`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtensionInstallStatus {
    pub user_data_directory: RuntimePath,
    pub installed: bool,
    pub enabled: bool,
    pub profiles: Vec<ExtensionProfileStatus>,
}

/// Parsed status returned by `check-native-host-manifest`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeHostManifestStatus {
    pub correct: bool,
    pub manifest_path: RuntimePath,
    pub exists: bool,
    pub manifest: Option<RuntimeJson>,
    pub problem: Option<RuntimeString>,
}

/// Exact native-host manifest installed by the Rust compatibility command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeHostManifest {
    pub path: RuntimePath,
}

impl NativeHostManifest {
    pub(crate) fn to_runtime_json(&self) -> RuntimeJson {
        let mut manifest = RuntimeObject::new();
        manifest.insert("name", RuntimeJson::from(NATIVE_HOST_NAME));
        manifest.insert(
            "description",
            RuntimeJson::from("Klaus Browser Connector Native Host"),
        );
        manifest.insert(
            "path",
            RuntimeJson::from(self.path.as_runtime_string().clone()),
        );
        manifest.insert("type", RuntimeJson::from("stdio"));
        manifest.insert(
            "allowed_origins",
            RuntimeJson::Array(vec![RuntimeJson::from(format!(
                "chrome-extension://{EXTENSION_ID}/"
            ))]),
        );
        RuntimeJson::Object(manifest)
    }
}

/// Successful result of `install-native-host-manifest`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstallNativeHostResult {
    pub manifest_path: RuntimePath,
    pub host_path: RuntimePath,
    pub manifest: NativeHostManifest,
    pub registry_key: Option<RuntimeString>,
}

/// One process row returned by `chrome-is-running`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChromeProcessStatus {
    pub pid: u32,
    pub process_name: RuntimeString,
    pub command: RuntimeString,
}

/// Successful (including swallowed-probe-failure) running status.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChromeRunningStatus {
    pub processes: Vec<ChromeProcessStatus>,
    pub platform: BrowserPlatform,
}

/// Target-specific browser record returned by `installed-browsers`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InstalledBrowser {
    MacOs {
        name: RuntimeString,
        bundle_id: RuntimeString,
        path: RuntimePath,
        version: Option<RuntimeString>,
    },
    Desktop {
        name: RuntimeString,
        path: RuntimePath,
        version: Option<RuntimeString>,
    },
}

/// Successful (including swallowed-probe-failure) installed browser result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstalledBrowsersResult {
    pub browsers: Vec<InstalledBrowser>,
    pub platform: BrowserPlatform,
}

/// Exact successful launch/dry-run fields. `command` and `args` are result
/// bytes derived from a sealed process plan, never caller-selected process API.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OpenChromeSuccess {
    pub launched: bool,
    pub command: RuntimeString,
    pub args: Vec<RuntimeString>,
    pub browser_path: RuntimePath,
    pub user_data_directory: RuntimePath,
    pub profile_directory: RuntimeString,
    pub preferences_path: RuntimePath,
}

pub(crate) fn render_open_chrome_success(
    result: &OpenChromeSuccess,
    dry_run: bool,
    json: bool,
) -> BrowserCliOutput {
    if json {
        let mut object = RuntimeObject::new();
        object.insert("ok", RuntimeJson::Bool(true));
        object.insert("launched", RuntimeJson::Bool(result.launched));
        object.insert("command", RuntimeJson::from(result.command.clone()));
        object.insert(
            "args",
            RuntimeJson::Array(result.args.iter().cloned().map(RuntimeJson::from).collect()),
        );
        object.insert(
            "browserPath",
            RuntimeJson::from(result.browser_path.as_runtime_string().clone()),
        );
        object.insert(
            "userDataDirectory",
            RuntimeJson::from(result.user_data_directory.as_runtime_string().clone()),
        );
        object.insert(
            "profileDirectory",
            RuntimeJson::from(result.profile_directory.clone()),
        );
        object.insert(
            "preferencesPath",
            RuntimeJson::from(result.preferences_path.as_runtime_string().clone()),
        );
        return json_result(RuntimeJson::Object(object), 0);
    }

    let mut text = RuntimeText::new();
    text.push_ascii(if dry_run { "dry-run " } else { "opened " });
    text.push_runtime(result.browser_path.as_runtime_string());
    text.push_ascii("\n");
    BrowserCliOutput::new(text.finish(), RuntimeString::default(), 0)
}

/// Render a successful extension status with exact field order and exit law.
pub fn render_check_extension_status(
    status: &ExtensionInstallStatus,
    json: bool,
) -> BrowserCliOutput {
    let exit_code = if status.enabled {
        0
    } else if status.installed {
        1
    } else {
        2
    };
    if json {
        let mut object = RuntimeObject::new();
        object.insert("ok", RuntimeJson::Bool(true));
        object.insert("extensionId", RuntimeJson::from(EXTENSION_ID));
        object.insert(
            "userDataDirectory",
            RuntimeJson::from(status.user_data_directory.as_runtime_string().clone()),
        );
        object.insert("installed", RuntimeJson::Bool(status.installed));
        object.insert("enabled", RuntimeJson::Bool(status.enabled));
        object.insert(
            "profiles",
            RuntimeJson::Array(status.profiles.iter().map(profile_json).collect()),
        );
        return json_result(RuntimeJson::Object(object), exit_code);
    }

    let mut text = RuntimeText::new();
    text.push_ascii(if status.enabled {
        "enabled "
    } else if status.installed {
        "installed-disabled "
    } else {
        "not-installed "
    });
    text.push_ascii(EXTENSION_ID);
    text.push_ascii("\n");
    for profile in &status.profiles {
        text.push_runtime(&profile.profile_directory);
        text.push_ascii(": installed=");
        text.push_bool(profile.installed);
        text.push_ascii(" enabled=");
        text.push_bool(profile.enabled);
        text.push_ascii(" path=");
        text.push_runtime(profile.profile_path.as_runtime_string());
        text.push_ascii("\n");
    }
    BrowserCliOutput::new(text.finish(), RuntimeString::default(), exit_code)
}

/// Render the exact catch branch for `check-extension-installed`.
pub fn render_check_extension_error(error: RuntimeString, json: bool) -> BrowserCliOutput {
    render_error(error, json, 3, Some(CHECK_EXTENSION_HINT))
}

/// Render a native-host manifest status with the source's conditional
/// `manifest` member position.
pub fn render_check_native_host_status(
    status: &NativeHostManifestStatus,
    json: bool,
) -> BrowserCliOutput {
    let exit_code = u8::from(!status.correct);
    if json {
        let mut object = RuntimeObject::new();
        object.insert("ok", RuntimeJson::Bool(true));
        object.insert("correct", RuntimeJson::Bool(status.correct));
        object.insert("hostName", RuntimeJson::from(NATIVE_HOST_NAME));
        object.insert("extensionId", RuntimeJson::from(EXTENSION_ID));
        object.insert(
            "expectedOrigin",
            RuntimeJson::from(format!("chrome-extension://{EXTENSION_ID}/")),
        );
        object.insert(
            "manifestPath",
            RuntimeJson::from(status.manifest_path.as_runtime_string().clone()),
        );
        object.insert("exists", RuntimeJson::Bool(status.exists));
        if let Some(manifest) = &status.manifest {
            object.insert("manifest", manifest.clone());
        }
        object.insert("problem", nullable_string(status.problem.as_ref()));
        return json_result(RuntimeJson::Object(object), exit_code);
    }

    let mut text = RuntimeText::new();
    text.push_ascii(if status.correct {
        "correct "
    } else {
        "incorrect "
    });
    text.push_ascii(NATIVE_HOST_NAME);
    text.push_ascii("\n");
    text.push_runtime(status.manifest_path.as_runtime_string());
    text.push_ascii("\n");
    if let Some(problem) = status.problem.as_ref().filter(|value| !value.is_empty()) {
        text.push_runtime(problem);
        text.push_ascii("\n");
    }
    BrowserCliOutput::new(text.finish(), RuntimeString::default(), exit_code)
}

/// Render the exact catch branch for `check-native-host-manifest`.
pub fn render_check_native_host_error(error: RuntimeString, json: bool) -> BrowserCliOutput {
    render_error(error, json, 2, Some(CHECK_MANIFEST_HINT))
}

/// Render a successful native-host manifest installation/dry-run result.
pub fn render_install_native_host_result(
    result: &InstallNativeHostResult,
    dry_run: bool,
    json: bool,
) -> BrowserCliOutput {
    if json {
        let mut object = RuntimeObject::new();
        object.insert("ok", RuntimeJson::Bool(true));
        object.insert("hostName", RuntimeJson::from(NATIVE_HOST_NAME));
        object.insert("extensionId", RuntimeJson::from(EXTENSION_ID));
        object.insert(
            "manifestPath",
            RuntimeJson::from(result.manifest_path.as_runtime_string().clone()),
        );
        object.insert(
            "hostPath",
            RuntimeJson::from(result.host_path.as_runtime_string().clone()),
        );
        object.insert("manifest", result.manifest.to_runtime_json());
        object.insert("registryKey", nullable_string(result.registry_key.as_ref()));
        return json_result(RuntimeJson::Object(object), 0);
    }

    let mut text = RuntimeText::new();
    text.push_ascii(if dry_run {
        "would install "
    } else {
        "installed "
    });
    text.push_ascii(NATIVE_HOST_NAME);
    text.push_ascii("\n");
    text.push_runtime(result.manifest_path.as_runtime_string());
    text.push_ascii("\n");
    if let Some(registry_key) = &result.registry_key {
        text.push_runtime(registry_key);
        text.push_ascii("\n");
    }
    BrowserCliOutput::new(text.finish(), RuntimeString::default(), 0)
}

/// Render the exact catch branch for `install-native-host-manifest`.
pub fn render_install_native_host_error(error: RuntimeString, json: bool) -> BrowserCliOutput {
    render_error(error, json, 1, Some(INSTALL_MANIFEST_HINT))
}

/// Render the exact running/not-running result and optional check exit law.
pub fn render_chrome_running_status(
    status: &ChromeRunningStatus,
    check: bool,
    json: bool,
) -> BrowserCliOutput {
    let running = !status.processes.is_empty();
    let exit_code = u8::from(check && !running);
    if json {
        let mut object = RuntimeObject::new();
        object.insert("running", RuntimeJson::Bool(running));
        object.insert(
            "processes",
            RuntimeJson::Array(status.processes.iter().map(process_json).collect()),
        );
        object.insert("platform", RuntimeJson::from(status.platform.node_name()));
        return json_result(RuntimeJson::Object(object), exit_code);
    }

    let mut text = RuntimeText::new();
    if running {
        text.push_ascii("running ");
        text.push_usize(status.processes.len());
    } else {
        text.push_ascii("not-running");
    }
    text.push_ascii("\n");
    BrowserCliOutput::new(text.finish(), RuntimeString::default(), exit_code)
}

/// Render the installed-browser listing. Probe failures are represented by an
/// empty `browsers` vector and remain a normal exit-zero result.
pub fn render_installed_browsers_result(
    result: &InstalledBrowsersResult,
    json: bool,
) -> BrowserCliOutput {
    if json {
        let mut object = RuntimeObject::new();
        object.insert("default_browser", RuntimeJson::Null);
        object.insert(
            "browsers",
            RuntimeJson::Array(result.browsers.iter().map(installed_browser_json).collect()),
        );
        object.insert("platform", RuntimeJson::from(result.platform.node_name()));
        return json_result(RuntimeJson::Object(object), 0);
    }

    let mut text = RuntimeText::new();
    if result.browsers.is_empty() {
        text.push_ascii("none\n");
    } else {
        for browser in &result.browsers {
            let (name, path, version) = match browser {
                InstalledBrowser::MacOs {
                    name,
                    path,
                    version,
                    ..
                }
                | InstalledBrowser::Desktop {
                    name,
                    path,
                    version,
                } => (name, path, version),
            };
            text.push_runtime(name);
            text.push_ascii(" ");
            match version.as_ref().filter(|value| !value.is_empty()) {
                Some(version) => text.push_runtime(version),
                None => text.push_ascii("unknown"),
            }
            if !path.as_runtime_string().is_empty() {
                text.push_ascii(" ");
                text.push_runtime(path.as_runtime_string());
            }
            text.push_ascii("\n");
        }
    }
    BrowserCliOutput::new(text.finish(), RuntimeString::default(), 0)
}

/// Render a synchronous open-plan construction failure. Deferred OS-create
/// errors are rendered by the closed launch-resolution plan instead.
pub fn render_open_chrome_error(error: RuntimeString, json: bool) -> BrowserCliOutput {
    render_error(error, json, 1, None)
}

fn profile_json(profile: &ExtensionProfileStatus) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    object.insert(
        "profileDirectory",
        RuntimeJson::from(profile.profile_directory.clone()),
    );
    object.insert(
        "profilePath",
        RuntimeJson::from(profile.profile_path.as_runtime_string().clone()),
    );
    object.insert(
        "preferencesPath",
        RuntimeJson::from(profile.preferences_path.as_runtime_string().clone()),
    );
    object.insert("installed", RuntimeJson::Bool(profile.installed));
    object.insert("enabled", RuntimeJson::Bool(profile.enabled));
    object.insert("registered", RuntimeJson::Bool(profile.registered));
    object.insert("state", profile.state.clone());
    object.insert(
        "disableReasons",
        RuntimeJson::Array(
            profile
                .disable_reasons
                .iter()
                .cloned()
                .map(RuntimeJson::from)
                .collect(),
        ),
    );
    object.insert(
        "versions",
        RuntimeJson::Array(
            profile
                .versions
                .iter()
                .cloned()
                .map(RuntimeJson::from)
                .collect(),
        ),
    );
    object.insert(
        "unpackedPath",
        profile
            .unpacked_path
            .as_ref()
            .map_or(RuntimeJson::Null, |path| {
                RuntimeJson::from(path.as_runtime_string().clone())
            }),
    );
    RuntimeJson::Object(object)
}

fn process_json(process: &ChromeProcessStatus) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    object.insert("pid", RuntimeJson::from(process.pid));
    object.insert(
        "process_name",
        RuntimeJson::from(process.process_name.clone()),
    );
    object.insert("command", RuntimeJson::from(process.command.clone()));
    RuntimeJson::Object(object)
}

fn installed_browser_json(browser: &InstalledBrowser) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    match browser {
        InstalledBrowser::MacOs {
            name,
            bundle_id,
            path,
            version,
        } => {
            object.insert("name", RuntimeJson::from(name.clone()));
            object.insert("bundle_id", RuntimeJson::from(bundle_id.clone()));
            object.insert("path", RuntimeJson::from(path.as_runtime_string().clone()));
            object.insert("version", nullable_string(version.as_ref()));
        }
        InstalledBrowser::Desktop {
            name,
            path,
            version,
        } => {
            object.insert("name", RuntimeJson::from(name.clone()));
            object.insert("path", RuntimeJson::from(path.as_runtime_string().clone()));
            object.insert("version", nullable_string(version.as_ref()));
        }
    }
    RuntimeJson::Object(object)
}

fn nullable_string(value: Option<&RuntimeString>) -> RuntimeJson {
    value
        .cloned()
        .map(RuntimeJson::from)
        .unwrap_or(RuntimeJson::Null)
}

fn render_error(
    error: RuntimeString,
    json: bool,
    exit_code: u8,
    text_hint: Option<&str>,
) -> BrowserCliOutput {
    if json {
        let mut object = RuntimeObject::new();
        object.insert("ok", RuntimeJson::Bool(false));
        object.insert("error", RuntimeJson::from(error));
        return json_result(RuntimeJson::Object(object), exit_code);
    }
    let mut stderr = RuntimeText::new();
    stderr.push_runtime(&error);
    stderr.push_ascii("\n");
    if let Some(hint) = text_hint {
        stderr.push_ascii(hint);
        stderr.push_ascii("\n");
    }
    BrowserCliOutput::new(RuntimeString::default(), stderr.finish(), exit_code)
}

fn json_result(value: RuntimeJson, exit_code: u8) -> BrowserCliOutput {
    let json = runtime_json::stringify_pretty(&value).concat(&RuntimeString::from("\n"));
    BrowserCliOutput::new(json, RuntimeString::default(), exit_code)
}

struct RuntimeText {
    units: Vec<u16>,
}

impl RuntimeText {
    fn new() -> Self {
        Self { units: Vec::new() }
    }

    fn push_ascii(&mut self, value: &str) {
        debug_assert!(value.is_ascii());
        self.units.extend(value.bytes().map(u16::from));
    }

    fn push_runtime(&mut self, value: &RuntimeString) {
        self.units.extend_from_slice(value.as_utf16());
    }

    fn push_bool(&mut self, value: bool) {
        self.push_ascii(if value { "true" } else { "false" });
    }

    fn push_usize(&mut self, value: usize) {
        self.push_ascii(&value.to_string());
    }

    fn finish(self) -> RuntimeString {
        RuntimeString::from_utf16(self.units)
    }
}
