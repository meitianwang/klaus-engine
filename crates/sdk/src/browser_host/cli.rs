use klaus_agent_core::runtime_values::RuntimeString;

use super::result::BrowserCliOutput;

const CHROME_RUNNING_USAGE: &str = "Usage: scripts/chrome-is-running.mjs [--check] [--json]\n";
const INSTALLED_BROWSERS_USAGE: &str = "Usage: scripts/installed-browsers.mjs [--json]\n";
const OPEN_CHROME_USAGE: &str = concat!(
    "Usage: scripts/open-chrome-window.mjs [--dry-run] [--json] [url]\n",
    "Optional profile-root override: KLAUS_CHROME_USER_DATA_DIR=/path/to/chrome-root\n",
    "Optional preferences-file override: KLAUS_CHROME_PREFERENCES_PATH=/path/to/Profile/Preferences\n",
);

/// One of the six frozen browser utility subcommands.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum BrowserUtilitySubcommand {
    CheckExtensionInstalled,
    CheckNativeHostManifest,
    InstallNativeHostManifest,
    ChromeIsRunning,
    InstalledBrowsers,
    OpenChromeWindow,
}

/// Identify an exact subcommand without defining an unapproved top-level CLI
/// error contract for missing or unknown commands.
pub fn identify_subcommand(value: &RuntimeString) -> Option<BrowserUtilitySubcommand> {
    match_ascii(value, "check-extension-installed")
        .then_some(BrowserUtilitySubcommand::CheckExtensionInstalled)
        .or_else(|| {
            match_ascii(value, "check-native-host-manifest")
                .then_some(BrowserUtilitySubcommand::CheckNativeHostManifest)
        })
        .or_else(|| {
            match_ascii(value, "install-native-host-manifest")
                .then_some(BrowserUtilitySubcommand::InstallNativeHostManifest)
        })
        .or_else(|| {
            match_ascii(value, "chrome-is-running")
                .then_some(BrowserUtilitySubcommand::ChromeIsRunning)
        })
        .or_else(|| {
            match_ascii(value, "installed-browsers")
                .then_some(BrowserUtilitySubcommand::InstalledBrowsers)
        })
        .or_else(|| {
            match_ascii(value, "open-chrome-window")
                .then_some(BrowserUtilitySubcommand::OpenChromeWindow)
        })
}

/// Parsed command inputs. Unknown-argument behavior is fixed independently for
/// each variant rather than normalized by a generic argument parser.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrowserUtilityInvocation {
    CheckExtensionInstalled {
        json: bool,
    },
    CheckNativeHostManifest {
        json: bool,
    },
    InstallNativeHostManifest {
        dry_run: bool,
        json: bool,
    },
    ChromeIsRunning {
        check: bool,
        json: bool,
    },
    InstalledBrowsers {
        json: bool,
    },
    OpenChromeWindow {
        dry_run: bool,
        json: bool,
        url: RuntimeString,
    },
}

/// Parsing either yields a command invocation or an immediate usage/help
/// result. Immediate output is already stream- and exit-code-specific.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrowserUtilityParse {
    Invoke(BrowserUtilityInvocation),
    Immediate(BrowserCliOutput),
}

/// Parse one already-identified browser subcommand exactly like its locked
/// Node reference. Arguments remain exact UTF-16 values.
pub fn parse_browser_utility(
    subcommand: BrowserUtilitySubcommand,
    arguments: &[RuntimeString],
) -> BrowserUtilityParse {
    match subcommand {
        BrowserUtilitySubcommand::CheckExtensionInstalled => {
            BrowserUtilityParse::Invoke(BrowserUtilityInvocation::CheckExtensionInstalled {
                json: contains(arguments, "--json"),
            })
        }
        BrowserUtilitySubcommand::CheckNativeHostManifest => {
            BrowserUtilityParse::Invoke(BrowserUtilityInvocation::CheckNativeHostManifest {
                json: contains(arguments, "--json"),
            })
        }
        BrowserUtilitySubcommand::InstallNativeHostManifest => {
            BrowserUtilityParse::Invoke(BrowserUtilityInvocation::InstallNativeHostManifest {
                dry_run: contains(arguments, "--dry-run"),
                json: contains(arguments, "--json"),
            })
        }
        BrowserUtilitySubcommand::ChromeIsRunning => parse_chrome_running(arguments),
        BrowserUtilitySubcommand::InstalledBrowsers => parse_installed_browsers(arguments),
        BrowserUtilitySubcommand::OpenChromeWindow => parse_open_chrome(arguments),
    }
}

fn parse_chrome_running(arguments: &[RuntimeString]) -> BrowserUtilityParse {
    if contains(arguments, "-h") || contains(arguments, "--help") {
        return BrowserUtilityParse::Immediate(BrowserCliOutput::from_stderr(
            CHROME_RUNNING_USAGE,
            0,
        ));
    }
    if arguments
        .iter()
        .any(|argument| !match_ascii(argument, "--check") && !match_ascii(argument, "--json"))
    {
        return BrowserUtilityParse::Immediate(BrowserCliOutput::from_stderr(
            CHROME_RUNNING_USAGE,
            2,
        ));
    }
    BrowserUtilityParse::Invoke(BrowserUtilityInvocation::ChromeIsRunning {
        check: contains(arguments, "--check"),
        json: contains(arguments, "--json"),
    })
}

fn parse_installed_browsers(arguments: &[RuntimeString]) -> BrowserUtilityParse {
    if contains(arguments, "-h") || contains(arguments, "--help") {
        return BrowserUtilityParse::Immediate(BrowserCliOutput::from_stderr(
            INSTALLED_BROWSERS_USAGE,
            0,
        ));
    }
    if arguments
        .iter()
        .any(|argument| !match_ascii(argument, "--json"))
    {
        return BrowserUtilityParse::Immediate(BrowserCliOutput::from_stderr(
            INSTALLED_BROWSERS_USAGE,
            2,
        ));
    }
    BrowserUtilityParse::Invoke(BrowserUtilityInvocation::InstalledBrowsers {
        json: contains(arguments, "--json"),
    })
}

fn parse_open_chrome(arguments: &[RuntimeString]) -> BrowserUtilityParse {
    let mut positional = Vec::new();
    let mut help = false;
    let mut dry_run = false;
    let mut json = false;
    let mut unsupported_flag = false;

    for argument in arguments {
        if starts_with_ascii(argument, "--") {
            if match_ascii(argument, "--help") {
                help = true;
            } else if match_ascii(argument, "--dry-run") {
                dry_run = true;
            } else if match_ascii(argument, "--json") {
                json = true;
            } else {
                unsupported_flag = true;
            }
        } else {
            positional.push(argument.clone());
        }
    }

    if help {
        return BrowserUtilityParse::Immediate(BrowserCliOutput::from_stderr(OPEN_CHROME_USAGE, 0));
    }
    if unsupported_flag || positional.len() > 1 {
        return BrowserUtilityParse::Immediate(BrowserCliOutput::from_stderr(OPEN_CHROME_USAGE, 2));
    }

    BrowserUtilityParse::Invoke(BrowserUtilityInvocation::OpenChromeWindow {
        dry_run,
        json,
        url: positional
            .pop()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| RuntimeString::from("about:blank")),
    })
}

fn contains(arguments: &[RuntimeString], expected: &str) -> bool {
    arguments
        .iter()
        .any(|argument| match_ascii(argument, expected))
}

fn match_ascii(value: &RuntimeString, expected: &str) -> bool {
    value
        .as_utf16()
        .iter()
        .copied()
        .eq(expected.bytes().map(u16::from))
}

fn starts_with_ascii(value: &RuntimeString, expected: &str) -> bool {
    let expected = expected.bytes().map(u16::from).collect::<Vec<_>>();
    value.as_utf16().starts_with(&expected)
}
