//! Flag-validation engine + UNC-path guard for the read-only classifier
//! ([`validate_flags`] / [`validate_flag_argument`] /
//! [`contains_vulnerable_unc_path`]). Every security comment below documents a
//! specific parser-differential / bypass that the code deliberately closes —
//! the behavior is load-bearing, not stylistic.

use std::sync::LazyLock;

use regex::Regex;

use super::types::{FlagArgType, FlagValidationConfig};

/// True only on Windows (UNC paths only matter there).
fn is_windows() -> bool {
    cfg!(windows)
}

// 1. General UNC paths with backslashes: \\server, \\server\share, \\server@port\share.
static BACKSLASH_UNC_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\\\\[^\s\\/]+(?:@(?:[0-9]+|ssl))?(?:[\\/]|$|\s)").unwrap());

// 2. Forward-slash UNC paths (//server/share), excluding URLs (https://) via lookbehind.
static FORWARD_SLASH_UNC_PATTERN: LazyLock<fancy_regex::Regex> = LazyLock::new(|| {
    fancy_regex::Regex::new(r"(?i)(?<!:)//[^\s\\/]+(?:@(?:[0-9]+|ssl))?(?:[\\/]|$|\s)").unwrap()
});

// 3. Mixed-separator UNC: /\\server (2+ backslashes after / — a single one just escapes).
static MIXED_SLASH_UNC_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"/\\{2,}[^\s\\/]").unwrap());

// 4. Mixed-separator UNC: \\/server.
static REVERSE_MIXED_SLASH_UNC_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\\{2,}/[^\s\\/]").unwrap());

// 5. WebDAV SSL/port patterns: \\server@SSL@8443\path, \\server@8443@SSL\path.
static WEBDAV_SSL_PORT_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)@SSL@[0-9]+").unwrap());
static WEBDAV_PORT_SSL_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)@[0-9]+@SSL").unwrap());

// 6. DavWWWRoot marker (Windows WebDAV redirector).
static DAV_WWW_ROOT_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)DavWWWRoot").unwrap());

// 7. UNC with IPv4 address.
static BACKSLASH_IPV4_UNC_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^\\\\([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})[\\/]").unwrap()
});
static FORWARD_SLASH_IPV4_UNC_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^//([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})[\\/]").unwrap()
});

// 8. UNC with bracketed IPv6 address.
static BACKSLASH_IPV6_UNC_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\\\\(\[[0-9a-fA-F:]+\])[\\/]").unwrap());
static FORWARD_SLASH_IPV6_UNC_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^//(\[[0-9a-fA-F:]+\])[\\/]").unwrap());

/// Detect a UNC path that could trigger network requests (NTLM/Kerberos
/// credential leak, WebDAV attacks). Windows-only.
pub fn contains_vulnerable_unc_path(path_or_command: &str) -> bool {
    if !is_windows() {
        return false;
    }

    // 1. General UNC paths with backslashes: \\server, \\server\share, \\server@port\share.
    if BACKSLASH_UNC_PATTERN.is_match(path_or_command) {
        return true;
    }

    // 2. Forward-slash UNC paths (//server/share), excluding URLs (https://) via lookbehind.
    if FORWARD_SLASH_UNC_PATTERN
        .is_match(path_or_command)
        .unwrap_or(false)
    {
        return true;
    }

    // 3. Mixed-separator UNC: /\\server (2+ backslashes after / — a single one just escapes).
    if MIXED_SLASH_UNC_PATTERN.is_match(path_or_command) {
        return true;
    }

    // 4. Mixed-separator UNC: \\/server.
    if REVERSE_MIXED_SLASH_UNC_PATTERN.is_match(path_or_command) {
        return true;
    }

    // 5. WebDAV SSL/port patterns: \\server@SSL@8443\path, \\server@8443@SSL\path.
    if WEBDAV_SSL_PORT_PATTERN.is_match(path_or_command)
        || WEBDAV_PORT_SSL_PATTERN.is_match(path_or_command)
    {
        return true;
    }

    // 6. DavWWWRoot marker (Windows WebDAV redirector).
    if DAV_WWW_ROOT_PATTERN.is_match(path_or_command) {
        return true;
    }

    // 7. UNC with IPv4 address.
    if BACKSLASH_IPV4_UNC_PATTERN.is_match(path_or_command)
        || FORWARD_SLASH_IPV4_UNC_PATTERN.is_match(path_or_command)
    {
        return true;
    }

    // 8. UNC with bracketed IPv6 address.
    if BACKSLASH_IPV6_UNC_PATTERN.is_match(path_or_command)
        || FORWARD_SLASH_IPV6_UNC_PATTERN.is_match(path_or_command)
    {
        return true;
    }

    false
}

/// Valid flag name: a hyphen followed by a letter/digit/underscore/hyphen.
pub static FLAG_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^-[a-zA-Z0-9_-]").unwrap());

/// Git's `--sort` reverse-sort exception: a leading `-` followed by a letter.
static GIT_REVERSE_SORT_VALUE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^-[a-zA-Z]").unwrap());

/// True if `value` consists solely of ASCII digits (and is non-empty).
fn is_all_digits(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit())
}

/// Validate a flag's argument value against its declared type.
pub fn validate_flag_argument(value: &str, arg_type: FlagArgType) -> bool {
    match arg_type {
        FlagArgType::None => false, // Should not have been called for the no-arg type
        FlagArgType::Number => is_all_digits(value),
        FlagArgType::String => true, // Any string including empty is valid
        // Single character (one UTF-16 code unit).
        FlagArgType::Char => value.encode_utf16().count() == 1,
        FlagArgType::Braces => value == "{}",
        FlagArgType::Eof => value == "EOF",
    }
}

/// Options for [`validate_flags`].
#[derive(Debug, Clone, Copy, Default)]
pub struct ValidateFlagsOptions<'a> {
    pub command_name: Option<&'a str>,
    pub raw_command: Option<&'a str>,
    pub xargs_target_commands: Option<&'a [&'a str]>,
}

/// Validate the flags/args portion of a tokenized command against a config —
/// the flag-walking loop with every documented anti-bypass mitigation (xargs
/// target detection, `--` handling per `respects_double_dash`, `-E=`
/// empty-inline parser differential, bundled-flag arg-taking rejection,
/// attached numeric for grep/rg, git numeric shorthand, string-arg
/// leading-`-` defense).
///
/// * `tokens` — pre-tokenized args
/// * `start_index` — where to start (after the command tokens)
/// * `config` — the safe-flags config
pub fn validate_flags(
    tokens: &[String],
    start_index: usize,
    config: &dyn FlagValidationConfig,
    options: Option<&ValidateFlagsOptions<'_>>,
) -> bool {
    let command_name = options.and_then(|o| o.command_name);
    let mut i = start_index;

    while i < tokens.len() {
        let mut token = tokens[i].as_str();
        if token.is_empty() {
            i += 1;
            continue;
        }

        // xargs: once we reach the target command, stop validating flags.
        if let Some(targets) = options.and_then(|o| o.xargs_target_commands) {
            if command_name == Some("xargs") && (!token.starts_with('-') || token == "--") {
                if token == "--" && i + 1 < tokens.len() {
                    i += 1;
                    token = tokens[i].as_str();
                }
                if !token.is_empty() && targets.contains(&token) {
                    break;
                }
                return false;
            }
        }

        if token == "--" {
            // SECURITY: only break if the tool respects POSIX `--` (default). Tools like
            // pyright don't — they treat `--` as a file path and keep parsing flags, so
            // breaking here would let `pyright -- --createstub os` auto-approve a
            // file-write flag.
            if config.respects_double_dash() != Some(false) {
                break; // Everything after -- is arguments
            }
            i += 1;
            continue;
        }

        if token.starts_with('-') && token.len() > 1 && FLAG_PATTERN.is_match(token) {
            // SECURITY: track whether the token CONTAINS `=` separately from whether the
            // value is non-empty. `-E=` has has_equals=true but inline_value="" (empty).
            // GNU getopt sees `-E=` as `-E` with attached arg `=`. Without has_equals, an
            // emptiness check would consume the next token — a parser differential
            // (`xargs -E= EOF echo foo` → RCE). When has_equals, use inline_value (even
            // empty) as the provided arg; don't consume the next token.
            let has_equals = token.contains('=');
            let mut parts = token.split('=');
            let flag = parts.next().unwrap_or("");
            let inline_value = parts.collect::<Vec<_>>().join("=");

            if flag.is_empty() {
                return false;
            }

            let flag_arg_type = config.safe_flags().get(flag).copied();

            let Some(flag_arg_type) = flag_arg_type else {
                // git supports -<number> as shorthand for -n <number>.
                if command_name == Some("git") && flag.len() > 1 && is_all_digits(&flag[1..]) {
                    i += 1;
                    continue;
                }

                // Flags with directly-attached numeric args (-A20, -B10) — grep/rg only.
                if (command_name == Some("grep") || command_name == Some("rg"))
                    && flag.starts_with('-')
                    && !flag.starts_with("--")
                    && flag.chars().count() > 2
                {
                    let potential_flag: String = flag.chars().take(2).collect();
                    let potential_value: String = flag.chars().skip(2).collect();

                    if let Some(attached_type) =
                        config.safe_flags().get(potential_flag.as_str()).copied()
                    {
                        if is_all_digits(&potential_value)
                            && (attached_type == FlagArgType::Number
                                || attached_type == FlagArgType::String)
                        {
                            if validate_flag_argument(&potential_value, attached_type) {
                                i += 1;
                                continue;
                            }
                            return false; // Invalid attached value
                        }
                    }
                }

                // Combined single-letter flags like -nr.
                // SECURITY: ALL bundled flags must be arg-type none. An arg-taking flag in
                // a bundle consumes the NEXT argv element in GNU getopt
                // (`xargs -rI echo sh -c id` → RCE), which a naive existence-check doesn't
                // model. Reject the whole bundle if any flag takes an arg.
                if flag.starts_with('-') && !flag.starts_with("--") && flag.chars().count() > 2 {
                    for c in flag.chars().skip(1) {
                        let single_flag = format!("-{c}");
                        match config.safe_flags().get(single_flag.as_str()) {
                            None => return false, // One of the combined flags is not safe
                            Some(FlagArgType::None) => {}
                            // Arg-taking flag in a bundle — cannot safely validate
                            Some(_) => return false,
                        }
                    }
                    i += 1;
                    continue;
                }
                return false; // Unknown flag
            };

            // Validate flag arguments.
            if flag_arg_type == FlagArgType::None {
                // SECURITY: has_equals covers `-FLAG=` (empty inline). Without it, `-FLAG=`
                // with the no-arg type would pass (empty inline_value looks like no value).
                if has_equals {
                    return false; // Flag should not have a value
                }
                i += 1;
            } else {
                let arg_value: String;
                // SECURITY: use has_equals (not inline_value emptiness). `-E=` must NOT
                // consume the next token — the user explicitly provided an empty value.
                if has_equals {
                    arg_value = inline_value;
                    i += 1;
                } else {
                    let next = tokens.get(i + 1).map(String::as_str);
                    let next_is_flag = next.is_some_and(|n| {
                        n.starts_with('-') && n.len() > 1 && FLAG_PATTERN.is_match(n)
                    });
                    if i + 1 >= tokens.len() || next_is_flag {
                        return false; // Missing required argument
                    }
                    arg_value = next.unwrap_or("").to_string();
                    i += 2;
                }

                // Defense-in-depth: for string args, reject values starting with '-'
                // (type-confusion flag injection). Exception: git's --sort allows a
                // leading '-' for reverse sorting.
                if flag_arg_type == FlagArgType::String && arg_value.starts_with('-') {
                    if flag == "--sort"
                        && command_name == Some("git")
                        && GIT_REVERSE_SORT_VALUE.is_match(&arg_value)
                    {
                        // reverse sort (e.g. -refname) — allowed
                    } else {
                        return false;
                    }
                }

                if !validate_flag_argument(&arg_value, flag_arg_type) {
                    return false;
                }
            }
        } else {
            // Non-flag argument (revision specs, file paths, etc.) — allowed.
            i += 1;
        }
    }

    true
}
