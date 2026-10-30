//! Read-only allowlist tables for external tools (docker, ripgrep, pyright)
//! and the cross-shell read-only command list.

use std::collections::HashMap;
use std::sync::LazyLock;

use super::types::{ExternalCommandConfig, FlagArgType};

/// Build a flag map from `(flag, argument type)` pairs.
fn flags(entries: &[(&'static str, FlagArgType)]) -> HashMap<&'static str, FlagArgType> {
    entries.iter().copied().collect()
}

// ---------------------------------------------------------------------------
// DOCKER_READ_ONLY_COMMANDS — docker inspect/logs read-only commands
// ---------------------------------------------------------------------------

pub static DOCKER_READ_ONLY_COMMANDS: LazyLock<Vec<(&'static str, ExternalCommandConfig)>> =
    LazyLock::new(|| {
        vec![
            (
                "docker logs",
                ExternalCommandConfig {
                    safe_flags: flags(&[
                        ("--follow", FlagArgType::None),
                        ("-f", FlagArgType::None),
                        ("--tail", FlagArgType::String),
                        ("-n", FlagArgType::String),
                        ("--timestamps", FlagArgType::None),
                        ("-t", FlagArgType::None),
                        ("--since", FlagArgType::String),
                        ("--until", FlagArgType::String),
                        ("--details", FlagArgType::None),
                    ]),
                    additional_command_is_dangerous_callback: None,
                    respects_double_dash: None,
                },
            ),
            (
                "docker inspect",
                ExternalCommandConfig {
                    safe_flags: flags(&[
                        ("--format", FlagArgType::String),
                        ("-f", FlagArgType::String),
                        ("--type", FlagArgType::String),
                        ("--size", FlagArgType::None),
                        ("-s", FlagArgType::None),
                    ]),
                    additional_command_is_dangerous_callback: None,
                    respects_double_dash: None,
                },
            ),
        ]
    });

// ---------------------------------------------------------------------------
// RIPGREP_READ_ONLY_COMMANDS — rg (ripgrep) read-only search
// ---------------------------------------------------------------------------

pub static RIPGREP_READ_ONLY_COMMANDS: LazyLock<Vec<(&'static str, ExternalCommandConfig)>> =
    LazyLock::new(|| {
        vec![(
            "rg",
            ExternalCommandConfig {
                safe_flags: flags(&[
                    // Pattern flags
                    ("-e", FlagArgType::String), // Pattern to search for
                    ("--regexp", FlagArgType::String),
                    ("-f", FlagArgType::String), // Read patterns from file
                    // Common search options
                    ("-i", FlagArgType::None), // Case insensitive
                    ("--ignore-case", FlagArgType::None),
                    ("-S", FlagArgType::None), // Smart case
                    ("--smart-case", FlagArgType::None),
                    ("-F", FlagArgType::None), // Fixed strings
                    ("--fixed-strings", FlagArgType::None),
                    ("-w", FlagArgType::None), // Word regexp
                    ("--word-regexp", FlagArgType::None),
                    ("-v", FlagArgType::None), // Invert match
                    ("--invert-match", FlagArgType::None),
                    // Output options
                    ("-c", FlagArgType::None), // Count matches
                    ("--count", FlagArgType::None),
                    ("-l", FlagArgType::None), // Files with matches
                    ("--files-with-matches", FlagArgType::None),
                    ("--files-without-match", FlagArgType::None),
                    ("-n", FlagArgType::None), // Line number
                    ("--line-number", FlagArgType::None),
                    ("-o", FlagArgType::None), // Only matching
                    ("--only-matching", FlagArgType::None),
                    ("-A", FlagArgType::Number), // After context
                    ("--after-context", FlagArgType::Number),
                    ("-B", FlagArgType::Number), // Before context
                    ("--before-context", FlagArgType::Number),
                    ("-C", FlagArgType::Number), // Context
                    ("--context", FlagArgType::Number),
                    ("-H", FlagArgType::None), // With filename
                    ("-h", FlagArgType::None), // No filename
                    ("--heading", FlagArgType::None),
                    ("--no-heading", FlagArgType::None),
                    ("-q", FlagArgType::None), // Quiet
                    ("--quiet", FlagArgType::None),
                    ("--column", FlagArgType::None),
                    // File filtering
                    ("-g", FlagArgType::String), // Glob
                    ("--glob", FlagArgType::String),
                    ("-t", FlagArgType::String), // Type
                    ("--type", FlagArgType::String),
                    ("-T", FlagArgType::String), // Type not
                    ("--type-not", FlagArgType::String),
                    ("--type-list", FlagArgType::None),
                    ("--hidden", FlagArgType::None),
                    ("--no-ignore", FlagArgType::None),
                    ("-u", FlagArgType::None), // Unrestricted
                    // Common options
                    ("-m", FlagArgType::Number), // Max count per file
                    ("--max-count", FlagArgType::Number),
                    ("-d", FlagArgType::Number), // Max depth
                    ("--max-depth", FlagArgType::Number),
                    ("-a", FlagArgType::None), // Text (search binary files)
                    ("--text", FlagArgType::None),
                    ("-z", FlagArgType::None), // Search zip
                    ("-L", FlagArgType::None), // Follow symlinks
                    ("--follow", FlagArgType::None),
                    // Display options
                    ("--color", FlagArgType::String),
                    ("--json", FlagArgType::None),
                    ("--stats", FlagArgType::None),
                    // Help and version
                    ("--help", FlagArgType::None),
                    ("--version", FlagArgType::None),
                    ("--debug", FlagArgType::None),
                    // Special argument separator
                    ("--", FlagArgType::None),
                ]),
                additional_command_is_dangerous_callback: None,
                respects_double_dash: None,
            },
        )]
    });

// ---------------------------------------------------------------------------
// PYRIGHT_READ_ONLY_COMMANDS — pyright static type checker
// ---------------------------------------------------------------------------

fn pyright_is_dangerous_callback(_raw_command: &str, args: &[String]) -> bool {
    // Check if --watch or -w appears as a standalone token (flag)
    args.iter().any(|t| t == "--watch" || t == "-w")
}

pub static PYRIGHT_READ_ONLY_COMMANDS: LazyLock<Vec<(&'static str, ExternalCommandConfig)>> =
    LazyLock::new(|| {
        vec![(
            "pyright",
            ExternalCommandConfig {
                safe_flags: flags(&[
                    ("--outputjson", FlagArgType::None),
                    ("--project", FlagArgType::String),
                    ("-p", FlagArgType::String),
                    ("--pythonversion", FlagArgType::String),
                    ("--pythonplatform", FlagArgType::String),
                    ("--typeshedpath", FlagArgType::String),
                    ("--venvpath", FlagArgType::String),
                    ("--level", FlagArgType::String),
                    ("--stats", FlagArgType::None),
                    ("--verbose", FlagArgType::None),
                    ("--version", FlagArgType::None),
                    ("--dependencies", FlagArgType::None),
                    ("--warnings", FlagArgType::None),
                ]),
                additional_command_is_dangerous_callback: Some(pyright_is_dangerous_callback),
                // pyright treats `--` as a file path, not end-of-options
                respects_double_dash: Some(false),
            },
        )]
    });

// ---------------------------------------------------------------------------
// EXTERNAL_READONLY_COMMANDS — cross-shell read-only commands
// Only commands that work identically in bash and PowerShell on Windows.
// Unix-specific commands (cat, head, wc, etc.) belong in the Bash tool's
// READONLY_COMMANDS.
// ---------------------------------------------------------------------------

pub const EXTERNAL_READONLY_COMMANDS: &[&str] = &[
    // Cross-platform external tools that work the same in bash and PowerShell on Windows
    "docker ps",
    "docker images",
];

#[cfg(test)]
mod tests {
    use super::*;

    fn args(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    fn get<'a>(
        table: &'a [(&'static str, ExternalCommandConfig)],
        key: &str,
    ) -> &'a ExternalCommandConfig {
        &table
            .iter()
            .find(|(k, _)| *k == key)
            .expect("entry present")
            .1
    }

    #[test]
    fn docker_tables() {
        assert_eq!(DOCKER_READ_ONLY_COMMANDS.len(), 2);
        let logs = get(&DOCKER_READ_ONLY_COMMANDS, "docker logs");
        assert_eq!(logs.safe_flags.len(), 9);
        // For `docker logs`, -f is --follow (no argument) and --tail takes a string.
        assert!(matches!(logs.safe_flags.get("-f"), Some(FlagArgType::None)));
        assert!(matches!(
            logs.safe_flags.get("--tail"),
            Some(FlagArgType::String)
        ));
        assert!(logs.additional_command_is_dangerous_callback.is_none());
        assert!(logs.respects_double_dash.is_none());

        let inspect = get(&DOCKER_READ_ONLY_COMMANDS, "docker inspect");
        assert_eq!(inspect.safe_flags.len(), 5);
        // For `docker inspect`, -f is --format and takes a string.
        assert!(matches!(
            inspect.safe_flags.get("-f"),
            Some(FlagArgType::String)
        ));
        assert!(matches!(
            inspect.safe_flags.get("--size"),
            Some(FlagArgType::None)
        ));
        assert!(inspect.additional_command_is_dangerous_callback.is_none());
    }

    #[test]
    fn ripgrep_table() {
        assert_eq!(RIPGREP_READ_ONLY_COMMANDS.len(), 1);
        let rg = get(&RIPGREP_READ_ONLY_COMMANDS, "rg");
        // `--` is listed as a safe no-argument separator.
        assert!(matches!(rg.safe_flags.get("--"), Some(FlagArgType::None)));
        assert!(matches!(rg.safe_flags.get("-A"), Some(FlagArgType::Number)));
        assert!(matches!(rg.safe_flags.get("-e"), Some(FlagArgType::String)));
        // rg --json takes no argument (unlike gh --json).
        assert!(matches!(
            rg.safe_flags.get("--json"),
            Some(FlagArgType::None)
        ));
        // --pre executes a program and must not be allowlisted.
        assert!(!rg.safe_flags.contains_key("--pre"));
        assert!(rg.additional_command_is_dangerous_callback.is_none());
        assert!(rg.respects_double_dash.is_none());
    }

    #[test]
    fn pyright_table() {
        assert_eq!(PYRIGHT_READ_ONLY_COMMANDS.len(), 1);
        let py = get(&PYRIGHT_READ_ONLY_COMMANDS, "pyright");
        // pyright treats `--` as a file path, not end-of-options.
        assert_eq!(py.respects_double_dash, Some(false));
        assert_eq!(py.safe_flags.len(), 13);
        assert!(matches!(
            py.safe_flags.get("--outputjson"),
            Some(FlagArgType::None)
        ));
        assert!(matches!(
            py.safe_flags.get("--project"),
            Some(FlagArgType::String)
        ));
    }

    #[test]
    fn pyright_watch_mode_is_dangerous() {
        let py = get(&PYRIGHT_READ_ONLY_COMMANDS, "pyright");
        let cb = py.additional_command_is_dangerous_callback.unwrap();
        assert!(cb("pyright --watch", &args(&["--watch"])));
        assert!(cb("pyright file.py -w", &args(&["file.py", "-w"])));
        // Only standalone tokens count.
        assert!(!cb("pyright --watch=x", &args(&["--watch=x"])));
        assert!(!cb("pyright -w2", &args(&["-w2"])));
        assert!(!cb("pyright", &args(&[])));
    }

    #[test]
    fn external_readonly_commands_list() {
        assert_eq!(EXTERNAL_READONLY_COMMANDS, ["docker ps", "docker images"]);
    }
}
