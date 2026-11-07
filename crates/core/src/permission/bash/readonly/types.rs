//! Shared types for the read-only command classifier (Stage B of the Bash
//! permission work).
//!
//! The data tables (which command flags are read-only) encode facts; the
//! validation algorithm evaluates those tables deterministically. This
//! classifier answers a single question per subcommand — "is this command
//! read-only?" — and is wired into the decision engine's
//! `is_read_only_subcommand` hook so an unruled read-only command auto-allows.

use std::collections::HashMap;

/// The kind of argument a flag accepts (drives
/// [`validate_flag_argument`](super::validate::validate_flag_argument)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FlagArgType {
    /// No argument (`--color`, `-n`).
    None,
    /// Integer argument (`--context=3`).
    Number,
    /// Any string argument (`--relative=path`).
    String,
    /// Single character (delimiter).
    Char,
    /// Literal `{}` only.
    Braces,
    /// Literal `EOF` only.
    Eof,
}

/// Map from flag name to the kind of argument it accepts.
pub type SafeFlags = HashMap<&'static str, FlagArgType>;

/// Returns true if the command is dangerous, false if safe. `args` is the
/// tokens AFTER the command name (e.g. after "git branch").
pub type DangerousCommandCallback = fn(raw_command: &str, args: &[String]) -> bool;

/// Config for an external command (git/gh/docker/rg/pyright subcommands).
#[derive(Debug, Clone, Default)]
pub struct ExternalCommandConfig {
    pub safe_flags: SafeFlags,
    /// Optional custom danger callback (true = dangerous).
    pub additional_command_is_dangerous_callback: Option<DangerousCommandCallback>,
    /// When `Some(false)`, the tool does NOT respect POSIX `--` end-of-options;
    /// `validate_flags` keeps checking flags after `--` instead of breaking.
    /// Default: respected.
    pub respects_double_dash: Option<bool>,
}

/// Config for a COMMAND_ALLOWLIST entry (bash builtins/tools); adds an
/// optional regex.
#[derive(Debug, Clone, Default)]
pub struct CommandConfig {
    pub safe_flags: SafeFlags,
    /// Optional regex used for additional validation beyond flag parsing.
    pub regex: Option<fancy_regex::Regex>,
    /// Optional custom danger callback (true = dangerous), used with
    /// safe-flags-based validation.
    pub additional_command_is_dangerous_callback: Option<DangerousCommandCallback>,
    /// When `Some(false)`, the tool does NOT respect POSIX `--` end-of-options.
    /// Default: respected.
    pub respects_double_dash: Option<bool>,
}

impl From<ExternalCommandConfig> for CommandConfig {
    fn from(config: ExternalCommandConfig) -> Self {
        CommandConfig {
            safe_flags: config.safe_flags,
            regex: None,
            additional_command_is_dangerous_callback: config
                .additional_command_is_dangerous_callback,
            respects_double_dash: config.respects_double_dash,
        }
    }
}

/// The structural view shared by [`ExternalCommandConfig`] and
/// [`CommandConfig`] that flag validation operates on.
pub trait FlagValidationConfig {
    fn safe_flags(&self) -> &SafeFlags;
    fn respects_double_dash(&self) -> Option<bool>;
}

impl FlagValidationConfig for ExternalCommandConfig {
    fn safe_flags(&self) -> &SafeFlags {
        &self.safe_flags
    }
    fn respects_double_dash(&self) -> Option<bool> {
        self.respects_double_dash
    }
}

impl FlagValidationConfig for CommandConfig {
    fn safe_flags(&self) -> &SafeFlags {
        &self.safe_flags
    }
    fn respects_double_dash(&self) -> Option<bool> {
        self.respects_double_dash
    }
}
