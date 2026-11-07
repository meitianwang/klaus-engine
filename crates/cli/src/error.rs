//! The CLI's single error type.
//!
//! Everything the CLI can fail at collapses into one enum because the terminal
//! only ever needs two things from a failure: a line to print and a process exit
//! code. The variants exist to split those two decisions — a wrong invocation is
//! the user's to fix and exits `2` like every other argument error, while an
//! engine or I/O failure exits `1`.

use std::fmt;

use klaus_agent_core::runtime_values::RuntimeCodecError;
use klaus_agent_server::{ClientError as FacadeError, ConnectError};

pub type CliResult<T> = Result<T, CliError>;

/// The exit code for a bad invocation. Matches clap's own code for a parse
/// error, so "you typed it wrong" is one code whether clap or a command handler
/// noticed.
pub const EXIT_USAGE: i32 = 2;
/// The exit code for a failure that is not the invocation's fault.
pub const EXIT_FAILURE: i32 = 1;

#[derive(Debug)]
pub enum CliError {
    /// The arguments cannot express what was asked (mutually exclusive flags, an
    /// unparseable JSON argument, a subcommand that needs a TTY it does not
    /// have).
    Usage(String),
    /// The engine answered with a correlated `error`, or the protocol stream
    /// ended before the reply arrived.
    Protocol(String),
    /// A turn was refused or failed. Split from [`Self::Protocol`] because the
    /// session survives it: the REPL keeps going and prompts again, while a
    /// one-shot command still exits non-zero.
    Turn(String),
    /// Assembling or connecting to the endpoint failed, before any op was sent.
    Engine(String),
    /// A wire value could not be encoded or decoded.
    Codec(String),
    Io(std::io::Error),
}

impl CliError {
    pub fn usage(message: impl Into<String>) -> Self {
        Self::Usage(message.into())
    }

    pub fn engine(message: impl Into<String>) -> Self {
        Self::Engine(message.into())
    }

    pub fn turn(message: impl Into<String>) -> Self {
        Self::Turn(message.into())
    }

    pub fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol(message.into())
    }

    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Usage(_) => EXIT_USAGE,
            _ => EXIT_FAILURE,
        }
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage(message) => write!(formatter, "{message}"),
            Self::Protocol(message) => write!(formatter, "engine error: {message}"),
            Self::Turn(message) => write!(formatter, "turn failed: {message}"),
            Self::Engine(message) => write!(formatter, "engine unavailable: {message}"),
            Self::Codec(message) => write!(formatter, "wire error: {message}"),
            Self::Io(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for CliError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for CliError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// A facade call that did not produce its declared result.
///
/// Everything the engine refused reads as [`CliError::Protocol`] regardless of
/// which JSON-RPC code it carried, because the terminal has one thing to do with
/// all of them. A caller that needs to BRANCH on the code — a stale config
/// revision worth retrying, say — matches on `FacadeError::code()` before
/// letting the error through `?`.
impl From<FacadeError> for CliError {
    fn from(error: FacadeError) -> Self {
        Self::Protocol(error.to_string())
    }
}

impl From<ConnectError> for CliError {
    fn from(error: ConnectError) -> Self {
        Self::Engine(error.to_string())
    }
}

impl From<RuntimeCodecError> for CliError {
    fn from(error: RuntimeCodecError) -> Self {
        Self::Codec(error.to_string())
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for CliError {
    fn from(error: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self::Engine(error.to_string())
    }
}
