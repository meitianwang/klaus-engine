//! Where output goes, and in which of the two shapes.
//!
//! The CLI has exactly one output decision and it is made once, at startup:
//! human text or `--json`. Keeping it in one place is what lets every command
//! stay honest about it — a command hands the printer the wire value plus a
//! closure that renders it, and the printer runs only the side that is live. So
//! `--json` cannot drift from human output by forgetting a field, and human mode
//! never pays to encode a value nobody prints.
//!
//! `--json` emits the engine's own wire JSON, one value per line, unindented.
//! That is deliberate: the machine-readable form of this CLI is the protocol
//! itself, so a caller piping `--json` into a program is reading exactly what the
//! engine said, not a shape the CLI invented over it.
//!
//! Diagnostics always go to stderr, in both modes, so a `--json` consumer can
//! read stdout as pure data.

use std::io::{IsTerminal, Write};

use klaus_agent_core::runtime_values::{RuntimeEncode, RuntimeJson};
use klaus_agent_server::app_server::runtime_wire::RawJson;

use crate::error::CliResult;
use crate::text;
use crate::tui::Painter;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    Human,
    Json,
}

pub struct Printer {
    mode: OutputMode,
    /// Whether colour is live, and which palette. Resolved once here because
    /// the printer is already the one thing every command holds — threading a
    /// second parameter through every call site for the same decision would
    /// give two places for it to drift.
    painter: Painter,
}

impl Printer {
    pub fn new(json: bool) -> Self {
        Self {
            mode: if json {
                OutputMode::Json
            } else {
                OutputMode::Human
            },
            // `--json` never carries styling: its stdout is data a program
            // parses, and an escape sequence in there is corruption.
            painter: if json {
                Painter::plain()
            } else {
                Painter::from_env()
            },
        }
    }

    /// The palette and the colour decision, for the renderers.
    pub fn painter(&self) -> &Painter {
        &self.painter
    }

    /// How wide the terminal is, for anything that lays out columns.
    ///
    /// 80 when there is no terminal to ask: it is the conventional assumption,
    /// and a pipe has no width to respect anyway.
    pub fn columns(&self) -> usize {
        std::env::var("COLUMNS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value >= 20)
            .unwrap_or(80)
    }

    pub fn is_json(&self) -> bool {
        self.mode == OutputMode::Json
    }

    /// Print a result: its wire JSON in `--json` mode, otherwise whatever
    /// `render` makes of it. Only the live side runs.
    pub fn value<T, F>(&self, value: &T, render: F) -> CliResult<()>
    where
        T: RuntimeEncode + ?Sized,
        F: FnOnce(&T) -> String,
    {
        match self.mode {
            OutputMode::Json => self.json(&text::encode(value)?),
            OutputMode::Human => self.line(&render(value)),
        }
    }

    /// The same, for a result the facade DECLARED rather than one it forwarded.
    ///
    /// Split from [`Self::value`] only by which codec reaches the wire form: the
    /// endpoint-administration replies are the facade's own structs, so they
    /// serialize through serde instead of the engine's runtime codec. Everything
    /// else holds — `--json` still emits what the engine said, and still only the
    /// live side runs.
    pub fn declared<T, F>(&self, value: &T, render: F) -> CliResult<()>
    where
        T: serde::Serialize,
        F: FnOnce(&T) -> String,
    {
        match self.mode {
            OutputMode::Json => {
                let encoded = serde_json::to_vec(value)
                    .map_err(|error| crate::error::CliError::Codec(error.to_string()))?;
                self.write_line(&encoded)
            }
            OutputMode::Human => self.line(&render(value)),
        }
    }

    /// Print one already-built JSON value as a line. In human mode this is
    /// suppressed: a raw JSON dump is the `--json` contract, not a fallback.
    pub fn json(&self, value: &RuntimeJson) -> CliResult<()> {
        if !self.is_json() {
            return Ok(());
        }
        self.write_line(text::json_line(value).as_bytes())
    }

    /// Print an opaque named reply without changing its content or scalar
    /// number tokens. Human renderers decode only the fields they need.
    pub fn raw_json(&self, value: &RawJson) -> CliResult<()> {
        if !self.is_json() {
            return Ok(());
        }
        self.write_line(value.as_str().as_bytes())
    }

    /// Print human text. Suppressed in `--json` mode so stdout stays parseable.
    pub fn line(&self, line: &str) -> CliResult<()> {
        if self.is_json() {
            return Ok(());
        }
        self.write_line(line.as_bytes())
    }

    /// Print streamed human text with no trailing newline (assistant deltas).
    /// Suppressed in `--json` mode, which emits the delta events instead.
    pub fn stream(&self, chunk: &str) -> CliResult<()> {
        if self.is_json() || chunk.is_empty() {
            return Ok(());
        }
        let mut out = std::io::stdout().lock();
        out.write_all(chunk.as_bytes())?;
        out.flush()?;
        Ok(())
    }

    /// A diagnostic. Goes to stderr in both modes and is never suppressed: a
    /// degraded run that says nothing is worse than noisy output.
    pub fn note(&self, line: &str) {
        eprintln!("{line}");
    }

    /// A question, with the cursor left after it. On stderr for the same reason
    /// notes are: the answer is part of the conversation, not part of the data a
    /// caller is piping off stdout.
    pub fn prompt(&self, text: &str) -> CliResult<()> {
        let mut err = std::io::stderr().lock();
        err.write_all(text.as_bytes())?;
        err.flush()?;
        Ok(())
    }

    /// Whether stdin is a terminal, i.e. whether the CLI may prompt. Approval
    /// and elicitation both need this, and both must decide it the same way.
    pub fn can_prompt(&self) -> bool {
        std::io::stdin().is_terminal()
    }

    fn write_line(&self, bytes: &[u8]) -> CliResult<()> {
        let mut out = std::io::stdout().lock();
        out.write_all(bytes)?;
        out.write_all(b"\n")?;
        out.flush()?;
        Ok(())
    }
}
