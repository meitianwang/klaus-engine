//! Structured, leveled logging with sensitive-field redaction, emitted through
//! the `tracing` facade.

use serde_json::{Map, Value};
use std::fmt;

/// Log severities in ascending order. `Silent` disables all output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
    Silent,
}

impl LogLevel {
    /// Parse a level name (`"trace"`, `"debug"`, `"info"`, `"warn"`, `"error"`,
    /// `"fatal"`, `"silent"`). Unknown names yield `None`.
    pub fn parse(s: &str) -> Option<LogLevel> {
        match s {
            "trace" => Some(LogLevel::Trace),
            "debug" => Some(LogLevel::Debug),
            "info" => Some(LogLevel::Info),
            "warn" => Some(LogLevel::Warn),
            "error" => Some(LogLevel::Error),
            "fatal" => Some(LogLevel::Fatal),
            "silent" => Some(LogLevel::Silent),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Trace => "trace",
            LogLevel::Debug => "debug",
            LogLevel::Info => "info",
            LogLevel::Warn => "warn",
            LogLevel::Error => "error",
            LogLevel::Fatal => "fatal",
            LogLevel::Silent => "silent",
        }
    }
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Options for [`create_logger`].
#[derive(Debug, Clone, Default)]
pub struct CreateLoggerOptions {
    pub level: Option<String>,
    pub name: Option<String>,
}

/// Value substituted for masked fields.
const REDACT_CENSOR: &str = "[redacted]";

/// Credential-bearing keys are masked at every nesting depth.
const REDACTED_KEYS: [&str; 15] = [
    "apiKey",
    "api_key",
    "authorization",
    "token",
    "password",
    "accessToken",
    "access_token",
    "clientIdentity",
    "client_identity",
    "refreshToken",
    "refresh_token",
    "verifier",
    "code_verifier",
    "audioBase64",
    "audio_base64",
];

/// The exact HTTP-header locations where auth material tends to land
/// (`headers.authorization`, `headers["x-api-key"]`).
const REDACTED_HEADER_KEYS: [&str; 3] = ["authorization", "x-api-key", "x-cursor-checksum"];

/// A leveled structured logger preconfigured to redact sensitive fields before
/// any record is written. Records are emitted as `tracing` events carrying the
/// logger name and the merged (bindings + call-site) fields as one JSON string.
#[derive(Debug, Clone)]
pub struct Logger {
    name: String,
    level: LogLevel,
    bindings: Map<String, Value>,
}

/// A logger preconfigured to redact sensitive fields. Level resolution: the
/// explicit option, else the `KLAUS_LOG_LEVEL` env var, else `info` (an
/// unrecognized name also falls back to `info`). Name defaults to
/// `klaus-agent`.
pub fn create_logger(opts: CreateLoggerOptions) -> Logger {
    let level = opts
        .level
        .or_else(|| std::env::var("KLAUS_LOG_LEVEL").ok())
        .as_deref()
        .and_then(LogLevel::parse)
        .unwrap_or(LogLevel::Info);
    let name = opts.name.unwrap_or_else(|| "klaus-agent".to_string());
    Logger {
        name,
        level,
        bindings: Map::new(),
    }
}

impl Logger {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn level(&self) -> LogLevel {
        self.level
    }

    /// Change the minimum emitted level.
    pub fn set_level(&mut self, level: LogLevel) {
        self.level = level;
    }

    /// Whether a record at `level` would be emitted.
    pub fn is_level_enabled(&self, level: LogLevel) -> bool {
        self.level != LogLevel::Silent && level != LogLevel::Silent && level >= self.level
    }

    /// A child logger that stamps `bindings` onto every record (call-site
    /// fields win on key conflicts). Name and level are inherited.
    pub fn child(&self, bindings: Map<String, Value>) -> Logger {
        let mut merged = self.bindings.clone();
        for (k, v) in bindings {
            merged.insert(k, v);
        }
        Logger {
            name: self.name.clone(),
            level: self.level,
            bindings: merged,
        }
    }

    /// The final record for a call: the logger's bindings merged with `fields`
    /// (a JSON object — anything else contributes nothing), with sensitive
    /// values masked.
    pub fn build_record(&self, fields: Value) -> Map<String, Value> {
        let mut record = self.bindings.clone();
        if let Value::Object(obj) = fields {
            for (k, v) in obj {
                record.insert(k, v);
            }
        }
        redact(&mut record);
        record
    }

    /// Emit a record at `level`. `fields` should be a JSON object (pass
    /// `Value::Null` for none); `msg` is the human-readable message.
    pub fn log(&self, level: LogLevel, fields: Value, msg: &str) {
        if !self.is_level_enabled(level) {
            return;
        }
        let record = Value::Object(self.build_record(fields)).to_string();
        match level {
            LogLevel::Trace => {
                tracing::trace!(logger = %self.name, fields = %record, "{}", msg)
            }
            LogLevel::Debug => {
                tracing::debug!(logger = %self.name, fields = %record, "{}", msg)
            }
            LogLevel::Info => tracing::info!(logger = %self.name, fields = %record, "{}", msg),
            LogLevel::Warn => tracing::warn!(logger = %self.name, fields = %record, "{}", msg),
            LogLevel::Error => {
                tracing::error!(logger = %self.name, fields = %record, "{}", msg)
            }
            // `tracing` has no fatal level — emit at error with a marker field.
            LogLevel::Fatal => {
                tracing::error!(logger = %self.name, level = "fatal", fields = %record, "{}", msg)
            }
            LogLevel::Silent => {}
        }
    }

    pub fn trace(&self, fields: Value, msg: &str) {
        self.log(LogLevel::Trace, fields, msg)
    }

    pub fn debug(&self, fields: Value, msg: &str) {
        self.log(LogLevel::Debug, fields, msg)
    }

    pub fn info(&self, fields: Value, msg: &str) {
        self.log(LogLevel::Info, fields, msg)
    }

    pub fn warn(&self, fields: Value, msg: &str) {
        self.log(LogLevel::Warn, fields, msg)
    }

    pub fn error(&self, fields: Value, msg: &str) {
        self.log(LogLevel::Error, fields, msg)
    }

    pub fn fatal(&self, fields: Value, msg: &str) {
        self.log(LogLevel::Fatal, fields, msg)
    }
}

/// Mask credential-bearing values before any record is written, including
/// nested imported snapshots and headers carried in diagnostic arrays.
fn redact(record: &mut Map<String, Value>) {
    fn visit(value: &mut Value) {
        match value {
            Value::Object(object) => redact(object),
            Value::Array(array) => array.iter_mut().for_each(visit),
            _ => {}
        }
    }
    for (key, value) in record.iter_mut() {
        if REDACTED_KEYS
            .iter()
            .chain(REDACTED_HEADER_KEYS.iter())
            .any(|sensitive| key.eq_ignore_ascii_case(sensitive))
        {
            *value = Value::String(REDACT_CENSOR.to_string());
        } else {
            visit(value);
        }
    }
}

#[cfg(test)]
mod credential_redaction_tests {
    use super::*;
    #[test]
    fn browser_grants_and_pkce_verifiers_are_secret_at_any_depth() {
        let mut record = serde_json::json!({"flow":{"verifier":"private-verifier","code_verifier":"private-code"},"stored":{"refreshToken":"private-refresh","refresh_token":"private-rotation"}}).as_object().unwrap().clone();
        redact(&mut record);
        let output = serde_json::to_string(&record).unwrap();
        assert!(!output.contains("private-"));
    }

    #[test]
    fn imported_identity_and_derived_headers_are_secret_at_any_depth() {
        let mut record = serde_json::json!({"clientIdentity":"identity-secret","nested":[{"userInfo":{"accessToken":"token-secret","client_identity":"other-secret"},"headers":{"X-Cursor-Checksum":"proof-secret"}}]}).as_object().unwrap().clone();
        redact(&mut record);
        let output = serde_json::to_string(&record).unwrap();
        for secret in [
            "identity-secret",
            "token-secret",
            "other-secret",
            "proof-secret",
        ] {
            assert!(!output.contains(secret));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex;

    /// Serializes tests that read or write `KLAUS_LOG_LEVEL`.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_level(level: &str) -> Logger {
        create_logger(CreateLoggerOptions {
            level: Some(level.to_string()),
            name: None,
        })
    }

    #[test]
    fn defaults_to_info_level_and_engine_name() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        crate::runtime_process::remove_process_environment_var("KLAUS_LOG_LEVEL");
        let log = create_logger(CreateLoggerOptions::default());
        assert_eq!(log.level(), LogLevel::Info);
        assert_eq!(log.name(), "klaus-agent");
    }

    #[test]
    fn explicit_options_win_over_env_and_defaults() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        crate::runtime_process::set_process_environment_var("KLAUS_LOG_LEVEL", "error");
        let log = create_logger(CreateLoggerOptions {
            level: Some("debug".to_string()),
            name: Some("custom".to_string()),
        });
        crate::runtime_process::remove_process_environment_var("KLAUS_LOG_LEVEL");
        assert_eq!(log.level(), LogLevel::Debug);
        assert_eq!(log.name(), "custom");
    }

    #[test]
    fn env_var_sets_the_level_when_no_option_given() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        crate::runtime_process::set_process_environment_var("KLAUS_LOG_LEVEL", "warn");
        let log = create_logger(CreateLoggerOptions::default());
        crate::runtime_process::remove_process_environment_var("KLAUS_LOG_LEVEL");
        assert_eq!(log.level(), LogLevel::Warn);
    }

    #[test]
    fn unknown_level_falls_back_to_info() {
        let log = with_level("bogus");
        assert_eq!(log.level(), LogLevel::Info);
    }

    #[test]
    fn level_gating() {
        let log = with_level("info");
        assert!(!log.is_level_enabled(LogLevel::Trace));
        assert!(!log.is_level_enabled(LogLevel::Debug));
        assert!(log.is_level_enabled(LogLevel::Info));
        assert!(log.is_level_enabled(LogLevel::Warn));
        assert!(log.is_level_enabled(LogLevel::Error));
        assert!(log.is_level_enabled(LogLevel::Fatal));

        let silent = with_level("silent");
        assert!(!silent.is_level_enabled(LogLevel::Fatal));
        assert!(!silent.is_level_enabled(LogLevel::Info));
    }

    #[test]
    fn child_bindings_are_stamped_onto_records() {
        let log = with_level("info");
        let mut bindings = Map::new();
        bindings.insert("component".to_string(), json!("engine"));
        let child = log.child(bindings);

        let record = child.build_record(json!({ "event": "x" }));
        assert_eq!(record["component"], json!("engine"));
        assert_eq!(record["event"], json!("x"));

        // Call-site fields win over bindings on key conflicts.
        let record = child.build_record(json!({ "component": "override" }));
        assert_eq!(record["component"], json!("override"));

        // The parent is unaffected.
        assert!(log.build_record(json!({})).get("component").is_none());
    }

    #[test]
    fn redacts_sensitive_fields() {
        let log = with_level("info");
        let record = log.build_record(json!({
            "config": { "apiKey": "sk-123", "model": "m", "audioBase64":"private-audio" },
            "auth": { "token": "t", "password": "p", "api_key": "k", "authorization": "a" },
            "headers": { "authorization": "Bearer abc", "x-api-key": "xyz", "accept": "*/*" },
        }));
        assert_eq!(record["config"]["apiKey"], json!("[redacted]"));
        assert_eq!(record["config"]["model"], json!("m"));
        assert_eq!(record["config"]["audioBase64"], json!("[redacted]"));
        assert_eq!(record["auth"]["token"], json!("[redacted]"));
        assert_eq!(record["auth"]["password"], json!("[redacted]"));
        assert_eq!(record["auth"]["api_key"], json!("[redacted]"));
        assert_eq!(record["auth"]["authorization"], json!("[redacted]"));
        assert_eq!(record["headers"]["authorization"], json!("[redacted]"));
        assert_eq!(record["headers"]["x-api-key"], json!("[redacted]"));
        assert_eq!(record["headers"]["accept"], json!("*/*"));
    }

    #[test]
    fn masks_sensitive_values_at_the_top_level_and_in_nested_objects() {
        let log = with_level("info");
        let record = log.build_record(json!({
            "token": "top",
            "outer": { "inner": { "apiKey": "deep" } },
        }));
        assert_eq!(record["token"], json!("[redacted]"));
        assert_eq!(record["outer"]["inner"]["apiKey"], json!("[redacted]"));
    }

    #[test]
    fn log_calls_do_not_panic_without_a_subscriber() {
        let log = with_level("trace");
        log.trace(json!({ "a": 1 }), "t");
        log.debug(Value::Null, "d");
        log.info(json!({ "headers": { "authorization": "x" } }), "i");
        log.warn(json!({}), "w");
        log.error(json!({ "e": true }), "e");
        log.fatal(Value::Null, "f");
    }
}
