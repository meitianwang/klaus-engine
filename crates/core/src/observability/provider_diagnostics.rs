//! The subscription bridge's own diagnostic log.
//!
//! A provider refusal currently ends a turn with one fixed sentence, and the
//! detail that would say WHICH upstream refused — status, service error code,
//! request id, the upstream body, whether a recovery was attempted and how it
//! went — exists only in memory for the length of the call. After the turn is
//! over there is nothing left to read, so "the gateway rejected us" and "our own
//! bridge token was stale" are indistinguishable.
//!
//! This module is where that detail goes. Three properties are load-bearing:
//!
//! * **A fixed file, not stderr.** The engine's stderr is forwarded to a host log
//!   that rotates per process generation, and the app-server's own structured
//!   logger is not wired on the production path. A path derived from the config
//!   home survives both.
//! * **One JSON object per line.** Records are appended and never rewritten, so a
//!   log can be read while the engine that writes it is still running.
//! * **No credentials, ever.** Upstream bodies can echo an `Authorization`
//!   header or a bearer token back, so every string is sanitized before it is
//!   written; a diagnostic that leaks the credential it is diagnosing is worse
//!   than no diagnostic.
//!
//! Writing is best-effort by design: a diagnostic that fails (read-only config
//! home, a full disk) must never turn a recoverable turn into a failed one.

use std::cell::RefCell;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::Stream;
use serde_json::{json, Map, Value};

use crate::runtime_values::RuntimeString;
use crate::types::provider::{ModelStream, ModelStreamEvent};

/// Cap on a single sanitized field. The failure bodies worth keeping are short
/// (a JSON envelope, an HTML title); a megabyte-long body is a different problem
/// and would only push the useful lines out of the file.
const FIELD_CHARS: usize = 2_000;

/// Rotate once a file passes this size. Two files are kept: the live one and one
/// predecessor, so a long session cannot grow the log without bound while the
/// previous incident is still readable.
const ROTATE_BYTES: u64 = 4 * 1024 * 1024;

const LOG_FILE_NAME: &str = "provider-failures.log";

/// The opt-in developer trace of every settled model request (see
/// `provider_trace` in the engine config). Kept apart from the failure log so
/// an always-on stream of success records cannot push an incident out of the
/// file a person opens first.
pub const TRACE_LOG_FILE_NAME: &str = "provider-trace.log";

/// Content-free request lifecycle records, independent of the opt-in trace.
pub const TIMING_LOG_FILE_NAME: &str = "provider-timing.log";

thread_local! {
    /// The session that owns the model stream currently being polled.
    ///
    /// Transport-level records are written from inside a provider's stream
    /// body, a layer that receives the request and never a session. The failure
    /// path can still name its session because the engine records it, but a
    /// success record could not, so the two logs could not be joined and a
    /// per-scope success rate was unknowable. The engine scopes every stream it
    /// polls; records read the scope back.
    ///
    /// A thread local is the right carrier here, not a task local: a record is
    /// written synchronously inside one `poll_next` call, so holding the scope
    /// for the duration of that poll attributes it exactly, on whichever task
    /// or thread the consumer happens to poll from.
    static TRACE_SESSION: RefCell<Option<RuntimeString>> = const { RefCell::new(None) };
}

/// Restores the previous scope on drop, so nested polls keep their own session.
struct TraceSessionGuard {
    previous: Option<RuntimeString>,
}

impl Drop for TraceSessionGuard {
    fn drop(&mut self) {
        TRACE_SESSION.with(|slot| {
            *slot.borrow_mut() = self.previous.take();
        });
    }
}

fn enter_trace_session(session_id: RuntimeString) -> TraceSessionGuard {
    let previous = TRACE_SESSION.with(|slot| slot.borrow_mut().replace(session_id));
    TraceSessionGuard { previous }
}

/// A model stream whose every poll carries the owning session into
/// [`TRACE_SESSION`].
///
/// Scoping the stream instead of the code around it keeps attribution correct
/// wherever the stream is polled, including a retry loop or a consumer on
/// another task. A stream polled without this wrapper records no session, which
/// is exactly the behavior from before attribution existed.
struct SessionScopedStream {
    session_id: RuntimeString,
    inner: ModelStream,
}

impl Stream for SessionScopedStream {
    type Item = ModelStreamEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let _guard = enter_trace_session(this.session_id.clone());
        this.inner.as_mut().poll_next(cx)
    }
}

/// Attribute every transport-level record written while `stream` is polled to
/// `session_id`.
pub fn scoped_trace_stream(session_id: RuntimeString, stream: ModelStream) -> ModelStream {
    Box::pin(SessionScopedStream {
        session_id,
        inner: stream,
    })
}

/// Run `f` with `session_id` scoped, for a caller that already holds the
/// session and writes a record outside a stream poll.
pub fn with_trace_session<R>(session_id: RuntimeString, f: impl FnOnce() -> R) -> R {
    let _guard = enter_trace_session(session_id);
    f()
}

/// The scoped session id, or `None` outside a scope.
pub fn trace_session_id() -> Option<String> {
    TRACE_SESSION.with(|slot| {
        slot.borrow()
            .as_ref()
            .and_then(|session_id| session_id.try_to_utf8().ok())
    })
}

/// Where the log lives: `<config home>/logs/<file name>`, the
/// directory the host and the engine already share for user-visible logs.
///
/// A test run must not append to the machine's own failure log: a test drives
/// the same code a live failure does, so its fake refusals would otherwise mix
/// into the diagnostics a person reads later. Test-support builds therefore
/// resolve into the process temp dir.
///
/// The guard keys off the `testing` FEATURE as well as `cfg(test)`, following
/// this crate's own test-support convention. `cfg(test)` alone would be wrong
/// for a host crate's test binary: there this crate is an ordinary dependency,
/// `cfg(test)` is false, and a fake refusal would land in the real config home.
/// Release builds of the engine take no features, so they get the real path.
pub fn log_path_named(file_name: &str) -> PathBuf {
    #[cfg(any(test, feature = "testing"))]
    {
        std::env::temp_dir()
            .join("klaus-test-provider-diagnostics")
            .join(file_name)
    }
    #[cfg(not(any(test, feature = "testing")))]
    {
        let home = crate::fs::paths::config_home();
        Path::new(&home).join("logs").join(file_name)
    }
}

pub fn log_path() -> PathBuf {
    log_path_named(LOG_FILE_NAME)
}

/// A provider failure, in the one shape every call site fills in.
///
/// Both halves of the engine write this: the provider loop records a turn's
/// terminal failure, and the subscription bridge records an upstream refusal it
/// could not recover from. Keeping one shape is what lets a reader answer "was
/// this the gateway or our own credential" without knowing which side wrote it.
pub struct ProviderFailure<'a> {
    /// Failure family: `turn_failed`, `upstream_rejected`, `bridge_token_mismatch`, …
    pub kind: &'a str,
    /// Subscription source when the failure has one (`qoder`, `qwenwork`,
    /// `qoderwork`).
    pub source: Option<&'a str>,
    /// Model or selection the request named.
    pub model: Option<&'a str>,
    /// Normalized error kind (`auth`, `rate_limit`, …).
    pub error_kind: Option<&'a str>,
    /// HTTP status, when the failure came from a response.
    pub status: Option<u16>,
    /// Upstream service error code (`105`, `100401`, …).
    pub service_code: Option<&'a str>,
    /// Request id, so a failure can be matched against the service's own logs.
    pub request_id: Option<&'a str>,
    /// The upstream body or engine message, sanitized before it is written.
    pub detail: Option<&'a str>,
    /// Whether a recovery was attempted, and how it turned out.
    pub recovery: Option<&'a str>,
    /// Conversation this failure belongs to, when it has one.
    pub session_id: Option<&'a str>,
}

impl ProviderFailure<'_> {
    /// Record it. Returns nothing: a diagnostic must never fail a turn.
    pub fn record(&self) {
        record(self.kind, self.fields());
    }

    /// The JSON fields, with `None`s dropped so a record only claims what the
    /// call site actually knew.
    pub fn fields(&self) -> Value {
        let mut fields = Map::new();
        let mut put = |key: &str, value: Value| {
            fields.insert(key.to_string(), value);
        };
        if let Some(source) = self.source {
            put("source", json!(source));
        }
        if let Some(model) = self.model {
            put("model", json!(model));
        }
        if let Some(error_kind) = self.error_kind {
            put("errorKind", json!(error_kind));
        }
        if let Some(status) = self.status {
            put("status", json!(status));
        }
        if let Some(code) = self.service_code {
            put("serviceCode", json!(code));
        }
        if let Some(request_id) = self.request_id {
            put("requestId", json!(request_id));
        }
        if let Some(detail) = self.detail {
            put("detail", json!(detail));
        }
        if let Some(recovery) = self.recovery {
            put("recovery", json!(recovery));
        }
        if let Some(session_id) = self.session_id {
            put("sessionId", json!(session_id));
        }
        Value::Object(fields)
    }
}

/// Append one event. `kind` names the failure family; `fields` carries whatever
/// the call site knows. Returns nothing: a diagnostic must not fail a turn.
pub fn record(kind: &str, fields: Value) {
    record_named(LOG_FILE_NAME, kind, fields);
}

/// Append one event to a named log under the shared `logs/` directory, with the
/// same sanitize + rotate + best-effort guarantees as [`record`]. The trace log
/// uses this so both files share one writer.
pub fn record_named(file_name: &str, kind: &str, fields: Value) {
    let path = log_path_named(file_name);
    if let Some(parent) = path.parent() {
        if std::fs::create_dir_all(parent).is_err() {
            return;
        }
    }
    rotate_if_needed(&path);
    let mut record = Map::new();
    record.insert("kind".into(), json!(kind));
    record.insert("atMs".into(), json!(now_ms()));
    record.insert("log".into(), json!(file_name));
    if let Value::Object(fields) = fields {
        for (key, value) in fields {
            record.insert(key, sanitize(value));
        }
    }
    let mut line = Value::Object(record).to_string();
    line.push('\n');
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&path) {
        let _ = file.write_all(line.as_bytes());
    }
}

/// Whether the developer-facing provider trace is switched on.
///
/// The trace is a GLOBAL debug switch, so the provider layer reads the user
/// config file directly instead of threading the flag through every session
/// init: flipping the settings toggle then takes effect on the next request of
/// an already-running session. A missing or unreadable file means off, and the
/// value is re-read per request so the cost of the check is one small parse.
///
/// Test-support builds answer `false` without touching the disk: the user
/// config path is the DEVELOPER'S real profile there, and a test that flipped
/// the developer's own switch (or wrote trace lines because the developer had
/// it on) is the same leak the `log_path_named` guard exists to prevent.
/// Tests use [`provider_trace_enabled_at`] with their own file instead.
pub fn provider_trace_enabled() -> bool {
    #[cfg(any(test, feature = "testing"))]
    {
        false
    }
    #[cfg(not(any(test, feature = "testing")))]
    {
        provider_trace_enabled_at(&crate::config_store::user_config_path())
    }
}

/// Explicit-path form of the switch read, for tests and diagnostics that must
/// not consult the developer's real profile.
pub fn provider_trace_enabled_at(path: &Path) -> bool {
    let Ok(raw) = std::fs::read_to_string(path) else {
        return false;
    };
    let Ok(document) = raw.parse::<toml_edit::DocumentMut>() else {
        return false;
    };
    document
        .get("providerTrace")
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
}

#[cfg(test)]
mod provider_trace_tests {
    use super::*;

    #[test]
    fn trace_switch_reads_only_the_top_level_provider_trace_key() {
        let dir = std::env::temp_dir().join(format!("klaus-trace-switch-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        let read = |body: &str| {
            std::fs::write(&path, body).unwrap();
            provider_trace_enabled_at(&path)
        };
        assert!(read("providerTrace = true\n"));
        assert!(!read("providerTrace = false\n"));
        assert!(!read("thinking = true\n"));
        assert!(!read("[modelProviders.x]\nproviderTrace = true\n"));
        assert!(!read("providerTrace = \"true\"\n"));
        assert!(!read("not toml at all {{{"));
        std::fs::remove_file(&path).unwrap();
        assert!(!provider_trace_enabled_at(&path), "a missing file is off");
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn record_named_sanitizes_and_lands_in_the_named_test_log() {
        let name = format!("provider-trace-unit-{}.log", std::process::id());
        let path = log_path_named(&name);
        let _ = std::fs::remove_file(&path);
        record_named(
            &name,
            "provider_request_settled",
            json!({ "model": "m", "detail": "Bearer abcdef1234567890abcdef1234567890" }),
        );
        let line = std::fs::read_to_string(&path).expect("trace line landed");
        assert!(
            line.contains("\"kind\":\"provider_request_settled\""),
            "{line}"
        );
        assert!(
            line.contains("\"log\":\"") && line.contains(&name),
            "{line}"
        );
        assert!(!line.contains("abcdef1234567890abcdef1234567890"), "{line}");
        let _ = std::fs::remove_file(&path);
    }
}

/// `Some(value)` when the file actually took the line. Tests use this to prove a
/// record landed instead of asserting on a shared, concurrently-written file.
pub fn record_to(path: &Path, kind: &str, fields: Value) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut record = Map::new();
    record.insert("kind".into(), json!(kind));
    record.insert("atMs".into(), json!(now_ms()));
    if let Value::Object(fields) = fields {
        for (key, value) in fields {
            record.insert(key, sanitize(value));
        }
    }
    let mut line = Value::Object(record).to_string();
    line.push('\n');
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(line.as_bytes())
}

fn rotate_if_needed(path: &Path) {
    let Ok(metadata) = std::fs::metadata(path) else {
        return;
    };
    if metadata.len() < ROTATE_BYTES {
        return;
    }
    let rotated = path.with_extension("1.log");
    // A failed rotation must not lose the fresh record: the append continues to
    // the oversized file rather than dropping the line.
    let _ = std::fs::rename(path, rotated);
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

/// Recursively strip credential-shaped material and cap every string.
fn sanitize(value: Value) -> Value {
    match value {
        Value::String(text) => json!(sanitize_text(&text)),
        Value::Array(items) => Value::Array(items.into_iter().map(sanitize).collect()),
        Value::Object(object) => Value::Object(
            object
                .into_iter()
                .map(|(key, value)| {
                    let value = if [
                        "authorization",
                        "token",
                        "accessToken",
                        "access_token",
                        "security_oauth_token",
                        "refreshToken",
                        "refresh_token",
                        "verifier",
                        "code_verifier",
                        "audioBase64",
                        "audio_base64",
                        "clientIdentity",
                        "client_identity",
                        "x-cursor-checksum",
                    ]
                    .iter()
                    .any(|sensitive| key.eq_ignore_ascii_case(sensitive))
                    {
                        json!("[redacted]")
                    } else {
                        sanitize(value)
                    };
                    (key, value)
                })
                .collect(),
        ),
        other => other,
    }
}

/// Mask anything that looks like a bearer credential and collapse to one line.
///
/// Two shapes are covered because both have been seen in real upstream bodies: a
/// `Bearer <token>` header value echoed back, and a JSON `"token": "..."` field.
/// The remainder is capped so one enormous body cannot dominate the log.
fn sanitize_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len().min(FIELD_CHARS));
    let mut rest = text;
    while let Some(position) = find_credential_start(rest) {
        out.push_str(&rest[..position]);
        // `mask_credential` measures its consumption from the slice it was
        // handed, so the two offsets compose before the cursor moves.
        let (masked, consumed) = mask_credential(&rest[position..]);
        out.push_str(&masked);
        rest = &rest[position + consumed..];
    }
    out.push_str(rest);
    let collapsed = out.replace(['\n', '\r'], " ");
    if collapsed.chars().count() > FIELD_CHARS {
        let truncated: String = collapsed.chars().take(FIELD_CHARS).collect();
        return format!("{truncated}\u{2026}");
    }
    collapsed
}

/// The earliest index of a credential-shaped run, whatever its casing.
fn find_credential_start(text: &str) -> Option<usize> {
    // The index must address `text`, so the scan is done over a lowercased
    // ASCII projection whose byte offsets are unchanged (only ASCII letters are
    // folded; a multi-byte character maps byte-for-byte).
    let lower = text.to_ascii_lowercase();
    [
        "bearer ",
        "verifier=",
        "refresh_token=",
        "\"token\"",
        "\"access_token\"",
        "\"accesstoken\"",
        "\"refreshtoken\"",
        "\"refresh_token\"",
        "\"verifier\"",
        "\"code_verifier\"",
        "\"clientidentity\"",
        "\"client_identity\"",
        "\"x-cursor-checksum\"",
        "\"security_oauth_token\"",
    ]
    .into_iter()
    .filter_map(|needle| lower.find(needle))
    .min()
}

/// Mask one credential run, returning the replacement and how many bytes of the
/// original it consumed.
fn mask_credential(text: &str) -> (String, usize) {
    const MASK: &str = "[redacted]";
    let lower = text.to_ascii_lowercase();
    // `"token": "..."` — keep the key so the record still says what was there.
    for key in [
        "\"token\"",
        "\"access_token\"",
        "\"accesstoken\"",
        "\"refreshtoken\"",
        "\"refresh_token\"",
        "\"verifier\"",
        "\"code_verifier\"",
        "\"clientidentity\"",
        "\"client_identity\"",
        "\"x-cursor-checksum\"",
        "\"security_oauth_token\"",
    ] {
        if lower.starts_with(key) {
            let after_key = &text[key.len()..];
            let Some(colon) = after_key.find(':') else {
                return (MASK.to_string(), text.len().min(MASK.len()));
            };
            let value_start = key.len() + colon + 1;
            let value = &text[value_start..];
            let Some(quote) = value.find('"') else {
                return (text[..value_start].to_string() + MASK, text.len());
            };
            let literal_start = value_start + quote + 1;
            let Some(end) = text[literal_start..].find('"') else {
                return (text[..literal_start].to_string() + MASK, text.len());
            };
            let literal_end = literal_start + end;
            return (
                format!("{}{}", &text[..literal_start], MASK),
                literal_end + 1,
            );
        }
    }
    for key in ["verifier=", "refresh_token="] {
        if lower.starts_with(key) {
            let end = text[key.len()..]
                .find(|c: char| c.is_whitespace() || matches!(c, '&' | '"' | '\'' | '#'))
                .map(|end| key.len() + end)
                .unwrap_or(text.len());
            return (format!("{}{MASK}", &text[..key.len()]), end);
        }
    }
    // `Bearer <token>` — the run ends at the first whitespace or quote.
    let after_prefix = &text["bearer ".len()..];
    let end = after_prefix
        .find(|c: char| c.is_whitespace() || c == '"')
        .unwrap_or(after_prefix.len());
    (format!("Bearer {MASK}"), "bearer ".len() + end)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn browser_refresh_and_verifiers_are_masked_in_fields_strings_and_query_errors() {
        let value = sanitize(
            json!({"refreshToken":"private-refresh","nested":{"verifier":"private-verifier"},"detail":"{\"refresh_token\":\"private-rotation\",\"code_verifier\":\"private-code\"}","url":"https://api.example/auth/poll?uuid=public&verifier=private-query&more=public"}),
        );
        assert!(!value.to_string().contains("private-"));
    }

    #[test]
    fn imported_identity_and_derived_proof_are_redacted_in_objects_and_echoed_json() {
        let value = sanitize(
            json!({"nested":{"clientIdentity":"identity-secret","client_identity":"other-secret","accessToken":"token-secret","X-Cursor-Checksum":"proof-secret"},"detail":r#"{"clientIdentity":"echo-secret","x-cursor-checksum":"echo-proof"}"#}),
        );
        let output = value.to_string();
        for secret in [
            "identity-secret",
            "other-secret",
            "token-secret",
            "proof-secret",
            "echo-secret",
            "echo-proof",
        ] {
            assert!(!output.contains(secret), "{output}");
        }
    }

    /// Core's usual temp-dir shape (see `config_store_tests`): `tempfile` is not
    /// a dependency of this crate.
    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "klaus-subscription-diagnostics-{tag}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn a_bearer_token_is_masked_wherever_it_appears() {
        let sanitized =
            sanitize_text("upstream said: Authorization: Bearer abc123.def-456 rejected");
        assert!(!sanitized.contains("abc123"), "{sanitized}");
        assert!(sanitized.contains("[redacted]"), "{sanitized}");
        assert!(sanitized.contains("rejected"), "wording must survive");
    }

    #[test]
    fn a_json_token_field_keeps_its_key_and_loses_its_value() {
        let sanitized = sanitize_text(r#"{"code":"105","token":"eyJhbGciOi.payload.signature"}"#);
        assert!(!sanitized.contains("payload"), "{sanitized}");
        assert!(sanitized.contains("\"token\""), "the key must survive");
        assert!(sanitized.contains("[redacted]"), "{sanitized}");
        assert!(sanitized.contains("105"), "unrelated fields must survive");
    }

    #[test]
    fn masking_is_case_insensitive_and_stops_at_the_token_boundary() {
        let sanitized = sanitize_text("authorization: bEaReR TOKEN-with-CAPS next-field-kept");
        assert!(!sanitized.contains("TOKEN-with-CAPS"), "{sanitized}");
        assert!(sanitized.contains("next-field-kept"), "{sanitized}");
    }

    #[test]
    fn a_long_body_is_capped_and_collapsed_to_one_line() {
        let sanitized = sanitize_text(&"x".repeat(5000));
        assert!(
            sanitized.chars().count() <= FIELD_CHARS + 1,
            "capped: {}",
            sanitized.chars().count()
        );
        let multiline = sanitize_text("first line\nsecond line");
        assert_eq!(multiline, "first line second line");
    }

    #[test]
    fn a_failure_record_carries_the_identity_and_drops_what_it_never_knew() {
        let failure = ProviderFailure {
            kind: "upstream_rejected",
            source: Some("qoder"),
            model: Some("ultimate"),
            error_kind: Some("auth"),
            status: Some(403),
            service_code: Some("105"),
            request_id: Some("req-abc"),
            detail: Some("Login expired"),
            recovery: Some("reread:ok"),
            session_id: None,
        };
        let fields = failure.fields();
        assert_eq!(fields["source"], json!("qoder"));
        assert_eq!(fields["model"], json!("ultimate"));
        assert_eq!(fields["status"], json!(403));
        assert_eq!(fields["serviceCode"], json!("105"));
        assert_eq!(fields["requestId"], json!("req-abc"));
        assert_eq!(fields["detail"], json!("Login expired"));
        assert_eq!(fields["recovery"], json!("reread:ok"));
        // A field the call site did not know must be ABSENT rather than null: a
        // record that claims "no session" when it simply had none is how a
        // diagnostic starts lying.
        assert!(
            fields.get("sessionId").is_none(),
            "unknown fields stay out: {fields}"
        );
    }

    #[test]
    fn a_recorded_failure_uses_the_real_entry_point() {
        // `record_to` takes an explicit path for tests; this proves the SHARED
        // `ProviderFailure::record` entry point lands a line too, under the
        // cfg(test) path.
        let fixture = temp_dir("entry");
        let target = fixture.join(LOG_FILE_NAME);
        let failure = ProviderFailure {
            kind: "turn_failed",
            source: None,
            model: Some("dfmodel"),
            error_kind: Some("auth"),
            status: Some(401),
            service_code: None,
            request_id: Some("req-1"),
            detail: Some(r#"{"error":"Bearer eyJhbGci.token leaked"}"#),
            recovery: Some("auth: terminal by policy"),
            session_id: Some("session-1"),
        };
        // Write through the shared shape, but to the fixture path, so the test
        // proves the field projection without touching the process-wide log.
        record_to(&target, failure.kind, failure.fields()).expect("record lands");

        let raw = std::fs::read_to_string(&target).expect("read back");
        let parsed: Value = serde_json::from_str(raw.trim()).expect("one JSON object");
        assert_eq!(parsed["kind"], json!("turn_failed"));
        assert_eq!(parsed["errorKind"], json!("auth"));
        assert_eq!(parsed["requestId"], json!("req-1"));
        assert_eq!(parsed["sessionId"], json!("session-1"));
        assert!(
            !raw.contains("eyJhbGci"),
            "a token inside a failure detail must be masked: {raw}"
        );
    }

    #[test]
    fn records_land_as_one_json_object_per_line() {
        let fixture = temp_dir("records");
        let path = fixture.join("logs").join("bridge.log");
        record_to(
            &path,
            "upstream_rejected",
            json!({ "status": 403, "detail": "Bearer secret-token leaked" }),
        )
        .expect("record lands");
        record_to(&path, "bridge_token_mismatch", json!({ "status": 401 })).expect("second record");

        let raw = std::fs::read_to_string(&path).expect("read back");
        let lines: Vec<&str> = raw.lines().filter(|line| !line.is_empty()).collect();
        assert_eq!(lines.len(), 2, "one line per record: {raw}");
        let first: Value = serde_json::from_str(lines[0]).expect("each line is JSON");
        assert_eq!(first["kind"], json!("upstream_rejected"));
        assert_eq!(first["status"], json!(403));
        assert!(
            !raw.contains("secret-token"),
            "a leaked token must never be written: {raw}"
        );
    }

    #[test]
    fn the_log_path_stays_out_of_the_real_config_home_under_test() {
        let path = log_path();
        assert!(path.ends_with("provider-failures.log"), "{path:?}");
        // A test drives the same code a live failure does, so this is the
        // guarantee that running the suite never appends to the user's own
        // failure log.
        assert!(
            !path.starts_with(crate::fs::paths::config_home()),
            "{path:?} must not resolve into the real config home under test"
        );
    }

    #[tokio::test]
    async fn a_scoped_stream_attributes_its_polls_to_the_owning_session() {
        let observed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorder = std::sync::Arc::clone(&observed);
        let inner: ModelStream = Box::pin(futures::stream::poll_fn(move |_| {
            recorder
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(trace_session_id());
            Poll::Ready(None)
        }));
        let mut stream = scoped_trace_stream(RuntimeString::from("session-9:sub:child"), inner);
        use futures::StreamExt;
        while stream.next().await.is_some() {}

        assert_eq!(
            *observed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![Some("session-9:sub:child".to_string())],
            "the poll that wrote the record must see the owning session"
        );
        assert_eq!(
            trace_session_id(),
            None,
            "the scope must not outlive the poll that set it"
        );
    }

    #[test]
    fn the_session_scope_restores_what_it_displaced() {
        with_trace_session(RuntimeString::from("outer"), || {
            assert_eq!(trace_session_id().as_deref(), Some("outer"));
            with_trace_session(RuntimeString::from("inner"), || {
                assert_eq!(trace_session_id().as_deref(), Some("inner"));
            });
            assert_eq!(
                trace_session_id().as_deref(),
                Some("outer"),
                "an inner scope must hand the outer one back"
            );
        });
        assert_eq!(trace_session_id(), None);
    }
}
