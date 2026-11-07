//! Cursor account models behind the engine's ordinary provider contract.
//! The engine owns imported snapshots, browser login grants and its agent loop.
//! The host opens login pages or supplies a local snapshot; no external agent runs.

mod account;
mod auth;
mod bridge;
mod capabilities;
mod capability_step;
mod catalog;
mod credential;
mod diagnostics;
mod informational;
#[cfg(test)]
mod live_vision_probe_tests;
mod metadata;
mod responses;
mod step;
mod step_history;
#[cfg(test)]
pub(crate) mod tests;
#[cfg(test)]
mod transport_tests;

use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

pub use auth::{login_cancel, login_poll, login_start, status_live};
#[cfg(test)]
pub(crate) use capabilities::tests::{native_fixture, NativeReply};
pub use capabilities::{generate_audio, generate_image, generate_video, search};
pub(crate) use capabilities::{NativeResponse, Operation, Operations};
#[cfg(test)]
pub(crate) use capability_step::tests::fixture as native_step_fixture;
pub use catalog::{fetch_models, model_capabilities, read_models, resolved_model};
pub use credential::{adopt_credential, logout, status, usage};
pub(super) use diagnostics::rpc as diagnostic_request;

#[derive(Default)]
struct State {
    root: Option<PathBuf>,
    generation: u64,
    rejected: bool,
    catalog: Option<(std::time::Instant, Vec<Value>)>,
    catalog_refreshing: bool,
    account_retry_at: Option<std::time::Instant>,
    login: Option<auth::PendingFlow>,
}

fn state() -> &'static Mutex<State> {
    static STATE: OnceLock<Mutex<State>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(State::default()))
}

fn changes() -> &'static tokio::sync::watch::Sender<u64> {
    static CHANGES: OnceLock<tokio::sync::watch::Sender<u64>> = OnceLock::new();
    CHANGES.get_or_init(|| tokio::sync::watch::channel(0).0)
}

fn tool_mismatch_details(actual: &Value, expected: &Value) -> Value {
    let kind = |value: &Value| match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    };
    let known = [
        "args",
        "result",
        "name",
        "toolCallId",
        "providerIdentifier",
        "toolName",
        "serverIdentifier",
        "smartModeApprovalOnly",
        "skipApproval",
        "symbolicPathArguments",
        "path",
        "offset",
        "limit",
        "pages",
        "namespace",
        "server",
        "arguments",
        "mcpDetails",
        "description",
        "requestSmartModeApproval",
        "smartModeBlockReason",
        "include_line_numbers",
    ];
    let fields=known.iter().filter(|key|actual.get(**key).is_some()||expected.get(**key).is_some()).map(|key|json!({"field":key,"actualType":actual.get(*key).map(kind).unwrap_or("missing"),"expectedType":expected.get(*key).map(kind).unwrap_or("missing"),"equal":actual.get(*key)==expected.get(*key)})).collect::<Vec<_>>();
    let outer = match actual["toolName"].as_str() {
        Some("Read") => "Read",
        Some("CallDynamicTool") => "CallDynamicTool",
        Some("CallMcpTool") => "CallMcpTool",
        Some(_) => "other",
        None => "missing",
    };
    let unknown = |value: &Value| {
        value
            .as_object()
            .map(|map| {
                map.keys()
                    .filter(|key| !known.contains(&key.as_str()))
                    .count()
            })
            .unwrap_or(0)
    };
    json!({"actualType":kind(actual),"expectedType":kind(expected),"knownOuter":outer,"fields":fields,"actualUnknownFieldCount":unknown(actual),"expectedUnknownFieldCount":unknown(expected)})
}

fn tool_mismatch(site: &'static str, actual: &Value, expected: &Value) -> String {
    eprintln!(
        "[Cursor] native tool mismatch site={site} details={}",
        tool_mismatch_details(actual, expected)
    );
    "cursor_native_tool_mismatch".into()
}

fn invalidate(state: &mut State) {
    auth::cancel_pending(state);
    state.generation = state.generation.wrapping_add(1);
    state.catalog = None;
    state.catalog_refreshing = false;
    state.account_retry_at = None;
    state.rejected = false;
    changes().send_replace(state.generation);
}

pub fn set_config_root(root: &Path) {
    let mut state = state().lock().unwrap_or_else(|p| p.into_inner());
    if state.root.as_deref() != Some(root) {
        state.root = Some(root.to_path_buf());
        invalidate(&mut state);
    }
}

fn config_root(state: &State) -> PathBuf {
    state.root.clone().unwrap_or_else(|| {
        std::env::var_os("KLAUS_CONFIG_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(".klaus")
            })
    })
}

pub async fn lookup_model(key: &str) -> Option<Value> {
    let models = read_models(super::wire::QoderCatalogRead::Cached, false)
        .await
        .ok()?;
    let selected = models.iter().find(|model| model["key"] == key)?;
    Some(catalog::with_max_context(&models, selected).clone())
}

pub async fn resolve(selection: &str) -> Result<(Value, Value), String> {
    auth::ensure_valid().await?;
    credential::snapshot()?;
    let model = resolved_model(selection).await?;
    let (base_url, token) = bridge::ensure_started().await?;
    Ok((
        json!({"type":"openai", "api":"responses", "model":model, "baseUrl":base_url,"compatibility":{"responsesFunctionTools":true,"extraHeaders":{"X-Klaus-Opaque-Replay-Scope":"cursor"}}}),
        json!({"type":"static_bearer", "token":token}),
    ))
}

#[cfg(test)]
pub(crate) fn test_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[cfg(test)]
pub(crate) fn read_operation_generation_for_test() -> u64 {
    credential::snapshot().expect("fixture credential").1
}
