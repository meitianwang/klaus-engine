//! Stateful transport and routing affinity for the Codex subscription Responses
//! backend: load-balancer cookies, per-turn sticky routing tokens, responses-lite
//! wire headers, and persistent WebSocket sessions with `previous_response_id`
//! incremental input continuation and automatic HTTP SSE fallback.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use sha2::{Digest, Sha256};
use tokio_tungstenite::tungstenite::handshake::{client::generate_key, derive_accept_key};
use tokio_tungstenite::tungstenite::protocol::{Message as WsMessage, Role, WebSocketConfig};
use tokio_tungstenite::WebSocketStream;

use super::{
    clamp_cache_key_64, classify_error, diagnostic_string, encode_openai_request_body,
    finish_responses_state, incomplete_responses_error, map_responses_event,
    prompt_cache_key_mode_of, record_provider_trace, stop_reason_name, update_responses_usage, vs,
    AuthSource, Map, ModelCapabilities, ModelRequest, ModelStreamEvent, ProviderError,
    ProviderErrorKind, ProviderTraceRecord, RawProviderError, ResponsesState, RuntimeString, Value,
};
use crate::schemas::message::Message;
use crate::sync_ext::MutexExt;
use crate::types::abort::AbortSignal;
use crate::types::provider::ReasoningSummary;

const MAX_AFFINITY_COOKIE_BYTES: usize = 8 * 1024;
const DEFAULT_COOKIE_TTL: Duration = Duration::from_secs(60 * 60);
const MAX_WEBSOCKET_AGE: Duration = Duration::from_secs(55 * 60);
const WS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
const WS_UPGRADE_REJECT_COOLDOWN: Duration = Duration::from_secs(60);

const AFFINITY_COOKIE_NAMES: [&str; 2] = ["__oailb", "__cflb"];

const WS_RETRYABLE_RESET_CODES: [&str; 4] = [
    "previous_response_not_found",
    "invalid_previous_response_id",
    "websocket_connection_limit_reached",
    "connection_expired",
];

#[derive(Clone, Debug)]
struct StoredCookie {
    value: String,
    expires_at: Instant,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct UserTurnFingerprint {
    non_meta_user_count: usize,
    last_non_meta_user_uuid: RuntimeString,
}

#[derive(Clone, Debug)]
struct TurnStickyEntry {
    auth_scope: String,
    fingerprint: UserTurnFingerprint,
    turn_state: Option<String>,
}

#[derive(Clone, Debug)]
pub(super) struct LastResponseAnchor {
    pub(super) response_id: String,
    pub(super) props: Value,
    pub(super) request_input: Vec<Value>,
    pub(super) output_items: Vec<Value>,
}

struct CodexWsSession {
    socket: WebSocketStream<reqwest::Upgraded>,
    opened_at: Instant,
    origin: String,
    auth_scope: String,
    last_response: Option<LastResponseAnchor>,
}

#[derive(Default)]
pub(super) struct CodexTransportState {
    cookies: Mutex<HashMap<(String, String), HashMap<String, StoredCookie>>>,
    turn_states: Mutex<HashMap<String, TurnStickyEntry>>,
    ws_sessions: Mutex<HashMap<String, CodexWsSession>>,
    ws_disabled_until: Mutex<HashMap<String, Instant>>,
}

impl CodexTransportState {
    pub(super) fn capture_http_headers(
        &self,
        origin: &str,
        auth_scope: &str,
        session_key: Option<&str>,
        headers: &HeaderMap,
    ) {
        self.capture_cookies(origin, auth_scope, headers);
        if let Some(session_key) = session_key.filter(|key| !key.is_empty()) {
            if let Some(token) = headers
                .get("x-codex-turn-state")
                .and_then(|val| val.to_str().ok())
                .map(str::trim)
                .filter(|val| !val.is_empty())
            {
                self.record_turn_state(session_key, auth_scope, token);
            }
        }
    }

    fn capture_cookies(&self, origin: &str, auth_scope: &str, headers: &HeaderMap) {
        let now = Instant::now();
        let mut updates: Vec<(String, Option<StoredCookie>)> = Vec::new();
        for raw in headers.get_all(reqwest::header::SET_COOKIE) {
            let Ok(line) = raw.to_str() else {
                continue;
            };
            if let Some((name, entry)) = parse_affinity_set_cookie(line, now) {
                updates.push((name, entry));
            }
        }
        if updates.is_empty() {
            return;
        }
        let mut guard = self.cookies.lock_recover();
        let jar = guard
            .entry((origin.to_string(), auth_scope.to_string()))
            .or_default();
        jar.retain(|_, cookie| cookie.expires_at > now);
        for (name, maybe_cookie) in updates {
            match maybe_cookie {
                Some(cookie) => {
                    jar.insert(name, cookie);
                }
                None => {
                    jar.remove(&name);
                }
            }
        }
    }

    pub(super) fn cookie_header_value(&self, origin: &str, auth_scope: &str) -> Option<String> {
        let now = Instant::now();
        let mut guard = self.cookies.lock_recover();
        let jar = guard.get_mut(&(origin.to_string(), auth_scope.to_string()))?;
        jar.retain(|_, cookie| cookie.expires_at > now);
        let mut parts = Vec::new();
        for name in AFFINITY_COOKIE_NAMES {
            if let Some(cookie) = jar.get(name) {
                parts.push(format!("{name}={}", cookie.value));
            }
        }
        if parts.is_empty() {
            return None;
        }
        let joined = parts.join("; ");
        (joined.len() <= MAX_AFFINITY_COOKIE_BYTES).then_some(joined)
    }

    pub(super) fn prepare_turn_state(
        &self,
        messages: &[Message],
        auth_scope: &str,
        session_key: Option<&str>,
    ) -> Option<String> {
        let session_key = session_key.filter(|key| !key.is_empty())?;
        let fingerprint = user_turn_fingerprint(messages);
        let mut guard = self.turn_states.lock_recover();
        match guard.get_mut(session_key) {
            Some(entry) if entry.auth_scope == auth_scope && entry.fingerprint == fingerprint => {
                entry.turn_state.clone()
            }
            _ => {
                guard.insert(
                    session_key.to_string(),
                    TurnStickyEntry {
                        auth_scope: auth_scope.to_string(),
                        fingerprint,
                        turn_state: None,
                    },
                );
                None
            }
        }
    }

    pub(super) fn current_turn_state(
        &self,
        auth_scope: &str,
        session_key: Option<&str>,
    ) -> Option<String> {
        let session_key = session_key.filter(|key| !key.is_empty())?;
        let guard = self.turn_states.lock_recover();
        guard
            .get(session_key)
            .filter(|entry| entry.auth_scope == auth_scope)
            .and_then(|entry| entry.turn_state.clone())
    }

    pub(super) fn record_turn_state(&self, session_key: &str, auth_scope: &str, token: &str) {
        if session_key.is_empty() || token.trim().is_empty() {
            return;
        }
        let mut guard = self.turn_states.lock_recover();
        if let Some(entry) = guard.get_mut(session_key) {
            if entry.auth_scope == auth_scope && entry.turn_state.is_none() {
                entry.turn_state = Some(token.trim().to_string());
            }
        }
    }

    pub(super) fn capture_ws_event_turn_state(
        &self,
        session_key: Option<&str>,
        auth_scope: &str,
        ev: &Value,
    ) {
        let Some(session_key) = session_key.filter(|key| !key.is_empty()) else {
            return;
        };
        let candidate = ev
            .get("headers")
            .and_then(|headers| {
                headers
                    .get("x-codex-turn-state")
                    .or_else(|| headers.get("X-Codex-Turn-State"))
            })
            .and_then(Value::as_str)
            .or_else(|| ev.get("x-codex-turn-state").and_then(Value::as_str))
            .or_else(|| {
                ev.get("response")
                    .and_then(|resp| resp.get("headers"))
                    .and_then(|headers| {
                        headers
                            .get("x-codex-turn-state")
                            .or_else(|| headers.get("X-Codex-Turn-State"))
                    })
                    .and_then(Value::as_str)
            });
        if let Some(token) = candidate {
            if let Ok(token_str) = token.try_to_utf8() {
                self.record_turn_state(session_key, auth_scope, &token_str);
            }
        }
    }

    pub(super) fn invalidate_session_anchor(&self, session_key: Option<&str>) {
        let Some(session_key) = session_key.filter(|key| !key.is_empty()) else {
            return;
        };
        if let Some(session) = self.ws_sessions.lock_recover().get_mut(session_key) {
            session.last_response = None;
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_codex_request_headers(
        &self,
        headers: &mut HashMap<String, String>,
        req: &ModelRequest,
        body: &Value,
        origin: &str,
        auth_scope: &str,
        responses_lite: bool,
        turn_state: Option<&str>,
    ) {
        let affinity_id = routing_thread_key(req);
        if let Some(affinity_id) = &affinity_id {
            headers.insert("session-id".to_string(), affinity_id.clone());
            headers.insert("thread-id".to_string(), affinity_id.clone());
            headers.insert("x-client-request-id".to_string(), affinity_id.clone());
        }
        if !headers.contains_key("x-codex-guardian") {
            let model = diagnostic_string(&req.model);
            if !model.is_empty() {
                let mut hint = format!("model={model}");
                if let Some(tier) = body
                    .get("service_tier")
                    .and_then(Value::as_str)
                    .and_then(|tier| tier.try_to_utf8().ok())
                    .filter(|tier| !tier.is_empty())
                {
                    hint.push_str(";tier=");
                    hint.push_str(&tier);
                }
                headers.insert("x-codex-routing-hint".to_string(), hint);
            }
        }
        if responses_lite {
            headers.insert(
                "x-openai-internal-codex-responses-lite".to_string(),
                "true".to_string(),
            );
        }
        // A Responses reply echoes the request's tool catalog back by default.
        // The engine drives the API statelessly and never reads the echo, so
        // ask the backend to omit it; on tool-heavy requests this trims a
        // fixed per-response payload that scales with the declaration size.
        headers
            .entry("x-codex-exclude".to_string())
            .or_insert_with(|| "tools".to_string());
        if let Some(token) = turn_state.filter(|token| !token.is_empty()) {
            headers.insert("x-codex-turn-state".to_string(), token.to_string());
        }
        if let Some(cookie) = self.cookie_header_value(origin, auth_scope) {
            match headers.get_mut("cookie") {
                Some(existing) if !existing.is_empty() => {
                    existing.push_str("; ");
                    existing.push_str(&cookie);
                }
                _ => {
                    headers.insert("cookie".to_string(), cookie);
                }
            }
        }
    }

    pub(super) fn should_use_websocket(
        &self,
        url: &str,
        origin: &str,
        force_ws: Option<bool>,
        has_interceptor: bool,
        review_stream: bool,
        req: &ModelRequest,
    ) -> bool {
        if has_interceptor || review_stream {
            return false;
        }
        if force_ws == Some(false) {
            return false;
        }
        // Only persistent session/subagent streams use WebSocket; ephemeral
        // prediction or precompute calls leave `transport_session_id` unset so
        // they never disturb the session's continuation anchor.
        let has_transport_session = req
            .wire_caps
            .as_ref()
            .and_then(|caps| caps.transport_session_id.as_ref())
            .is_some_and(|id| !id.is_empty());
        if !has_transport_session && force_ws != Some(true) {
            return false;
        }
        let scheme_allowed = url.starts_with("https://")
            || url.starts_with("wss://")
            || url.starts_with("ws://")
            || (force_ws == Some(true) && url.starts_with("http://"));
        if !scheme_allowed {
            return false;
        }
        let now = Instant::now();
        let mut disabled = self.ws_disabled_until.lock_recover();
        if let Some(until) = disabled.get(origin).copied() {
            if now < until {
                return false;
            }
            disabled.remove(origin);
        }
        true
    }

    fn disable_ws_temporarily(&self, origin: &str) {
        self.ws_disabled_until.lock_recover().insert(
            origin.to_string(),
            Instant::now() + WS_UPGRADE_REJECT_COOLDOWN,
        );
    }

    fn take_ws_session(
        &self,
        session_key: &str,
        origin: &str,
        auth_scope: &str,
    ) -> Option<CodexWsSession> {
        let session = self.ws_sessions.lock_recover().remove(session_key)?;
        if session.origin != origin
            || session.auth_scope != auth_scope
            || session.opened_at.elapsed() >= MAX_WEBSOCKET_AGE
        {
            return None;
        }
        Some(session)
    }

    fn put_ws_session(&self, session_key: String, session: CodexWsSession) {
        if session.opened_at.elapsed() < MAX_WEBSOCKET_AGE {
            self.ws_sessions.lock_recover().insert(session_key, session);
        }
    }
}

pub(super) fn effective_reasoning_summary(
    req: &ModelRequest,
    caps: &ModelCapabilities,
) -> ReasoningSummary {
    let explicit = req
        .wire_caps
        .as_ref()
        .is_some_and(|policy| policy.reasoning_summary_explicit);
    if explicit || req.reasoning_summary != ReasoningSummary::Auto {
        return req.reasoning_summary;
    }
    req.wire_caps
        .as_ref()
        .and_then(|policy| policy.default_reasoning_summary)
        .or(caps.default_reasoning_summary)
        .unwrap_or(ReasoningSummary::Auto)
}

pub(super) fn responses_lite_enabled(
    req: &ModelRequest,
    caps: &ModelCapabilities,
    codex_backend: bool,
) -> bool {
    if !codex_backend {
        return false;
    }
    req.wire_caps
        .as_ref()
        .and_then(|policy| policy.responses_lite)
        .or(caps.responses_lite)
        .unwrap_or(false)
}

pub(super) fn adapt_codex_responses_request(
    body: &mut Value,
    req: &ModelRequest,
    caps: &ModelCapabilities,
    responses_lite: bool,
) {
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    obj.remove("max_output_tokens");
    obj.remove("prompt_cache_retention");
    obj.remove("prompt_cache_options");
    // Subscription requests do not bill cache writes separately, so `prompt_cache_key`
    // is purely the shard-routing key and must remain present even for detached
    // forks, predictions, and background precompute queries.
    if prompt_cache_key_mode_of(req, caps) != "disabled" {
        if let Some(key) = req.cache_domain.as_ref().or(req.cache_key.as_ref()) {
            obj.insert("prompt_cache_key", vs(clamp_cache_key_64(key)));
        }
    }
    if responses_lite {
        // Responses-Lite carries tool declarations inside `input[0]` (`additional_tools`)
        // and rejects requests unless `parallel_tool_calls` is explicitly `false`.
        obj.insert("parallel_tool_calls", Value::Bool(false));
        if let Some(reasoning) = obj.get_mut("reasoning").and_then(Value::as_object_mut) {
            reasoning.insert("context", vs("all_turns"));
        }
    }
}

pub(super) fn transport_session_key(req: &ModelRequest) -> Option<String> {
    req.wire_caps
        .as_ref()
        .and_then(|policy| policy.transport_session_id.as_ref())
        .or(req.cache_domain.as_ref())
        .or(req.cache_key.as_ref())
        .map(diagnostic_string)
        .filter(|value| !value.is_empty())
}

pub(super) fn routing_thread_key(req: &ModelRequest) -> Option<String> {
    req.cache_domain
        .as_ref()
        .or(req.cache_key.as_ref())
        .or_else(|| {
            req.wire_caps
                .as_ref()
                .and_then(|policy| policy.transport_session_id.as_ref())
        })
        .map(diagnostic_string)
        .filter(|value| !value.is_empty())
}

pub(super) async fn resolve_codex_auth_scope(
    auth: Option<&Arc<dyn AuthSource>>,
    api_key: Option<&str>,
    signal: &AbortSignal,
) -> (String, Option<HashMap<String, String>>) {
    if let Some(auth) = auth {
        if let Ok(headers) = auth.get_headers(signal).await {
            let account = headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("chatgpt-account-id"))
                .map(|(_, v)| v.as_str())
                .or_else(|| {
                    headers
                        .iter()
                        .find(|(k, _)| k.eq_ignore_ascii_case("authorization"))
                        .map(|(_, v)| v.as_str())
                })
                .unwrap_or("oauth");
            let scope = format!("{:x}", Sha256::digest(account.as_bytes()));
            return (scope, Some(headers));
        }
        if let Some(account_id) = auth.credential_account_id() {
            let scope = format!("{:x}", Sha256::digest(account_id.as_bytes()));
            return (scope, None);
        }
        return ("oauth-unresolved".to_string(), None);
    }
    if let Some(key) = api_key {
        let scope = format!("{:x}", Sha256::digest(key.as_bytes()));
        return (scope, None);
    }
    ("anonymous".to_string(), None)
}

fn user_turn_fingerprint(messages: &[Message]) -> UserTurnFingerprint {
    let mut non_meta_user_count = 0usize;
    let mut last_non_meta_user_uuid = RuntimeString::default();
    for msg in messages {
        if let Message::User(user) = msg {
            if user.is_meta != Some(true) {
                non_meta_user_count += 1;
                last_non_meta_user_uuid = user.uuid.clone();
            }
        }
    }
    UserTurnFingerprint {
        non_meta_user_count,
        last_non_meta_user_uuid,
    }
}

fn parse_affinity_set_cookie(line: &str, now: Instant) -> Option<(String, Option<StoredCookie>)> {
    let mut segments = line.split(';');
    let first = segments.next()?.trim();
    let (name, value) = first.split_once('=')?;
    let name = name.trim();
    if !AFFINITY_COOKIE_NAMES.contains(&name) {
        return None;
    }
    let value = value.trim();
    if value.len() > MAX_AFFINITY_COOKIE_BYTES {
        return None;
    }
    let mut ttl = DEFAULT_COOKIE_TTL;
    let mut expired = value.is_empty();
    for attr in segments {
        let attr = attr.trim();
        if let Some((k, v)) = attr.split_once('=') {
            if k.trim().eq_ignore_ascii_case("max-age") {
                if let Ok(secs) = v.trim().parse::<i64>() {
                    if secs <= 0 {
                        expired = true;
                    } else {
                        ttl = Duration::from_secs(secs as u64);
                    }
                }
            }
        }
    }
    if expired {
        return Some((name.to_string(), None));
    }
    let expires_at = now.checked_add(ttl).unwrap_or(now + DEFAULT_COOKIE_TTL);
    Some((
        name.to_string(),
        Some(StoredCookie {
            value: value.to_string(),
            expires_at,
        }),
    ))
}

pub(super) fn response_create_props(full_body: &Value) -> Value {
    let mut props = Map::new();
    if let Some(obj) = full_body.as_object() {
        for (key, value) in obj.entries() {
            let Ok(key_str) = key.try_to_utf8() else {
                continue;
            };
            if matches!(
                key_str.as_str(),
                "input" | "stream" | "previous_response_id" | "type" | "client_metadata"
            ) {
                continue;
            }
            props.insert(key.clone(), value.clone());
        }
    }
    Value::Object(props)
}

pub(super) fn build_ws_response_create_payload(
    full_body: &Value,
    anchor: Option<&LastResponseAnchor>,
    session_id: Option<&str>,
    thread_id: Option<&str>,
    turn_state: Option<&str>,
    responses_lite: bool,
) -> (Value, bool) {
    let full_input = full_body
        .get("input")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let current_props = response_create_props(full_body);
    let incremental = anchor.and_then(|prev| {
        if prev.response_id.is_empty() || prev.props != current_props {
            return None;
        }
        let req_len = prev.request_input.len();
        let expected_replayed = filter_replayed_server_outputs(&prev.output_items);
        let out_len = expected_replayed.len();
        let prefix_len = req_len + out_len;
        if full_input.len() < prefix_len {
            return None;
        }
        if full_input[..req_len] != prev.request_input[..] {
            return None;
        }
        for (server_item, replayed_item) in expected_replayed
            .iter()
            .zip(&full_input[req_len..prefix_len])
        {
            if !output_item_matches_replayed_input(server_item, replayed_item) {
                return None;
            }
        }
        Some((prev.response_id.clone(), full_input[prefix_len..].to_vec()))
    });

    let mut frame = Map::new();
    frame.insert("type", vs("response.create"));
    if let Some(obj) = full_body.as_object() {
        // Keep routing-critical fields near the front of the JSON object.
        for priority_key in [
            "model",
            "stream",
            "store",
            "prompt_cache_key",
            "service_tier",
            "reasoning",
            "include",
            "text",
            "instructions",
            "tools",
            "tool_choice",
            "parallel_tool_calls",
        ] {
            if let Some(val) = obj.get(priority_key) {
                frame.insert(priority_key, val.clone());
            }
        }
        for (key, val) in obj.entries() {
            let Ok(k) = key.try_to_utf8() else {
                continue;
            };
            if k == "input" || frame.contains_key(k.as_str()) {
                continue;
            }
            frame.insert(key.clone(), val.clone());
        }
    }
    let used_incremental = if let Some((prev_id, delta_input)) = incremental {
        frame.insert("previous_response_id", vs(prev_id));
        frame.insert("input", Value::Array(delta_input));
        true
    } else {
        frame.insert("input", Value::Array(full_input));
        false
    };

    let mut metadata = Map::new();
    if let Some(sid) = session_id.filter(|s| !s.is_empty()) {
        metadata.insert("session_id", vs(sid));
    }
    if let Some(tid) = thread_id.filter(|s| !s.is_empty()) {
        metadata.insert("thread_id", vs(tid));
    }
    if let Some(ts) = turn_state.filter(|s| !s.is_empty()) {
        metadata.insert("x-codex-turn-state", vs(ts));
    }
    if responses_lite {
        metadata.insert(
            "ws_request_header_x_openai_internal_codex_responses_lite",
            vs("true"),
        );
    }
    if !metadata.is_empty() {
        frame.insert("client_metadata", Value::Object(metadata));
    }

    (Value::Object(frame), used_incremental)
}

fn filter_replayed_server_outputs(output_items: &[Value]) -> Vec<Value> {
    let mut kept = Vec::new();
    for (idx, item) in output_items.iter().enumerate() {
        let Some(kind) = item
            .get("type")
            .and_then(Value::as_str)
            .and_then(|s| s.try_to_utf8().ok())
        else {
            continue;
        };
        match kind.as_str() {
            "reasoning" => {
                let has_encrypted = item
                    .get("encrypted_content")
                    .and_then(Value::as_str)
                    .is_some_and(|enc| !enc.is_empty());
                if !has_encrypted {
                    continue;
                }
                let followed = output_items[idx + 1..]
                    .iter()
                    .any(is_non_empty_output_follower);
                if followed {
                    kept.push(item.clone());
                }
            }
            "message" => {
                if !extract_message_text(item).is_empty() {
                    kept.push(item.clone());
                }
            }
            "function_call" | "custom_tool_call" | "tool_search_call" => {
                kept.push(item.clone());
            }
            "tool_search_output" => {
                // Only provider-executed (`server`) tool_search_output items are
                // recorded on the assistant message and replayed before client outputs.
                let is_server = item
                    .get("execution")
                    .and_then(Value::as_str)
                    .is_none_or(|exec| exec != "client");
                if is_server {
                    kept.push(item.clone());
                }
            }
            _ => {}
        }
    }
    kept
}

fn is_non_empty_output_follower(item: &Value) -> bool {
    let Some(kind) = item
        .get("type")
        .and_then(Value::as_str)
        .and_then(|s| s.try_to_utf8().ok())
    else {
        return false;
    };
    match kind.as_str() {
        "function_call" | "custom_tool_call" | "tool_search_call" => true,
        "message" => !extract_message_text(item).is_empty(),
        _ => false,
    }
}

fn extract_message_text(item: &Value) -> RuntimeString {
    if let Some(s) = item.get("content").and_then(Value::as_str) {
        return s.clone();
    }
    if let Some(arr) = item.get("content").and_then(Value::as_array) {
        return arr
            .iter()
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .fold(RuntimeString::default(), |acc, part| acc.concat(part));
    }
    RuntimeString::default()
}

pub(super) fn output_item_matches_replayed_input(
    server_item: &Value,
    replayed_item: &Value,
) -> bool {
    let server_type = server_item
        .get("type")
        .and_then(Value::as_str)
        .and_then(|s| s.try_to_utf8().ok());
    let replayed_type = replayed_item
        .get("type")
        .and_then(Value::as_str)
        .and_then(|s| s.try_to_utf8().ok());
    match server_type.as_deref() {
        Some("reasoning") => {
            replayed_type.as_deref() == Some("reasoning")
                && server_item.get("encrypted_content") == replayed_item.get("encrypted_content")
        }
        Some("message") => {
            let replayed_is_assistant_msg = (replayed_type.is_none()
                || replayed_type.as_deref() == Some("message"))
                && replayed_item
                    .get("role")
                    .and_then(Value::as_str)
                    .is_some_and(|role| role == "assistant");
            replayed_is_assistant_msg
                && extract_message_text(server_item) == extract_message_text(replayed_item)
        }
        Some("function_call") => {
            if replayed_type.as_deref() != Some("function_call") {
                return false;
            }
            let server_call_id = server_item
                .get("call_id")
                .or_else(|| server_item.get("id"))
                .and_then(Value::as_str);
            let replayed_call_id = replayed_item.get("call_id").and_then(Value::as_str);
            if server_call_id != replayed_call_id
                || server_item.get("name") != replayed_item.get("name")
                || server_item.get("namespace") != replayed_item.get("namespace")
            {
                return false;
            }
            json_strings_semantically_equal(
                server_item.get("arguments").and_then(Value::as_str),
                replayed_item.get("arguments").and_then(Value::as_str),
            )
        }
        Some("custom_tool_call") => {
            replayed_type.as_deref() == Some("custom_tool_call")
                && server_item.get("call_id").or_else(|| server_item.get("id"))
                    == replayed_item.get("call_id")
                && server_item.get("name") == replayed_item.get("name")
                && server_item.get("namespace") == replayed_item.get("namespace")
                && server_item.get("input") == replayed_item.get("input")
        }
        Some("tool_search_call") => {
            replayed_type.as_deref() == Some("tool_search_call")
                && server_item.get("call_id").or_else(|| server_item.get("id"))
                    == replayed_item.get("call_id")
        }
        Some("tool_search_output") => {
            replayed_type.as_deref() == Some("tool_search_output")
                && server_item.get("call_id") == replayed_item.get("call_id")
        }
        _ => server_item == replayed_item,
    }
}

fn json_strings_semantically_equal(
    left: Option<&RuntimeString>,
    right: Option<&RuntimeString>,
) -> bool {
    match (left, right) {
        (Some(l), Some(r)) => {
            if l == r {
                return true;
            }
            match (super::runtime_json::parse(l), super::runtime_json::parse(r)) {
                (Ok(lv), Ok(rv)) => lv == rv,
                _ => false,
            }
        }
        (None, None) => true,
        _ => false,
    }
}

fn http_upgrade_url(url: &str) -> String {
    if let Some(rest) = url.strip_prefix("wss://") {
        format!("https://{rest}")
    } else if let Some(rest) = url.strip_prefix("ws://") {
        format!("http://{rest}")
    } else {
        url.to_string()
    }
}

#[allow(clippy::too_many_arguments)]
async fn open_codex_websocket(
    client: &reqwest::Client,
    url: &str,
    origin: &str,
    auth_scope: &str,
    session_key: &str,
    extra_headers: &HashMap<String, String>,
    auth_headers: Option<&HashMap<String, String>>,
    api_key: Option<&str>,
    transport_state: &CodexTransportState,
    signal: &AbortSignal,
) -> Result<CodexWsSession, (ProviderError, bool)> {
    let upgrade_url = http_upgrade_url(url);
    let mut header_map = HeaderMap::new();
    header_map.insert(
        "openai-beta",
        HeaderValue::from_static("responses_websockets=2026-02-06"),
    );
    let challenge = generate_key();
    header_map.insert("connection", HeaderValue::from_static("Upgrade"));
    header_map.insert("upgrade", HeaderValue::from_static("websocket"));
    header_map.insert("sec-websocket-version", HeaderValue::from_static("13"));
    if let Ok(val) = HeaderValue::from_str(&challenge) {
        header_map.insert("sec-websocket-key", val);
    }
    for (name, value) in extra_headers {
        if let (Ok(hn), Ok(hv)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            header_map.insert(hn, hv);
        }
    }
    if let Some(auth_headers) = auth_headers {
        for (name, value) in auth_headers {
            if let (Ok(hn), Ok(hv)) = (
                HeaderName::from_bytes(name.as_bytes()),
                HeaderValue::from_str(value),
            ) {
                header_map.insert(hn, hv);
            }
        }
    } else if let Some(key) = api_key {
        if let Ok(hv) = HeaderValue::from_str(&format!("Bearer {key}")) {
            header_map.insert(reqwest::header::AUTHORIZATION, hv);
        }
    }

    let connect = async {
        let response = client
            .get(&upgrade_url)
            .version(reqwest::Version::HTTP_11)
            .headers(header_map)
            .send()
            .await
            .map_err(|err| {
                (
                    classify_error(&RawProviderError {
                        message: err.to_string(),
                        ..Default::default()
                    }),
                    false,
                )
            })?;

        let status = response.status();
        transport_state.capture_http_headers(
            origin,
            auth_scope,
            Some(session_key),
            response.headers(),
        );
        if status != reqwest::StatusCode::SWITCHING_PROTOCOLS {
            let disable_ws = matches!(status.as_u16(), 400 | 404 | 405 | 426 | 501);
            let body_text = response.text().await.unwrap_or_default();
            return Err((
                classify_error(&RawProviderError {
                    status: Some(status.as_u16()),
                    message: if body_text.is_empty() {
                        format!("WebSocket upgrade failed with HTTP {}", status.as_u16())
                    } else {
                        body_text
                    },
                    ..Default::default()
                }),
                disable_ws,
            ));
        }

        let valid_upgrade = response
            .headers()
            .get("upgrade")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.eq_ignore_ascii_case("websocket"));
        let expected_accept = derive_accept_key(challenge.as_bytes());
        let valid_accept = response
            .headers()
            .get("sec-websocket-accept")
            .and_then(|v| v.to_str().ok())
            == Some(expected_accept.as_str());
        if !valid_upgrade || !valid_accept {
            return Err((
                ProviderError {
                    kind: ProviderErrorKind::Network,
                    message: "Invalid WebSocket upgrade handshake headers".into(),
                    ..super::aborted_error()
                },
                true,
            ));
        }

        let upgraded = response.upgrade().await.map_err(|err| {
            (
                classify_error(&RawProviderError {
                    message: err.to_string(),
                    ..Default::default()
                }),
                false,
            )
        })?;
        let ws_config = WebSocketConfig {
            max_message_size: Some(64 * 1024 * 1024),
            max_frame_size: Some(16 * 1024 * 1024),
            ..Default::default()
        };
        let socket =
            WebSocketStream::from_raw_socket(upgraded, Role::Client, Some(ws_config)).await;
        Ok(CodexWsSession {
            socket,
            opened_at: Instant::now(),
            origin: origin.to_string(),
            auth_scope: auth_scope.to_string(),
            last_response: None,
        })
    };

    tokio::select! {
        _ = signal.cancelled() => Err((super::aborted_error(), false)),
        res = tokio::time::timeout(WS_HANDSHAKE_TIMEOUT, connect) => match res {
            Ok(inner) => inner,
            Err(_) => Err((
                ProviderError {
                    kind: ProviderErrorKind::Network,
                    message: "Responses WebSocket connection timed out".into(),
                    ..super::aborted_error()
                },
                false,
            )),
        }
    }
}

fn is_ws_reset_retryable_error(ev: &Value) -> bool {
    let code = ev
        .get("code")
        .and_then(Value::as_str)
        .or_else(|| {
            ev.get("error")
                .and_then(|err| err.get("code"))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            ev.get("response")
                .and_then(|r| r.get("error"))
                .and_then(|err| err.get("code"))
                .and_then(Value::as_str)
        })
        .and_then(|c| c.try_to_utf8().ok());
    if let Some(code) = code {
        if WS_RETRYABLE_RESET_CODES
            .iter()
            .any(|candidate| code.eq_ignore_ascii_case(candidate))
        {
            return true;
        }
    }
    let status = ev.get("status").and_then(Value::as_u64).or_else(|| {
        ev.get("error")
            .and_then(|err| err.get("status"))
            .and_then(Value::as_u64)
    });
    status == Some(404)
}

pub(super) enum WsStreamOutcome {
    Completed,
    FallbackToHttp,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn drive_codex_websocket_turn(
    transport_state: Arc<CodexTransportState>,
    client: reqwest::Client,
    url: String,
    origin: String,
    auth: Option<Arc<dyn AuthSource>>,
    api_key: Option<String>,
    req: ModelRequest,
    full_body: Value,
    extra_headers: HashMap<String, String>,
    review_stream: bool,
    started: Instant,
    signal: AbortSignal,
    tx: tokio::sync::mpsc::UnboundedSender<ModelStreamEvent>,
) -> WsStreamOutcome {
    let Some(session_key) = transport_session_key(&req) else {
        return WsStreamOutcome::FallbackToHttp;
    };
    let thread_id = routing_thread_key(&req);
    let (auth_scope, resolved_auth_headers) =
        resolve_codex_auth_scope(auth.as_ref(), api_key.as_deref(), &signal).await;
    if signal.is_cancelled() {
        return WsStreamOutcome::Completed;
    }

    let mut ws_session = transport_state.take_ws_session(&session_key, &origin, &auth_scope);
    let mut allow_full_retry = true;

    loop {
        if signal.is_cancelled() {
            return WsStreamOutcome::Completed;
        }
        if ws_session.is_none() {
            let mut handshake_headers = extra_headers.clone();
            let current_ts = transport_state.current_turn_state(&auth_scope, Some(&session_key));
            if let Some(ts) = current_ts.as_deref() {
                handshake_headers.insert("x-codex-turn-state".to_string(), ts.to_string());
            }
            if let Some(cookie) = transport_state.cookie_header_value(&origin, &auth_scope) {
                handshake_headers.insert("cookie".to_string(), cookie);
            }
            match open_codex_websocket(
                &client,
                &url,
                &origin,
                &auth_scope,
                &session_key,
                &handshake_headers,
                resolved_auth_headers.as_ref(),
                api_key.as_deref(),
                &transport_state,
                &signal,
            )
            .await
            {
                Ok(opened) => {
                    ws_session = Some(opened);
                }
                Err((_err, disable_ws)) => {
                    if disable_ws {
                        transport_state.disable_ws_temporarily(&origin);
                    }
                    return WsStreamOutcome::FallbackToHttp;
                }
            }
        }

        let mut active = ws_session.take().expect("checked Some above");
        let turn_state = transport_state.current_turn_state(&auth_scope, Some(&session_key));
        let responses_lite = extra_headers
            .get("x-openai-internal-codex-responses-lite")
            .is_some_and(|v| v == "true");
        let (frame_body, used_incremental) = build_ws_response_create_payload(
            &full_body,
            active.last_response.as_ref(),
            thread_id.as_deref().or(Some(&session_key)),
            thread_id.as_deref(),
            turn_state.as_deref(),
            responses_lite,
        );
        let encoded_bytes = encode_openai_request_body(&frame_body);
        let Ok(encoded_text) = String::from_utf8(encoded_bytes) else {
            return WsStreamOutcome::FallbackToHttp;
        };

        let send_res = tokio::select! {
            _ = signal.cancelled() => return WsStreamOutcome::Completed,
            res = active.socket.send(WsMessage::Text(encoded_text)) => res,
        };
        if send_res.is_err() {
            // Stale idle socket closed before any frame was read: reconnect once
            // or fall back cleanly to HTTP SSE.
            if allow_full_retry {
                allow_full_retry = false;
                continue;
            }
            return WsStreamOutcome::FallbackToHttp;
        }

        let mut state = ResponsesState {
            review_continuations: review_stream,
            ..Default::default()
        };
        let mut mapped_any = false;
        let mut emitted_downstream = false;
        let mut pending_tool_stops = Vec::new();
        let mut completed_output_items: Vec<Value> = Vec::new();
        let mut response_id = String::new();
        let mut retry_without_anchor = false;
        let mut reuse_active_on_retry = false;
        let mut fallback_to_http = false;

        'frames: loop {
            let next_msg = tokio::select! {
                _ = signal.cancelled() => return WsStreamOutcome::Completed,
                msg = active.socket.next() => msg,
            };
            let raw_text = match next_msg {
                Some(Ok(WsMessage::Text(text))) => RuntimeString::from(text.as_str()),
                Some(Ok(WsMessage::Binary(bytes))) => match String::from_utf8(bytes.to_vec()) {
                    Ok(text) => RuntimeString::from(text),
                    Err(_) => continue,
                },
                Some(Ok(WsMessage::Ping(payload))) => {
                    let _ = active.socket.send(WsMessage::Pong(payload)).await;
                    continue;
                }
                Some(Ok(WsMessage::Pong(_))) | Some(Ok(WsMessage::Frame(_))) => continue,
                Some(Ok(WsMessage::Close(_))) | None => {
                    if !emitted_downstream {
                        if allow_full_retry {
                            allow_full_retry = false;
                            retry_without_anchor = true;
                        } else {
                            fallback_to_http = true;
                        }
                    }
                    break 'frames;
                }
                Some(Err(err)) => {
                    if signal.is_cancelled() {
                        return WsStreamOutcome::Completed;
                    }
                    if !emitted_downstream {
                        if allow_full_retry {
                            allow_full_retry = false;
                            retry_without_anchor = true;
                        } else {
                            fallback_to_http = true;
                        }
                        break 'frames;
                    }
                    let error = classify_error(&RawProviderError {
                        message: err.to_string(),
                        ..Default::default()
                    });
                    let _ = tx.send(ModelStreamEvent::Error {
                        error: if review_stream {
                            error.mark_stream_interrupted()
                        } else {
                            error
                        },
                    });
                    return WsStreamOutcome::Completed;
                }
            };

            let Ok(evj) = super::runtime_json::parse(&raw_text) else {
                continue;
            };
            transport_state.capture_ws_event_turn_state(Some(&session_key), &auth_scope, &evj);

            let ev_type = evj
                .get("type")
                .and_then(Value::as_str)
                .and_then(|s| s.try_to_utf8().ok())
                .unwrap_or_default();

            if matches!(ev_type.as_str(), "error" | "response.failed")
                && !emitted_downstream
                && (used_incremental || allow_full_retry)
                && is_ws_reset_retryable_error(&evj)
            {
                active.last_response = None;
                let code_str = evj
                    .get("code")
                    .or_else(|| evj.get("error").and_then(|e| e.get("code")))
                    .and_then(Value::as_str)
                    .and_then(|c| c.try_to_utf8().ok())
                    .unwrap_or_default();
                let reconnect_needed =
                    code_str.contains("connection") || code_str.contains("limit");
                if allow_full_retry {
                    allow_full_retry = false;
                    retry_without_anchor = true;
                    reuse_active_on_retry = !reconnect_needed;
                    break 'frames;
                }
            }

            if matches!(
                ev_type.as_str(),
                "response.created" | "response.in_progress" | "response.completed"
            ) {
                if let Some(id) = evj
                    .get("response")
                    .and_then(|r| r.get("id"))
                    .and_then(Value::as_str)
                    .and_then(|s| s.try_to_utf8().ok())
                    .filter(|s| !s.is_empty())
                {
                    response_id = id;
                }
            }
            if ev_type == "response.output_item.done" {
                if let Some(item) = evj.get("item") {
                    completed_output_items.push(item.clone());
                }
            }

            if update_responses_usage(&evj, &mut state) {
                emitted_downstream = true;
                let _ = tx.send(ModelStreamEvent::UsageSnapshot {
                    usage: state.usage(),
                });
            }
            let (evs, terminate) = map_responses_event(&evj, &mut state);
            if !evs.is_empty() {
                mapped_any = true;
            }
            for e in evs {
                if let ModelStreamEvent::TextDelta { text } = &e {
                    state.visible_chars += text.as_utf16().len() as u64;
                }
                if matches!(e, ModelStreamEvent::ToolUseStop { .. }) {
                    pending_tool_stops.push(e);
                } else {
                    emitted_downstream = true;
                    let _ = tx.send(e);
                }
            }
            if terminate {
                return WsStreamOutcome::Completed;
            }
            if state.terminal_received {
                break 'frames;
            }
        }

        if retry_without_anchor {
            if reuse_active_on_retry {
                ws_session = Some(active);
            }
            continue;
        }
        if fallback_to_http {
            return WsStreamOutcome::FallbackToHttp;
        }

        if !state.terminal_received {
            let mut error =
                incomplete_responses_error(Some(&RuntimeString::from("missing_terminal_event")));
            if review_stream {
                error.code = None;
                error = error.mark_stream_interrupted();
            }
            let _ = tx.send(ModelStreamEvent::Error { error });
            return WsStreamOutcome::Completed;
        }

        for event in pending_tool_stops {
            let _ = tx.send(event);
        }

        let responses_model = req.model.try_to_utf8().unwrap_or_default();
        let responses_raw_stop = state
            .raw_stop_reason
            .as_ref()
            .and_then(|reason| reason.try_to_utf8().ok());
        record_provider_trace(
            crate::observability::provider_diagnostics::provider_trace_enabled(),
            &ProviderTraceRecord {
                api: "responses_ws",
                model: &responses_model,
                request_id: (!response_id.is_empty()).then_some(response_id.as_str()),
                raw_finish_reason: responses_raw_stop.as_deref(),
                saw_done: true,
                mapped_any,
                buffered: false,
                stop_reason: stop_reason_name(state.stop_reason),
                input_tokens: state.input_tokens,
                output_tokens: state.output_tokens,
                cache_read_tokens: state.cache_read,
                total_tokens: state.total_tokens,
                visible_chars: state.visible_chars,
                elapsed_ms: started.elapsed().as_millis(),
            },
        );

        for event in finish_responses_state(&state, &req.model) {
            let _ = tx.send(event);
        }

        if !response_id.is_empty() {
            let full_input = full_body
                .get("input")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            active.last_response = Some(LastResponseAnchor {
                response_id,
                props: response_create_props(&full_body),
                request_input: full_input,
                output_items: completed_output_items,
            });
        }
        transport_state.put_ws_session(session_key, active);
        return WsStreamOutcome::Completed;
    }
}
