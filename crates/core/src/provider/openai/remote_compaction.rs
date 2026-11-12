//! Authenticated native checkpoint requests and exact replay.

use super::*;
use crate::schemas::message::{
    native_retained_item_tokens, native_retained_part_tokens, NativeCompactionCheckpoint,
    NativeRetainedInputMetadata,
};
use crate::types::provider::{AuthHeaders, RemoteCompactionResult, RemoteCompactionSupport};
use sha2::{Digest, Sha256};

const RETAINED_TEXT_TOKENS: u64 = 64_000;
const MAX_RETAINED_AGENT_TOKENS: u64 = 10_000;
const MAX_FRAME_BYTES: usize = 32 * 1024 * 1024;

fn invalid(message: &str) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::InvalidRequest,
        message: message.into(),
        code: Some("invalid_native_compaction".into()),
        ..aborted_error()
    }
}

pub(super) fn validate_checkpoint(
    checkpoint: &NativeCompactionCheckpoint,
) -> Result<(), ProviderError> {
    if checkpoint.source.is_empty()
        || checkpoint.auth_scope.is_empty()
        || checkpoint.response_id.is_empty()
        || checkpoint.encrypted_content.is_empty()
    {
        return Err(invalid("The native context checkpoint is incomplete."));
    }
    if !checkpoint.retained_input_metadata.is_empty()
        && checkpoint.retained_input_metadata.len() != checkpoint.retained_input.len()
    {
        return Err(invalid(
            "The native context checkpoint has misaligned retained provenance.",
        ));
    }
    for item in &checkpoint.retained_input {
        if item
            .get("role")
            .and_then(Value::as_str)
            .is_none_or(|role| role != "user")
            || item.get("content").is_none()
            || item
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| kind != "message")
        {
            return Err(invalid(
                "The native context checkpoint contains an invalid retained item.",
            ));
        }
    }
    for (item, metadata) in checkpoint
        .retained_input
        .iter()
        .zip(&checkpoint.retained_input_metadata)
    {
        if let Some(metadata) = metadata {
            if metadata.source_message_id.is_empty()
                || metadata.provenance.len() > 1
                || metadata
                    .provenance
                    .iter()
                    .any(|block| !is_agent_origin(block))
                || !retained_item_has_provenance(item, metadata)
            {
                return Err(invalid(
                    "The native context checkpoint has invalid retained provenance.",
                ));
            }
        }
    }
    Ok(())
}

fn digest_scope(identity: &str) -> RuntimeString {
    format!("{:x}", Sha256::digest(identity.as_bytes())).into()
}

fn account_scope(
    headers: &AuthHeaders,
    auth: Option<&Arc<dyn AuthSource>>,
    key: Option<&str>,
) -> Result<RuntimeString, ProviderError> {
    if let Some(key) = key.filter(|_| auth.is_none()) {
        return Ok(digest_scope(&format!("api-key:{key}")));
    }
    let account = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("chatgpt-account-id"))
        .map(|(_, value)| value.clone())
        .or_else(|| auth.and_then(|auth| auth.credential_account_id()));
    account
        .filter(|account| !account.is_empty())
        .map(|account| digest_scope(&format!("account:{account}")))
        .ok_or_else(|| {
            invalid("Native context compaction requires a stable authenticated account identity.")
        })
}

struct ScopedAuth {
    inner: Arc<dyn AuthSource>,
    scope: RuntimeString,
}

#[async_trait::async_trait]
impl AuthSource for ScopedAuth {
    async fn get_headers(&self, signal: &AbortSignal) -> Result<AuthHeaders, ProviderError> {
        let headers = self.inner.get_headers(signal).await?;
        if account_scope(&headers, Some(&self.inner), None)? != self.scope {
            return Err(crate::provider::native_compaction::unsupported_checkpoint());
        }
        Ok(headers)
    }
    fn base_url(&self) -> Option<&str> {
        self.inner.base_url()
    }
    async fn force_refresh(&self) -> Option<bool> {
        self.inner.force_refresh().await
    }
}

pub(super) async fn scope_auth(
    auth: Option<Arc<dyn AuthSource>>,
    expected: Result<Option<RuntimeString>, ProviderError>,
    api_key: Option<&str>,
    signal: &AbortSignal,
) -> Result<Option<Arc<dyn AuthSource>>, ProviderError> {
    let Some(expected) = expected? else {
        return Ok(auth);
    };
    let headers = match &auth {
        Some(auth) => auth.get_headers(signal).await?,
        None => HashMap::new(),
    };
    if account_scope(&headers, auth.as_ref(), api_key)? != expected {
        return Err(crate::provider::native_compaction::unsupported_checkpoint());
    }
    Ok(auth.map(|inner| {
        Arc::new(ScopedAuth {
            inner,
            scope: expected,
        }) as Arc<dyn AuthSource>
    }))
}

impl OpenAIProvider {
    pub(super) fn native_execution_context(
        &self,
        model: &RuntimeString,
    ) -> Option<crate::types::provider::NativeCompactionContext> {
        if self.remote_compaction_support(model) == RemoteCompactionSupport::Unsupported {
            return None;
        }
        let (owner, generation) = if let Some(auth) = &self.auth_source {
            (
                digest_scope(&format!("account:{}", auth.credential_account_id()?)),
                auth.credential_changes()
                    .map(|state| state.borrow().owner_generation)
                    .unwrap_or(0),
            )
        } else {
            (
                digest_scope(&format!("api-key:{}", self.api_key.as_ref()?)),
                0,
            )
        };
        Some(crate::types::provider::NativeCompactionContext {
            identity: digest_scope(&format!(
                "native-compaction-v1\n{}\n{:?}\n{:?}\n{:?}\n{:?}\n{}",
                self.request_base_url.trim_end_matches('/'),
                self.remote_compaction_support(model),
                model,
                self.compaction_compatibility_hash(model),
                owner,
                generation,
            )),
            auth_scope: owner,
        })
    }
    pub(super) async fn native_checkpoint_matches(
        &self,
        checkpoint: &NativeCompactionCheckpoint,
        model: &RuntimeString,
        signal: &AbortSignal,
    ) -> Result<bool, ProviderError> {
        validate_checkpoint(checkpoint)?;
        if signal.is_cancelled() {
            return Err(aborted_error());
        }
        if self.api != OpenAiApi::Responses
            || self.remote_compaction_support(model) == RemoteCompactionSupport::Unsupported
            || checkpoint.source != self.request_base_url.trim_end_matches('/')
        {
            return Ok(false);
        }
        // Subscription checkpoints are portable input for this endpoint. Account
        // changes affect request authentication, not the saved opaque context.
        if self.is_codex_subscription_backend() {
            return Ok(true);
        }
        let headers = match &self.auth_source {
            Some(auth) => auth.get_headers(signal).await?,
            None => HashMap::new(),
        };
        Ok(
            account_scope(&headers, self.auth_source.as_ref(), self.api_key.as_deref())?
                == checkpoint.auth_scope,
        )
    }

    pub(super) fn native_replay_scope(
        &self,
        request: &ModelRequest,
    ) -> Result<Option<RuntimeString>, ProviderError> {
        // This transport cannot read an encrypted checkpoint. Do not refuse the
        // turn; portable text and retained messages still replay.
        if self.remote_compaction_support(&request.model) == RemoteCompactionSupport::Unsupported {
            return Ok(None);
        }
        let mut scope = None;
        let bind_checkpoint_account = !self.is_codex_subscription_backend();
        for message in &request.messages {
            let Message::CompactBoundary(boundary) = message else {
                continue;
            };
            let Some(checkpoint) = &boundary.native_compaction else {
                continue;
            };
            validate_checkpoint(checkpoint)?;
            if self.api != OpenAiApi::Responses
                || self.remote_compaction_support(&request.model)
                    == RemoteCompactionSupport::Unsupported
                || checkpoint.source != self.request_base_url.trim_end_matches('/')
                || scope
                    .as_ref()
                    .is_some_and(|scope| scope != &checkpoint.auth_scope)
            {
                return Err(crate::provider::native_compaction::unsupported_checkpoint());
            }
            if bind_checkpoint_account {
                scope = Some(checkpoint.auth_scope.clone());
            }
        }
        Ok(scope)
    }

    pub(super) async fn native_compact(
        &self,
        request: ModelRequest,
        signal: AbortSignal,
        idle_ms: Option<u64>,
    ) -> Result<RemoteCompactionResult, ProviderError> {
        self.native_compact_observed(request, signal, idle_ms, None, None)
            .await
    }

    pub(super) async fn native_compact_observed(
        &self,
        mut request: ModelRequest,
        signal: AbortSignal,
        idle_ms: Option<u64>,
        observer: Option<crate::types::provider::NativeCompactionUsageObserver>,
        expected_scope: Option<RuntimeString>,
    ) -> Result<RemoteCompactionResult, ProviderError> {
        let support = self.remote_compaction_support(&request.model);
        if support == RemoteCompactionSupport::Unsupported {
            return Err(crate::provider::native_compaction::unsupported_checkpoint());
        }
        let headers = match &self.auth_source {
            Some(auth) => auth.get_headers(&signal).await?,
            None => HashMap::new(),
        };
        let auth_scope =
            account_scope(&headers, self.auth_source.as_ref(), self.api_key.as_deref())?;
        if expected_scope
            .as_ref()
            .is_some_and(|expected| expected != &auth_scope)
        {
            return Err(crate::provider::native_compaction::unsupported_checkpoint());
        }
        let auth = scope_auth(
            self.auth_source.clone(),
            Ok(Some(auth_scope.clone())),
            self.api_key.as_deref(),
            &signal,
        )
        .await?;
        if self
            .native_replay_scope(&request)?
            .is_some_and(|scope| scope != auth_scope)
        {
            return Err(crate::provider::native_compaction::unsupported_checkpoint());
        }
        request.output_schema = None;
        request.tool_choice = None;
        request.task_budget = None;
        let caps = self.capabilities_for(&request.model);
        let function_tools = self
            .compatibility
            .as_ref()
            .is_some_and(OpenAICompatibilityOptions::responses_function_tools);
        let mut body = build_responses_params_with_tool_mode(
            &request,
            &caps,
            self.reasoning_dialect,
            function_tools,
        )?;
        if self.uses_responses_backend_format() {
            adapt_codex_responses_params(&mut body);
            if self.fast_mode {
                body["service_tier"] = vs("priority");
            }
        }
        let original = body
            .get("input")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let retention_source = retention_input(&request);
        let retained = retain_user_input(&retention_source);
        let endpoint = match support {
            RemoteCompactionSupport::ResponsesV2 => {
                let input = body
                    .get_mut("input")
                    .and_then(Value::as_array_mut)
                    .ok_or_else(|| invalid("Native compaction requires an input array."))?;
                input.push(vobj([("type", vs("compaction_trigger"))]));
                "/responses"
            }
            RemoteCompactionSupport::ResponsesV1 => {
                let object = body
                    .as_object_mut()
                    .ok_or_else(|| invalid("Native compaction requires an object request."))?;
                for field in [
                    "stream",
                    "store",
                    "include",
                    "tools",
                    "tool_choice",
                    "parallel_tool_calls",
                    "max_output_tokens",
                ] {
                    object.remove(field);
                }
                "/responses/compact"
            }
            RemoteCompactionSupport::Unsupported => unreachable!(),
        };
        let mut retries = self.retry.clone();
        retries.max_attempts = Some(retries.max_attempts.unwrap_or(3).clamp(1, 3));
        retries.congestion_hold_ms = Some(0);
        retries.unbounded_network_retries = Some(false);
        retries.retry_class_policy = None;
        let mut one_attempt = retries.clone();
        one_attempt.max_attempts = Some(1);
        let url = self.endpoint(endpoint);
        let extra_headers = self
            .compatibility
            .as_ref()
            .map(|options| options.extra_headers().clone())
            .unwrap_or_default();
        let max_attempts = retries.max_attempts.unwrap_or(3);
        let mut attempt = 0;
        let result = loop {
            if signal.is_cancelled() {
                return Err(aborted_error());
            }
            let response = establish_stream(
                self.client.clone(),
                url.clone(),
                body.clone(),
                auth.clone(),
                self.api_key.clone(),
                extra_headers.clone(),
                one_attempt.clone(),
                signal.clone(),
                (
                    self.interceptor.clone(),
                    diagnostic_string(&self.info.name),
                    diagnostic_string(&request.model),
                ),
            )
            .await;
            let outcome = match response {
                Ok(response) => match support {
                    RemoteCompactionSupport::ResponsesV2 => {
                        collect_v2_observed(response, &signal, idle_ms, observer.clone()).await
                    }
                    RemoteCompactionSupport::ResponsesV1 => {
                        collect_v1_observed(response, &signal, &original, idle_ms, observer.clone())
                            .await
                    }
                    RemoteCompactionSupport::Unsupported => unreachable!(),
                },
                Err(error) => Err(error),
            };
            match outcome {
                Ok(result) => break result,
                Err(error) => {
                    if signal.is_cancelled() {
                        return Err(aborted_error());
                    }
                    if attempt + 1 >= max_attempts || !crate::provider::retry::is_retryable(&error)
                    {
                        return Err(error);
                    }
                    let random = retries
                        .random
                        .as_ref()
                        .map(|random| random())
                        .unwrap_or_else(rand::random::<f64>);
                    let delay = crate::provider::retry::compute_delay(
                        attempt,
                        &error,
                        retries.base_delay_ms.unwrap_or(200),
                        retries.max_delay_ms.unwrap_or(30_000),
                        retries.jitter.unwrap_or(0.1),
                        || random,
                    );
                    if let Some(observer) = &retries.on_retry {
                        observer(crate::provider::retry::RetryInfo {
                            attempt: attempt + 1,
                            max_attempts,
                            delay_ms: delay,
                            error: error.clone(),
                            network_wait: false,
                        });
                    }
                    if let Some(sleep) = &retries.sleep {
                        sleep(delay, signal.clone())
                            .await
                            .map_err(|_| aborted_error())?;
                    } else {
                        tokio::select! { biased; _ = signal.cancelled() => return Err(aborted_error()), _ = tokio::time::sleep(std::time::Duration::from_millis(delay)) => {} }
                    }
                    attempt += 1;
                }
            }
        };
        let retained = match result.retained {
            Some(items) => annotate_returned_input(items, &retention_source)?,
            None => retained,
        };
        let (retained_input, mut retained_input_metadata): (Vec<_>, Vec<_>) = retained
            .into_iter()
            .map(|item| (item.item, item.metadata))
            .unzip();
        if retained_input_metadata.iter().all(Option::is_none) {
            retained_input_metadata.clear();
        }
        let checkpoint = NativeCompactionCheckpoint {
            source: self.request_base_url.trim_end_matches('/').into(),
            auth_scope,
            model: request.model.clone(),
            comp_hash: self.compaction_compatibility_hash(&request.model),
            response_id: result.response_id,
            encrypted_content: result.encrypted_content,
            retained_input,
            retained_input_metadata,
        };
        validate_checkpoint(&checkpoint)?;
        Ok(RemoteCompactionResult {
            checkpoint,
            usage: result.usage,
        })
    }
}

async fn timed_read<T>(
    read: impl std::future::Future<Output = T>,
    idle_ms: Option<u64>,
) -> Result<T, ProviderError> {
    match idle_ms.filter(|ms| *ms > 0) {
        Some(ms) => tokio::time::timeout(std::time::Duration::from_millis(ms), read)
            .await
            .map_err(|_| ProviderError {
                kind: ProviderErrorKind::Timeout,
                code: Some("native_compaction_idle_timeout".into()),
                message: "Native compaction stream idle timeout.".into(),
                ..aborted_error()
            }),
        None => Ok(read.await),
    }
}

struct Collected {
    encrypted_content: RuntimeString,
    response_id: RuntimeString,
    usage: Option<Usage>,
    retained: Option<Vec<Value>>,
}

struct AttemptUsage {
    observer: Option<crate::types::provider::NativeCompactionUsageObserver>,
    actual: Option<Usage>,
}

impl Drop for AttemptUsage {
    fn drop(&mut self) {
        if let (Some(observer), Some(actual)) = (&self.observer, self.actual.take()) {
            observer(actual);
        }
    }
}

fn is_compaction_item(item: &Value) -> bool {
    item.get("type")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind == "compaction" || kind == "compaction_summary")
}

fn encrypted_item(item: &Value) -> Option<RuntimeString> {
    item.get("type")
        .and_then(Value::as_str)
        .filter(|kind| *kind == "compaction" || *kind == "compaction_summary")?;
    item.get("encrypted_content")
        .and_then(Value::as_str)
        .filter(|content| !content.is_empty())
        .cloned()
}

async fn collect_v2_observed(
    response: reqwest::Response,
    signal: &AbortSignal,
    idle_ms: Option<u64>,
    observer: Option<crate::types::provider::NativeCompactionUsageObserver>,
) -> Result<Collected, ProviderError> {
    let mut receipt = AttemptUsage {
        observer,
        actual: None,
    };
    let mut stream = response.bytes_stream();
    let mut decoder = SseDecoder::new();
    let mut outputs = Vec::new();
    let mut state = ResponsesState::default();
    loop {
        while let Some(event) = decoder.next_event() {
            if event.data.is_empty() {
                continue;
            }
            let value = runtime_json::parse(&event.data)
                .map_err(|_| invalid("Native compaction returned malformed stream data."))?;
            if update_responses_usage(&value, &mut state) {
                receipt.actual = Some(state.usage());
            }
            let kind = value.get("type").and_then(Value::as_str);
            let incomplete = kind.is_some_and(|kind| kind == "response.incomplete");
            let incomplete_reason = value
                .get("response")
                .and_then(|response| response.get("incomplete_details"))
                .and_then(|details| details.get("reason"))
                .and_then(Value::as_str);
            let interrupted =
                incomplete && incomplete_reason.is_some_and(|reason| reason == "interrupted");
            if kind.is_some_and(|kind| kind == "response.output_item.done") {
                if let Some(item) = value.get("item") {
                    outputs.push(item.clone());
                }
            } else if kind.is_some_and(|kind| kind == "response.completed") || interrupted {
                let response = value
                    .get("response")
                    .ok_or_else(|| invalid("Native compaction completed without a response."))?;
                if response
                    .get("status")
                    .and_then(Value::as_str)
                    .is_some_and(|status| {
                        status != "completed" && !(interrupted && status == "incomplete")
                    })
                {
                    return Err(invalid("Native compaction did not complete."));
                }
                // Some endpoints include output only in the authoritative terminal snapshot.
                if outputs.is_empty() {
                    outputs = response
                        .get("output")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default();
                }
                let native: Vec<_> = outputs.iter().filter_map(encrypted_item).collect();
                let native_count = outputs
                    .iter()
                    .filter(|item| is_compaction_item(item))
                    .count();
                if native.len() != 1 || native_count != 1 {
                    return Err(invalid(
                        "Native compaction must return exactly one nonempty encrypted checkpoint.",
                    ));
                }
                let response_id = response
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .cloned()
                    .ok_or_else(|| {
                        invalid("Native compaction completed without a response identity.")
                    })?;
                let usage = receipt.actual.clone();
                return Ok(Collected {
                    encrypted_content: native[0].clone(),
                    response_id,
                    usage,
                    retained: None,
                });
            } else if kind.is_some_and(|kind| {
                matches!(
                    kind.try_to_utf8().ok().as_deref(),
                    Some("response.failed" | "response.incomplete" | "error")
                )
            }) {
                if incomplete && incomplete_reason.is_some_and(|reason| reason == "content_filter")
                {
                    return Err(ProviderError {
                        kind: ProviderErrorKind::Withheld,
                        code: Some("content_filter".into()),
                        message: "Native compaction was stopped by content filtering.".into(),
                        ..aborted_error()
                    });
                }
                let error = value
                    .get("response")
                    .and_then(|response| response.get("error"))
                    .or_else(|| value.get("error"));
                let mut error = classify_error(&RawProviderError {
                    message: error
                        .and_then(|error| error.get("message"))
                        .and_then(Value::as_str)
                        .map(diagnostic_string)
                        .unwrap_or_else(|| {
                            "Native compaction response failed or was incomplete.".to_owned()
                        }),
                    code: error
                        .and_then(|error| error.get("code"))
                        .and_then(Value::as_str)
                        .map(diagnostic_string),
                    error_type: error
                        .and_then(|error| error.get("type"))
                        .and_then(Value::as_str)
                        .map(diagnostic_string),
                    ..Default::default()
                });
                if incomplete && error.kind == ProviderErrorKind::Unknown {
                    error.kind = ProviderErrorKind::Network;
                    if error.code.is_none() {
                        error.code = Some("native_compaction_incomplete".into());
                    }
                }
                return Err(error);
            }
        }
        if decoder.buffered_bytes() > MAX_FRAME_BYTES {
            return Err(invalid(
                "Native compaction stream exceeded the frame limit.",
            ));
        }
        match tokio::select! { biased; _ = signal.cancelled() => return Err(aborted_error()), next = timed_read(stream.next(), idle_ms) => next? }
        {
            Some(Ok(bytes)) => decoder.push(bytes.as_ref()),
            Some(Err(error)) => {
                return Err(classify_error(&RawProviderError {
                    message: error.to_string(),
                    ..Default::default()
                }))
            }
            None => {
                return Err(ProviderError {
                    kind: ProviderErrorKind::Network,
                    message: "Native compaction stream closed before response.completed.".into(),
                    ..aborted_error()
                })
            }
        }
    }
}

async fn collect_v1_observed(
    response: reqwest::Response,
    signal: &AbortSignal,
    original: &[Value],
    idle_ms: Option<u64>,
    observer: Option<crate::types::provider::NativeCompactionUsageObserver>,
) -> Result<Collected, ProviderError> {
    let mut receipt = AttemptUsage {
        observer,
        actual: None,
    };
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    loop {
        let next = tokio::select! { biased; _ = signal.cancelled() => return Err(aborted_error()), next = timed_read(stream.next(), idle_ms) => next? };
        match next {
            Some(Ok(chunk)) => bytes.extend_from_slice(&chunk),
            Some(Err(error)) => {
                return Err(classify_error(&RawProviderError {
                    message: error.to_string(),
                    ..Default::default()
                }))
            }
            None => break,
        }
        if bytes.len() > MAX_FRAME_BYTES {
            return Err(invalid(
                "Native compaction response exceeded the payload limit.",
            ));
        }
    }
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(invalid(
            "Native compaction response exceeded the payload limit.",
        ));
    }
    let mut decoder = WhatwgUtf8::new();
    let source = decoder.feed(&bytes);
    let value = runtime_json::parse(&source)
        .map_err(|_| invalid("Native compaction returned invalid JSON."))?;
    let mut state = ResponsesState::default();
    receipt.actual = update_responses_usage(&value, &mut state).then(|| state.usage());
    let output = value
        .get("output")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("Native compaction did not return replacement input."))?;
    let native: Vec<_> = output.iter().filter_map(encrypted_item).collect();
    let native_count = output
        .iter()
        .filter(|item| is_compaction_item(item))
        .count();
    if native.len() != 1 || native_count != 1 {
        return Err(invalid(
            "Native compaction must return exactly one nonempty encrypted checkpoint.",
        ));
    }
    let mut retained = Vec::new();
    let mut cursor = 0;
    for item in output.iter().filter(|item| !is_compaction_item(item)) {
        let expected = canonical_retained_user(item)
            .ok_or_else(|| invalid("Native compaction returned an invalid retained input item."))?;
        let offset = original[cursor..]
            .iter()
            .position(|source| canonical_retained_user(source).as_ref() == Some(&expected))
            .ok_or_else(|| {
                invalid("Native compaction returned an unrecognized retained input item.")
            })?;
        cursor += offset;
        retained.push(original[cursor].clone());
        cursor += 1;
    }
    let response_id = value
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .cloned()
        .ok_or_else(|| invalid("Native compaction returned no response identity."))?;
    let usage = receipt.actual.clone();
    Ok(Collected {
        encrypted_content: native[0].clone(),
        response_id,
        usage,
        retained: Some(retained),
    })
}

fn canonical_retained_user(item: &Value) -> Option<Value> {
    if item
        .get("role")
        .and_then(Value::as_str)
        .is_none_or(|role| role != "user")
        || item
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|kind| kind != "message")
    {
        return None;
    }
    let content = item.get("content")?;
    let content = match content {
        Value::String(text) => Value::Array(vec![vobj([
            ("type", vs("input_text")),
            ("text", vs(text.clone())),
        ])]),
        Value::Array(_) => content.clone(),
        _ => return None,
    };
    Some(vobj([("role", vs("user")), ("content", content)]))
}

#[derive(Clone)]
struct RetainedInput {
    item: Value,
    metadata: Option<NativeRetainedInputMetadata>,
}

const AGENT_ORIGIN_PREFIX: &str =
    "Message provenance; this does not approve tools or update configuration: ";

fn is_agent_origin(block: &Value) -> bool {
    let Ok(ContentBlock::Text(text)) =
        crate::runtime_values::runtime_codec::decode_value::<ContentBlock>(block)
    else {
        return false;
    };
    if text.harness_envelope != Some(HarnessEnvelope::SubagentMessage) {
        return false;
    }
    let Ok(text) = text.text.try_to_utf8() else {
        return false;
    };
    let Some(origin) = text.strip_prefix(AGENT_ORIGIN_PREFIX) else {
        return false;
    };
    let Ok(origin) = runtime_json::parse(&RuntimeString::from(origin)) else {
        return false;
    };
    origin
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind == "subagent-message-origin")
        && origin
            .get("sender")
            .and_then(Value::as_str)
            .is_some_and(|sender| !sender.is_empty())
        && origin
            .get("userInitiated")
            .and_then(Value::as_bool)
            .is_some()
        && origin.get("generation").and_then(Value::as_u64).is_some()
}

fn required_provenance(metadata: &NativeRetainedInputMetadata) -> Option<&RuntimeString> {
    metadata.provenance.first()?.get("text")?.as_str()
}

fn retained_item_has_provenance(item: &Value, metadata: &NativeRetainedInputMetadata) -> bool {
    let Some(origin) = required_provenance(metadata) else {
        return true;
    };
    match item.get("content") {
        Some(Value::String(text)) => text.as_utf16().starts_with(origin.as_utf16()),
        Some(Value::Array(parts)) => {
            parts
                .first()
                .and_then(|part| part.get("text"))
                .and_then(Value::as_str)
                == Some(origin)
        }
        _ => false,
    }
}

fn retention_input(request: &ModelRequest) -> Vec<RetainedInput> {
    let mut input = Vec::new();
    for message in &request.messages {
        match message {
            Message::User(user) => {
                let agent = user.content.iter().any(|block| matches!(block,
                    ContentBlock::Text(text) if text.harness_envelope == Some(HarnessEnvelope::SubagentMessage)));
                if user.is_meta == Some(true) && !agent {
                    continue;
                }
                let metadata = agent.then(|| NativeRetainedInputMetadata {
                    source_message_id: user.uuid.clone(),
                    provenance: user
                        .content
                        .first()
                        .and_then(|block| {
                            crate::runtime_values::runtime_codec::encode_value(block).ok()
                        })
                        .filter(is_agent_origin)
                        .into_iter()
                        .collect(),
                });
                let mut candidates = Vec::new();
                let agent_blocks = agent.then(|| user.content.iter().filter(|block| !matches!(block,
                    ContentBlock::Text(text) if text.harness_envelope == Some(HarnessEnvelope::SystemReminder))).cloned().collect::<Vec<_>>());
                if let Some(blocks) = agent_blocks {
                    candidates.push(RetainedInput {
                        item: vobj([
                            ("role", vs("user")),
                            ("content", responses_user_content(&blocks)),
                        ]),
                        metadata,
                    });
                } else {
                    for (guidance, blocks) in harness_user_segments(&user.content) {
                        if !guidance {
                            candidates.push(RetainedInput {
                                item: vobj([
                                    ("role", vs("user")),
                                    ("content", responses_user_content(blocks)),
                                ]),
                                metadata: metadata.clone(),
                            });
                        }
                    }
                }
                let cost = candidates
                    .iter()
                    .map(|entry| native_retained_item_tokens(&entry.item))
                    .fold(0, u64::saturating_add);
                if !agent || cost <= MAX_RETAINED_AGENT_TOKENS {
                    input.extend(candidates);
                }
            }
            Message::CompactBoundary(boundary) => {
                if let Some(checkpoint) = &boundary.native_compaction {
                    input.extend(checkpoint.retained_input.iter().enumerate().map(
                        |(index, item)| {
                            RetainedInput {
                                item: item.clone(),
                                metadata: checkpoint
                                    .retained_input_metadata
                                    .get(index)
                                    .cloned()
                                    .flatten(),
                            }
                        },
                    ));
                }
            }
            _ => {}
        }
    }
    input
}

fn annotate_returned_input(
    items: Vec<Value>,
    source: &[RetainedInput],
) -> Result<Vec<RetainedInput>, ProviderError> {
    items
        .into_iter()
        .map(|item| {
            let canonical = canonical_retained_user(&item);
            let mut matches = source
                .iter()
                .filter(|entry| canonical_retained_user(&entry.item) == canonical);
            let metadata = matches.next().and_then(|entry| entry.metadata.clone());
            if matches.any(|entry| entry.metadata != metadata) {
                return Err(invalid(
                    "Native compaction returned an ambiguous retained input origin.",
                ));
            }
            Ok(RetainedInput { item, metadata })
        })
        .collect()
}

fn retained_text_prefix(text: &RuntimeString, max_bytes: u64) -> (RuntimeString, u64) {
    let units = text.as_utf16();
    let mut end = 0;
    let mut bytes = 0;
    while end < units.len() {
        let unit = units[end];
        let paired = (0xd800..=0xdbff).contains(&unit)
            && units
                .get(end + 1)
                .is_some_and(|next| (0xdc00..=0xdfff).contains(next));
        let size = if paired {
            4
        } else if unit <= 0x7f {
            1
        } else if unit <= 0x7ff {
            2
        } else {
            3
        };
        if bytes + size > max_bytes {
            break;
        }
        bytes += size;
        end += if paired { 2 } else { 1 };
    }
    (text.slice_utf16(..end), bytes)
}

fn retain_user_input(input: &[RetainedInput]) -> Vec<RetainedInput> {
    retain_input_with_budget(input, RETAINED_TEXT_TOKENS)
}

fn retain_input_with_budget(input: &[RetainedInput], mut remaining: u64) -> Vec<RetainedInput> {
    let mut retained = Vec::new();
    for item in input.iter().rev().filter(|item| {
        item.item
            .get("role")
            .and_then(Value::as_str)
            .is_some_and(|role| role == "user")
    }) {
        if remaining == 0 {
            break;
        }
        let cost = native_retained_item_tokens(&item.item).max(1);
        if cost <= remaining {
            retained.push(item.clone());
            remaining -= cost;
        } else {
            let mut boundary = item.clone();
            if truncate_retained_item(&mut boundary, remaining) {
                retained.push(boundary);
            }
            break;
        }
    }
    retained.reverse();
    retained
}

fn retained_text_middle(text: &RuntimeString, tokens: u64) -> RuntimeString {
    let limit = tokens.saturating_mul(4);
    let total = text.to_node_utf8().len() as u64;
    if total <= limit {
        return text.clone();
    }
    let (prefix, _) = retained_text_prefix(text, limit / 2);
    let units = text.as_utf16();
    let mut start = units.len();
    let mut bytes = 0;
    let tail_budget = limit - limit / 2;
    while start > 0 {
        let unit = units[start - 1];
        let paired = (0xdc00..=0xdfff).contains(&unit)
            && start >= 2
            && (0xd800..=0xdbff).contains(&units[start - 2]);
        let size = if paired {
            4
        } else if unit <= 0x7f {
            1
        } else if unit <= 0x7ff {
            2
        } else {
            3
        };
        if bytes + size > tail_budget {
            break;
        }
        bytes += size;
        start -= if paired { 2 } else { 1 };
    }
    prefix
        .concat(
            &format!(
                "…{} tokens truncated…",
                total.saturating_sub(limit).div_ceil(4)
            )
            .into(),
        )
        .concat(&text.slice_utf16(start..))
}

fn is_media_part(part: &Value) -> bool {
    part.get("type")
        .and_then(Value::as_str)
        .is_some_and(|kind| {
            matches!(
                kind.try_to_utf8().ok().as_deref(),
                Some("input_image" | "input_file" | "input_audio" | "input_video")
            )
        })
}

fn image_label(part: &Value, open: bool) -> bool {
    let Some(text) = part
        .get("text")
        .and_then(Value::as_str)
        .and_then(|text| text.try_to_utf8().ok())
    else {
        return false;
    };
    if open {
        text == "<image>" || (text.starts_with("<image name=") && text.ends_with('>'))
    } else {
        text == "</image>"
    }
}

fn truncate_retained_parts(parts: &[Value], mut remaining: u64) -> Vec<Value> {
    if !parts.iter().any(is_media_part) {
        let mut retained = Vec::new();
        for part in parts {
            let mut part = part.clone();
            if let Some(Value::String(text)) = part.get_mut("text") {
                if remaining == 0 {
                    continue;
                }
                let cost = (text.to_node_utf8().len() as u64).div_ceil(4);
                if cost > remaining {
                    *text = retained_text_middle(text, remaining);
                    remaining = 0;
                } else {
                    remaining -= cost;
                }
                if text.is_empty() {
                    continue;
                }
            }
            retained.push(part);
        }
        return retained;
    }
    let mut end = parts.len();
    let mut retained = Vec::new();
    while end > 0 {
        let last = end - 1;
        let image = if parts[last]
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|kind| kind == "input_image")
        {
            Some(last)
        } else if image_label(&parts[last], false)
            && last > 0
            && parts[last - 1]
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| kind == "input_image")
        {
            Some(last - 1)
        } else {
            None
        };
        if let Some(image) = image {
            let start = image - usize::from(image > 0 && image_label(&parts[image - 1], true));
            let cost = parts[start..end]
                .iter()
                .map(native_retained_part_tokens)
                .fold(0, u64::saturating_add);
            if cost <= remaining {
                remaining -= cost;
                retained.extend(parts[start..end].iter().rev().cloned());
            } else {
                remaining = 0;
            }
            end = start;
            continue;
        }
        let mut part = parts[last].clone();
        end = last;
        if let Some(Value::String(text)) = part.get_mut("text") {
            if remaining == 0 {
                continue;
            }
            let cost = (text.to_node_utf8().len() as u64).div_ceil(4);
            if cost > remaining {
                *text = retained_text_middle(text, remaining);
                remaining = 0;
            } else {
                remaining -= cost;
            }
            if text.is_empty() {
                continue;
            }
        } else {
            let cost = native_retained_part_tokens(&part);
            if cost > remaining {
                remaining = 0;
                continue;
            }
            remaining -= cost;
        }
        retained.push(part);
    }
    retained.reverse();
    retained
}

fn truncate_retained_item(entry: &mut RetainedInput, budget: u64) -> bool {
    let origin = entry
        .metadata
        .as_ref()
        .and_then(required_provenance)
        .cloned();
    let Some(content) = entry.item.get_mut("content") else {
        return false;
    };
    match content {
        Value::String(text) => {
            if let Some(origin) = origin {
                let prefix = origin;
                let cost = (prefix.to_node_utf8().len() as u64).div_ceil(4);
                if cost >= budget {
                    return false;
                }
                let body = text.slice_utf16(prefix.len_utf16()..);
                *text = prefix.concat(&retained_text_middle(&body, budget - cost));
            } else {
                *text = retained_text_middle(text, budget);
            }
            !text.is_empty()
        }
        Value::Array(parts) => {
            let (prefix, body) = if origin.is_some() {
                parts.split_at(1)
            } else {
                parts.split_at(0)
            };
            let cost = prefix
                .iter()
                .map(native_retained_part_tokens)
                .fold(0, u64::saturating_add);
            if cost >= budget {
                return false;
            }
            let body = truncate_retained_parts(body, budget - cost);
            if body.is_empty() {
                return false;
            }
            *parts = prefix.iter().cloned().chain(body).collect();
            true
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::runtime_codec;
    use crate::schemas::message::CompactBoundaryMessage;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn request() -> ModelRequest {
        runtime_codec::decode_value(&runtime_json::parse(&RuntimeString::from(
            r#"{"model":"selected","system":[],"messages":[{"type":"user","uuid":"user-1","content":[{"type":"text","text":"Keep this requirement."}]}],"tools":[],"maxOutputTokens":1024}"#,
        )).unwrap()).unwrap()
    }

    fn complete(encrypted: &str) -> String {
        format!("data: {{\"type\":\"response.output_item.done\",\"item\":{{\"type\":\"compaction\",\"encrypted_content\":\"{encrypted}\"}}}}\n\ndata: {{\"type\":\"response.completed\",\"response\":{{\"id\":\"response-1\",\"status\":\"completed\",\"usage\":{{\"input_tokens\":100,\"output_tokens\":12,\"total_tokens\":112}}}}}}\n\n")
    }

    fn incomplete(reason: &str, output: &str) -> String {
        format!("data: {{\"type\":\"response.incomplete\",\"response\":{{\"id\":\"response-1\",\"status\":\"incomplete\",\"incomplete_details\":{{\"reason\":\"{reason}\"}},\"output\":{output}}}}}\n\n")
    }

    async fn fixture(
        responses: Vec<(&'static str, String)>,
    ) -> (String, tokio::task::JoinHandle<Vec<(String, Value)>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut captured = Vec::new();
            for (content_type, body) in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let (header_end, length) = loop {
                    let mut buffer = [0; 4096];
                    let count = socket.read(&mut buffer).await.unwrap();
                    assert!(count > 0, "The request must finish its headers");
                    bytes.extend_from_slice(&buffer[..count]);
                    if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..end]);
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                line.split_once(':')
                                    .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                                    .map(|(_, value)| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap();
                        break (end + 4, length);
                    }
                };
                while bytes.len() < header_end + length {
                    let mut buffer = [0; 4096];
                    let count = socket.read(&mut buffer).await.unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&buffer[..count]);
                }
                let headers = String::from_utf8_lossy(&bytes[..header_end]).into_owned();
                let body_json = runtime_json::parse(&RuntimeString::from(
                    String::from_utf8(bytes[header_end..header_end + length].to_vec()).unwrap(),
                ))
                .unwrap();
                captured.push((headers, body_json));
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            }
            captured
        });
        (base, task)
    }

    fn provider(base: String, support: RemoteCompactionSupport) -> OpenAIProvider {
        OpenAIProvider::new(OpenAIProviderOptions {
            base_url: Some(base),
            api_key: Some("fixture-key".into()),
            api: Some(OpenAiApi::Responses),
            remote_compaction: support,
            retry: Some(RetryOptions {
                max_attempts: Some(3),
                sleep: Some(Arc::new(|_, _| Box::pin(async { Ok(()) }))),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    #[test]
    fn generic_responses_endpoints_do_not_imply_native_support() {
        let provider = provider(
            "http://127.0.0.1:1".into(),
            RemoteCompactionSupport::Unsupported,
        );
        assert_eq!(
            provider.remote_compaction_support(&"selected".into()),
            RemoteCompactionSupport::Unsupported
        );
        assert!(!provider.supports_compaction_model_fallback());
    }

    #[tokio::test]
    async fn native_v2_uses_the_ordinary_response_route_and_replays_exactly() {
        let (base, server) = fixture(vec![("text/event-stream", complete("opaque-checkpoint")), ("text/event-stream", "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"next\",\"status\":\"completed\"}}\n\n".into())]).await;
        let provider = provider(base, RemoteCompactionSupport::ResponsesV2);
        let result = provider
            .compact_remote(request(), AbortSignal::new())
            .await
            .unwrap();
        assert_eq!(result.usage.as_ref().unwrap().output_tokens, 12);
        assert_eq!(result.checkpoint.retained_input.len(), 1);
        let boundary = Message::CompactBoundary(CompactBoundaryMessage {
            uuid: "boundary-1".into(),
            summary: "Displayed description".into(),
            native_compaction: Some(result.checkpoint),
        });
        let encoded = runtime_codec::encode_value(&boundary).unwrap();
        let restored: Message = runtime_codec::decode_value(&encoded).unwrap();
        assert_eq!(restored, boundary);
        let mut replay = request();
        replay.messages = vec![restored];
        replay.skip_cache_write = Some(true);
        let mut stream = provider.stream(replay, AbortSignal::new());
        while let Some(event) = stream.next().await {
            assert!(
                !matches!(event, ModelStreamEvent::Error { .. }),
                "{event:?}"
            );
        }
        let captured = server.await.unwrap();
        assert!(captured[0].0.starts_with("POST /responses HTTP/1.1"));
        assert_eq!(
            captured[0].1["input"].as_array().unwrap().last().unwrap()["type"],
            vs("compaction_trigger")
        );
        let replay = captured[1].1["input"].as_array().unwrap();
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[1]["type"], vs("compaction"));
        assert_eq!(replay[1]["encrypted_content"], vs("opaque-checkpoint"));
        assert!(!runtime_json::stringify(&captured[1].1).contains("Displayed description"));
    }

    #[tokio::test]
    async fn native_precompute_fork_keeps_transport_but_not_foreground_request_state() {
        use crate::context::compact::engine::Compactor;
        use crate::context::compact::{create_compactor, CompactorOptions};
        use crate::schemas::config::EngineConfig;
        let (base, server) =
            fixture(vec![("text/event-stream", complete("background-native"))]).await;
        let provider = Arc::new(provider(base, RemoteCompactionSupport::ResponsesV2));
        let parent = create_compactor(CompactorOptions {
            call_model: Box::new(|_, _| {
                panic!("native compaction must not request a text summary")
            }),
            model: "selected".into(),
            max_output_tokens: Some(8_000),
            uuid: Box::new(|| uuid::Uuid::new_v4().to_string()),
        });
        parent.bind_native_provider(provider);
        let config = EngineConfig {
            model: "selected".into(),
            ..Default::default()
        };
        let isolated = parent
            .fork_isolated(Box::new(|_, _| panic!("unused")), &config)
            .unwrap();
        assert!(
            !isolated.native_summary_active(),
            "internal isolated queries remain text-only"
        );
        let fork = parent
            .fork_precompute(
                Box::new(|_, _| panic!("must use native transport")),
                &config,
            )
            .unwrap();
        assert!(fork.native_summary_active());
        assert_eq!(
            fork.native_precompute_context(),
            parent.native_precompute_context()
        );
        assert!(fork.native_precompute_context().is_some());
        fork.prepare_request(&request(), &AbortSignal::new());
        let result = fork
            .threshold_compact(request().messages, &config, None)
            .await;
        assert!(result.compacted, "{result:?}");
        assert!(
            matches!(result.messages.last(), Some(Message::CompactBoundary(boundary))
            if boundary.native_compaction.as_ref().is_some_and(|checkpoint| checkpoint.encrypted_content == "background-native"))
        );
        let captured = server.await.unwrap();
        assert_eq!(
            captured[0].1["input"].as_array().unwrap().last().unwrap()["type"],
            vs("compaction_trigger")
        );
    }

    struct ChangingAuth {
        owner: std::sync::Mutex<String>,
        calls: std::sync::atomic::AtomicUsize,
        switch_on_call: usize,
    }

    #[async_trait::async_trait]
    impl AuthSource for ChangingAuth {
        async fn get_headers(&self, _: &AbortSignal) -> Result<AuthHeaders, ProviderError> {
            let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            let mut owner = self.owner.lock().unwrap();
            if call == self.switch_on_call {
                *owner = "account-b".into();
            }
            Ok(HashMap::from([
                ("chatgpt-account-id".into(), owner.clone()),
                ("authorization".into(), format!("Bearer refreshed-{call}")),
            ]))
        }
        fn credential_account_id(&self) -> Option<String> {
            Some(self.owner.lock().unwrap().clone())
        }
    }

    #[tokio::test]
    async fn native_precompute_rejects_owner_changes_before_sending_history() {
        use crate::context::compact::engine::Compactor;
        use crate::context::compact::{create_compactor, CompactorOptions};
        use crate::schemas::config::EngineConfig;
        // Cover a queued task, the initial header read, scope binding, and the
        // actual send. All must reject before the listening endpoint sees data.
        for switch_on_call in 0..=3 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let auth = Arc::new(ChangingAuth {
                owner: std::sync::Mutex::new("account-a".into()),
                calls: std::sync::atomic::AtomicUsize::new(0),
                switch_on_call,
            });
            let provider = Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
                base_url: Some(format!("http://{}", listener.local_addr().unwrap())),
                api: Some(OpenAiApi::Responses),
                remote_compaction: RemoteCompactionSupport::ResponsesV2,
                auth_source: Some(auth.clone()),
                ..Default::default()
            }));
            let parent = create_compactor(CompactorOptions {
                call_model: Box::new(|_, _| panic!("native transport required")),
                model: "selected".into(),
                max_output_tokens: Some(1024),
                uuid: Box::new(|| uuid::Uuid::new_v4().to_string()),
            });
            parent.bind_native_provider(provider);
            let config = EngineConfig {
                model: "selected".into(),
                ..Default::default()
            };
            let fork = parent
                .fork_precompute(
                    Box::new(|_, _| panic!("native transport required")),
                    &config,
                )
                .unwrap();
            if switch_on_call == 0 {
                *auth.owner.lock().unwrap() = "account-b".into();
            }
            fork.prepare_request(&request(), &AbortSignal::new());
            assert!(
                !fork
                    .threshold_compact(request().messages, &config, None)
                    .await
                    .compacted
            );
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(20), listener.accept())
                    .await
                    .is_err(),
                "an invalidated account must receive no speculative history"
            );
        }
    }

    #[tokio::test]
    async fn native_precompute_allows_same_account_token_refresh() {
        let (base, server) = fixture(vec![("text/event-stream", complete("refreshed"))]).await;
        let provider = OpenAIProvider::new(OpenAIProviderOptions {
            base_url: Some(base),
            api: Some(OpenAiApi::Responses),
            remote_compaction: RemoteCompactionSupport::ResponsesV2,
            auth_source: Some(Arc::new(ChangingAuth {
                owner: std::sync::Mutex::new("account-a".into()),
                calls: std::sync::atomic::AtomicUsize::new(0),
                switch_on_call: usize::MAX,
            })),
            ..Default::default()
        });
        let context = provider
            .native_compaction_context(&"selected".into())
            .unwrap();
        let result = provider
            .compact_remote_with_usage(
                request(),
                AbortSignal::new(),
                5_000,
                Arc::new(|_| {}),
                Some(context.auth_scope),
            )
            .await
            .unwrap();
        assert_eq!(result.checkpoint.encrypted_content, "refreshed");
        assert_eq!(
            provider
                .native_compaction_context(&"selected".into())
                .unwrap()
                .identity,
            context.identity
        );
        assert_eq!(server.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn metered_native_compaction_cannot_bypass_a_captured_model_allowlist() {
        let (base, server) = fixture(vec![("text/event-stream", complete("must-not-send"))]).await;
        let captured = crate::provider::model_catalog::capture_model_provider(
            Arc::new(provider(base, RemoteCompactionSupport::ResponsesV2)),
            &["selected".into()],
        );
        let mut denied = request();
        denied.model = "not-captured".into();
        let result = captured
            .compact_remote_with_usage(denied, AbortSignal::new(), 5_000, Arc::new(|_| {}), None)
            .await;
        server.abort();
        assert_eq!(
            result.unwrap_err().code.as_ref(),
            Some(&RuntimeString::from("native_compaction_incompatible"))
        );
    }

    #[tokio::test]
    async fn native_usage_is_reported_once_per_attempt_even_when_output_is_rejected() {
        let incomplete = "data: {\"type\":\"response.incomplete\",\"response\":{\"id\":\"first\",\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"usage\":{\"input_tokens\":30,\"output_tokens\":7}}}\n\n";
        let invalid = "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"bad\",\"status\":\"completed\",\"usage\":{\"input_tokens\":50,\"output_tokens\":9},\"output\":[]}}\n\n";
        let (base, server) = fixture(vec![
            ("text/event-stream", incomplete.into()),
            ("text/event-stream", complete("retried")),
            ("text/event-stream", invalid.into()),
        ])
        .await;
        let provider = provider(base, RemoteCompactionSupport::ResponsesV2);
        let receipts = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = receipts.clone();
        let observer: crate::types::provider::NativeCompactionUsageObserver =
            Arc::new(move |usage| captured.lock().unwrap().push(usage));
        let result = provider
            .compact_remote_with_usage(request(), AbortSignal::new(), 5_000, observer.clone(), None)
            .await
            .unwrap();
        assert_eq!(result.usage.unwrap().output_tokens, 12);
        assert!(provider
            .compact_remote_with_usage(request(), AbortSignal::new(), 5_000, observer, None)
            .await
            .is_err());
        assert_eq!(server.await.unwrap().len(), 3);
        let usages = receipts.lock().unwrap();
        assert_eq!(
            usages
                .iter()
                .map(|usage| (usage.input_tokens, usage.output_tokens))
                .collect::<Vec<_>>(),
            vec![(30, 7), (100, 12), (50, 9)]
        );
    }

    #[tokio::test]
    #[ignore = "Requires an explicitly supplied isolated subscription credential file and model"]
    async fn live_native_precompute_runs_beside_foreground_and_replays() {
        use crate::context::compact::engine::Compactor;
        use crate::context::compact::{create_compactor, CompactorOptions};
        use crate::provider::codex_subscription::{
            codex_oauth_defaults, create_codex_subscription_provider, CodexSubscriptionOptions,
        };
        use crate::provider::oauth::OAuthTokens;
        use crate::schemas::config::EngineConfig;
        let user = |id: &str, body: &str| {
            Message::User(crate::schemas::message::UserMessage {
                uuid: id.into(),
                is_meta: None,
                question_answer: None,
                content: vec![crate::schemas::content::ContentBlock::Text(
                    crate::schemas::content::TextBlock {
                        text: body.into(),
                        citations: None,
                        harness_envelope: None,
                        message_id: None,
                        phase: None,
                    },
                )],
            })
        };
        let path =
            std::env::var("KLAUS_NATIVE_LIVE_AUTH").expect("isolated credential path is required");
        let model = std::env::var("KLAUS_NATIVE_LIVE_MODEL")
            .expect("an explicitly selected model is required");
        let bytes = std::fs::read(path).expect("cannot read isolated credentials");
        let tokens: serde_json::Value =
            serde_json::from_slice(&bytes).expect("invalid isolated credential JSON");
        let (provider, _) = create_codex_subscription_provider(CodexSubscriptionOptions {
            oauth: codex_oauth_defaults("live-probe", None),
            store: None,
            tokens: Some(OAuthTokens {
                access_token: tokens["accessToken"]
                    .as_str()
                    .expect("missing access token")
                    .into(),
                id_token: tokens["idToken"].as_str().map(Into::into),
                expires_at: tokens["expiresAt"].as_i64(),
                refresh_token: None,
            }),
            base_url: Some("https://chatgpt.com/backend-api/codex".into()),
            default_model: Some(model.clone()),
            access_policy: None,
            identity: None,
        });
        let provider = Arc::new(provider);
        let mut request = request();
        request.model = model.clone().into();
        request.max_output_tokens = 256;
        request.system = vec![crate::types::provider::SystemPromptPart {
            name: "probe".into(),
            text: "Answer briefly. Preserve the exact probe marker when asked.".into(),
            cacheable: None,
            cache_scope: None,
        }];
        request.messages = vec![user("probe-user", "Remember the exact marker native_probe_lighthouse_7481 and that the pending task is to verify replay.")];
        let compactor = create_compactor(CompactorOptions {
            call_model: Box::new(|_, _| panic!("native path required")),
            model: model.clone().into(),
            max_output_tokens: Some(256),
            uuid: Box::new(|| uuid::Uuid::new_v4().to_string()),
        });
        compactor.bind_native_provider(provider.clone());
        let config = EngineConfig {
            model: model.into(),
            stream_idle_timeout_ms: 120_000,
            ..Default::default()
        };
        let fork = compactor
            .fork_precompute(Box::new(|_, _| panic!("native path required")), &config)
            .unwrap();
        assert!(fork.native_summary_active());
        assert!(fork.native_precompute_context().is_some());
        fork.prepare_request(&request, &AbortSignal::new());
        let started = std::time::Instant::now();
        let background = {
            let messages = request.messages.clone();
            tokio::spawn(async move { fork.threshold_compact(messages, &config, None).await })
        };
        let mut foreground = request.clone();
        foreground.messages = vec![user("foreground", "Reply exactly: foreground_ready")];
        let foreground_events: Vec<_> = provider
            .stream(foreground, AbortSignal::new())
            .collect()
            .await;
        assert!(
            foreground_events
                .iter()
                .all(|event| !matches!(event, ModelStreamEvent::Error { .. })),
            "foreground request failed"
        );
        let foreground_text: String = foreground_events
            .iter()
            .filter_map(|event| match event {
                ModelStreamEvent::TextDelta { text } => text.try_to_utf8().ok(),
                _ => None,
            })
            .collect();
        assert!(foreground_text.contains("foreground_ready"));
        println!(
            "foreground completed after {} ms",
            started.elapsed().as_millis()
        );
        let compacted = background.await.unwrap();
        println!(
            "native result available after {} ms; compacted={}",
            started.elapsed().as_millis(),
            compacted.compacted
        );
        assert!(compacted.compacted, "native background compaction failed");
        assert!(
            matches!(compacted.messages.last(), Some(Message::CompactBoundary(boundary)) if boundary.native_compaction.is_some())
        );
        let mut replay = request;
        replay.messages = compacted.messages;
        replay.messages.push(user(
            "verify",
            "Return the exact probe marker from the earlier context.",
        ));
        let events: Vec<_> = provider.stream(replay, AbortSignal::new()).collect().await;
        assert!(
            events
                .iter()
                .all(|event| !matches!(event, ModelStreamEvent::Error { .. })),
            "native replay failed"
        );
        let text: String = events
            .iter()
            .filter_map(|event| match event {
                ModelStreamEvent::TextDelta { text } => text.try_to_utf8().ok(),
                _ => None,
            })
            .collect();
        assert!(
            text.contains("native_probe_lighthouse_7481"),
            "replayed marker was not recovered"
        );
        println!("native background, parallel foreground and checkpoint replay passed");
    }

    #[tokio::test]
    async fn explicit_text_summary_replays_and_replaces_an_existing_checkpoint() {
        use crate::context::compact::engine::Compactor;
        use crate::context::compact::{create_compactor, CompactorOptions};
        use crate::provider::codex_subscription::{
            codex_oauth_defaults, create_codex_subscription_provider, CodexSubscriptionOptions,
        };
        use crate::provider::oauth::OAuthTokens;
        use crate::schemas::config::EngineConfig;

        let reply = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Portable task summary.\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"summary\",\"status\":\"completed\",\"usage\":{\"input_tokens\":100,\"output_tokens\":10}}}\n\n";
        let (base, server) = fixture(vec![("text/event-stream", reply.into())]).await;
        let (provider, _) = create_codex_subscription_provider(CodexSubscriptionOptions {
            oauth: codex_oauth_defaults("fixture-client", None),
            store: None,
            tokens: Some(OAuthTokens {
                access_token: "fixture-token".into(),
                expires_at: Some(4_000_000_000_000),
                ..Default::default()
            }),
            base_url: Some(base.clone()),
            default_model: Some("selected".into()),
            access_policy: None,
            identity: None,
        });
        let saved = checkpoint(&base, "fixture-key");
        assert!(provider
            .native_checkpoint_compatibility(&saved, &"selected".into(), &AbortSignal::new())
            .await
            .unwrap());
        let original = vec![Message::CompactBoundary(CompactBoundaryMessage {
            uuid: "old-checkpoint".into(),
            summary: RuntimeString::default(),
            native_compaction: Some(saved),
        })];
        let provider = Arc::new(provider);
        let transport = provider.clone();
        let compactor = create_compactor(CompactorOptions {
            call_model: Box::new(move |request, signal| transport.stream(request, signal)),
            model: "selected".into(),
            max_output_tokens: Some(8_000),
            uuid: Box::new(|| uuid::Uuid::new_v4().to_string()),
        });
        compactor.bind_native_provider(provider);
        assert!(compactor.native_summary_active());
        compactor.prepare_request(&request(), &AbortSignal::new());
        let result = compactor
            .local_compact(original, &EngineConfig::default(), None)
            .await;
        assert!(result.compacted, "{result:?}");
        let Message::CompactBoundary(boundary) = result.messages.last().unwrap() else {
            panic!("expected a completed text checkpoint");
        };
        assert!(boundary.native_compaction.is_none());
        assert!(boundary.summary.contains("Portable task summary."));
        let captured = server.await.unwrap();
        assert_eq!(captured.len(), 1);
        assert!(captured[0].0.starts_with("POST /responses HTTP/1.1"));
        let input = captured[0].1["input"].as_array().unwrap();
        assert_eq!(input[0]["type"], vs("compaction"));
        assert_eq!(input[0]["encrypted_content"], vs("opaque"));
        assert!(input
            .iter()
            .all(|item| item.get("type") != Some(&vs("compaction_trigger"))));
        assert!(runtime_json::stringify(input.last().unwrap()).contains("context checkpoint"));
    }

    #[tokio::test]
    async fn subscription_factory_records_checkpoint_account_without_restricting_replay() {
        use crate::provider::codex_subscription::{
            codex_oauth_defaults, create_codex_subscription_provider, CodexSubscriptionOptions,
        };
        use crate::provider::oauth::OAuthTokens;

        let (base, server) = fixture(vec![("text/event-stream", complete("accepted"))]).await;
        let subscription = |account: &str| {
            let claims = format!(
                r#"{{"https://api.openai.com/auth":{{"chatgpt_account_id":"{account}"}}}}"#
            );
            let payload = base64::Engine::encode(
                &base64::engine::general_purpose::URL_SAFE_NO_PAD,
                claims.as_bytes(),
            );
            create_codex_subscription_provider(CodexSubscriptionOptions {
                oauth: codex_oauth_defaults("fixture-client", None),
                store: None,
                tokens: Some(OAuthTokens {
                    access_token: "fixture-access-token".into(),
                    id_token: Some(format!("e30.{payload}.fixture-signature").into()),
                    expires_at: Some(4_000_000_000_000),
                    ..Default::default()
                }),
                base_url: Some(base.clone()),
                default_model: Some("selected".into()),
                access_policy: None,
                identity: None,
            })
        };
        let (provider, auth) = subscription("fixture-account");
        let headers = auth.get_headers(&AbortSignal::new()).await.unwrap();
        assert!(headers
            .keys()
            .all(|name| !name.eq_ignore_ascii_case("chatgpt-account-id")));
        assert_eq!(
            auth.credential_account_id().as_deref(),
            Some("fixture-account")
        );
        let result = provider
            .compact_remote(request(), AbortSignal::new())
            .await
            .unwrap();
        assert_eq!(
            result.checkpoint.auth_scope,
            digest_scope("account:fixture-account")
        );
        assert!(provider
            .native_checkpoint_compatibility(
                &result.checkpoint,
                &"selected".into(),
                &AbortSignal::new(),
            )
            .await
            .unwrap());
        let (other, _) = subscription("other-account");
        assert!(other
            .native_checkpoint_compatibility(
                &result.checkpoint,
                &"selected".into(),
                &AbortSignal::new(),
            )
            .await
            .unwrap());
        let captured = server.await.unwrap();
        assert_eq!(captured.len(), 1);
        assert!(captured[0].0.starts_with("POST /responses HTTP/1.1"));
        assert!(captured[0]
            .0
            .lines()
            .any(|line| line.eq_ignore_ascii_case("authorization: Bearer fixture-access-token")));
        assert_eq!(
            captured[0].1["input"].as_array().unwrap().last().unwrap()["type"],
            vs("compaction_trigger")
        );
    }

    #[tokio::test]
    async fn incomplete_native_streams_retry_but_install_no_partial_output() {
        let (base, server) = fixture(vec![("text/event-stream", "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"compaction\",\"encrypted_content\":\"discarded\"}}\n\n".into()), ("text/event-stream", complete("accepted"))]).await;
        let provider = provider(base, RemoteCompactionSupport::ResponsesV2);
        let result = provider
            .compact_remote(request(), AbortSignal::new())
            .await
            .unwrap();
        assert_eq!(result.checkpoint.encrypted_content, "accepted");
        assert_eq!(server.await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn incomplete_response_without_nested_error_retries_before_installing_a_checkpoint() {
        let (base, server) = fixture(vec![
            (
                "text/event-stream",
                incomplete(
                    "max_output_tokens",
                    r#"[{"type":"compaction","encrypted_content":"discarded"}]"#,
                ),
            ),
            ("text/event-stream", complete("accepted")),
        ])
        .await;
        let result = provider(base, RemoteCompactionSupport::ResponsesV2)
            .compact_remote(request(), AbortSignal::new())
            .await
            .unwrap();
        assert_eq!(result.checkpoint.encrypted_content, "accepted");
        assert_eq!(server.await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn incomplete_response_retries_are_capped_at_two() {
        let (base, server) = fixture(vec![
            ("text/event-stream", incomplete("max_output_tokens", "[]")),
            ("text/event-stream", incomplete("max_output_tokens", "[]")),
            ("text/event-stream", incomplete("max_output_tokens", "[]")),
        ])
        .await;
        let mut provider = provider(base, RemoteCompactionSupport::ResponsesV2);
        provider.retry.max_attempts = Some(100);
        let error = provider
            .compact_remote(request(), AbortSignal::new())
            .await
            .unwrap_err();
        assert_eq!(error.kind, ProviderErrorKind::Network);
        assert_eq!(error.code, Some("native_compaction_incomplete".into()));
        assert_eq!(server.await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn content_filtered_native_responses_are_terminal() {
        let (base, server) = fixture(vec![(
            "text/event-stream",
            incomplete(
                "content_filter",
                r#"[{"type":"compaction","encrypted_content":"discarded"}]"#,
            ),
        )])
        .await;
        let error = provider(base, RemoteCompactionSupport::ResponsesV2)
            .compact_remote(request(), AbortSignal::new())
            .await
            .unwrap_err();
        assert_eq!(error.kind, ProviderErrorKind::Withheld);
        assert_eq!(error.code, Some("content_filter".into()));
        assert_eq!(server.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn interrupted_native_response_accepts_one_authoritative_checkpoint() {
        let (base, server) = fixture(vec![(
            "text/event-stream",
            incomplete(
                "interrupted",
                r#"[{"type":"compaction","encrypted_content":"accepted"}]"#,
            ),
        )])
        .await;
        let result = provider(base, RemoteCompactionSupport::ResponsesV2)
            .compact_remote(request(), AbortSignal::new())
            .await
            .unwrap();
        assert_eq!(result.checkpoint.encrypted_content, "accepted");
        assert_eq!(result.checkpoint.response_id, "response-1");
        assert_eq!(server.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn interrupted_native_response_rejects_missing_or_invalid_checkpoints() {
        let mut bodies = [
            "[]",
            r#"[{"type":"compaction","encrypted_content":""}]"#,
            r#"[{"type":"compaction","encrypted_content":"first"},{"type":"compaction","encrypted_content":"second"}]"#,
        ]
        .map(|output| incomplete("interrupted", output))
        .to_vec();
        bodies.push("data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"interrupted\"},\"output\":[{\"type\":\"compaction\",\"encrypted_content\":\"discarded\"}]}}\n\n".into());
        for body in bodies {
            let (base, server) = fixture(vec![("text/event-stream", body)]).await;
            let error = provider(base, RemoteCompactionSupport::ResponsesV2)
                .compact_remote(request(), AbortSignal::new())
                .await
                .unwrap_err();
            assert_eq!(error.kind, ProviderErrorKind::InvalidRequest);
            assert_eq!(server.await.unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn malformed_native_output_is_terminal() {
        let body = "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"response\",\"output\":[]}}\n\n".to_owned();
        let (base, server) = fixture(vec![("text/event-stream", body)]).await;
        let error = provider(base, RemoteCompactionSupport::ResponsesV2)
            .compact_remote(request(), AbortSignal::new())
            .await
            .unwrap_err();
        assert_eq!(
            error.code.as_ref(),
            Some(&RuntimeString::from("invalid_native_compaction"))
        );
        assert_eq!(server.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn checkpoint_replay_rejects_another_account_before_transport() {
        let provider = provider(
            "http://127.0.0.1:1".into(),
            RemoteCompactionSupport::ResponsesV2,
        );
        let checkpoint = NativeCompactionCheckpoint {
            source: "http://127.0.0.1:1".into(),
            auth_scope: digest_scope("api-key:other-key"),
            model: "selected".into(),
            comp_hash: None,
            response_id: "response".into(),
            encrypted_content: "opaque".into(),
            retained_input: Vec::new(),
            retained_input_metadata: Vec::new(),
        };
        let mut replay = request();
        replay.messages = vec![Message::CompactBoundary(CompactBoundaryMessage {
            uuid: "boundary".into(),
            summary: "".into(),
            native_compaction: Some(checkpoint),
        })];
        let mut stream = provider.stream(replay, AbortSignal::new());
        let Some(ModelStreamEvent::Error { error }) = stream.next().await else {
            panic!("Replay must be rejected");
        };
        assert_eq!(
            error.code.as_ref(),
            Some(&RuntimeString::from("native_compaction_incompatible"))
        );
    }

    #[test]
    fn retention_uses_the_latest_user_text_budget_and_opaque_binary_estimate() {
        let mut req = request();
        let Message::User(user) = &mut req.messages[0] else {
            unreachable!()
        };
        let ContentBlock::Text(text) = &mut user.content[0] else {
            unreachable!()
        };
        text.text = "x".repeat(64_000 * 4 + 8).into();
        let retained = retain_user_input(&retention_input(&req));
        let text = retained[0].item["content"].as_str().unwrap();
        assert!(text.contains("…2 tokens truncated…"));
        let text_tokens = (text.to_node_utf8().len() as u64).div_ceil(4);
        let checkpoint = NativeCompactionCheckpoint {
            source: "endpoint".into(),
            auth_scope: "account".into(),
            model: "selected".into(),
            comp_hash: None,
            response_id: "response".into(),
            encrypted_content: "a".repeat(4_000).into(),
            retained_input: retained.into_iter().map(|entry| entry.item).collect(),
            retained_input_metadata: Vec::new(),
        };
        assert_eq!(
            checkpoint.estimated_context_tokens(),
            text_tokens + (3_000u64 - 650).div_ceil(4)
        );
    }

    #[test]
    fn completed_text_items_preserve_authoritative_text_once() {
        let item = runtime_json::parse(&RuntimeString::from(r#"{"type":"response.output_item.done","item":{"type":"message","id":"message-1","phase":"final_answer","content":[{"type":"output_text","text":"Accepted summary."}]}}"#)).unwrap();
        let mut state = ResponsesState::default();
        let (events, _) = map_responses_event(&item, &mut state);
        let completed = events
            .into_iter()
            .find(|event| matches!(event, ModelStreamEvent::TextMessageCompleted { .. }))
            .unwrap();
        assert_eq!(
            completed,
            ModelStreamEvent::TextMessageCompleted {
                id: "message-1".into(),
                phase: Some("final_answer".into()),
                text: "Accepted summary.".into()
            }
        );
        let wire = runtime_codec::encode_value(&completed).unwrap();
        assert_eq!(
            runtime_codec::decode_value::<ModelStreamEvent>(&wire).unwrap(),
            completed
        );
        let (duplicate, _) = map_responses_event(&item, &mut state);
        assert!(!duplicate
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::TextMessageCompleted { .. })));
    }

    #[tokio::test]
    async fn native_stream_retries_are_capped_at_two() {
        let (base, server) = fixture(vec![
            ("text/event-stream", String::new()),
            ("text/event-stream", String::new()),
            ("text/event-stream", String::new()),
        ])
        .await;
        let mut provider = provider(base, RemoteCompactionSupport::ResponsesV2);
        provider.retry.max_attempts = Some(100);
        assert!(provider
            .compact_remote(request(), AbortSignal::new())
            .await
            .is_err());
        assert_eq!(server.await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn a_stalled_native_stream_uses_the_configured_idle_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = [0; 4096];
            assert!(socket.read(&mut bytes).await.unwrap() > 0);
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
            std::future::pending::<()>().await;
        });
        let mut provider = provider(base, RemoteCompactionSupport::ResponsesV2);
        provider.retry.max_attempts = Some(1);
        let error = provider
            .compact_remote_with_idle_timeout(request(), AbortSignal::new(), 10)
            .await
            .unwrap_err();
        server.abort();
        assert_eq!(error.kind, ProviderErrorKind::Timeout);
        assert_eq!(error.code, Some("native_compaction_idle_timeout".into()));
    }

    #[tokio::test]
    async fn an_explicit_v1_endpoint_preserves_only_validated_retained_input() {
        let body = r#"{"id":"compact-1","output":[{"type":"message","id":"server-user","role":"user","content":[{"type":"input_text","text":"Keep this requirement."}]},{"type":"compaction","encrypted_content":"opaque-v1"}]}"#.to_owned();
        let (base, server) = fixture(vec![("application/json", body)]).await;
        let result = provider(base, RemoteCompactionSupport::ResponsesV1)
            .compact_remote(request(), AbortSignal::new())
            .await
            .unwrap();
        assert_eq!(result.checkpoint.encrypted_content, "opaque-v1");
        let captured = server.await.unwrap();
        assert!(captured[0]
            .0
            .starts_with("POST /responses/compact HTTP/1.1"));
        assert!(captured[0].1.get("stream").is_none());
    }

    fn checkpoint(source: &str, key: &str) -> NativeCompactionCheckpoint {
        NativeCompactionCheckpoint {
            source: source.into(),
            auth_scope: digest_scope(&format!("api-key:{key}")),
            model: "old-model".into(),
            comp_hash: Some("old-hash".into()),
            response_id: "response".into(),
            encrypted_content: "opaque".into(),
            retained_input: Vec::new(),
            retained_input_metadata: Vec::new(),
        }
    }

    #[tokio::test]
    async fn capable_endpoints_probe_source_compatibility_before_transport() {
        let provider = provider(
            "http://127.0.0.1:1/target".into(),
            RemoteCompactionSupport::ResponsesV2,
        );
        let checkpoint = checkpoint("http://127.0.0.1:1/source", "fixture-key");
        assert!(!provider
            .native_checkpoint_compatibility(&checkpoint, &"selected".into(), &AbortSignal::new())
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn capable_endpoints_probe_current_account_scope_without_restricting_model_hash() {
        let provider = provider(
            "http://127.0.0.1:1".into(),
            RemoteCompactionSupport::ResponsesV2,
        );
        let wrong_account = checkpoint("http://127.0.0.1:1", "other-key");
        assert!(!provider
            .native_checkpoint_compatibility(
                &wrong_account,
                &"new-model".into(),
                &AbortSignal::new()
            )
            .await
            .unwrap());
        let same_account = checkpoint("http://127.0.0.1:1", "fixture-key");
        assert!(provider
            .native_checkpoint_compatibility(
                &same_account,
                &"new-model".into(),
                &AbortSignal::new()
            )
            .await
            .unwrap());
    }

    #[test]
    fn retained_text_budget_charges_utf8_and_preserves_surrogate_pairs() {
        let (prefix, bytes) = retained_text_prefix(&RuntimeString::from("中中中"), 7);
        assert_eq!(prefix, "中中");
        assert_eq!(bytes, 6);
        let (prefix, bytes) = retained_text_prefix(&RuntimeString::from("A🦀"), 4);
        assert_eq!(prefix, "A");
        assert_eq!(bytes, 1);
        let (prefix, bytes) = retained_text_prefix(&RuntimeString::from("A🦀"), 5);
        assert_eq!(prefix, "A🦀");
        assert_eq!(bytes, 5);
    }

    fn retained_item(content: Value) -> RetainedInput {
        RetainedInput {
            item: vobj([("role", vs("user")), ("content", content)]),
            metadata: None,
        }
    }

    fn image_part() -> Value {
        vobj([
            ("type", vs("input_image")),
            ("image_url", vs("data:image/png;base64,QQ==")),
        ])
    }

    fn text_part(text: &str) -> Value {
        vobj([("type", vs("input_text")), ("text", vs(text))])
    }

    fn agent_request(body: &str) -> ModelRequest {
        let mut request = request();
        let Message::User(user) = &mut request.messages[0] else {
            unreachable!()
        };
        let ContentBlock::Text(mut origin) = user.content[0].clone() else {
            unreachable!()
        };
        origin.text = format!("{AGENT_ORIGIN_PREFIX}{{\"type\":\"subagent-message-origin\",\"sender\":\"agent-1\",\"userInitiated\":false,\"generation\":0}}").into();
        origin.harness_envelope = Some(HarnessEnvelope::SubagentMessage);
        let mut content = origin.clone();
        content.text = body.into();
        user.uuid = "agent-source-1".into();
        user.is_meta = Some(true);
        user.content = vec![ContentBlock::Text(origin), ContentBlock::Text(content)];
        request
    }

    #[test]
    fn image_boundaries_share_media_cost_and_never_backfill_older_messages() {
        let old = retained_item(vs("old"));
        let image = retained_item(Value::Array(vec![image_part()]));
        let newest = retained_item(vs("new"));
        let input = vec![old, image.clone(), newest.clone()];
        let image_tokens = native_retained_item_tokens(&image.item);
        assert_eq!(image_tokens, crate::context::tokens::MEDIA_TOKENS);
        for (budget, expected) in [
            (
                image_tokens + 1,
                vec![image.item.clone(), newest.item.clone()],
            ),
            (image_tokens, vec![newest.item.clone()]),
            (1, vec![newest.item.clone()]),
        ] {
            let retained = retain_input_with_budget(&input, budget);
            assert_eq!(
                retained
                    .into_iter()
                    .map(|entry| entry.item)
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[test]
    fn image_groups_keep_later_content_and_their_adjacent_labels_atomic() {
        let group = vec![
            text_part("<image name=[Image #7] path=\"image.png\">"),
            image_part(),
            text_part("</image>"),
        ];
        let group_tokens = group.iter().map(native_retained_part_tokens).sum::<u64>();
        let parts = std::iter::once(text_part("earlier text"))
            .chain(group.clone())
            .chain([text_part("keep")])
            .collect::<Vec<_>>();
        let input = [retained_item(Value::Array(parts))];
        for (budget, expected) in [
            (group_tokens, vec![text_part("keep")]),
            (
                group_tokens + 1,
                group
                    .clone()
                    .into_iter()
                    .chain([text_part("keep")])
                    .collect(),
            ),
        ] {
            let retained = retain_input_with_budget(&input, budget);
            assert_eq!(retained[0].item["content"], Value::Array(expected));
        }
    }

    #[test]
    fn text_boundaries_preserve_both_ends_and_budget_parts_in_order() {
        assert_eq!(
            retained_text_middle(&"abcdefghijklmnop".into(), 2),
            "abcd…2 tokens truncated…mnop"
        );
        let input = [retained_item(Value::Array(vec![
            text_part("start"),
            text_part("abcdefghijklmnopqrstuvwxyz"),
            text_part("end"),
        ]))];
        let retained = retain_input_with_budget(&input, 5);
        assert_eq!(
            retained[0].item["content"],
            Value::Array(vec![
                text_part("start"),
                text_part("abcdef…4 tokens truncated…uvwxyz")
            ])
        );
        let unicode = retained_text_middle(&"中🦀中🦀中".into(), 2);
        assert_eq!(unicode, "中…3 tokens truncated…中");
    }

    #[test]
    fn agent_boundaries_preserve_required_origin_without_duplicating_the_body() {
        let mut request = agent_request(&format!("HEAD{}TAIL", "x".repeat(200)));
        let source = retention_input(&request);
        let metadata = source[0].metadata.as_ref().unwrap();
        let origin = required_provenance(metadata).unwrap();
        let origin_tokens = (origin.to_node_utf8().len() as u64).div_ceil(4);
        assert_eq!(metadata.source_message_id, "agent-source-1");
        assert_eq!(metadata.provenance.len(), 1);
        let retained = retain_input_with_budget(&source, origin_tokens + 3);
        assert_eq!(retained[0].metadata, source[0].metadata);
        assert!(retained_item_has_provenance(&retained[0].item, metadata));
        let text = retained[0].item["content"].as_str().unwrap();
        assert!(
            text.contains("HEAD") && text.ends_with("TAIL") && text.contains("tokens truncated")
        );
        assert!(!runtime_json::stringify(&metadata.provenance[0]).contains("HEAD"));
        assert!(retain_input_with_budget(&source, origin_tokens).is_empty());
        let mut partial_checkpoint = checkpoint("http://127.0.0.1:1", "fixture-key");
        partial_checkpoint.retained_input =
            retained.iter().map(|entry| entry.item.clone()).collect();
        partial_checkpoint.retained_input_metadata = retained
            .iter()
            .map(|entry| entry.metadata.clone())
            .collect();
        let encoded = runtime_codec::encode_value(&partial_checkpoint).unwrap();
        let restored = runtime_codec::decode_value(&encoded).unwrap();
        request.messages = vec![Message::CompactBoundary(CompactBoundaryMessage {
            uuid: "partial-checkpoint".into(),
            summary: "".into(),
            native_compaction: Some(restored),
        })];
        let repeated = retain_user_input(&retention_input(&request));
        assert_eq!(repeated[0].metadata, retained[0].metadata);
        assert_eq!(repeated[0].item, retained[0].item);
    }

    #[test]
    fn agent_retention_requires_structural_provenance_and_excludes_oversized_communication() {
        let empty = agent_request("");
        let prefix_bytes = native_retained_item_tokens(&retention_input(&empty)[0].item) * 4;
        let fitting =
            agent_request(&"x".repeat((MAX_RETAINED_AGENT_TOKENS * 4 - prefix_bytes) as usize));
        assert_eq!(retention_input(&fitting).len(), 1);
        assert!(retention_input(&agent_request(
            &"x".repeat((MAX_RETAINED_AGENT_TOKENS * 4) as usize)
        ))
        .is_empty());
        let mut ordinary = fitting.clone();
        {
            let Message::User(user) = &mut ordinary.messages[0] else {
                unreachable!()
            };
            user.is_meta = None;
            for block in &mut user.content {
                if let ContentBlock::Text(text) = block {
                    text.harness_envelope = None;
                }
            }
        }
        assert!(retention_input(&ordinary)[0].metadata.is_none());
        let Message::User(user) = &mut ordinary.messages[0] else {
            unreachable!()
        };
        user.is_meta = Some(true);
        assert!(retention_input(&ordinary).is_empty());
    }

    #[tokio::test]
    async fn retained_agent_classification_and_origin_survive_two_native_requests() {
        let (base, server) = fixture(vec![
            ("text/event-stream", complete("first")),
            ("text/event-stream", complete("second")),
        ])
        .await;
        let provider = provider(base, RemoteCompactionSupport::ResponsesV2);
        let mut request = agent_request("Keep this direct communication.");
        let mut ordinary = request.messages[0].clone();
        let Message::User(user) = &mut ordinary else {
            unreachable!()
        };
        user.uuid = "ordinary-source".into();
        user.is_meta = None;
        for block in &mut user.content {
            if let ContentBlock::Text(text) = block {
                text.harness_envelope = None;
            }
        }
        request.messages.push(ordinary);
        let first = provider
            .compact_remote(request.clone(), AbortSignal::new())
            .await
            .unwrap()
            .checkpoint;
        assert_eq!(first.retained_input_metadata.len(), 2);
        assert_eq!(
            first.retained_input_metadata[0]
                .as_ref()
                .unwrap()
                .source_message_id,
            "agent-source-1"
        );
        assert!(first.retained_input_metadata[1].is_none());
        let encoded = runtime_codec::encode_value(&first).unwrap();
        let restored: NativeCompactionCheckpoint = runtime_codec::decode_value(&encoded).unwrap();
        assert_eq!(restored, first);
        request.messages = vec![Message::CompactBoundary(CompactBoundaryMessage {
            uuid: "first-checkpoint".into(),
            summary: "".into(),
            native_compaction: Some(restored),
        })];
        let second = provider
            .compact_remote(request, AbortSignal::new())
            .await
            .unwrap()
            .checkpoint;
        assert_eq!(second.retained_input, first.retained_input);
        assert_eq!(
            second.retained_input_metadata,
            first.retained_input_metadata
        );
        for (_, request) in server.await.unwrap() {
            let wire = runtime_json::stringify(&request);
            assert!(
                !wire.contains("retainedInputMetadata")
                    && !wire.contains("sourceMessageId")
                    && !wire.contains("harnessEnvelope")
            );
        }
    }

    #[tokio::test]
    async fn malformed_retained_provenance_is_rejected_before_transport_and_legacy_is_ordinary() {
        let source = "http://127.0.0.1:1";
        let provider = provider(source.into(), RemoteCompactionSupport::ResponsesV2);
        let old = checkpoint(source, "fixture-key");
        let wire = runtime_codec::encode_value(&old).unwrap();
        assert!(wire.get("retainedInputMetadata").is_none());
        let restored: NativeCompactionCheckpoint = runtime_codec::decode_value(&wire).unwrap();
        assert!(restored.retained_input_metadata.is_empty());
        let mut null_sidecar = wire.clone();
        null_sidecar
            .as_object_mut()
            .unwrap()
            .insert("retainedInputMetadata", Value::Null);
        assert!(runtime_codec::decode_value::<NativeCompactionCheckpoint>(&null_sidecar).is_err());
        let agent = retention_input(&agent_request("body")).remove(0);
        for (items, metadata) in [
            (Vec::new(), vec![agent.metadata.clone()]),
            (vec![agent.item.clone()], vec![None, None]),
            (
                vec![retained_item(vs("ordinary")).item],
                vec![agent.metadata.clone()],
            ),
            (
                vec![agent.item.clone()],
                vec![Some(NativeRetainedInputMetadata {
                    source_message_id: "".into(),
                    provenance: Vec::new(),
                })],
            ),
            (
                vec![agent.item.clone()],
                vec![Some(NativeRetainedInputMetadata {
                    source_message_id: "agent-source-1".into(),
                    provenance: vec![vobj([
                        ("type", vs("text")),
                        ("text", vs("unmarked origin")),
                    ])],
                })],
            ),
        ] {
            let mut invalid = old.clone();
            invalid.retained_input = items;
            invalid.retained_input_metadata = metadata;
            let error = provider
                .native_checkpoint_compatibility(&invalid, &"selected".into(), &AbortSignal::new())
                .await
                .unwrap_err();
            assert_eq!(error.code, Some("invalid_native_compaction".into()));
        }
    }
}
