//! Bounded, connection-owned plain-text inference through normal model sources.

use super::{
    model_sources, resolve_tier_target, send_invalid_params, wire, AppServerState, Outgoing,
};
use futures::StreamExt;
use klaus_agent_core::engine::session::InferenceTargetState;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{Message, UserMessage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent, ReasoningSummary,
    SystemPromptPart,
};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

const OPERATION_SOURCE: &str = "text-completion";
const MAX_INPUT_BYTES: usize = 128 * 1024;
const MAX_OUTPUT_BYTES: usize = 64 * 1024;
const MAX_OUTPUT_TOKENS: u64 = 32768;
type PublicationCheck = Arc<dyn Fn() -> bool + Send + Sync>;
type CredentialReader = fn() -> Option<(String, Value)>;

struct Response {
    result: wire::EngineCompleteTextResult,
    publication_check: Option<PublicationCheck>,
}

fn source_pin(source: &str, root: &std::path::Path) -> Result<Option<PublicationCheck>, String> {
    if source == "kiro" {
        super::super::kiro::set_config_root(root);
        let expected =
            super::super::kiro::auth::generation().ok_or("text_completion_model_unavailable")?;
        return Ok(Some(Arc::new(move || {
            super::super::kiro::auth::is_current(&expected)
        })));
    }
    if source == "trae" {
        super::super::trae::set_config_root(root);
        let expected =
            super::super::trae::auth::generation().ok_or("text_completion_model_unavailable")?;
        return Ok(Some(Arc::new(move || {
            super::super::trae::auth::is_current(&expected)
        })));
    }
    use super::super::{qoder, qoderwork, qwenwork, subscription_speech, workbuddy};
    let read: Option<CredentialReader> = match source {
        "qoder" => Some(qoder::auth::credential),
        "qoderwork" => Some(qoderwork::auth::credential),
        "qwenwork" => Some(qwenwork::auth::credential),
        "workbuddy" => Some(workbuddy::auth::credential),
        _ => None,
    };
    if let Some(read) = read {
        let expected = read().ok_or("text_completion_model_unavailable")?;
        return Ok(Some(Arc::new(move || read().as_ref() == Some(&expected))));
    }
    let source = match source {
        "subscription" => Some(subscription_speech::Source::Claude),
        "codex" => Some(subscription_speech::Source::Codex),
        _ => None,
    };
    source
        .map(|source| subscription_speech::pin(root, source))
        .transpose()
        .map_err(|_| "text_completion_model_unavailable".into())
}

async fn prepare_token(source: &str) -> Result<(), String> {
    if source == "kiro" {
        super::super::kiro::auth::credential(None).await?;
        return Ok(());
    }
    if source == "trae" {
        super::super::trae::auth::credential(None).await?;
        return Ok(());
    }
    use super::super::{qoder, qoderwork, qwenwork};
    let read: Option<CredentialReader> = match source {
        "qoder" => Some(qoder::auth::credential),
        "qoderwork" => Some(qoderwork::auth::credential),
        "qwenwork" => Some(qwenwork::auth::credential),
        _ => None,
    };
    let Some(read) = read else {
        return Ok(());
    };
    let before = read().ok_or("text_completion_model_unavailable")?;
    let refresh = match source {
        "qoder" => qoder::auth::ensure_fresh_token().await,
        "qoderwork" => qoderwork::auth::ensure_fresh_token().await,
        "qwenwork" => qwenwork::ensure_fresh_token().await,
        _ => unreachable!(),
    };
    refresh.map_err(|_| "text_completion_model_unavailable")?;
    let after = read().ok_or("provider_credential_changed")?;
    if account_identity(&before) != account_identity(&after) {
        return Err("provider_credential_changed".into());
    }
    Ok(())
}

fn account_identity(profile: &(String, Value)) -> (String, String) {
    let uid = profile.1["uid"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| profile.1["uid"].as_i64().map(|id| id.to_string()))
        .unwrap_or_default();
    (profile.0.clone(), uid)
}

fn validate(params: &wire::EngineCompleteTextParams) -> Result<(), &'static str> {
    if params.source.is_empty()
        || params.source.len() > 64
        || params.model_id.is_empty()
        || params.model_id.len() > 1024
        || params.source.chars().any(char::is_control)
        || params.model_id.chars().any(char::is_control)
        || params.source.trim() != params.source
        || params.model_id.trim() != params.model_id
        || params.prompt.trim().is_empty()
        || params.system.contains('\0')
        || params.prompt.contains('\0')
        || params.system.len().saturating_add(params.prompt.len()) > MAX_INPUT_BYTES
        || !(1..=MAX_OUTPUT_TOKENS).contains(&params.max_output_tokens)
    {
        return Err("text_completion_invalid_request");
    }
    Ok(())
}

pub(super) async fn dispatch(state: Arc<AppServerState>, id: Value, method: &str, params: Value) {
    if method == "engine/cancelTextCompletion" {
        let params =
            match serde_json::from_value::<wire::SubscriptionCancelTranscriptionParams>(params) {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
        match state
            .provider_operations
            .cancel(OPERATION_SOURCE, &params.operation_id)
        {
            Ok(cancelled) => {
                state
                    .send(wire::response(
                        &id,
                        serde_json::json!({"cancelled":cancelled}),
                    ))
                    .await
            }
            Err(error) => {
                state
                    .send(wire::error_response(&id, wire::INVALID_PARAMS, &error))
                    .await
            }
        }
        return;
    }
    let params = match serde_json::from_value::<wire::EngineCompleteTextParams>(params) {
        Ok(params) => params,
        Err(error) => return send_invalid_params(&state, &id, &error).await,
    };
    if let Err(error) = validate(&params) {
        return state
            .send(wire::error_response(&id, wire::INVALID_PARAMS, error))
            .await;
    }
    let operation = match state
        .provider_operations
        .register(OPERATION_SOURCE, Some(&params.operation_id))
    {
        Ok(operation) => operation,
        Err(error) => {
            return state
                .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                .await
        }
    };
    tokio::spawn(async move {
        let signal = operation.signal();
        let outcome = tokio::select! {
            biased;
            _ = state.out_tx.closed() => return,
            _ = signal.cancelled() => Err("provider_operation_cancelled".to_string()),
            outcome = tokio::time::timeout(Duration::from_secs(120), complete(&state, params, signal.clone())) => {
                match outcome {
                    Ok(outcome) => outcome,
                    Err(_) => { signal.cancel(); Err("text_completion_timed_out".to_string()) }
                }
            }
        };
        let mut operation = operation;
        let message = match outcome {
            Ok(response) => {
                if let Some(check) = response.publication_check {
                    operation.bind_publication_check(check);
                }
                wire::response(&id, serde_json::to_value(response.result).unwrap())
            }
            Err(error) => wire::error_response(&id, wire::ENGINE_ERROR, &error),
        };
        let _ = state
            .out_tx
            .send(Outgoing {
                message: message.into(),
                cursor_operation: None,
                workbuddy_operation: None,
                provider_operation: Some(operation),
            })
            .await;
    });
}

async fn complete(
    state: &AppServerState,
    params: wire::EngineCompleteTextParams,
    signal: AbortSignal,
) -> Result<Response, String> {
    let root = model_sources::config_root_of(&state.config_path);
    match params.source.as_str() {
        "kiro" => super::super::kiro::set_config_root(&root),
        "trae" => super::super::trae::set_config_root(&root),
        "qoder" => super::super::qoder::set_config_root(&root),
        "qoderwork" => super::super::qoderwork::set_config_root(&root),
        "qwenwork" => super::super::qwenwork::set_config_root(&root),
        "workbuddy" => super::super::workbuddy::set_config_root(&root),
        _ => {}
    }
    // Token preparation may legitimately rotate credentials for the same account.
    // Pin the refreshed generation before the provider's own request is prepared.
    prepare_token(&params.source).await?;
    let publication_check = source_pin(&params.source, &root)?;
    let resolved = resolve_tier_target(
        state,
        wire::ModelTierTarget::Source {
            source: params.source.clone(),
            model_id: params.model_id.clone(),
        },
    )
    .await
    .map_err(|_| "text_completion_model_unavailable")?;
    let decorate = model_sources::provider_decorator(&root);
    let target = klaus_agent_protocol::server::resolved_model_target(
        &resolved,
        state.credential_store.clone(),
        &decorate,
    )
    .await
    .map_err(|_| "text_completion_model_unavailable")?;
    let InferenceTargetState::Ready(target) = target else {
        return Err("text_completion_model_unavailable".into());
    };
    if signal.is_cancelled() {
        return Err("provider_operation_cancelled".into());
    }
    let capabilities = target.provider.capabilities(&target.model);
    let request = ModelRequest {
        model: target.model.clone(),
        system: vec![SystemPromptPart {
            name: RuntimeString::from("text-completion"),
            text: RuntimeString::from(params.system),
            cacheable: None,
            cache_scope: None,
        }],
        messages: vec![Message::User(UserMessage {
            question_answer: None,
            uuid: RuntimeString::from(params.operation_id),
            content: vec![ContentBlock::Text(TextBlock {
                text: RuntimeString::from(params.prompt),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
        })],
        tools: Vec::new(),
        tool_choice: None,
        max_output_tokens: params.max_output_tokens.min(capabilities.max_output_tokens),
        task_budget: None,
        output_schema: None,
        web_search: None,
        thinking: None,
        effort: None,
        skip_cache_write: Some(true),
        cache_ttl: None,
        cache_key: None,
        context_management: None,
        context_hints: None,
        thinking_context: None,
        image_limits: None,
        max_media_blocks: None,
        model_verbosity: None,
        reasoning_summary: ReasoningSummary::Auto,
        supports_image_detail_original: false,
    };
    let text = collect_checked(
        target.provider.stream(request, signal.clone()),
        signal,
        publication_check.as_ref(),
    )
    .await?;
    if publication_check.as_ref().is_some_and(|check| !check()) {
        return Err("provider_credential_changed".into());
    }
    // Sources without an account-lineage reader still retain the captured
    // provider. Reject a changed configured route before publishing its answer.
    if publication_check.is_none() {
        let current = resolve_tier_target(
            state,
            wire::ModelTierTarget::Source {
                source: params.source.clone(),
                model_id: params.model_id.clone(),
            },
        )
        .await
        .map_err(|_| "text_completion_model_unavailable")?;
        if current["type"] != "resolved"
            || current["auth"] != resolved["auth"]
            || current["provider"] != resolved["provider"]
        {
            return Err("provider_credential_changed".into());
        }
    }
    Ok(Response {
        result: wire::EngineCompleteTextResult {
            text,
            source: params.source,
            model_id: params.model_id,
        },
        publication_check,
    })
}

#[cfg(test)]
async fn collect(stream: ModelStream, signal: AbortSignal) -> Result<String, String> {
    collect_checked(stream, signal, None).await
}

async fn collect_checked(
    mut stream: ModelStream,
    signal: AbortSignal,
    publication_check: Option<&PublicationCheck>,
) -> Result<String, String> {
    let mut parts: Vec<(Option<String>, String)> = vec![(None, String::new())];
    let mut active = 0;
    loop {
        if publication_check.is_some_and(|check| !check()) {
            return Err("provider_credential_changed".into());
        }
        let event = tokio::select! {
            biased;
            _ = signal.cancelled() => return Err("provider_operation_cancelled".into()),
            _ = tokio::time::sleep(Duration::from_millis(50)), if publication_check.is_some() => continue,
            event = stream.next() => event.ok_or("text_completion_incomplete")?,
        };
        match event {
            ModelStreamEvent::TextMessage { id, .. } => {
                active = text_part(&mut parts, &utf8(&id)?, active);
            }
            ModelStreamEvent::TextDelta { text } => parts[active].1.push_str(&utf8(&text)?),
            ModelStreamEvent::TextMessageCompleted { id, text, .. } => {
                active = text_part(&mut parts, &utf8(&id)?, active);
                parts[active].1 = utf8(&text)?;
            }
            ModelStreamEvent::MessageStop { stop_reason, .. } => {
                if !matches!(
                    stop_reason,
                    ModelStopReason::EndTurn | ModelStopReason::StopSequence
                ) {
                    return Err("text_completion_incomplete".into());
                }
                let text = parts
                    .into_iter()
                    .map(|(_, text)| text)
                    .collect::<String>()
                    .trim()
                    .to_owned();
                if text.is_empty() {
                    return Err("text_completion_empty".into());
                }
                return Ok(text);
            }
            ModelStreamEvent::Error { .. } => return Err("text_completion_provider_failed".into()),
            ModelStreamEvent::ToolUseStart { .. }
            | ModelStreamEvent::ToolUseInputDelta { .. }
            | ModelStreamEvent::ToolUseStop { .. }
            | ModelStreamEvent::ServerSearchQuery { .. }
            | ModelStreamEvent::WebSearchResult { .. } => {
                return Err("text_completion_tools_not_allowed".into())
            }
            _ => {}
        }
        if parts.len() > 128
            || parts
                .iter()
                .map(|(id, text)| {
                    id.as_ref()
                        .map_or(0, String::len)
                        .saturating_add(text.len())
                })
                .sum::<usize>()
                > MAX_OUTPUT_BYTES
        {
            return Err("text_completion_result_too_large".into());
        }
    }
}

fn utf8(value: &RuntimeString) -> Result<String, &'static str> {
    value
        .try_to_utf8()
        .map_err(|_| "text_completion_non_scalar_text")
}

fn text_part(parts: &mut Vec<(Option<String>, String)>, id: &str, active: usize) -> usize {
    if let Some(index) = parts
        .iter()
        .position(|(existing, _)| existing.as_deref() == Some(id))
    {
        return index;
    }
    if parts[active].0.is_none() {
        parts[active].0 = Some(id.to_owned());
        return active;
    }
    parts.push((Some(id.to_owned()), String::new()));
    parts.len() - 1
}

#[cfg(test)]
#[path = "text_completion_tests.rs"]
mod tests;
