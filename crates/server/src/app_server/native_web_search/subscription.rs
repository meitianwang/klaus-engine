//! Claude subscription search backend.
//!
//! One search-only Messages request against the subscription's own endpoint:
//! the hosted web search server tool is the only search surface offered and the
//! request asks for it, so the service searches instead of answering from
//! memory. The hits are the search results and citations the stream carries;
//! when the stream returns none, the answer's own source lines are parsed as a
//! last resort. The credential is the stored subscription login, renewed by the
//! shared OAuth source; a missing or expired credential fails the search with
//! the reason — never a silent fallback to another backend.

use futures::StreamExt;
use klaus_agent_core::provider::claude_subscription::{
    create_claude_subscription_provider, ClaudeSubscriptionOptions,
};
use klaus_agent_core::provider::oauth::OAuthTokens;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{Message, UserMessage};
use klaus_agent_core::tools::builtins::web_search::{WebSearchBackend, WebSearchResult};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStreamEvent, SystemPromptPart, WebSearchRequest,
};
use serde_json::Value;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

/// Upper bound on collected hits; a search answer cites far fewer.
const MAX_HITS: usize = 20;

pub(crate) fn backend(
    search_access: klaus_agent_core::schemas::config::WebSearchAccess,
) -> WebSearchBackend {
    Arc::new(move |query, signal| {
        Box::pin(async move { search(query, signal, search_access).await })
    })
}

fn text(value: &RuntimeString) -> String {
    value.try_to_utf8().unwrap_or_default()
}

fn fail(message: impl Into<String>) -> RuntimeString {
    RuntimeString::from(message.into())
}

/// The subscription provider bound to the first stored account. A search spends
/// one request, so account rotation — a quota concern across long sessions — is
/// deliberately left to the session path.
fn provider(root: &Path) -> Result<impl ModelProvider, RuntimeString> {
    let accounts = crate::app_server::model_sources::claude_accounts(root).map_err(|_| {
        fail(
            "The Claude subscription is not signed in. Sign in on the Models page to use its search service.",
        )
    })?;
    let head = accounts.first().ok_or_else(|| {
        fail(
            "The Claude subscription is not signed in. Sign in on the Models page to use its search service.",
        )
    })?;
    let tokens = head.get("tokens").cloned().unwrap_or(Value::Null);
    let access = tokens
        .get("accessToken")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .ok_or_else(|| {
            fail("The stored Claude credential has no access token. Sign in again on the Models page.")
        })?;
    let (provider, _auth) = create_claude_subscription_provider(ClaudeSubscriptionOptions {
        oauth: crate::app_server::model_sources::claude_oauth_config(),
        store: None,
        tokens: Some(OAuthTokens {
            access_token: RuntimeString::from(access),
            refresh_token: tokens
                .get("refreshToken")
                .and_then(Value::as_str)
                .filter(|token| !token.is_empty())
                .map(RuntimeString::from),
            expires_at: tokens.get("expiresAt").and_then(Value::as_i64),
            id_token: None,
        }),
        base_url: None,
        beta_header: None,
        default_model: None,
        access_policy: None,
        identity: None,
    });
    Ok(provider)
}

async fn search(
    query: RuntimeString,
    signal: AbortSignal,
    search_access: klaus_agent_core::schemas::config::WebSearchAccess,
) -> Result<Vec<WebSearchResult>, RuntimeString> {
    if search_access == klaus_agent_core::schemas::config::WebSearchAccess::Disabled {
        return Err(fail("Web search is disabled for this conversation."));
    }
    let query_text = text(&query);
    if query_text.trim().is_empty() {
        return Err(fail("The search query is empty."));
    }
    let root = super::config_root();
    let provider = provider(&root)?;
    let request = ModelRequest {
        supports_image_detail_original: false,
        // A search-only request needs any web-search-capable subscription
        // model, not a specific one; the provider's own default is the lineup
        // head the catalog would hand a bare selection.
        model: provider.info().default_model.clone(),
        system: vec![SystemPromptPart {
            name: RuntimeString::from("search"),
            text: RuntimeString::from(
                "You are a search service. Run the web search tool for the user's query, then answer with ONLY a newline-separated list of the sources you found, one per line as \"title - url\". No commentary.",
            ),
            cacheable: None,
            cache_scope: None,
        }],
        messages: vec![Message::User(UserMessage {
            question_answer: None,
            uuid: RuntimeString::from(crate::app_server::qoder::uuid_v4()),
            content: vec![ContentBlock::Text(TextBlock {
                text: RuntimeString::from(query_text),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
        })],
        tools: Vec::new(),
        tool_choice: None,
        max_output_tokens: 1024,
        task_budget: None,
        output_schema: None,
        web_search: Some(WebSearchRequest {
            access: search_access,
            max_uses: Some(3),
            allowed_domains: None,
            blocked_domains: None,
            user_location: None,
            context_size: None,
        }),
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
        reasoning_summary: klaus_agent_core::types::provider::ReasoningSummary::Auto,
    };

    let mut stream = provider.stream(request, signal);
    let mut hits: Vec<WebSearchResult> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut answer = String::new();
    let mut failure: Option<String> = None;
    let push = |title: Option<String>,
                url: String,
                snippet: Option<String>,
                hits: &mut Vec<WebSearchResult>,
                seen: &mut HashSet<String>| {
        if hits.len() >= MAX_HITS || !url.starts_with("http") || !seen.insert(url.clone()) {
            return;
        }
        hits.push(WebSearchResult {
            title: RuntimeString::from(
                title
                    .filter(|t| !t.is_empty())
                    .unwrap_or_else(|| "(untitled)".to_string()),
            ),
            url: RuntimeString::from(url),
            snippet: snippet.map(RuntimeString::from),
        });
    };
    while let Some(event) = stream.next().await {
        match event {
            ModelStreamEvent::Citation { citation } => {
                let url = text(&citation.url);
                let title = citation.title.as_ref().map(text);
                let snippet = citation.cited_text.as_ref().map(text);
                push(title, url, snippet, &mut hits, &mut seen);
            }
            ModelStreamEvent::WebSearchResult { results, error, .. } => {
                if let Some(error) = error {
                    failure.get_or_insert_with(|| text(&error));
                }
                for result in results {
                    let url = text(&result.url);
                    push(Some(text(&result.title)), url, None, &mut hits, &mut seen);
                }
            }
            ModelStreamEvent::TextDelta { text: delta } => answer.push_str(&text(&delta)),
            ModelStreamEvent::Error { error } => {
                failure.get_or_insert_with(|| error.to_string());
                break;
            }
            ModelStreamEvent::MessageStop { .. } => break,
            _ => {}
        }
    }
    if let Some(failure) = failure {
        return Err(fail(format!("Claude search failed: {failure}")));
    }
    if hits.is_empty() {
        // The citations path found nothing: fall back to the source lines the
        // answer itself listed, which is what a search-only answer is for.
        for line in answer.lines() {
            let Some(position) = line.find("http") else {
                continue;
            };
            let url: String = line[position..]
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .trim_end_matches([')', ']', ',', '.', ';', ':', '"', '\''])
                .to_string();
            let title = line[..position]
                .trim()
                .trim_end_matches(['-', '—', ':'])
                .trim();
            push(
                (!title.is_empty()).then(|| title.to_string()),
                url,
                None,
                &mut hits,
                &mut seen,
            );
        }
    }
    if hits.is_empty() {
        return Err(fail(
            "The Claude search service returned no sources for this query.",
        ));
    }
    Ok(hits)
}
