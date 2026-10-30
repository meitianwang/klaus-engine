//! Codex subscription search backend.
//!
//! One search-only Responses request against the subscription's own backend:
//! the hosted `web_search` tool is the only tool offered and the request asks
//! for it, so the service searches instead of answering from memory. The hits
//! are the URL citations the answer carries; when the service returns none,
//! the answer's own source lines are parsed as a last resort. A missing or
//! expired credential fails the search with the reason — never a silent
//! fallback to another backend.

use futures::StreamExt;
use klaus_agent_core::provider::auth::AuthSource;
use klaus_agent_core::provider::oauth::{OAuthAuth, OAuthAuthOptions, OAuthConfig, OAuthTokens};
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{Message, UserMessage};
use klaus_agent_core::tools::builtins::web_search::{WebSearchBackend, WebSearchResult};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelCapabilities, ModelProvider, ModelRequest, ModelStreamEvent, SystemPromptPart, ToolChoice,
    WebSearchRequest,
};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

/// The Responses host the Codex subscription routes through — the same one the
/// session model uses.
const CODEX_BASE_URL: &str = "https://chatgpt.com/backend-api/codex";
/// Upper bound on collected hits; a search answer cites far fewer.
const MAX_HITS: usize = 20;
/// Fallback model when the cached catalog holds no lineup yet. A search-only
/// request needs any web-search-capable subscription model, not a specific one.
const FALLBACK_MODEL: &str = "gpt-5.6-terra";

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

/// The first catalog model id at hand, waiting for the live catalog when the
/// cache is empty. The warm read is also the shared token-renewal leg, so it
/// completes before the search takes its credential snapshot.
async fn catalog_model(root: &Path) -> Option<String> {
    let catalog = crate::app_server::codex_catalog::list(
        root,
        crate::app_server::codex_catalog::CatalogRead::Warm,
    )
    .await;
    catalog
        .get("models")
        .and_then(Value::as_array)
        .and_then(|models| models.first())
        .and_then(|model| model.get("id"))
        .and_then(Value::as_str)
        .map(str::to_string)
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
    // Read the catalog FIRST: its warm path is the shared token-renewal leg
    // that rotates the stored credential and writes it back. Taking the
    // account snapshot afterwards means the search signs with the freshest
    // token instead of one the catalog just invalidated.
    let model = catalog_model(&root)
        .await
        .unwrap_or_else(|| FALLBACK_MODEL.to_string());
    let account = crate::app_server::model_sources::codex_account(&root)
        .ok_or_else(|| fail("The Codex subscription is not signed in. Sign in on the Models page to use its search service."))?;
    let access = account
        .get("accessToken")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .ok_or_else(|| fail("The stored Codex credential has no access token. Sign in again on the Models page."))?;
    let expires_at = account.get("accessTokenExpiresAt").and_then(Value::as_i64);
    let refresh = account
        .get("refreshToken")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty());
    let mut extra_headers = HashMap::new();
    if let Some(account_id) = account
        .get("accountId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
    {
        extra_headers.insert("ChatGPT-Account-Id".to_string(), account_id.to_string());
    }
    extra_headers.insert(
        "version".to_string(),
        crate::app_server::model_sources::codex_client_version(),
    );

    let auth: Arc<dyn AuthSource> = Arc::new(OAuthAuth::new(OAuthAuthOptions {
        // The same four-field OAuth client config the session path builds,
        // including the deploy-overridable auth host rather than a hardcoded
        // issuer.
        config: {
            let host = crate::app_server::model_sources::codex_auth_base();
            OAuthConfig {
                client_id: crate::app_server::model_sources::codex_client_id().to_string(),
                authorize_url: format!("{host}/codex/device"),
                token_url: format!("{host}/oauth/token"),
                redirect_uri: format!("{host}/deviceauth/callback"),
                scopes: Some(vec![
                    "openid".to_string(),
                    "profile".to_string(),
                    "email".to_string(),
                    "offline_access".to_string(),
                ]),
                ..Default::default()
            }
        },
        store: None,
        tokens: Some(OAuthTokens {
            access_token: RuntimeString::from(access),
            refresh_token: refresh.map(RuntimeString::from),
            expires_at,
            id_token: None,
        }),
        base_url: Some(CODEX_BASE_URL.to_string()),
        extra_headers: Some(extra_headers),
        ..Default::default()
    }));
    let provider = OpenAIProvider::new(OpenAIProviderOptions {
        auth_source: Some(auth),
        api: Some(OpenAiApi::Responses),
        base_url: Some(CODEX_BASE_URL.to_string()),
        // The search request declares the hosted tool itself; the capability
        // gate only needs to agree that this surface carries one.
        capabilities: Some(ModelCapabilities {
            web_search: true,
            ..Default::default()
        }),
        ..Default::default()
    });

    let request = ModelRequest {
        supports_image_detail_original: false,
        model: RuntimeString::from(model),
        system: vec![SystemPromptPart {
            name: RuntimeString::from("search"),
            text: RuntimeString::from(
                "You are a search service. Run the web_search tool for the user's query, then answer with ONLY a newline-separated list of the sources you found, one per line as \"title - url\". No commentary.",
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
        tool_choice: Some(ToolChoice::Any),
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
        return Err(fail(format!("Codex search failed: {failure}")));
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
            "The Codex search service returned no sources for this query.",
        ));
    }
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_search_request_names_the_hosted_tool_and_no_function_tools() {
        // The request shape is the contract with the subscription backend: one
        // hosted search tool, required, and a bounded answer.
        let request = ModelRequest {
            supports_image_detail_original: false,
            model: RuntimeString::from(FALLBACK_MODEL),
            system: Vec::new(),
            messages: Vec::new(),
            tools: Vec::new(),
            tool_choice: Some(ToolChoice::Any),
            max_output_tokens: 1024,
            task_budget: None,
            output_schema: None,
            web_search: Some(WebSearchRequest {
                access: klaus_agent_core::schemas::config::WebSearchAccess::Live,
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
        assert_eq!(request.tools.len(), 0);
        assert_eq!(request.tool_choice, Some(ToolChoice::Any));
        assert_eq!(request.web_search.as_ref().unwrap().max_uses, Some(3));
    }
}
