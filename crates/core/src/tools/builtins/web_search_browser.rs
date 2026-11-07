//! Browser retrieval for the canonical search tool. The browser owns temporary
//! tabs and cleanup; only Bing result cards cross back into model context.

use std::sync::Arc;

use crate::browser::{
    BrowserOperation, BrowserRuntimeErrorCode, BrowserRuntimeRequest, BrowserTarget,
    BrowserWaitUntil,
};
use crate::runtime_values::{RuntimeJson, RuntimeString};
use crate::types::abort::AbortSignal;
use crate::types::tool::ToolContext;

use super::browser::resolve_runtime;
use super::web_search::WebSearchResult;
use super::web_search_bing_html::{parse_bing_page, BingPage};

pub(super) const BROWSER_TIMEOUT_MS: u64 = 20_000;
const MAX_HTML_CHARS: u32 = 200_000;

pub(super) struct SearchFailure {
    pub message: RuntimeString,
    pub pause_source: bool,
}

impl SearchFailure {
    fn new(message: impl Into<RuntimeString>, pause_source: bool) -> Self {
        Self {
            message: message.into(),
            pause_source,
        }
    }
}

pub(super) async fn search(
    query: &RuntimeString,
    ctx: &ToolContext,
    signal: AbortSignal,
) -> Result<Vec<WebSearchResult>, SearchFailure> {
    let runtime = resolve_runtime(ctx).map_err(|error| SearchFailure::new(
        RuntimeString::from("Bing browser search is unavailable. Enable or connect a browser for this session. ").concat(&error.message),
        false,
    ))?;
    let mut url = reqwest::Url::parse("https://www.bing.com/search").expect("fixed search URL");
    // URL input uses Web-IDL USVString semantics at this terminal boundary.
    let query: String = query
        .code_points()
        .map(|point| char::from_u32(point).unwrap_or(char::REPLACEMENT_CHARACTER))
        .collect();
    url.query_pairs_mut().append_pair("q", &query);
    let mut request = BrowserRuntimeRequest::new(
        BrowserOperation::TabsContent {
            urls: vec![url.as_str().into()],
            content_type: Some("html".into()),
            max_chars: Some(MAX_HTML_CHARS),
            wait_until: Some(BrowserWaitUntil::DomContentLoaded),
        },
        BrowserTarget::default(),
        Some(BROWSER_TIMEOUT_MS),
    );
    request
        .metadata
        .insert("toolName".into(), RuntimeJson::from("web_search"));
    let response = runtime
        .execute(request, signal, Arc::new(|_| {}))
        .await
        .map_err(|error| {
            SearchFailure::new(
                RuntimeString::from("Bing browser request failed: ").concat(&error.message),
                matches!(
                    error.code,
                    BrowserRuntimeErrorCode::Timeout
                        | BrowserRuntimeErrorCode::NavigationFailed
                        | BrowserRuntimeErrorCode::HostError
                ),
            )
        })?;
    // Desktop returns cleanup metadata alongside pages; the bundled browser
    // returns the page array directly. Both are existing browser contracts.
    let value = response
        .value
        .as_ref()
        .ok_or_else(|| SearchFailure::new("Bing browser search returned no page data.", false))?;
    let pages = value.get("pages").unwrap_or(value).as_array();
    let page = pages
        .filter(|pages| pages.len() == 1)
        .and_then(|pages| pages.first())
        .ok_or_else(|| {
            SearchFailure::new(
                "Bing browser search returned an invalid page response.",
                false,
            )
        })?;
    if page.get("timedOut").and_then(RuntimeJson::as_bool) == Some(true) {
        return Err(SearchFailure::new(
            "Bing search timed out while loading its results page.",
            true,
        ));
    }
    if let Some(error) = page.get("error").and_then(RuntimeJson::as_string) {
        return Err(SearchFailure::new(
            RuntimeString::from("Bing search page could not be loaded: ").concat(error),
            true,
        ));
    }
    if page.get("ok").and_then(RuntimeJson::as_bool) == Some(false) {
        return Err(SearchFailure::new(
            "Bing search page could not be loaded.",
            true,
        ));
    }
    if let Some(status) = page.get("status").and_then(RuntimeJson::as_u64) {
        if status >= 400 {
            return Err(SearchFailure::new(
                format!("Bing search rejected the request (HTTP {status})."),
                true,
            ));
        }
    }
    let html = page
        .get("content")
        .and_then(RuntimeJson::as_string)
        .ok_or_else(|| {
            SearchFailure::new("Bing browser search returned no rendered HTML.", false)
        })?;
    let final_url = page
        .get("url")
        .and_then(RuntimeJson::as_string)
        .ok_or_else(|| SearchFailure::new("Bing browser search returned no page URL.", false))?;
    let html = html
        .try_to_utf8()
        .map_err(|_| SearchFailure::new("Bing search page contains invalid text.", false))?;
    let final_url = final_url
        .try_to_utf8()
        .map_err(|_| SearchFailure::new("Bing search returned an invalid page URL.", false))?;
    match parse_bing_page(&html, &final_url) {
        BingPage::Results(results) => Ok(results),
        BingPage::NoResults if page.get("truncated").and_then(RuntimeJson::as_bool) != Some(true) && !html.ends_with("\n[truncated]") => Ok(Vec::new()),
        BingPage::VerificationRequired => Err(SearchFailure::new(
            "Bing requires browser verification. Search could not be completed; this is not an empty result set. Use the browser to complete any required verification before retrying.", true,
        )),
        BingPage::NoResults | BingPage::Unrecognized => Err(SearchFailure::new(
            "Bing returned an incomplete or unrecognized search page. No results were accepted; retry later or read a known official website with web_fetch.", true,
        )),
    }
}
