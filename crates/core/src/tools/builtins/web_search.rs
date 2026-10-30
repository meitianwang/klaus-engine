//! The `web_search` tool: search the web and return a ranked list of result
//! titles, URLs, and snippets. Uses the session browser to search Bing without
//! API credentials; a host may inject its own backend. Supports per-call domain
//! allow/deny filters and session-stable `[N]` result ids for inline citation.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use tokio::time::Instant;

use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimeString};
use crate::tools::build_tool::text_result;
use crate::types::abort::AbortSignal;
use crate::types::errors::ToolError;
use crate::types::events::ToolDomainEvent;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{BoxFuture, Tool, ToolContext, ToolProgress, ToolResult};

use super::host_policy::host_matches_domain;
use super::web_search_browser;

const SEARCH_TIMEOUT: Duration = Duration::from_secs(25);
const CANCEL_GRACE: Duration = Duration::from_secs(1);
const SOURCE_PAUSE: Duration = Duration::from_secs(30);

const WEB_SEARCH_DESCRIPTION: &str = r#"Search the web and return ranked result titles, URLs, snippets, and session-stable source references. Use focused queries and refine weak results.

When to search:
- Honor the user's explicit request to search, browse, verify, or look something up, and explicit instructions not to browse.
- Verify facts that may have changed: news, prices, laws, rules, schedules, specifications, exchange rates, public or company roles, standards, and software versions or libraries. For news, compare publication dates with the dates of the events.
- Search for recommendations that may lead the user to spend substantial time or money, niche or emerging subjects, uncertain facts, and precise attribution, quotes, or links. When uncertain whether verification is needed, search.
- Search by default when medical, legal, or financial accuracy matters. Read a referenced page, paper, dataset, PDF, or website when its contents were not supplied; snippets alone are not the page's contents.

Sources and citations:
- Use web_fetch or available browser reading tools to inspect supporting pages before relying on details or quotations that snippets do not establish. If reading fails, state the limitation; never claim to have read the source.
- For technical questions, rely only on primary sources such as official documentation and research papers. Prefer authoritative sources generally, use independent sources when multiple perspectives help, and identify inferences explicitly. For app usage, consult its local documentation and live state when available before browsing.
- Cite supported web claims near the relevant sentence or paragraph. Prefer descriptive Markdown links to the actual supporting page, such as [source title](https://example.com/page). The returned [N] references may also cite their actual sources; include the corresponding title and URL when presenting a reference list. Never invent a URL, citation, quote, statistic, or result ID, cite a search result page, or expose unrelated internal runtime identifiers.

Source limits:
- Do not reproduce full articles. Quote no more than 25 words verbatim from one non-lyrical source, or 10 words of song lyrics. Longer Reddit quotes may use a linked Markdown blockquote.
- Keep all material derived from a single page within its stated word limit, or 200 words when no limit is supplied. Count non-contiguous quotations and paraphrases together."#;

/// A single search hit, as returned by a backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebSearchResult {
    pub title: RuntimeString,
    pub url: RuntimeString,
    pub snippet: Option<RuntimeString>,
}

/// A host-injected search backend: `(query, signal) -> Result<hits>`.
/// An `Err` carries the failure message surfaced to the model.
pub type WebSearchBackend = Arc<
    dyn Fn(
            RuntimeString,
            AbortSignal,
        ) -> BoxFuture<'static, Result<Vec<WebSearchResult>, RuntimeString>>
        + Send
        + Sync,
>;

/// Chooses the search backend for a session from its RESOLVED web search
/// config, after the config layers are merged. A factory returning `None`
/// leaves the ordinary backend in place; a named subscription source returns
/// that source's own search service, and a source that cannot serve (missing
/// credentials) fails loudly from inside the backend instead of silently
/// falling back to the browser. A selected backend must honor `config.access`;
/// if it cannot provide cached or indexed access, it must return an explicit
/// error without issuing a live search.
pub type WebSearchBackendFactory =
    Arc<dyn Fn(&crate::schemas::config::WebSearchConfig) -> Option<WebSearchBackend> + Send + Sync>;

fn runtime_object<const N: usize>(entries: [(&'static str, RuntimeJson); N]) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    for (name, value) in entries {
        object.insert(name, value);
    }
    RuntimeJson::Object(object)
}

fn runtime_concat(parts: impl IntoIterator<Item = RuntimeString>) -> RuntimeString {
    parts
        .into_iter()
        .fold(RuntimeString::default(), |output, part| {
            output.concat(&part)
        })
}

fn runtime_join(parts: &[RuntimeString], separator: &str) -> RuntimeString {
    let separator = RuntimeString::from(separator);
    let mut output = RuntimeString::default();
    for (index, part) in parts.iter().enumerate() {
        if index != 0 {
            output = output.concat(&separator);
        }
        output = output.concat(part);
    }
    output
}

/// WHATWG URL APIs consume Web-IDL `USVString` values. Preserve the exact
/// runtime string until this parser boundary, then replace each unpaired
/// surrogate exactly once as Web IDL requires.
fn runtime_string_to_web_idl_usv(value: &RuntimeString) -> String {
    value
        .code_points()
        .map(|point| char::from_u32(point).unwrap_or(char::REPLACEMENT_CHARACTER))
        .collect()
}

/// Does `url`'s hostname equal `domain` or sit under it?
fn host_matches(url: &RuntimeString, domain: &RuntimeString) -> bool {
    let url = runtime_string_to_web_idl_usv(url);
    let domain = runtime_string_to_web_idl_usv(domain);
    match reqwest::Url::parse(&url) {
        Ok(u) => host_matches_domain(u.host_str().unwrap_or(""), &domain),
        Err(_) => false,
    }
}

/// The prefix of `s` up to `n` UTF-16 code units (JS `slice(0, n)`).
fn utf16_prefix(s: &RuntimeString, n: usize) -> RuntimeString {
    s.slice_utf16(0..n)
}

/// Options for [`create_web_search_tool`].
#[derive(Default)]
pub struct WebSearchToolOptions {
    /// Override browser-backed Bing search with an explicit host backend.
    pub search: Option<WebSearchBackend>,
    /// Resolve a source against the access captured by the current tool call.
    pub search_factory: Option<WebSearchBackendFactory>,
    pub config: Option<crate::schemas::config::WebSearchConfig>,
    /// Session/tool-instance cap on client-side search calls.
    pub max_uses: Option<u64>,
    /// Configured allow-list. When non-empty, every result must match one of
    /// these domains before any per-call filters are applied.
    pub allowed_domains: Option<Vec<RuntimeString>>,
    /// Configured deny-list. Wins over configured and per-call allow-lists.
    pub blocked_domains: Option<Vec<RuntimeString>>,
}

/// Read an optional `string[]` field from the input.
fn extract_domains(input: &Json, key: &str) -> Option<Vec<RuntimeString>> {
    input.get(key).and_then(RuntimeJson::as_array).map(|arr| {
        arr.iter()
            .filter_map(RuntimeJson::as_string)
            .cloned()
            .collect()
    })
}

/// Build the tool's input JSON Schema.
fn build_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "The search query"
            },
            "allowedDomains": {
                "description": "If set, only keep results whose host matches one of these domains",
                "type": "array",
                "items": { "type": "string" }
            },
            "blockedDomains": {
                "description": "Drop results whose host matches one of these domains",
                "type": "array",
                "items": { "type": "string" }
            }
        },
        "required": ["query"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap()
}

async fn emit_search_progress(
    on_progress: &Option<ToolProgress>,
    phase: &str,
    query: &RuntimeString,
    result_count: Option<usize>,
) {
    if let Some(cb) = on_progress {
        cb(ToolDomainEvent::WebSearch {
            phase: phase.into(),
            query: query.clone(),
            result_count: result_count.map(|n| n as u64),
        })
        .await;
    }
}

/// Search the web with the session browser and stable result citations.
pub struct WebSearchTool {
    input_schema: JsonSchema,
    backend: Option<WebSearchBackend>,
    backend_factory: Option<WebSearchBackendFactory>,
    config: crate::schemas::config::WebSearchConfig,
    browser_retry_after: Mutex<Option<Instant>>,
    max_uses: Option<u64>,
    uses: Mutex<u64>,
    configured_allowed_domains: Option<Vec<RuntimeString>>,
    configured_blocked_domains: Option<Vec<RuntimeString>>,
    /// Session-stable result ids: each distinct URL gets a `[N]` id reused across
    /// calls, so a result can be cited by a stable anchor.
    url_to_id: Mutex<HashMap<RuntimeString, u64>>,
}

impl WebSearchTool {
    fn id_for(&self, url: &RuntimeString) -> u64 {
        let mut map = self.url_to_id.lock().unwrap();
        if let Some(&id) = map.get(url) {
            return id;
        }
        let id = map.len() as u64 + 1;
        map.insert(url.clone(), id);
        id
    }
}

/// Construct the `web_search` tool as an erased registry-side [`Tool`].
pub fn create_web_search_tool(opts: WebSearchToolOptions) -> Arc<dyn Tool> {
    Arc::new(WebSearchTool {
        input_schema: build_input_schema(),
        backend: opts.search,
        backend_factory: opts.search_factory,
        config: opts.config.unwrap_or_default(),
        browser_retry_after: Mutex::new(None),
        max_uses: opts.max_uses,
        uses: Mutex::new(0),
        configured_allowed_domains: opts.allowed_domains,
        configured_blocked_domains: opts.blocked_domains,
        url_to_id: Mutex::new(HashMap::new()),
    })
}

#[async_trait]
impl Tool for WebSearchTool {
    fn static_name(&self) -> &str {
        "web_search"
    }

    fn static_description(&self) -> &str {
        WEB_SEARCH_DESCRIPTION
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let query = input
            .get("query")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .unwrap_or_default();
        Some(runtime_concat([
            RuntimeString::from("Searching: "),
            utf16_prefix(&query, 60),
        ]))
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let query = input
            .get("query")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .unwrap_or_default();
        let allowed = extract_domains(&input, "allowedDomains");
        let blocked = extract_domains(&input, "blockedDomains");
        if query.len_utf16() > 4096 || runtime_string_to_web_idl_usv(&query).trim().is_empty() {
            return Ok(text_result(
                runtime_object([
                    ("query", RuntimeJson::String(query.clone())),
                    ("count", RuntimeJson::from(0)),
                ]),
                "Web search requires a non-empty query of at most 4096 UTF-16 code units.",
                true,
            ));
        }
        if allowed.as_ref().is_some_and(|d| !d.is_empty())
            && blocked.as_ref().is_some_and(|d| !d.is_empty())
        {
            return Ok(text_result(
                runtime_object([
                    ("query", RuntimeJson::String(query.clone())),
                    ("count", RuntimeJson::from(0)),
                ]),
                "Cannot specify both allowedDomains and blockedDomains in the same web_search call."
                    .to_string(),
                true,
            ));
        }

        if let Some(max) = self.max_uses {
            let mut uses = self.uses.lock().unwrap();
            if *uses >= max {
                return Ok(text_result(
                    runtime_object([
                        ("query", RuntimeJson::String(query.clone())),
                        ("count", RuntimeJson::from(0)),
                        ("maxUses", RuntimeJson::from(max)),
                    ]),
                    format!("Exceeded maximum web_search uses for this session ({max})."),
                    true,
                ));
            }
            *uses += 1;
        }

        emit_search_progress(&on_progress, "query_update", &query, None).await;

        let mut config = self.config.clone();
        if let Some(access) = ctx.web_search_access {
            config.access = access;
        }
        if config.access == crate::schemas::config::WebSearchAccess::Disabled {
            return Err(ToolError::new("Web search is disabled for this request."));
        }
        let selected_backend = self
            .backend_factory
            .as_ref()
            .and_then(|factory| factory(&config));
        // The factory owns freshness for its named source. An ordinary injected
        // backend or browser has no cache contract, so do not issue live traffic
        // when cached-only access was requested.
        if selected_backend.is_none()
            && config.access != crate::schemas::config::WebSearchAccess::Live
        {
            return Err(ToolError::new(format!(
                "The selected web search source does not support {} access. Choose a compatible source or live access.",
                config.access.as_str()
            )));
        }
        let backend = selected_backend.or_else(|| self.backend.clone());
        let signal = ctx.signal.child_token();
        let _cancel_on_drop = signal.clone().drop_guard();
        let search = async {
            if signal.is_cancelled() {
                return Err(RuntimeString::from("Web search was cancelled."));
            }
            if let Some(backend) = &backend {
                return backend(query.clone(), signal.clone()).await;
            }
            let retry_after = *self.browser_retry_after.lock().unwrap();
            if let Some(remaining) = retry_after
                .and_then(|until| until.checked_duration_since(Instant::now()))
                .filter(|remaining| !remaining.is_zero())
            {
                return Err(format!("Bing browser search is temporarily paused after a search failure. Retry in {} seconds or use web_fetch for a known website.", remaining.as_secs() + 1).into());
            }
            match web_search_browser::search(&query, ctx, signal.clone()).await {
                Ok(results) => {
                    *self.browser_retry_after.lock().unwrap() = None;
                    Ok(results)
                }
                Err(error) => {
                    if error.pause_source {
                        *self.browser_retry_after.lock().unwrap() =
                            Some(Instant::now() + SOURCE_PAUSE);
                    }
                    Err(error.message)
                }
            }
        };
        tokio::pin!(search);
        let result = tokio::select! {
            biased;
            _ = ctx.signal.cancelled() => {
                signal.cancel();
                // Runtime adapters need a poll after cancellation to retire
                // sidecar requests and their temporary browser pages.
                let _ = tokio::time::timeout(CANCEL_GRACE, &mut search).await;
                Err(RuntimeString::from("Web search was cancelled."))
            },
            _ = tokio::time::sleep(SEARCH_TIMEOUT) => {
                signal.cancel();
                let _ = tokio::time::timeout(CANCEL_GRACE, &mut search).await;
                if backend.is_none() {
                    *self.browser_retry_after.lock().unwrap() = Some(Instant::now() + SOURCE_PAUSE);
                }
                Err(RuntimeString::from("Web search timed out after 25 seconds."))
            }
            result = &mut search => result,
        };
        let results = match result {
            Ok(r) => r,
            Err(e) => {
                return Ok(text_result(
                    runtime_object([
                        ("query", RuntimeJson::String(query.clone())),
                        ("count", RuntimeJson::from(0)),
                    ]),
                    RuntimeString::from("Search backend error: ").concat(&e),
                    true,
                ));
            }
        };

        let mut filtered = results;
        if let Some(ad) = &self.configured_allowed_domains {
            if !ad.is_empty() {
                filtered.retain(|r| ad.iter().any(|d| host_matches(&r.url, d)));
            }
        }
        if let Some(ad) = &allowed {
            if !ad.is_empty() {
                filtered.retain(|r| ad.iter().any(|d| host_matches(&r.url, d)));
            }
        }
        if let Some(bd) = &self.configured_blocked_domains {
            if !bd.is_empty() {
                filtered.retain(|r| !bd.iter().any(|d| host_matches(&r.url, d)));
            }
        }
        if let Some(bd) = &blocked {
            if !bd.is_empty() {
                filtered.retain(|r| !bd.iter().any(|d| host_matches(&r.url, d)));
            }
        }

        emit_search_progress(
            &on_progress,
            "search_results_received",
            &query,
            Some(filtered.len()),
        )
        .await;

        if filtered.is_empty() {
            return Ok(text_result(
                runtime_object([
                    ("query", RuntimeJson::String(query.clone())),
                    ("count", RuntimeJson::from(0)),
                ]),
                runtime_concat([
                    RuntimeString::from("No results for \""),
                    query.clone(),
                    RuntimeString::from("\"."),
                ]),
                false,
            ));
        }

        let mut parts: Vec<RuntimeString> = Vec::new();
        let mut structured: Vec<RuntimeJson> = Vec::new();
        for r in &filtered {
            let id = self.id_for(&r.url);
            let snippet = match &r.snippet {
                Some(s) if !s.is_empty() => RuntimeString::from("\n   ").concat(s),
                _ => RuntimeString::default(),
            };
            parts.push(runtime_concat([
                RuntimeString::from(format!("[{id}] ")),
                r.title.clone(),
                RuntimeString::from("\n   "),
                r.url.clone(),
                snippet,
            ]));
            structured.push(runtime_object([
                ("id", RuntimeJson::from(id)),
                ("title", RuntimeJson::String(r.title.clone())),
                ("url", RuntimeJson::String(r.url.clone())),
                (
                    "snippet",
                    r.snippet
                        .clone()
                        .map(RuntimeJson::String)
                        .unwrap_or(RuntimeJson::Null),
                ),
            ]));
        }
        let list = runtime_join(&parts, "\n\n");
        let text = runtime_concat([
            RuntimeString::from("Results for \""),
            query.clone(),
            RuntimeString::from("\" (cite a result inline as [N]):\n\n"),
            list,
        ]);
        let mut out = RuntimeObject::new();
        out.insert("query", RuntimeJson::String(query.clone()));
        out.insert("count", RuntimeJson::from(filtered.len()));
        out.insert("results", RuntimeJson::Array(structured));
        Ok(text_result(RuntimeJson::Object(out), text, false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::browser::{
        attest_spawn_free, BrowserEventSink, BrowserExecutor, BrowserOperation,
        BrowserRuntimeError, BrowserRuntimeRequest, BrowserRuntimeResponse, BrowserWaitUntil,
    };
    use crate::runtime_values::runtime_json;
    use crate::schemas::content::OutputBlock;
    use crate::tools::builtins::test_ctx::make_ctx;

    macro_rules! runtime_value {
        ([$($value:expr),* $(,)?]) => {
            RuntimeJson::Array(vec![$(RuntimeJson::from($value)),*])
        };
        ($value:expr) => {
            RuntimeJson::from($value)
        };
    }

    macro_rules! runtime_json {
        ({ $($key:literal : $value:tt),* $(,)? }) => {{
            let mut object = RuntimeObject::new();
            $(object.insert($key, runtime_value!($value));)*
            RuntimeJson::Object(object)
        }};
    }

    fn text_of(res: &ToolResult) -> String {
        match &res.serializable.model_content[0] {
            OutputBlock::Text(t) => t
                .text
                .try_to_utf8()
                .expect("ordinary web_search test output is scalar UTF-8"),
            _ => panic!("expected a text block"),
        }
    }

    // ---- backend fake (injected directly into the tool) ----

    /// Wrap a synchronous `query -> hits` closure as a [`WebSearchBackend`].
    fn fn_backend<F>(f: F) -> WebSearchBackend
    where
        F: Fn(&RuntimeString) -> Vec<WebSearchResult> + Send + Sync + 'static,
    {
        Arc::new(move |query: RuntimeString, _signal: AbortSignal| {
            let r = f(&query);
            Box::pin(async move { Ok(r) })
                as BoxFuture<'static, Result<Vec<WebSearchResult>, RuntimeString>>
        })
    }

    fn hit(title: &str, url: &str, snippet: Option<&str>) -> WebSearchResult {
        WebSearchResult {
            title: title.into(),
            url: url.into(),
            snippet: snippet.map(RuntimeString::from),
        }
    }

    const ORGANIC_HTML: &str = r#"<ol id="b_results"><li class="b_algo"><h2><a href="https://docs.python.org/3/">Python docs</a></h2><div class="b_caption"><p>Official documentation.</p></div></li></ol>"#;
    const EMPTY_HTML: &str =
        r#"<ol id="b_results"><li class="b_no"><h1>There are no results for test.</h1></li></ol>"#;

    struct SearchBrowser {
        page: Json,
        desktop_shape: bool,
        pending: bool,
        calls: Mutex<Vec<(BrowserRuntimeRequest, AbortSignal)>>,
        cleanup_acknowledged: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    unsafe impl BrowserExecutor for SearchBrowser {
        async fn execute(
            &self,
            request: BrowserRuntimeRequest,
            signal: AbortSignal,
            _emit: BrowserEventSink,
        ) -> Result<BrowserRuntimeResponse, BrowserRuntimeError> {
            self.calls.lock().unwrap().push((request, signal.clone()));
            if self.pending {
                signal.cancelled().await;
                self.cleanup_acknowledged
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                return Err(BrowserRuntimeError::new(
                    crate::browser::BrowserRuntimeErrorCode::Cancelled,
                    "Fixture browser cleanup completed.",
                ));
            }
            let pages = Json::Array(vec![self.page.clone()]);
            Ok(BrowserRuntimeResponse {
                value: Some(if self.desktop_shape {
                    runtime_object([("pages", pages)])
                } else {
                    pages
                }),
                ..Default::default()
            })
        }
    }

    fn browser_context(
        page: serde_json::Value,
        desktop_shape: bool,
        pending: bool,
    ) -> (ToolContext, Arc<SearchBrowser>) {
        let browser = Arc::new(SearchBrowser {
            page: runtime_json::parse(&RuntimeString::from(page.to_string())).unwrap(),
            desktop_shape,
            pending,
            calls: Mutex::new(Vec::new()),
            cleanup_acknowledged: Default::default(),
        });
        let mut ctx = make_ctx(".");
        // SAFETY: this in-memory fixture never creates or controls OS processes.
        ctx.browser = Some(unsafe { attest_spawn_free(browser.clone()) });
        (ctx, browser)
    }

    fn page(html: &str) -> serde_json::Value {
        serde_json::json!({"url": "https://www.bing.com/search?q=test", "content": html, "status": 200})
    }

    #[tokio::test]
    async fn default_search_uses_bounded_background_browser_and_preserves_result_contract() {
        for desktop_shape in [true, false] {
            let (ctx, browser) = browser_context(page(ORGANIC_HTML), desktop_shape, false);
            let tool = create_web_search_tool(WebSearchToolOptions::default());
            let result = tool
                .call(runtime_json!({"query": "Python 中文 & docs"}), &ctx, None)
                .await
                .unwrap();
            assert_ne!(result.serializable.is_error, Some(true));
            let output = &result.serializable.output;
            assert_eq!(output.get("count").and_then(Json::as_u64), Some(1));
            let hit = &output.get("results").and_then(Json::as_array).unwrap()[0];
            assert_eq!(
                hit.get("url").and_then(Json::as_string),
                Some(&"https://docs.python.org/3/".into())
            );
            assert_eq!(hit.get("id").and_then(Json::as_u64), Some(1));
            assert!(text_of(&result).contains("Official documentation."));
            let calls = browser.calls.lock().unwrap();
            let request = &calls[0].0;
            assert_eq!(request.target, Default::default());
            assert_eq!(request.timeout_ms, Some(20_000));
            assert_eq!(
                request.metadata.get(&RuntimeString::from("toolName")),
                Some(&Json::from("web_search"))
            );
            let BrowserOperation::TabsContent {
                urls,
                content_type,
                max_chars,
                wait_until,
            } = &request.operation
            else {
                panic!("search must read a background URL without arbitrary browser actions");
            };
            assert_eq!(urls.len(), 1);
            let url = reqwest::Url::parse(&urls[0].try_to_utf8().unwrap()).unwrap();
            assert_eq!(url.origin().ascii_serialization(), "https://www.bing.com");
            assert_eq!(url.path(), "/search");
            assert_eq!(
                url.query_pairs().collect::<Vec<_>>(),
                vec![("q".into(), "Python 中文 & docs".into())]
            );
            assert_eq!(content_type.as_ref(), Some(&"html".into()));
            assert_eq!(*max_chars, Some(200_000));
            assert_eq!(*wait_until, Some(BrowserWaitUntil::DomContentLoaded));
        }
    }

    #[tokio::test]
    async fn missing_browser_is_an_explicit_error() {
        let tool = create_web_search_tool(WebSearchToolOptions::default());
        let result = tool
            .call(runtime_json!({"query": "test"}), &make_ctx("."), None)
            .await
            .unwrap();
        assert_eq!(result.serializable.is_error, Some(true));
        assert!(text_of(&result).contains("Enable or connect a browser"));
    }

    #[tokio::test]
    async fn only_explicit_empty_pages_are_successful_empty_results() {
        let cases = [
            (page(EMPTY_HTML), false, "No results"),
            (
                page("<div id='b_captcha'>Complete this check</div>"),
                true,
                "verification",
            ),
            (
                page("<html><body>Gateway error</body></html>"),
                true,
                "unrecognized",
            ),
            (
                serde_json::json!({"error": "net::ERR_CONNECTION_RESET"}),
                true,
                "could not be loaded",
            ),
            (
                serde_json::json!({"timedOut": true, "content": ORGANIC_HTML}),
                true,
                "timed out",
            ),
            (
                serde_json::json!({"url": "https://www.bing.com/search?q=test", "content": EMPTY_HTML, "truncated": true}),
                true,
                "incomplete",
            ),
        ];
        for (page, is_error, message) in cases.into_iter().chain([(
            page(&format!("{EMPTY_HTML}\n[truncated]")),
            true,
            "incomplete",
        )]) {
            for desktop_shape in [true, false] {
                let (ctx, _) = browser_context(page.clone(), desktop_shape, false);
                let result = create_web_search_tool(WebSearchToolOptions::default())
                    .call(runtime_json!({"query": "test"}), &ctx, None)
                    .await
                    .unwrap();
                assert_eq!(
                    result.serializable.is_error == Some(true),
                    is_error,
                    "{message}"
                );
                assert!(text_of(&result).contains(message), "{}", text_of(&result));
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn blocked_searches_pause_browser_retries_then_allow_a_new_attempt() {
        let (ctx, browser) = browser_context(page("<div id='b_captcha'>Verify</div>"), true, false);
        let tool = create_web_search_tool(WebSearchToolOptions::default());
        let first = tool
            .call(runtime_json!({"query": "test"}), &ctx, None)
            .await
            .unwrap();
        assert!(text_of(&first).contains("verification"));
        let second = tool
            .call(runtime_json!({"query": "rephrased"}), &ctx, None)
            .await
            .unwrap();
        assert!(text_of(&second).contains("temporarily paused"));
        assert_eq!(browser.calls.lock().unwrap().len(), 1);
        tokio::time::advance(SOURCE_PAUSE).await;
        let third = tool
            .call(runtime_json!({"query": "test"}), &ctx, None)
            .await
            .unwrap();
        assert!(text_of(&third).contains("verification"));
        assert_eq!(browser.calls.lock().unwrap().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn search_deadline_cancels_the_browser_request_without_cancelling_the_turn() {
        let (ctx, browser) = browser_context(page(ORGANIC_HTML), true, true);
        let tool = create_web_search_tool(WebSearchToolOptions::default());
        let result = tool
            .call(runtime_json!({"query": "test"}), &ctx, None)
            .await
            .unwrap();
        assert_eq!(result.serializable.is_error, Some(true));
        assert!(text_of(&result).contains("timed out after 25 seconds"));
        assert!(browser.calls.lock().unwrap()[0].1.is_cancelled());
        assert!(browser
            .cleanup_acknowledged
            .load(std::sync::atomic::Ordering::SeqCst));
        assert!(!ctx.signal.is_cancelled());
    }

    #[tokio::test]
    async fn cancelling_the_turn_stops_an_inflight_browser_search() {
        let (ctx, browser) = browser_context(page(ORGANIC_HTML), true, true);
        let tool = create_web_search_tool(WebSearchToolOptions::default());
        let search = tool.call(runtime_json!({"query": "test"}), &ctx, None);
        let cancel = async {
            while browser.calls.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
            ctx.signal.cancel();
        };
        let (result, ()) = tokio::join!(search, cancel);
        let result = result.unwrap();
        assert_eq!(result.serializable.is_error, Some(true));
        assert!(text_of(&result).contains("cancelled"));
        assert!(browser.calls.lock().unwrap()[0].1.is_cancelled());
        assert!(browser
            .cleanup_acknowledged
            .load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn invalid_queries_never_reach_the_browser() {
        let (ctx, browser) = browser_context(page(ORGANIC_HTML), true, false);
        let tool = create_web_search_tool(WebSearchToolOptions::default());
        for query in [" ".to_owned(), "a".repeat(4097)] {
            let input = runtime_object([("query", Json::from(query))]);
            let result = tool.call(input, &ctx, None).await.unwrap();
            assert_eq!(result.serializable.is_error, Some(true));
        }
        assert!(browser.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn already_cancelled_turn_never_starts_browser_search() {
        let (ctx, browser) = browser_context(page(ORGANIC_HTML), true, false);
        ctx.signal.cancel();
        let tool = create_web_search_tool(WebSearchToolOptions::default());
        let result = tool
            .call(runtime_json!({"query": "test"}), &ctx, None)
            .await
            .unwrap();
        assert_eq!(result.serializable.is_error, Some(true));
        assert!(text_of(&result).contains("cancelled"));
        assert!(browser.calls.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn an_uncooperative_backend_cannot_exceed_deadline_and_cleanup_grace() {
        let tool = create_web_search_tool(WebSearchToolOptions {
            search: Some(Arc::new(|_, _| Box::pin(std::future::pending()))),
            ..Default::default()
        });
        let start = Instant::now();
        let result = tool
            .call(runtime_json!({"query": "test"}), &make_ctx("."), None)
            .await
            .unwrap();
        assert_eq!(result.serializable.is_error, Some(true));
        assert_eq!(start.elapsed(), SEARCH_TIMEOUT + CANCEL_GRACE);
    }

    #[test]
    fn utf16_prefix_preserves_runtime_lone_surrogate_at_the_limit() {
        let json = format!("\"{}\\ud800x\"", "a".repeat(199));
        let value = runtime_json::parse(&RuntimeString::from(json)).unwrap();
        let prefix = utf16_prefix(value.as_str().unwrap(), 200);
        assert_eq!(
            runtime_json::stringify(&Json::String(prefix)),
            RuntimeString::from(format!("\"{}\\ud800\"", "a".repeat(199)))
        );
    }

    #[test]
    fn exposes_expected_metadata() {
        let tool = create_web_search_tool(WebSearchToolOptions::default());
        assert_eq!(tool.name(), RuntimeString::from("web_search"));
        assert_eq!(
            tool.description(),
            RuntimeString::from(WEB_SEARCH_DESCRIPTION)
        );
        let input = runtime_json!({ "query": "hello world" });
        assert!(tool.is_read_only(&input));
        assert!(tool.is_concurrency_safe(&input));
        assert!(tool.is_enabled());
        assert_eq!(tool.is_destructive(&input), Some(false));
        assert_eq!(
            tool.summarize(&input),
            Some(RuntimeString::from("Searching: hello world"))
        );
    }

    #[test]
    fn input_schema_describes_query_and_optional_domain_filters() {
        let tool = create_web_search_tool(WebSearchToolOptions::default());
        let schema = tool.input_schema();
        assert_eq!(
            schema.get("type").and_then(RuntimeJson::as_string),
            Some(&RuntimeString::from("object"))
        );
        assert_eq!(
            schema.get("additionalProperties"),
            Some(&RuntimeJson::Bool(false))
        );
        assert_eq!(
            schema.get("required").unwrap(),
            &RuntimeJson::Array(vec![RuntimeJson::from("query")])
        );
        let props = schema.get("properties").unwrap();
        assert_eq!(
            props
                .get("query")
                .and_then(|p| p.get("description"))
                .and_then(RuntimeJson::as_string),
            Some(&RuntimeString::from("The search query"))
        );
        assert_eq!(
            props
                .get("allowedDomains")
                .and_then(|p| p.get("type"))
                .and_then(RuntimeJson::as_string),
            Some(&RuntimeString::from("array"))
        );
        assert_eq!(
            props
                .get("blockedDomains")
                .and_then(|p| p.get("items"))
                .and_then(|i| i.get("type"))
                .and_then(RuntimeJson::as_string),
            Some(&RuntimeString::from("string"))
        );
    }

    #[tokio::test]
    async fn explicit_host_backend_overrides_browser_search() {
        let tool = create_web_search_tool(WebSearchToolOptions {
            search: Some(fn_backend(|_q: &RuntimeString| {
                vec![hit("T", "https://t.example/", Some("S"))]
            })),
            ..Default::default()
        });
        let ctx = make_ctx(".");
        let r = tool
            .call(runtime_json!({ "query": "q" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(
            r.serializable
                .output
                .get("count")
                .and_then(RuntimeJson::as_i64),
            Some(1)
        );
        assert!(text_of(&r).contains("t.example"));
    }

    #[tokio::test]
    async fn applies_domain_filters() {
        let tool = create_web_search_tool(WebSearchToolOptions {
            search: Some(fn_backend(|_q: &RuntimeString| {
                vec![
                    hit("A", "https://keep.example/", None),
                    hit("B", "https://drop.example/", None),
                ]
            })),
            ..Default::default()
        });
        let ctx = make_ctx(".");
        let r = tool
            .call(
                runtime_json!({ "query": "q", "allowedDomains": ["keep.example"] }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            r.serializable
                .output
                .get("count")
                .and_then(RuntimeJson::as_i64),
            Some(1)
        );
    }

    #[tokio::test]
    async fn applies_configured_domain_filters_before_per_call_filters() {
        let tool = create_web_search_tool(WebSearchToolOptions {
            search: Some(fn_backend(|_q: &RuntimeString| {
                vec![
                    hit("Docs", "https://docs.example/a", None),
                    hit("Blog", "https://blog.example/a", None),
                    hit("Blocked", "https://blocked.example/a", None),
                ]
            })),
            allowed_domains: Some(vec!["example".into()]),
            blocked_domains: Some(vec!["blocked.example".into()]),
            ..Default::default()
        });
        let ctx = make_ctx(".");
        let r = tool
            .call(
                runtime_json!({ "query": "q", "allowedDomains": ["docs.example"] }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        let text = text_of(&r);
        assert!(text.contains("Docs"));
        assert!(!text.contains("Blog"));
        assert!(!text.contains("Blocked"));
        let results = r
            .serializable
            .output
            .get("results")
            .and_then(RuntimeJson::as_array)
            .expect("structured results");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("id").and_then(RuntimeJson::as_u64), Some(1));
    }

    #[tokio::test]
    async fn rejects_mutually_exclusive_allowed_and_blocked_domains() {
        let tool = create_web_search_tool(WebSearchToolOptions {
            search: Some(fn_backend(|_q: &RuntimeString| {
                vec![hit("A", "https://a.example/", None)]
            })),
            ..Default::default()
        });
        let ctx = make_ctx(".");
        let r = tool
            .call(
                runtime_json!({
                    "query": "q",
                    "allowedDomains": ["a.example"],
                    "blockedDomains": ["b.example"]
                }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(r.serializable.is_error, Some(true));
        assert!(text_of(&r).contains("Cannot specify both"));
    }

    #[tokio::test]
    async fn enforces_configured_max_uses() {
        let tool = create_web_search_tool(WebSearchToolOptions {
            search: Some(fn_backend(|_q: &RuntimeString| {
                vec![hit("A", "https://a.example/", None)]
            })),
            max_uses: Some(1),
            ..Default::default()
        });
        let ctx = make_ctx(".");
        let first = tool
            .call(runtime_json!({ "query": "first" }), &ctx, None)
            .await
            .unwrap();
        assert_ne!(first.serializable.is_error, Some(true));
        let second = tool
            .call(runtime_json!({ "query": "second" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(second.serializable.is_error, Some(true));
        assert!(text_of(&second).contains("maximum web_search uses"));
    }

    #[tokio::test]
    async fn renders_stable_ids_reused_across_calls() {
        let tool = create_web_search_tool(WebSearchToolOptions {
            search: Some(fn_backend(|q: &RuntimeString| {
                if q == "first" {
                    vec![
                        hit("A", "https://a.example/", None),
                        hit("B", "https://b.example/", None),
                    ]
                } else {
                    vec![
                        hit("B-again", "https://b.example/", None),
                        hit("C", "https://c.example/", None),
                    ]
                }
            })),
            ..Default::default()
        });
        let ctx = make_ctx(".");

        let t1 = text_of(
            &tool
                .call(runtime_json!({ "query": "first" }), &ctx, None)
                .await
                .unwrap(),
        );
        assert!(t1.contains("[1] A"));
        assert!(t1.contains("[2] B"));

        let t2 = text_of(
            &tool
                .call(runtime_json!({ "query": "second" }), &ctx, None)
                .await
                .unwrap(),
        );
        assert!(t2.contains("[2] B-again")); // b.example keeps id 2
        assert!(t2.contains("[3] C")); // first-seen c.example gets 3, not 1
    }

    #[tokio::test]
    async fn reports_no_results() {
        let tool = create_web_search_tool(WebSearchToolOptions {
            search: Some(fn_backend(|_q: &RuntimeString| vec![])),
            ..Default::default()
        });
        let ctx = make_ctx(".");
        let r = tool
            .call(runtime_json!({ "query": "q" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(r.serializable.is_error, Some(false));
        assert_eq!(text_of(&r), "No results for \"q\".");
    }

    #[tokio::test]
    async fn cached_access_refuses_unqualified_live_backends() {
        use crate::schemas::config::WebSearchAccess;
        let calls = Arc::new(Mutex::new(0));
        let observed = calls.clone();
        let tool = create_web_search_tool(WebSearchToolOptions {
            search: Some(fn_backend(move |_| {
                *observed.lock().unwrap() += 1;
                vec![]
            })),
            ..Default::default()
        });
        let mut ctx = make_ctx(".");
        for access in [
            WebSearchAccess::Cached,
            WebSearchAccess::Indexed,
            WebSearchAccess::Disabled,
        ] {
            ctx.web_search_access = Some(access);
            let error = tool
                .call(runtime_json!({"query":"fixture"}), &ctx, None)
                .await
                .err()
                .expect("unsupported access must fail before search");
            assert!(error.message.contains("support") || error.message.contains("disabled"));
        }
        assert_eq!(*calls.lock().unwrap(), 0);
        ctx.web_search_access = Some(WebSearchAccess::Live);
        tool.call(runtime_json!({"query":"fixture"}), &ctx, None)
            .await
            .unwrap();
        assert_eq!(*calls.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn source_factory_receives_each_calls_captured_search_access() {
        use crate::schemas::config::{WebSearchAccess, WebSearchConfig};
        let observed = Arc::new(Mutex::new(Vec::new()));
        let captures = observed.clone();
        let factory: WebSearchBackendFactory = Arc::new(move |config| {
            assert_eq!(config.source.as_deref(), Some("codex"));
            let mut captures = captures.lock().unwrap();
            assert!(captures.len() < 3);
            captures.push(config.access);
            Some(fn_backend(|_| vec![]))
        });
        let tool = create_web_search_tool(WebSearchToolOptions {
            search_factory: Some(factory),
            config: Some(WebSearchConfig {
                source: Some("codex".into()),
                ..Default::default()
            }),
            ..Default::default()
        });
        let mut ctx = make_ctx(".");
        for access in [
            WebSearchAccess::Live,
            WebSearchAccess::Cached,
            WebSearchAccess::Indexed,
        ] {
            ctx.web_search_access = Some(access);
            tool.call(runtime_json!({"query":"fixture"}), &ctx, None)
                .await
                .unwrap();
        }
        assert_eq!(
            *observed.lock().unwrap(),
            [
                WebSearchAccess::Live,
                WebSearchAccess::Cached,
                WebSearchAccess::Indexed
            ]
        );
    }
}
