//! Extract organic results from a rendered Bing search page without treating
//! network interstitials or unrecognized markup as an empty search.

use std::collections::HashSet;
use std::sync::LazyLock;

use base64::engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD};
use base64::Engine;
use scraper::{ElementRef, Html, Selector};
use url::Url;

use super::web_search::WebSearchResult;

const MAX_RESULTS: usize = 10;
const MAX_TITLE_CHARS: usize = 500;
const MAX_SNIPPET_CHARS: usize = 1_600;

static ORGANIC_CARDS: LazyLock<Selector> =
    LazyLock::new(|| Selector::parse("#b_results li.b_algo").unwrap());
static TITLE_LINKS: LazyLock<Selector> = LazyLock::new(|| Selector::parse("h2 a[href]").unwrap());
static SNIPPETS: LazyLock<Selector> = LazyLock::new(|| {
    Selector::parse(".b_caption p, .b_snippet, .b_caption .b_lineclamp2, .b_caption .b_lineclamp3")
        .unwrap()
});
static AD_MARKERS: LazyLock<Selector> =
    LazyLock::new(|| Selector::parse(".b_ad, .b_adlabel, .b_adLabel, .b_adBadge").unwrap());
static CHALLENGE_MARKERS: LazyLock<Selector> = LazyLock::new(|| {
    Selector::parse(
        "#b_captcha, #b_captchaForm, #captcha, #captchaPage, #challenge-form, \
         form[action*='/turing/captcha'], form[action*='/challenge'], \
         iframe[src*='challenges.cloudflare.com'], iframe[src*='hcaptcha.com']",
    )
    .unwrap()
});
static CHALLENGE_TEXT: LazyLock<Selector> =
    LazyLock::new(|| Selector::parse("h1, h2, p, [role='alert']").unwrap());
static NO_RESULTS: LazyLock<Selector> =
    LazyLock::new(|| Selector::parse("#b_results .b_no, #b_results .b_noResults").unwrap());

#[derive(Debug, PartialEq, Eq)]
pub(super) enum BingPage {
    Results(Vec<WebSearchResult>),
    NoResults,
    VerificationRequired,
    Unrecognized,
}

fn is_bing_host(url: &Url) -> bool {
    url.host_str()
        .is_some_and(|host| host == "bing.com" || host.ends_with(".bing.com"))
}

fn is_http_url_without_credentials(url: &Url) -> bool {
    matches!(url.scheme(), "http" | "https")
        && url.host_str().is_some()
        && url.username().is_empty()
        && url.password().is_none()
}

fn is_bing_ad_link(url: &Url) -> bool {
    is_bing_host(url) && url.path().trim_end_matches('/') == "/aclick"
}

fn has_class(element: ElementRef<'_>, class: &str) -> bool {
    element
        .value()
        .classes()
        .any(|candidate| candidate == class)
}

fn hidden_element(element: ElementRef<'_>) -> bool {
    let value = element.value();
    if matches!(
        value.name(),
        "head" | "script" | "style" | "template" | "noscript"
    ) || value.attr("hidden").is_some()
        || value
            .attr("aria-hidden")
            .is_some_and(|value| value.eq_ignore_ascii_case("true"))
        || has_class(element, "b_hide")
        || has_class(element, "b_hidden")
    {
        return true;
    }
    value.attr("style").is_some_and(|style| {
        let compact: String = style
            .chars()
            .filter(|character| !character.is_whitespace())
            .flat_map(char::to_lowercase)
            .collect();
        compact.split(';').any(|rule| {
            matches!(
                rule,
                "display:none" | "visibility:hidden" | "visibility:collapse"
            ) || rule.starts_with("display:none!")
                || rule.starts_with("visibility:hidden!")
                || rule.starts_with("visibility:collapse!")
        })
    })
}

fn is_hidden(element: ElementRef<'_>) -> bool {
    hidden_element(element)
        || element
            .ancestors()
            .filter_map(ElementRef::wrap)
            .any(hidden_element)
}

fn in_search_results(element: ElementRef<'_>) -> bool {
    element.value().id() == Some("b_results")
        || element
            .ancestors()
            .filter_map(ElementRef::wrap)
            .any(|ancestor| ancestor.value().id() == Some("b_results"))
}

fn in_result_card(element: ElementRef<'_>) -> bool {
    let is_card = |node| has_class(node, "b_algo") || has_class(node, "b_ad");
    is_card(element)
        || element
            .ancestors()
            .filter_map(ElementRef::wrap)
            .any(is_card)
}

fn is_ad(element: ElementRef<'_>) -> bool {
    let ad_container = |node: ElementRef<'_>| {
        ["b_ad", "b_adTop", "b_adBottom"]
            .iter()
            .any(|class| has_class(node, class))
    };
    ad_container(element)
        || element
            .ancestors()
            .filter_map(ElementRef::wrap)
            .any(ad_container)
        || element.select(&AD_MARKERS).any(|marker| !is_hidden(marker))
}

/// HTML entities have already been decoded by the DOM parser. Preserve inline
/// text boundaries and limit by Unicode characters rather than UTF-8 bytes.
fn visible_text(element: ElementRef<'_>, max_chars: usize) -> String {
    let mut output = String::new();
    let mut length = 0;
    let mut pending_space = false;
    for node in element.descendants() {
        let Some(text) = node.value().as_text() else {
            if node
                .value()
                .as_element()
                .is_some_and(|element| element.name() == "br")
            {
                pending_space = true;
            }
            continue;
        };
        if node
            .parent()
            .and_then(ElementRef::wrap)
            .is_none_or(is_hidden)
        {
            continue;
        }
        for character in text.text.chars() {
            if character.is_whitespace() {
                pending_space = !output.is_empty();
                continue;
            }
            if pending_space {
                if length + 1 >= max_chars {
                    return output;
                }
                output.push(' ');
                length += 1;
                pending_space = false;
            }
            if length == max_chars {
                return output;
            }
            output.push(character);
            length += 1;
        }
    }
    output
}

fn is_challenge(document: &Html) -> bool {
    if document
        .select(&CHALLENGE_MARKERS)
        .any(|element| !is_hidden(element) && !in_result_card(element))
    {
        return true;
    }
    document.select(&CHALLENGE_TEXT).any(|element| {
        if is_hidden(element) || in_search_results(element) {
            return false;
        }
        let text = visible_text(element, 500).to_lowercase();
        [
            "verify you are human",
            "verify that you are human",
            "please solve the challenge below",
            "our systems have detected unusual traffic",
            "请完成以下验证",
            "请验证你是人类",
            "请验证您是真人",
            "请确认你是真人",
        ]
        .iter()
        .any(|phrase| text.starts_with(phrase))
    })
}

fn explicit_no_results(document: &Html) -> bool {
    document.select(&NO_RESULTS).any(|element| {
        if is_hidden(element) {
            return false;
        }
        let text = visible_text(element, 500).to_lowercase();
        [
            "there are no results for",
            "no results found",
            "we couldn't find any results",
            "we did not find results for",
            "没有找到",
            "未找到",
            "找不到",
            "沒有找到",
        ]
        .iter()
        .any(|phrase| text.starts_with(phrase))
    })
}

fn result_url(href: &str, page_url: &Url) -> Option<String> {
    if href.is_empty() || href.chars().any(char::is_control) {
        return None;
    }
    let mut url = page_url.join(href).ok()?;
    if !is_http_url_without_credentials(&url) || is_bing_ad_link(&url) {
        return None;
    }
    let is_bing_redirect = is_bing_host(&url) && url.path().trim_end_matches('/') == "/ck/a";
    if Url::parse(href).is_err() && !href.starts_with("//") && !is_bing_redirect {
        return None;
    }
    if is_bing_redirect {
        let mut destinations = url.query_pairs().filter(|(name, _)| name == "u");
        let (_, destination) = destinations.next()?;
        if destinations.next().is_some() {
            return None;
        }
        let encoded = destination.strip_prefix("a1")?;
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .or_else(|_| URL_SAFE.decode(encoded))
            .ok()?;
        let decoded = String::from_utf8(bytes).ok()?;
        if decoded.chars().any(char::is_control) {
            return None;
        }
        let target = Url::parse(&decoded).ok()?;
        if !is_http_url_without_credentials(&target)
            || is_bing_ad_link(&target)
            || (is_bing_host(&target) && target.path().trim_end_matches('/') == "/ck/a")
        {
            return None;
        }
        url = target;
    }
    // A fragment does not identify another indexed document. Keep query
    // parameters, which may be necessary to identify the requested resource.
    url.set_fragment(None);
    Some(url.into())
}

/// Parse only a trusted HTTPS Bing search page. A loaded page with no usable
/// organic cards is an error unless Bing explicitly marks it as an empty search.
pub(super) fn parse_bing_page(html: &str, page_url: &str) -> BingPage {
    let Ok(page_url) = Url::parse(page_url) else {
        return BingPage::Unrecognized;
    };
    if page_url.scheme() != "https"
        || page_url.port_or_known_default() != Some(443)
        || !is_http_url_without_credentials(&page_url)
        || !is_bing_host(&page_url)
    {
        return BingPage::Unrecognized;
    }
    let path = page_url.path().to_ascii_lowercase();
    if path
        .split('/')
        .any(|segment| matches!(segment, "captcha" | "challenge"))
    {
        return BingPage::VerificationRequired;
    }
    if page_url.path().trim_end_matches('/') != "/search" {
        return BingPage::Unrecognized;
    }
    let document = Html::parse_document(html);
    if is_challenge(&document) {
        return BingPage::VerificationRequired;
    }
    let mut results = Vec::new();
    let mut seen = HashSet::new();
    let mut saw_organic_card = false;
    for card in document.select(&ORGANIC_CARDS) {
        if is_hidden(card) || is_ad(card) {
            continue;
        }
        saw_organic_card = true;
        let Some((title, url)) = card.select(&TITLE_LINKS).find_map(|anchor| {
            if is_hidden(anchor) {
                return None;
            }
            let title = visible_text(anchor, MAX_TITLE_CHARS);
            if title.is_empty() {
                return None;
            }
            let url = result_url(anchor.value().attr("href")?, &page_url)?;
            Some((title, url))
        }) else {
            continue;
        };
        if !seen.insert(url.clone()) {
            continue;
        }
        let snippet = card.select(&SNIPPETS).find_map(|element| {
            let text = visible_text(element, MAX_SNIPPET_CHARS);
            (!text.is_empty()).then(|| text.into())
        });
        results.push(WebSearchResult {
            title: title.into(),
            url: url.into(),
            snippet,
        });
        if results.len() == MAX_RESULTS {
            break;
        }
    }
    if !results.is_empty() {
        BingPage::Results(results)
    } else if !saw_organic_card && explicit_no_results(&document) {
        BingPage::NoResults
    } else {
        BingPage::Unrecognized
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::RuntimeString;

    const SEARCH_URL: &str = "https://www.bing.com/search?q=rust";
    // Compact representative markup preserves the organic card structure of
    // rendered Bing pages, without cookies, tracking tokens, or account data.
    const ORGANIC_HTML: &str = r#"
        <!doctype html><html><body><ol id="b_results">
          <li class="b_ad"><h2><a href="https://ads.example/">Sponsored</a></h2></li>
          <li class="b_algo">
            <div class="b_tpcn"><a href="https://ignored.example/">Attribution</a></div>
            <h2><a href="/ck/a?u=a1aHR0cHM6Ly9kb2MucnVzdC1sYW5nLm9yZy9ib29rLw">
              The <strong>Rust</strong> Programming Language &amp; Guide
            </a></h2>
            <div class="b_caption"><p class="b_lineclamp2">Learn <b>Rust</b> &amp; build
              reliable software.<span hidden>Hidden text</span><script>captcha()</script></p></div>
          </li>
          <li class="b_algo"><h2><a href="https://docs.python.org/3/#tutorial">Python 文档</a></h2>
            <div class="b_caption"><p>官方文档&nbsp;与教程。</p></div></li>
          <li class="b_algo"><h2><a href="https://doc.rust-lang.org/book/#another">Duplicate</a></h2></li>
          <li class="b_algo"><h2><a href="https://example.org/missing-snippet">No snippet</a></h2></li>
        </ol></body></html>
    "#;

    fn results(html: &str) -> Vec<WebSearchResult> {
        match parse_bing_page(html, SEARCH_URL) {
            BingPage::Results(results) => results,
            other => panic!("expected search results, got {other:?}"),
        }
    }

    fn card(url: &str, title: &str) -> String {
        format!(r#"<li class="b_algo"><h2><a href="{url}">{title}</a></h2></li>"#)
    }

    #[test]
    fn parses_organic_titles_snippets_and_real_destinations() {
        let results = results(ORGANIC_HTML);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].title, "The Rust Programming Language & Guide");
        assert_eq!(results[0].url, "https://doc.rust-lang.org/book/");
        assert_eq!(
            results[0].snippet,
            Some(RuntimeString::from("Learn Rust & build reliable software."))
        );
        assert_eq!(results[1].title, "Python 文档");
        assert_eq!(results[1].url, "https://docs.python.org/3/");
        assert_eq!(
            results[1].snippet,
            Some(RuntimeString::from("官方文档 与教程。"))
        );
        assert_eq!(results[2].snippet, None);
    }

    #[test]
    fn ignores_hidden_cards_and_ads_inside_organic_markup() {
        let html = format!(
            r#"<ol id="b_results">
              <li class="b_algo b_ad"><h2><a href="https://ads.example/">Ad</a></h2></li>
              <li class="b_algo"><span class="b_adlabel">Ad</span><h2><a href="https://ads.example/">Ad</a></h2></li>
              <li class="b_algo" style="DISPLAY: none !important"><h2><a href="https://hidden.example/">Hidden</a></h2></li>
              <li class="b_algo" aria-hidden="true"><h2><a href="https://hidden.example/">Hidden</a></h2></li>
              {}
            </ol>"#,
            card("https://example.com/", "Organic")
        );
        assert_eq!(results(&html).len(), 1);
        assert_eq!(results(&html)[0].title, "Organic");
    }

    #[test]
    fn caps_results_and_bounds_unicode_snippets() {
        let cards: String = (0..20)
            .map(|index| {
                format!(
                    r#"<li class="b_algo"><h2><a href="https://example.com/{index}">{}</a></h2>
                    <div class="b_caption"><p>{}</p></div></li>"#,
                    "字".repeat(1_000),
                    "文".repeat(2_000),
                )
            })
            .collect();
        let results = results(&format!(r#"<ol id="b_results">{cards}</ol>"#));
        assert_eq!(results.len(), MAX_RESULTS);
        assert_eq!(
            results[0].title.try_to_utf8().unwrap().chars().count(),
            MAX_TITLE_CHARS
        );
        assert_eq!(
            results[0]
                .snippet
                .as_ref()
                .unwrap()
                .try_to_utf8()
                .unwrap()
                .chars()
                .count(),
            MAX_SNIPPET_CHARS
        );
        assert_eq!(results[9].url, "https://example.com/9");
    }

    #[test]
    fn rejects_unsafe_or_unresolvable_destinations() {
        let targets = [
            "javascript:alert(1)",
            "data:text/html,example",
            "file:///etc/passwd",
            "#footer",
            "/search?q=other",
            "https://www.bing.com/aclick?ld=advertisement",
            "https://user:secret@example.com/",
            "https://user@example.com/",
            "/ck/a?u=not-encoded",
            "/ck/a?u=a1%%%",
            "/ck/a?u=a1aHR0cHM6Ly9leGFtcGxlLmNvbS8&u=a1aHR0cHM6Ly9vdGhlci5jb20v",
        ];
        for target in targets {
            let html = format!(r#"<ol id="b_results">{}</ol>"#, card(target, "Invalid"));
            assert_eq!(
                parse_bing_page(&html, SEARCH_URL),
                BingPage::Unrecognized,
                "{target}"
            );
        }
        for target in [
            "javascript:alert(1)",
            "https://user:secret@example.com/",
            "https://example.com/\n",
        ] {
            let encoded = URL_SAFE_NO_PAD.encode(target);
            let html = format!(
                r#"<ol id="b_results">{}</ol>"#,
                card(&format!("/ck/a?u=a1{encoded}"), "Invalid")
            );
            assert_eq!(
                parse_bing_page(&html, SEARCH_URL),
                BingPage::Unrecognized,
                "{target}"
            );
        }
    }

    #[test]
    fn accepts_padded_redirects_and_protocol_relative_https_links() {
        let target = "https://example.com/a?language=zh&v=1";
        let encoded = URL_SAFE.encode(target);
        assert!(encoded.ends_with('='));
        let encoded = encoded.replace('=', "%3D");
        let html = format!(
            r#"<ol id="b_results">{}{}</ol>"#,
            card(
                &format!("https://www.bing.com/ck/a?u=a1{encoded}"),
                "Redirect"
            ),
            card("//other.example/path", "Direct"),
        );
        let results = results(&html);
        assert_eq!(results[0].url, target);
        assert_eq!(results[1].url, "https://other.example/path");
    }

    #[test]
    fn requires_trusted_https_bing_search_location() {
        for url in [
            "https://www.bing.com.evil.example/search?q=rust",
            "https://example.com/search?q=rust",
            "http://www.bing.com/search?q=rust",
            "https://user@www.bing.com/search?q=rust",
            "https://www.bing.com:8443/search?q=rust",
            "https://www.bing.com/images/search?q=rust",
            "https://www.bing.com/",
            "not a url",
        ] {
            assert_eq!(
                parse_bing_page(ORGANIC_HTML, url),
                BingPage::Unrecognized,
                "{url}"
            );
        }
        assert!(matches!(
            parse_bing_page(ORGANIC_HTML, "https://cn.bing.com/search?q=rust"),
            BingPage::Results(_)
        ));
    }

    #[test]
    fn recognizes_visible_challenges_and_challenge_redirects() {
        for html in [
            r#"<div id="b_captcha">Complete this check</div>"#,
            r#"<ol id="b_results"><li><div id="b_captcha">Complete this check</div></li></ol>"#,
            r#"<form action="/turing/captcha/challenge"><input name="answer"></form>"#,
            "<h1>One last step</h1><p>Please solve the challenge below to continue.</p>",
            "<h1>Verify you are human</h1>",
            "<p>请完成以下验证，以继续。</p>",
        ] {
            assert_eq!(
                parse_bing_page(html, SEARCH_URL),
                BingPage::VerificationRequired
            );
        }
        assert_eq!(
            parse_bing_page("", "https://www.bing.com/turing/captcha/challenge"),
            BingPage::VerificationRequired
        );
    }

    #[test]
    fn does_not_mistake_scripts_hidden_challenges_or_search_hits_for_verification() {
        let html = format!(
            r#"<script>const captcha = "Verify you are human";</script>
            <style>.captcha {{ display: block }}</style>
            <div id="b_captcha" hidden>Verify you are human</div>
            <div style="visibility: hidden"><p>Verify you are human</p></div>
            <ol id="b_results">{}</ol>"#,
            card(
                "https://example.com/captcha",
                "Verify you are human: a CAPTCHA guide"
            )
        );
        assert_eq!(results(&html).len(), 1);
    }

    #[test]
    fn recognizes_only_explicit_visible_empty_results() {
        for message in [
            "There are no results for unlikely-query.",
            "No results found.",
            "没有找到相关结果。",
        ] {
            let html =
                format!(r#"<ol id="b_results"><li class="b_no"><h1>{message}</h1></li></ol>"#);
            assert_eq!(parse_bing_page(&html, SEARCH_URL), BingPage::NoResults);
        }
        for html in [
            "",
            "<html><body></body></html>",
            "<html><body>Service unavailable</body></html>",
            r#"<ol id="b_results"></ol>"#,
            r#"<ol id="b_results"><li class="b_no"></li></ol>"#,
            r#"<ol id="b_results"><li class="b_no" hidden>No results found.</li></ol>"#,
            r#"<ol id="b_results"><li class="b_no">Unexpected response</li></ol>"#,
            r#"<h1>No results found</h1>"#,
            r#"<ol id="b_results"><li class="b_algo"><h2>Missing result link</h2></li><li class="b_no">No results found</li></ol>"#,
        ] {
            assert_eq!(
                parse_bing_page(html, SEARCH_URL),
                BingPage::Unrecognized,
                "{html}"
            );
        }
    }
}
