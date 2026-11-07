//! Account-native finite web search and page extraction.
use super::{
    media,
    native::{self, NativeResponse, NativeSession, MAX_JSON},
};
use crate::app_server::wire::{WorkBuddyFetchParams, WorkBuddySearchParams};
use klaus_agent_core::types::abort::AbortSignal;
use serde_json::{json, Value};

fn domain(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 253
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-'))
}
fn public_url(value: &str) -> Result<(), String> {
    let url = reqwest::Url::parse(value).map_err(|_| "workbuddy_web_url_invalid")?;
    if !matches!(url.scheme(), "https" | "http")
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err("workbuddy_web_url_invalid".into());
    }
    let host = url.host_str().ok_or("workbuddy_web_url_invalid")?;
    if host == "localhost" || host.ends_with(".localhost") || host.ends_with(".local") {
        return Err("workbuddy_web_url_invalid".into());
    }
    if let Ok(ip) = host.trim_matches(['[', ']']).parse::<std::net::IpAddr>() {
        match ip {
            std::net::IpAddr::V4(ip)
                if ip.is_private()
                    || ip.is_loopback()
                    || ip.is_link_local()
                    || ip.is_unspecified() =>
            {
                return Err("workbuddy_web_url_invalid".into())
            }
            std::net::IpAddr::V6(ip)
                if ip.is_loopback()
                    || ip.is_unspecified()
                    || ip.segments()[0] & 0xfe00 == 0xfc00
                    || ip.segments()[0] & 0xffc0 == 0xfe80 =>
            {
                return Err("workbuddy_web_url_invalid".into())
            }
            _ => {}
        }
    }
    Ok(())
}
pub(crate) fn search_body(params: &WorkBuddySearchParams) -> Result<Value, String> {
    native::text(&params.query, 4096, "workbuddy_search_query_invalid")?;
    if params.query.chars().count() < 2
        || params.allowed_domains.len() > 32
        || params.blocked_domains.len() > 32
        || params
            .allowed_domains
            .iter()
            .chain(&params.blocked_domains)
            .any(|d| !domain(d))
    {
        return Err("workbuddy_search_query_invalid".into());
    }
    let mut query = params.query.clone();
    if !params.blocked_domains.is_empty() {
        query.push_str(" (");
        query.push_str(
            &params
                .blocked_domains
                .iter()
                .map(|d| format!("-site:{d}"))
                .collect::<Vec<_>>()
                .join(" "),
        );
        query.push(')');
    }
    let mut body = json!({"query":query,"type":"text2text","max_results":5});
    if !params.allowed_domains.is_empty() {
        body["allowed_domains"] = json!(params.allowed_domains);
    }
    if let Some(freshness) = &params.freshness {
        native::text(freshness, 32, "workbuddy_search_freshness_invalid")?;
        body["freshness"] = json!(freshness);
    }
    Ok(body)
}
pub async fn search(
    params: WorkBuddySearchParams,
    signal: AbortSignal,
) -> Result<NativeResponse, String> {
    native::bounded(&signal,30,async{
    let body=search_body(&params)?;let session=NativeSession::start().await?;let config=session.config().await?;
    if !media::enabled(&config,"WebSearch",true){return Err("workbuddy_capability_unavailable".into());}
    let value=session.json(reqwest::Method::POST,"/agenttool/v1/search",Some(body),params.session_id.as_deref(),MAX_JSON).await?;
    let rows=value["results"].as_array().ok_or("workbuddy_search_response_invalid")?;if rows.len()>100{return Err("workbuddy_search_response_invalid".into());}
    let mut results=Vec::new();for row in rows{
        let url=row["url"].as_str().ok_or("workbuddy_search_response_invalid")?;public_url(url)?;
        let title=row["title"].as_str().unwrap_or("");let snippet=row["snippet"].as_str().unwrap_or("");if title.len()>16_384||snippet.len()>128*1024{return Err("workbuddy_search_response_invalid".into());}
        results.push(json!({"title":title,"url":url,"snippet":snippet}));
    }
    session.response(json!({"results":results,"totalResults":value["total_results"].as_u64().unwrap_or(results.len() as u64)}))
}).await
}
pub async fn fetch_web_page(
    params: WorkBuddyFetchParams,
    signal: AbortSignal,
) -> Result<NativeResponse, String> {
    native::bounded(&signal,45,async{
    native::text(&params.url,8192,"workbuddy_web_url_invalid")?;public_url(&params.url)?;
    let prompt=params.prompt.as_deref().unwrap_or("");if prompt.len()>4096||prompt.contains('\0'){return Err("workbuddy_fetch_prompt_invalid".into());}
    let session=NativeSession::start().await?;let config=session.config().await?;
    if !media::enabled(&config,"WebFetch",true)||!media::enabled(&config,"WebFetchRemoteApi",true){return Err("workbuddy_capability_unavailable".into());}
    let value=session.json(reqwest::Method::POST,"/agenttool/v1/webfetch",Some(json!({"url":params.url,"format":"markdown","prompt":prompt,"timeout":30,"max_length":100_000})),params.session_id.as_deref(),MAX_JSON).await?;
    if value["content_type"].as_str().is_some_and(|mime|!(mime.starts_with("text/")||mime=="application/xhtml+xml"||mime=="application/json")){return Err("workbuddy_fetch_binary_unsupported".into());}
    let content=value["content"].as_str().filter(|s|!s.trim().is_empty()&&s.len()<=512*1024).ok_or("workbuddy_fetch_response_invalid")?;
    let url=value["url"].as_str().unwrap_or(&params.url);public_url(url)?;
    session.response(json!({"url":url,"content":content,"contentType":value["content_type"].as_str().unwrap_or("text/markdown")}))
}).await
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn search_preserves_filters_without_request_injection() {
        let p = WorkBuddySearchParams {
            query: "rust documentation".into(),
            allowed_domains: vec!["rust-lang.org".into()],
            blocked_domains: vec!["example.com".into()],
            freshness: None,
            session_id: None,
            operation_id: None,
        };
        let body = search_body(&p).unwrap();
        assert_eq!(body["query"], "rust documentation (-site:example.com)");
        assert_eq!(body["max_results"], 5);
        let mut invalid = p;
        invalid.blocked_domains = vec!["example.com) OR secret".into()];
        assert!(search_body(&invalid).is_err());
    }
    #[test]
    fn web_urls_reject_local_and_credential_targets() {
        for url in [
            "file:///tmp/file",
            "http://127.0.0.1",
            "https://user:pass@example.com",
            "http://[::1]/",
            "https://host.local",
        ] {
            assert!(public_url(url).is_err(), "{url}");
        }
        assert!(public_url("https://example.com/docs").is_ok());
    }
}
