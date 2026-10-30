//! Page extraction always uses the built-in reader.
//! A previously stored provider name, including `workbuddy`, is ignored so an
//! existing config still starts and reads the HTTP response locally.
use klaus_agent_core::schemas::config::WebFetchConfig;
use klaus_agent_core::tools::builtins::web_fetch::{PageFetcher, PageFetcherFactory};
use std::sync::Arc;

pub(crate) fn source_page_fetcher_factory() -> PageFetcherFactory {
    Arc::new(source_page_fetcher)
}

fn source_page_fetcher(
    config: Option<&WebFetchConfig>,
) -> Result<Option<Arc<dyn PageFetcher>>, String> {
    match config.and_then(|config| config.source.as_deref()) {
        None | Some("" | "builtin" | "workbuddy") => Ok(None),
        Some(_) => Err("The selected page extraction source is unavailable".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_page_fetch_is_not_selectable() {
        let factory = source_page_fetcher_factory();
        assert!(factory(None).unwrap().is_none());
        let config = |source: &str| WebFetchConfig {
            source: Some(source.into()),
            ..Default::default()
        };
        assert!(factory(Some(&config("builtin"))).unwrap().is_none());
        assert!(factory(Some(&config("workbuddy"))).unwrap().is_none());
        assert!(factory(Some(&config("unknown-source"))).is_err());
    }
}
