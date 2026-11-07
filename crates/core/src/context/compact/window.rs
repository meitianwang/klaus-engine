//! Resolve actual model capacity separately from an earlier automatic window.
//! Runtime overrides are bound from an explicit environment snapshot; hot-path
//! threshold calculations never read the process environment.

use std::sync::LazyLock;

use regex::Regex;

use crate::context::total_tokens_reminder::bind_reminder_environment;
use crate::runtime_process::RuntimeEnv;
use crate::runtime_values::RuntimeString;
use crate::schemas::config::ResolvedConfig;

pub const AUTO_WINDOW_ENV: &str = "KLAUS_AUTO_COMPACT_WINDOW";
pub const DISABLE_COMPACT_ENV: &str = "KLAUS_DISABLE_COMPACT";
pub const DISABLE_AUTO_COMPACT_ENV: &str = "KLAUS_DISABLE_AUTO_COMPACT";
const MIN_ENV_WINDOW: u64 = 100_000;
const MAX_ENV_WINDOW: u64 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoWindowSource {
    Automatic,
    Environment,
    Configuration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutoWindowSelection {
    /// Validated actual provider capacity, including any smaller declared limit.
    pub actual: u64,
    /// The selected automatic window, capped by actual capacity.
    pub selected: u64,
    /// Original configured limit before capping it by actual capacity.
    pub configured: Option<u64>,
    pub source: AutoWindowSource,
}

pub fn actual_model_window(config: &ResolvedConfig) -> u64 {
    config.model_context_window.unwrap_or(config.context_window)
}

pub fn resolve_auto_window(config: &ResolvedConfig) -> AutoWindowSelection {
    let actual = actual_model_window(config);
    let configured = config.auto_compact_window;
    AutoWindowSelection {
        actual,
        selected: configured.map_or(actual, |window| window.min(actual)),
        configured,
        source: if configured.is_some() {
            config
                .auto_compact_window_source
                .unwrap_or(AutoWindowSource::Configuration)
        } else {
            AutoWindowSource::Automatic
        },
    }
}

/// Apply applicable runtime overrides after stored/session layers are resolved.
/// Model binding subsequently supplies actual capacity without changing these
/// automatic-window limits or their source.
pub fn bind_context_runtime_environment(config: &mut ResolvedConfig, env: &RuntimeEnv) {
    let enabled = |key: &str| {
        env.get(&RuntimeString::from(key))
            .and_then(environment_text)
            .is_some_and(|value| {
                matches!(
                    value.to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
    };
    config.compaction_disabled |= enabled(DISABLE_COMPACT_ENV);
    if config.compaction_disabled || enabled(DISABLE_AUTO_COMPACT_ENV) {
        config.auto_compaction = false;
    }
    if let Some(window) = env
        .get(&RuntimeString::from(AUTO_WINDOW_ENV))
        .and_then(parse_environment_integer)
        .filter(|window| *window > 0.0)
    {
        config.auto_compact_window =
            Some(window.clamp(MIN_ENV_WINDOW as f64, MAX_ENV_WINDOW as f64) as u64);
        config.auto_compact_window_source = Some(AutoWindowSource::Environment);
    }
    bind_reminder_environment(config, env);
}

pub(crate) fn environment_text(raw: &RuntimeString) -> Option<String> {
    let text = raw.try_to_utf8().ok()?;
    let text = text.trim_matches(|ch| {
        matches!(ch,
            '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}' |
            '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' |
            '\u{205f}' | '\u{3000}' | '\u{feff}')
    });
    (!text.is_empty()).then(|| text.to_owned())
}

/// Read integer environment values, including integral scientific notation,
/// consistently grouped thousands and the leading decimal integer fallback.
pub(crate) fn parse_environment_integer(raw: &RuntimeString) -> Option<f64> {
    static SCIENTIFIC: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)[eE][+-]?[0-9]+$").unwrap()
    });
    let text = environment_text(raw)?;
    let (negative, unsigned) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text.as_str()),
    };
    if text.encode_utf16().count() <= 32 {
        if SCIENTIFIC.is_match(&text) {
            let value = text.parse::<f64>().ok()?;
            return (value.is_finite() && value.fract() == 0.0).then_some(value);
        }
        if let Some(separator) = unsigned.chars().find(|ch| !ch.is_ascii_digit()) {
            if matches!(separator, '_' | ',' | '\u{00a0}' | '\u{202f}' | ' ') {
                let groups = unsigned.split(separator).collect::<Vec<_>>();
                if (1..=3).contains(&groups[0].len())
                    && groups[0].bytes().all(|byte| byte.is_ascii_digit())
                    && groups[1..].iter().all(|group| {
                        group.len() == 3 && group.bytes().all(|byte| byte.is_ascii_digit())
                    })
                {
                    let value = groups.concat().parse::<f64>().ok()?;
                    return Some(if negative { -value } else { value });
                }
            }
        }
    }
    let end = unsigned
        .bytes()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if end == 0 {
        return None;
    }
    let value = unsigned[..end].parse::<f64>().ok()?;
    Some(if negative { -value } else { value })
}
