//! Redact obvious secrets from text before it is PERSISTED.
//!
//! Scope — deliberately narrow. This runs on content written to durable memory
//! (the `TypedMemoryStore` and the `save_memory` tool), where a leaked key would
//! persist across sessions and re-enter the model's context on every reload. It
//! is NOT applied to:
//!  - live tool output on its way to the model (the model frequently NEEDS to
//!    see a value to do its job), or
//!  - the rollout transcript (redacting it would desync resume).
//!
//! The patterns favour precision over recall: well-known key shapes (OpenAI /
//! AWS / GitHub / Slack) and explicit `secret-name = value` assignments. We do
//! NOT redact bare long hex/base64 blobs — those collide with git SHAs, hashes,
//! and base64 data and would corrupt legitimate memories.

use fancy_regex::Regex;
use std::sync::LazyLock;

/// `(pattern, replacement)` pairs applied in order. Each pattern is applied
/// globally (all matches replaced).
static PATTERNS: LazyLock<Vec<(Regex, &'static str)>> = LazyLock::new(|| {
    vec![
        // OpenAI-style keys: sk-... / sk-proj-... (20+ token chars).
        (
            Regex::new(r"\bsk-(?:proj-)?[A-Za-z0-9_-]{20,}\b").unwrap(),
            "[REDACTED]",
        ),
        // AWS access key IDs: AKIA/ASIA + exactly 16 uppercase alphanumerics.
        (
            Regex::new(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b").unwrap(),
            "[REDACTED]",
        ),
        // GitHub tokens (ghp_/gho_/ghu_/ghs_/ghr_ + 20+).
        (
            Regex::new(r"\bgh[porus]_[A-Za-z0-9]{20,}\b").unwrap(),
            "[REDACTED]",
        ),
        // Slack tokens (xoxb-/xoxp-/xoxa-/xoxr-).
        (
            Regex::new(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b").unwrap(),
            "[REDACTED]",
        ),
        // `Authorization: Bearer <token>` — keep the scheme, redact the
        // credential. Case-insensitive with a 16-char floor (avoids redacting
        // short words like `Bearer optional`).
        (
            Regex::new(r"(?i)\bBearer\s+[A-Za-z0-9._~+/-]{16,}=*").unwrap(),
            "Bearer [REDACTED]",
        ),
        // Explicit secret-named assignments: api_key=..., token: "...",
        // password = '...'. Preserves the key + operator (+ any surrounding
        // quote), redacts the value. The separator is SAME-LINE (`[ \t]`, not
        // `\s`) so a `token:`-style heading followed by a paragraph isn't
        // mangled across the newline; the value floor is 16 characters.
        (
            Regex::new(
                r#"(?i)\b((?:api[_-]?key|secret|token|password|passwd|credential)s?[ \t]*[:=][ \t]*)(["']?)[^\s"']{8,}\2"#,
            )
            .unwrap(),
            "${1}${2}[REDACTED]${2}",
        ),
    ]
});

/// Replace detected secrets in `text` with `[REDACTED]`. Returns `text`
/// unchanged if none match.
pub fn sanitize_secrets(text: &str) -> String {
    let mut out = text.to_string();
    for (pattern, replacement) in PATTERNS.iter() {
        out = pattern.replace_all(&out, *replacement).into_owned();
    }
    out
}

/// True if `text` contains at least one detectable secret (does not mutate).
pub fn contains_secret(text: &str) -> bool {
    sanitize_secrets(text) != text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_openai_keys() {
        assert_eq!(
            sanitize_secrets("key sk-abcdEFGH1234ijklMNOP5678 here"),
            "key [REDACTED] here"
        );
        assert_eq!(
            sanitize_secrets("sk-proj-abcdEFGH1234ijklMNOP5678qrst"),
            "[REDACTED]"
        );
    }

    #[test]
    fn redacts_aws_access_key_ids() {
        let out = sanitize_secrets("id=AKIAIOSFODNN7EXAMPLE done");
        assert!(out.contains("[REDACTED]"));
        assert!(!out.contains("AKIA"));
    }

    #[test]
    fn redacts_github_and_slack_tokens() {
        assert_eq!(
            sanitize_secrets("ghp_0123456789abcdefABCDEF0123456789"),
            "[REDACTED]"
        );
        assert_eq!(
            sanitize_secrets("xoxb-123456789012-abcdefghijkl"),
            "[REDACTED]"
        );
    }

    #[test]
    fn keeps_bearer_scheme_redacts_credential() {
        assert_eq!(
            sanitize_secrets("Authorization: Bearer abc123.def456-ghi789"),
            "Authorization: Bearer [REDACTED]"
        );
    }

    #[test]
    fn redacts_named_assignments_preserving_quotes() {
        assert_eq!(
            sanitize_secrets("api_key=supersecret123"),
            "api_key=[REDACTED]"
        );
        assert_eq!(
            sanitize_secrets("password: \"hunter2hunter2\""),
            "password: \"[REDACTED]\""
        );
        assert_eq!(
            sanitize_secrets("token = 'abcdef123456'"),
            "token = '[REDACTED]'"
        );
    }

    #[test]
    fn leaves_ordinary_text_untouched() {
        let safe = "The DB host is db.internal:5432 and the commit is a1b2c3d4e5f6.";
        assert_eq!(sanitize_secrets(safe), safe);
        assert!(sanitize_secrets("sha 9f86d081884c7d659a2feaa0c55ad015").contains("9f86d081"));
    }

    #[test]
    fn contains_secret_detects_without_mutating() {
        assert!(!contains_secret("nothing here"));
        assert!(contains_secret("token=abcdef123456"));
    }
}
