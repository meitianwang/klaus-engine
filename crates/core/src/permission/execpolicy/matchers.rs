//! Structured argument-matcher model for the exec-policy engine.
//!
//! This is an idiomatic Rust rule model for classifying a command's arguments.
//! It fuses a `PatternToken`/`PrefixPattern` pattern model with a structured
//! `flag`/`opt`/`ARG_*` matcher model over individual arguments and options.
//! It is regex-free by design — every
//! matcher is a plain string / set / structural comparison, so there is no ReDoS
//! surface and no opaque bypass vector. Nothing here ever executes a command; the
//! matchers only *classify* a tokenized argv.
//!
//! A command's argv (the tokens AFTER the program name) is matched against an
//! ordered list of [`ArgMatcher`]s. Matching is a small deterministic state
//! machine (see `engine.rs`): positional patterns consume one or more tokens,
//! flags/options consume option tokens wherever they appear, and a trailing
//! "rest" matcher soaks up any remaining tokens.

/// A single fixed token, or a choice between several allowed tokens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PatternToken {
    /// A single fixed token that must match exactly.
    Single(String),
    /// A choice between several allowed tokens (any-of).
    Alts(Vec<String>),
}

impl PatternToken {
    /// Convenience constructor from a `&str`.
    pub fn single(s: impl Into<String>) -> PatternToken {
        PatternToken::Single(s.into())
    }

    /// Convenience constructor from a slice of `&str`.
    pub fn alts<S: Into<String> + Clone>(alts: &[S]) -> PatternToken {
        PatternToken::Alts(alts.iter().map(|s| s.clone().into()).collect())
    }
}

/// True when `token` satisfies the [`PatternToken`] (Single = exact, Alts = any-of).
pub fn pattern_token_matches(pattern: &PatternToken, token: &str) -> bool {
    match pattern {
        PatternToken::Single(p) => p == token,
        PatternToken::Alts(alts) => alts.iter().any(|alt| alt == token),
    }
}

/// Render a [`PatternToken`] for diagnostics, e.g. `status` or `[diff|show]`.
pub fn render_pattern_token(pattern: &PatternToken) -> String {
    match pattern {
        PatternToken::Single(s) => s.clone(),
        PatternToken::Alts(alts) => format!("[{}]", alts.join("|")),
    }
}

/// The kinds of argument matchers.
///
///  - `Literal`   — one positional token that must equal a fixed value (or be one
///    of several alternatives).
///  - `Value`     — one positional token of any value (an opaque operand such as a
///    path or pattern).
///  - `PosInt`    — one positional token that must parse as a positive integer.
///  - `Flag`      — a boolean option token (no value), e.g. `-l`, `--oneline`.
///  - `Option`    — an option token that consumes the following token as its value
///    (`-n 20`), or the inline `--name=value` form. `value_is_pos_int`
///    tightens the value to a positive integer.
///  - `Rest`      — soak up any number of remaining positional tokens (zero or
///    more). `at_least_one` makes it require >=1 token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArgMatcher {
    Literal {
        token: PatternToken,
    },
    Value,
    PosInt,
    Flag {
        names: PatternToken,
        required: bool,
    },
    Option {
        names: PatternToken,
        required: bool,
        value_is_pos_int: bool,
    },
    Rest {
        at_least_one: bool,
    },
}

// ── Concise constructors (read like a compact policy DSL) ────────────────────

/// Match one positional token equal to `token` (or any of the alternatives).
pub fn literal(token: PatternToken) -> ArgMatcher {
    ArgMatcher::Literal { token }
}

/// Match one positional operand of any value (path, pattern, opaque string).
pub fn value() -> ArgMatcher {
    ArgMatcher::Value
}

/// Match one positional token that parses as a positive integer.
pub fn pos_int() -> ArgMatcher {
    ArgMatcher::PosInt
}

/// Match a boolean flag option, e.g. `flag('-l')` or `flag(['-r', '-R', '--recursive'])`.
pub fn flag(names: PatternToken, required: bool) -> ArgMatcher {
    ArgMatcher::Flag { names, required }
}

/// Options for [`option`].
#[derive(Debug, Clone, Copy, Default)]
pub struct OptionOpts {
    pub required: bool,
    pub value_is_pos_int: bool,
}

/// Match an option that takes a value: `option('-n')` accepts `-n 20` and
/// `-n=20` / `--lines=20`. Set `value_is_pos_int` to require a positive integer.
pub fn option(names: PatternToken, opts: OptionOpts) -> ArgMatcher {
    ArgMatcher::Option {
        names,
        required: opts.required,
        value_is_pos_int: opts.value_is_pos_int,
    }
}

/// Soak up any remaining positional tokens (zero or more).
pub fn rest() -> ArgMatcher {
    ArgMatcher::Rest {
        at_least_one: false,
    }
}

/// Soak up one-or-more remaining positional tokens (the form fails on zero).
pub fn rest_at_least_one() -> ArgMatcher {
    ArgMatcher::Rest { at_least_one: true }
}

/// True when `token` parses as a strictly-positive base-10 integer.
///
/// The rule: only ASCII digits, and after stripping
/// leading zeros the canonical form is not `"0"` and the value is > 0.
pub fn is_positive_integer(token: &str) -> bool {
    if token.is_empty() || !token.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    // Not all zeros → contains at least one nonzero digit.
    token.bytes().any(|b| b != b'0')
}

/// True when `token` looks like an option (`-x`, `--name`, `--name=value`) but not `-`/`--`.
pub fn looks_like_option(token: &str) -> bool {
    token.chars().count() > 1 && token.starts_with('-') && token != "--"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pattern_token_matches_single_and_alternatives() {
        assert!(pattern_token_matches(&PatternToken::single("a"), "a"));
        assert!(!pattern_token_matches(&PatternToken::single("a"), "b"));
        assert!(pattern_token_matches(&PatternToken::alts(&["a", "b"]), "b"));
        assert!(!pattern_token_matches(
            &PatternToken::alts(&["a", "b"]),
            "c"
        ));
    }

    #[test]
    fn is_positive_integer_cases() {
        assert!(is_positive_integer("1"));
        assert!(is_positive_integer("20"));
        assert!(!is_positive_integer("0"));
        assert!(!is_positive_integer("-1"));
        assert!(!is_positive_integer("1.5"));
        assert!(!is_positive_integer("x"));
        assert!(is_positive_integer("007"));
    }

    #[test]
    fn looks_like_option_cases() {
        assert!(looks_like_option("-r"));
        assert!(looks_like_option("--recursive"));
        assert!(looks_like_option("--name=value"));
        assert!(!looks_like_option("-"));
        assert!(!looks_like_option("--"));
        assert!(!looks_like_option("file"));
    }
}
