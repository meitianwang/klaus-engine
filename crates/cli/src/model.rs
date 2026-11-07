//! What a `--model` value names.
//!
//! Two different things can be written on that flag, and the engine takes them
//! through two different fields:
//!
//!   * a SELECTION — WHICH configured model to run on. The endpoint resolves it
//!     against `[modelProviders]` and the credential store, and settles the
//!     session's provider and its auth from what it finds. Carried as the
//!     app-server's `modelSelection`.
//!   * a NAME — the model id to ask an ALREADY-resolved provider for. Carried as
//!     the session config's `model`.
//!
//! They are told apart by the source prefix, and that is the whole point: the
//! desktop client writes model ids in exactly this form, so an id copied out of
//! it means the same thing here. Before this existed the CLI put every `--model`
//! value into the config's `model`, which is why it could name a model but never
//! choose which configured provider — and therefore which credential — answered.
//!
//! The two are mutually exclusive on purpose. Sending both would pick a provider
//! by selection and then ask it for a different model by name, and the engine has
//! no rule for which of the two wins.

/// The model-source prefixes, the same set the desktop client writes.
///
/// Held with the colon attached so a model id that merely contains one (`us.
/// anthropic.claude-opus-4:1`) is not read as a source. A value carrying none of
/// these is a model name.
pub const SOURCE_PREFIXES: &[&str] = &[
    "trae:",
    "kiro:",
    "subscription:",
    "codex:",
    "qoder:",
    "qwenwork:",
    "qoderwork:",
    "workbuddy:",
    "cursor:",
    "kimi:",
    "bailian:",
    "minimax:",
    "zhipu:",
    "ark:",
    "local:",
    "custom:",
];

/// The selection that leaves the choice to the endpoint's own default.
pub const AUTO: &str = "auto";

/// What one `--model` / `/model` value names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelRef<'a> {
    /// Resolved by the endpoint into a provider plus the credential it holds.
    Selection(&'a str),
    /// A model id for whichever provider the endpoint already resolved.
    Name(&'a str),
}

impl<'a> ModelRef<'a> {
    /// Read a value as one or the other. Whitespace is trimmed first so a quoted
    /// argument with a stray space is not mistaken for a name.
    pub fn parse(value: &'a str) -> Self {
        let value = value.trim();
        if value == AUTO
            || SOURCE_PREFIXES
                .iter()
                .any(|prefix| value.starts_with(prefix))
        {
            Self::Selection(value)
        } else {
            Self::Name(value)
        }
    }

    pub fn as_str(&self) -> &'a str {
        match self {
            Self::Selection(value) | Self::Name(value) => value,
        }
    }

    /// The source a selection names, without the colon. `None` for `auto` and for
    /// a plain model name, neither of which names one.
    pub fn source(&self) -> Option<&'a str> {
        match self {
            Self::Selection(value) => value.split_once(':').map(|(source, _)| source),
            Self::Name(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_source_prefix_makes_it_a_selection() {
        for value in [
            "subscription:opus",
            "codex:gpt-5.6-sol",
            "qoder:auto",
            "qwenwork:auto",
            "local:llama3",
            "custom:5f344546",
        ] {
            assert_eq!(
                ModelRef::parse(value),
                ModelRef::Selection(value),
                "{value}"
            );
        }
        assert_eq!(ModelRef::parse(AUTO), ModelRef::Selection(AUTO));
    }

    /// A bare id stays a name — including one that contains a colon of its own,
    /// which is why the prefixes are matched with the colon attached.
    #[test]
    fn anything_else_is_a_model_name() {
        for value in [
            "claude-sonnet-4-6",
            "gpt-5.5",
            "us.anthropic.claude-opus-4:1",
            "moonshotai/kimi-k2.6",
        ] {
            assert_eq!(ModelRef::parse(value), ModelRef::Name(value), "{value}");
        }
    }

    #[test]
    fn surrounding_space_does_not_change_what_it_names() {
        assert_eq!(
            ModelRef::parse("  custom:abc  "),
            ModelRef::Selection("custom:abc")
        );
        assert_eq!(ModelRef::parse(" gpt-5.5 "), ModelRef::Name("gpt-5.5"));
    }

    #[test]
    fn only_a_selection_has_a_source() {
        assert_eq!(
            ModelRef::parse("subscription:opus").source(),
            Some("subscription")
        );
        assert_eq!(ModelRef::parse(AUTO).source(), None);
        assert_eq!(ModelRef::parse("gpt-5.5").source(), None);
    }
}
