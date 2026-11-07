//! A regex-free static exec policy engine: a classifier that maps a shell
//! command to `allow` / `prompt` / `forbidden` before the interactive
//! permission flow runs.

pub mod engine;
pub mod matchers;
pub mod policy;
pub mod rules;

// Public, stable surface.
pub use engine::{ExecEvaluation, ExecPolicy, ExecPolicyOptions, FromPolicyTextOptions};
pub use matchers::{
    flag, literal, option, pos_int, rest, rest_at_least_one, value, ArgMatcher, OptionOpts,
    PatternToken,
};
pub use policy::{
    parse_exec_policy, NetworkRule, NetworkRuleProtocol, ParsedPolicy, PolicyParseError,
    PolicyValidation,
};
pub use rules::{default_exec_rules, ExecDecision, ExecRule};
