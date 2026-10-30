use crate::runtime_values::RuntimeString;
use crate::types::errors::{ProviderError, ProviderErrorKind};
use crate::types::provider::ToolChoice;

pub fn validate_tool_choice(
    provider: &str,
    choice: Option<&ToolChoice>,
    tool_names: &[RuntimeString],
    has_anonymous_tools: bool,
) -> Option<ProviderError> {
    let choice = match choice {
        None | Some(ToolChoice::Auto | ToolChoice::None) => return None,
        Some(choice) => choice,
    };

    match choice {
        ToolChoice::Any if tool_names.is_empty() && !has_anonymous_tools => {
            Some(invalid_tool_choice(
                provider,
                "`any` requires at least one provider-visible tool",
            ))
        }
        ToolChoice::Tool { name } if !tool_names.iter().any(|candidate| candidate == name) => {
            let available = if tool_names.is_empty() {
                RuntimeString::from("(none)")
            } else {
                join_runtime_strings(tool_names, ", ")
            };
            let reason = RuntimeString::from("forced tool \"")
                .concat(name)
                .concat(&RuntimeString::from(
                    "\" is not provider-visible; available tools: ",
                ))
                .concat(&available);
            Some(invalid_tool_choice(provider, reason))
        }
        ToolChoice::Auto | ToolChoice::Any | ToolChoice::Tool { .. } | ToolChoice::None => None,
    }
}

pub fn invalid_tool_choice(provider: &str, reason: impl Into<RuntimeString>) -> ProviderError {
    let message = RuntimeString::from(provider)
        .concat(&RuntimeString::from(" tool choice is invalid: "))
        .concat(&reason.into())
        .concat(&RuntimeString::from("."));
    ProviderError {
        kind: ProviderErrorKind::InvalidRequest,
        message,
        status: Some(400),
        code: None,
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        retry_after_ms: None,
        request_id: None,
    }
}

fn join_runtime_strings(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let mut values = values.iter();
    let Some(mut output) = values.next().cloned() else {
        return RuntimeString::default();
    };
    let separator = RuntimeString::from(separator);
    for value in values {
        output = output.concat(&separator).concat(value);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_invalid(error: ProviderError, message: &str) {
        assert_eq!(error.kind, ProviderErrorKind::InvalidRequest);
        assert_eq!(error.message, message);
        assert_eq!(error.status, Some(400));
        assert_eq!(error.retry_after_ms, None);
        assert_eq!(error.request_id, None);
        assert!(!error.message.ends_with(".."));
    }

    #[test]
    fn tool_choice_rejects_any_without_named_or_anonymous_tools() {
        let error = validate_tool_choice("OpenAI", Some(&ToolChoice::Any), &[], false)
            .expect("any without provider-visible tools must fail closed");

        assert_invalid(
            error,
            "OpenAI tool choice is invalid: `any` requires at least one provider-visible tool.",
        );
    }

    #[test]
    fn forced_choice_lists_the_final_available_names() {
        let names = vec![RuntimeString::from("Read"), RuntimeString::from("Bash")];
        let error = validate_tool_choice(
            "Anthropic",
            Some(&ToolChoice::Tool {
                name: "missing".into(),
            }),
            &names,
            false,
        )
        .expect("a missing forced name must fail closed");
        assert_invalid(
            error,
            "Anthropic tool choice is invalid: forced tool \"missing\" is not provider-visible; available tools: Read, Bash.",
        );

        let duplicates = vec!["Read".into(), "Read".into(), "Bash".into()];
        let duplicate_error = validate_tool_choice(
            "Anthropic",
            Some(&ToolChoice::Tool {
                name: "missing".into(),
            }),
            &duplicates,
            false,
        )
        .expect("available names preserve duplicates");
        assert_eq!(
            duplicate_error.message,
            "Anthropic tool choice is invalid: forced tool \"missing\" is not provider-visible; available tools: Read, Read, Bash."
        );

        let empty_error = validate_tool_choice(
            "Anthropic",
            Some(&ToolChoice::Tool {
                name: "missing".into(),
            }),
            &[],
            false,
        )
        .expect("an empty final set must list none");
        assert_eq!(
            empty_error.message,
            "Anthropic tool choice is invalid: forced tool \"missing\" is not provider-visible; available tools: (none)."
        );

        assert_eq!(
            validate_tool_choice(
                "Anthropic",
                Some(&ToolChoice::Tool {
                    name: "Read".into(),
                }),
                &names,
                false,
            ),
            None
        );
        let case_error = validate_tool_choice(
            "Anthropic",
            Some(&ToolChoice::Tool {
                name: "read".into(),
            }),
            &names,
            false,
        )
        .expect("forced matching is case-sensitive");
        assert_eq!(
            case_error.message,
            "Anthropic tool choice is invalid: forced tool \"read\" is not provider-visible; available tools: Read, Bash."
        );
    }

    #[test]
    fn anonymous_native_search_satisfies_any_but_not_forced_name() {
        assert_eq!(
            validate_tool_choice("OpenAI Responses", Some(&ToolChoice::Any), &[], true),
            None
        );

        let forced_error = validate_tool_choice(
            "OpenAI Responses",
            Some(&ToolChoice::Tool {
                name: "web_search".into(),
            }),
            &[],
            true,
        )
        .expect("anonymous tools have no forceable name");
        assert_invalid(
            forced_error,
            "OpenAI Responses tool choice is invalid: forced tool \"web_search\" is not provider-visible; available tools: (none).",
        );

        let names = vec![RuntimeString::from("read")];
        assert_eq!(validate_tool_choice("OpenAI", None, &[], false), None);
        assert_eq!(
            validate_tool_choice("OpenAI", Some(&ToolChoice::Auto), &[], false),
            None
        );
        assert_eq!(
            validate_tool_choice("OpenAI", Some(&ToolChoice::None), &[], false),
            None
        );
        assert_eq!(
            validate_tool_choice(
                "OpenAI",
                Some(&ToolChoice::Tool {
                    name: "read".into(),
                }),
                &names,
                false,
            ),
            None
        );
    }
}
