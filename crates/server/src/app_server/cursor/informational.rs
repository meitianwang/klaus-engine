//! Validate optional presentation updates without adopting their contents into
//! the caller's conversation, execution state, or primary answer.
use serde_json::{Map, Value};

const INVALID: &str = "cursor_native_update_not_allowed";
const MAX_HINT_TEXT: usize = 64 * 1024;
const MAX_COMPARISON_TEXT: usize = 8 * 1024 * 1024;
const MAX_CATEGORIES: usize = 64;
const MAX_CATEGORY_GROUPS: usize = 16;

pub(super) fn is_informational(case: &str) -> bool {
    matches!(
        case,
        "promptSuggestion"
            | "postRequestPrompt"
            | "feedbackRequest"
            | "responseComparison"
            | "grokBotNudge"
    )
}

fn fields<'a>(value: &'a Value, allowed: &[&str]) -> Result<&'a Map<String, Value>, String> {
    let fields = value.as_object().ok_or(INVALID)?;
    if fields.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(INVALID.into());
    }
    Ok(fields)
}

fn strings(value: &Value, allowed: &[&str], max: usize) -> Result<(), String> {
    if fields(value, allowed)?
        .values()
        .any(|value| value.as_str().is_none_or(|text| text.len() > max))
    {
        return Err(INVALID.into());
    }
    Ok(())
}

fn string_fields(value: &Value, names: &[&str], max: usize) -> Result<(), String> {
    if names.iter().any(|name| {
        value
            .get(*name)
            .is_some_and(|value| value.as_str().is_none_or(|text| text.len() > max))
    }) {
        return Err(INVALID.into());
    }
    Ok(())
}

fn category(value: &Value) -> Result<(), String> {
    strings(value, &["id", "label"], MAX_HINT_TEXT)
}

fn list(
    value: Option<&Value>,
    max: usize,
    validate: fn(&Value) -> Result<(), String>,
) -> Result<(), String> {
    let Some(value) = value else { return Ok(()) };
    let values = value.as_array().ok_or(INVALID)?;
    if values.len() > max {
        return Err(INVALID.into());
    }
    values.iter().try_for_each(validate)
}

fn category_group(value: &Value) -> Result<(), String> {
    fields(value, &["id", "prompt", "categories"])?;
    string_fields(value, &["id", "prompt"], MAX_HINT_TEXT)?;
    list(value.get("categories"), MAX_CATEGORIES, category)
}

fn known_enum(value: Option<&Value>, names: &[&str]) -> Result<(), String> {
    if value.is_none_or(|value| {
        value
            .as_u64()
            .is_some_and(|number| number < names.len() as u64)
            || value.as_str().is_some_and(|name| names.contains(&name))
    }) {
        Ok(())
    } else {
        Err(INVALID.into())
    }
}

fn comparison(value: &Value) -> Result<(), String> {
    let map = fields(
        value,
        &[
            "comparisonId",
            "started",
            "textDelta",
            "completed",
            "skipped",
        ],
    )?;
    string_fields(value, &["comparisonId"], MAX_HINT_TEXT)?;
    let events = map
        .keys()
        .filter(|key| key.as_str() != "comparisonId")
        .collect::<Vec<_>>();
    if events.len() != 1 {
        return Err(INVALID.into());
    }
    let event = &value[events[0]];
    match events[0].as_str() {
        "started" => {
            let text = [
                "parentInvocationId",
                "alternateInvocationId",
                "comparisonConfigId",
                "alternateModelId",
            ];
            fields(
                event,
                &[
                    "displayOrder",
                    "parentInvocationId",
                    "alternateInvocationId",
                    "parentResponse",
                    "comparisonConfigId",
                    "alternateModelId",
                ],
            )?;
            string_fields(event, &text, MAX_HINT_TEXT)?;
            string_fields(event, &["parentResponse"], MAX_COMPARISON_TEXT)?;
            known_enum(
                event.get("displayOrder"),
                &[
                    "RESPONSE_COMPARISON_DISPLAY_ORDER_UNSPECIFIED",
                    "RESPONSE_COMPARISON_DISPLAY_ORDER_PARENT_FIRST",
                    "RESPONSE_COMPARISON_DISPLAY_ORDER_ALTERNATE_FIRST",
                ],
            )
        }
        "textDelta" => strings(event, &["text"], MAX_COMPARISON_TEXT),
        "completed" => fields(event, &[]).map(|_| ()),
        "skipped" => {
            fields(event, &["reason"])?;
            known_enum(
                event.get("reason"),
                &[
                    "RESPONSE_COMPARISON_SKIP_REASON_UNSPECIFIED",
                    "RESPONSE_COMPARISON_SKIP_REASON_ALTERNATE_TOOL_CALL",
                    "RESPONSE_COMPARISON_SKIP_REASON_INFERENCE_ERROR",
                    "RESPONSE_COMPARISON_SKIP_REASON_TIMEOUT",
                    "RESPONSE_COMPARISON_SKIP_REASON_CANCELLED",
                ],
            )
        }
        _ => Err(INVALID.into()),
    }
}

pub(super) fn validate(case: &str, value: &Value) -> Result<(), String> {
    match case {
        "promptSuggestion" => strings(value, &["suggestion"], MAX_HINT_TEXT),
        "postRequestPrompt" => strings(
            value,
            &["title", "message", "buttonLabel", "buttonUrl"],
            MAX_HINT_TEXT,
        ),
        "feedbackRequest" => {
            fields(
                value,
                &[
                    "requestId",
                    "canonicalModelName",
                    "categories",
                    "categoryGroups",
                    "showFormImmediately",
                    "title",
                    "negativeTitle",
                    "commentPlaceholder",
                ],
            )?;
            string_fields(
                value,
                &[
                    "requestId",
                    "canonicalModelName",
                    "title",
                    "negativeTitle",
                    "commentPlaceholder",
                ],
                MAX_HINT_TEXT,
            )?;
            if value
                .get("showFormImmediately")
                .is_some_and(|value| !value.is_boolean())
            {
                return Err(INVALID.into());
            }
            list(value.get("categories"), MAX_CATEGORIES, category)?;
            list(
                value.get("categoryGroups"),
                MAX_CATEGORY_GROUPS,
                category_group,
            )
        }
        "responseComparison" => comparison(value),
        "grokBotNudge" => {
            fields(value, &["job", "description", "showNumber"])?;
            string_fields(value, &["job", "description"], MAX_HINT_TEXT)?;
            if value.get("showNumber").is_some_and(|value| {
                value
                    .as_u64()
                    .is_none_or(|number| u32::try_from(number).is_err())
            }) {
                return Err(INVALID.into());
            }
            Ok(())
        }
        _ => Err(INVALID.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn informational_payloads_accept_only_known_bounded_fields_and_default_scalars() {
        for (case, value) in [
            ("promptSuggestion", json!({"suggestion":"Next task"})),
            (
                "postRequestPrompt",
                json!({"title":"Title","message":"Message","buttonLabel":"Label","buttonUrl":"https://example.invalid/action"}),
            ),
            (
                "feedbackRequest",
                json!({"requestId":"feedback","canonicalModelName":"model","categories":[{"id":"helpful","label":"Helpful"}],"categoryGroups":[{"id":"group","prompt":"Which?","categories":[{"id":"one","label":"One"}]}],"showFormImmediately":true,"title":"Title","negativeTitle":"Issue","commentPlaceholder":"Comment"}),
            ),
            (
                "grokBotNudge",
                json!({"job":"job","description":"Description","showNumber":u32::MAX}),
            ),
        ] {
            assert!(is_informational(case));
            validate(case, &value).unwrap();
            validate(case, &json!({})).unwrap();
            for malformed in [Value::Null, json!([]), json!({"unknown":"value"})] {
                assert!(validate(case, &malformed).is_err(), "{case}: {malformed}");
            }
            let mut unknown = value;
            unknown["unknown"] = json!(true);
            assert!(validate(case, &unknown).is_err(), "{case}");
        }
        for value in [
            json!({"started":{}}),
            json!({"comparisonId":"comparison","started":{"displayOrder":"RESPONSE_COMPARISON_DISPLAY_ORDER_PARENT_FIRST","parentInvocationId":"parent","alternateInvocationId":"alternate","parentResponse":"Other answer","comparisonConfigId":"config","alternateModelId":"other"}}),
            json!({"textDelta":{"text":"Alternate text"}}),
            json!({"completed":{}}),
            json!({"skipped":{"reason":"RESPONSE_COMPARISON_SKIP_REASON_ALTERNATE_TOOL_CALL"}}),
            json!({"skipped":{"reason":4}}),
        ] {
            validate("responseComparison", &value).unwrap();
        }
    }

    #[test]
    fn informational_payloads_reject_nested_type_confusion_oversize_and_ambiguous_events() {
        for (case, value) in [
            ("promptSuggestion", json!({"suggestion":false})),
            (
                "promptSuggestion",
                json!({"suggestion":"x".repeat(MAX_HINT_TEXT+1)}),
            ),
            ("postRequestPrompt", json!({"buttonUrl":null})),
            ("feedbackRequest", json!({"requestId":1})),
            ("feedbackRequest", json!({"showFormImmediately":"true"})),
            ("feedbackRequest", json!({"categories":{}})),
            ("feedbackRequest", json!({"categories":[{"label":false}]})),
            (
                "feedbackRequest",
                json!({"categories":[{"id":"one","command":"run"}]}),
            ),
            (
                "feedbackRequest",
                json!({"categories":vec![json!({});MAX_CATEGORIES+1]}),
            ),
            (
                "feedbackRequest",
                json!({"categoryGroups":vec![json!({});MAX_CATEGORY_GROUPS+1]}),
            ),
            (
                "feedbackRequest",
                json!({"categoryGroups":[{"categories":[{"extra":true}]}]}),
            ),
            (
                "feedbackRequest",
                json!({"categoryGroups":[{"prompt":"x".repeat(MAX_HINT_TEXT+1)}]}),
            ),
            ("grokBotNudge", json!({"showNumber":-1})),
            ("grokBotNudge", json!({"showNumber":u64::from(u32::MAX)+1})),
            ("grokBotNudge", json!({"showNumber":"1"})),
            ("responseComparison", json!({})),
            ("responseComparison", json!({"completed":{},"skipped":{}})),
            ("responseComparison", json!({"textDelta":{"text":false}})),
            (
                "responseComparison",
                json!({"textDelta":{"text":"x".repeat(MAX_COMPARISON_TEXT+1)}}),
            ),
            ("responseComparison", json!({"started":{"displayOrder":3}})),
            (
                "responseComparison",
                json!({"started":{"displayOrder":"PARENT_FIRST"}}),
            ),
            (
                "responseComparison",
                json!({"started":{"alternateInvocationId":{}}}),
            ),
            (
                "responseComparison",
                json!({"completed":{"text":"Injected answer"}}),
            ),
            ("responseComparison", json!({"skipped":{"reason":5}})),
            (
                "responseComparison",
                json!({"skipped":{"reason":"UNKNOWN"}}),
            ),
        ] {
            assert_eq!(validate(case, &value).unwrap_err(), INVALID, "{case}");
        }
        assert!(!is_informational("userMessageAppended"));
        assert!(validate("futureHint", &json!({})).is_err());
    }
}
