//! Runtime tool-input policies.
//!
//! Native function tools use their Draft 2020-12 schema as the erased form of
//! the tool's typed input contract. MCP tools deliberately use a permissive
//! loose-object boundary instead, while freeform tools bypass this module.

use regex::Regex;

use crate::runtime_values::{runtime_json, RuntimeNumber, RuntimeObject, RuntimeString};
use crate::types::errors::ToolError;
use crate::types::json::Json;

const JS_MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

#[derive(Debug)]
struct ParseIssue {
    path: Vec<String>,
    message: String,
}

type PropertyOrderResolver<'a> = dyn Fn(&[String]) -> Option<&'static [&'static str]> + 'a;

impl ParseIssue {
    fn render(&self) -> String {
        let path = if self.path.is_empty() {
            "(root)".to_string()
        } else {
            self.path.join(".")
        };
        format!("{path}: {}", self.message)
    }
}

/// Parse one native function-tool input through the schema-derived Zod
/// compatibility boundary.
pub(crate) fn parse_native_input(
    schema: &Json,
    input: &Json,
    property_order: &PropertyOrderResolver<'_>,
) -> Result<Json, ToolError> {
    // An actually empty JSON Schema is the Draft 2020-12 unconstrained schema.
    // Test/host tools use this intentionally; provider rendering may still add
    // an object type later, but runtime parsing must honor the declared schema.
    if schema.as_object().is_some_and(RuntimeObject::is_empty) {
        return Ok(input.clone());
    }

    let mut issues = Vec::new();
    let parsed = parse_schema_value(
        schema,
        Some(input),
        &mut Vec::new(),
        &mut issues,
        property_order,
    );
    if issues.is_empty() {
        Ok(parsed.unwrap_or(Json::Null))
    } else {
        Err(ToolError::new(format!(
            "Invalid arguments: {}",
            issues
                .iter()
                .map(ParseIssue::render)
                .collect::<Vec<_>>()
                .join("; ")
        )))
    }
}

/// MCP's fixed wrapper is `z.looseObject({})`: reject only a non-object root
/// and preserve every server-owned key/value recursively.
pub(crate) fn parse_mcp_loose_object(input: &Json) -> Result<Json, ToolError> {
    if input.is_object() {
        Ok(input.clone())
    } else {
        Err(ToolError::new(format!(
            "Invalid arguments: (root): Invalid input: expected object, received {}",
            json_type(input)
        )))
    }
}

fn parse_schema_value(
    schema: &Json,
    input: Option<&Json>,
    path: &mut Vec<String>,
    issues: &mut Vec<ParseIssue>,
    property_order: &PropertyOrderResolver<'_>,
) -> Option<Json> {
    match schema {
        Json::Bool(true) => {
            return input.cloned().or_else(|| {
                push_issue(path, "Invalid input: received undefined", issues);
                None
            });
        }
        Json::Bool(false) => {
            push_issue(path, "Invalid input", issues);
            return None;
        }
        Json::Object(_) => {}
        _ => return input.cloned(),
    }

    let object = schema.as_object().expect("matched object schema");
    let input = match input {
        Some(input) => input,
        None => {
            if let Some(default) = object.get("default") {
                return parse_schema_value(schema, Some(default), path, issues, property_order);
            }
            push_missing_issue(object, path, issues);
            return None;
        }
    };

    if let Some(branches) = object.get("anyOf").and_then(Json::as_array) {
        return parse_union(branches, input, path, issues, property_order, false);
    }
    if let Some(branches) = object.get("oneOf").and_then(Json::as_array) {
        return parse_union(branches, input, path, issues, property_order, true);
    }

    if let Some(expected) = object.get("const") {
        if input != expected {
            push_issue(
                path,
                format!("Invalid input: expected {}", render_json(expected)),
                issues,
            );
            return None;
        }
    }
    if let Some(values) = object.get("enum").and_then(Json::as_array) {
        if !values.iter().any(|expected| expected == input) {
            push_issue(
                path,
                format!(
                    "Invalid option: expected one of {}",
                    values.iter().map(render_json).collect::<Vec<_>>().join("|")
                ),
                issues,
            );
            return None;
        }
    }

    let schema_type = object.get("type");
    let selected_type = match schema_type {
        Some(Json::String(value)) => Some(value.clone()),
        Some(Json::Array(types)) => {
            let selected = types
                .iter()
                .filter_map(Json::as_str)
                .find(|candidate| value_matches_type(input, candidate))
                .cloned();
            if selected.is_none() {
                push_issue(path, "Invalid input", issues);
                return None;
            }
            selected
        }
        _ if object.contains_key("properties") || object.contains_key("required") => {
            Some(RuntimeString::from("object"))
        }
        _ if object.contains_key("items") || object.contains_key("prefixItems") => {
            Some(RuntimeString::from("array"))
        }
        _ => None,
    };

    match selected_type.as_ref() {
        Some(expected) if expected == "object" => {
            parse_object(object, input, path, issues, property_order)
        }
        Some(expected) if expected == "array" => {
            parse_array(object, input, path, issues, property_order)
        }
        Some(expected) if expected == "string" => parse_string(object, input, path, issues),
        Some(expected) if expected == "integer" => parse_number(object, input, path, issues, true),
        Some(expected) if expected == "number" => parse_number(object, input, path, issues, false),
        Some(expected) if expected == "boolean" => {
            if input.is_boolean() {
                Some(input.clone())
            } else {
                push_type_issue(path, "boolean", input, issues);
                None
            }
        }
        Some(expected) if expected == "null" => {
            if input.is_null() {
                Some(Json::Null)
            } else {
                push_type_issue(path, "null", input, issues);
                None
            }
        }
        Some(expected) => {
            if value_matches_type(input, expected) {
                Some(input.clone())
            } else {
                push_type_issue(path, &schema_string(expected), input, issues);
                None
            }
        }
        None => Some(input.clone()),
    }
}

fn parse_union(
    branches: &[Json],
    input: &Json,
    path: &mut Vec<String>,
    issues: &mut Vec<ParseIssue>,
    property_order: &PropertyOrderResolver<'_>,
    require_exactly_one: bool,
) -> Option<Json> {
    let mut matches = Vec::new();
    for branch in branches {
        let mut branch_issues = Vec::new();
        if let Some(parsed) = parse_schema_value(
            branch,
            Some(input),
            path,
            &mut branch_issues,
            property_order,
        ) {
            if branch_issues.is_empty() {
                matches.push(parsed);
                if !require_exactly_one {
                    break;
                }
            }
        }
    }
    if matches.len() == 1 || (!require_exactly_one && !matches.is_empty()) {
        matches.into_iter().next()
    } else {
        push_issue(path, "Invalid input", issues);
        None
    }
}

fn parse_object(
    schema: &RuntimeObject,
    input: &Json,
    path: &mut Vec<String>,
    issues: &mut Vec<ParseIssue>,
    property_order_resolver: &PropertyOrderResolver<'_>,
) -> Option<Json> {
    let Some(input) = input.as_object() else {
        let expected = if schema.contains_key("propertyNames") {
            "record"
        } else {
            "object"
        };
        push_type_issue(path, expected, input, issues);
        return None;
    };
    let issue_start = issues.len();
    let properties = schema
        .get("properties")
        .and_then(Json::as_object)
        .cloned()
        .unwrap_or_default();
    let required = schema
        .get("required")
        .and_then(Json::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Json::as_str)
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    // JSON maps in this crate do not retain declaration order. Tools whose Zod
    // declaration order differs from BTree order provide a path-aware sidecar;
    // required order remains the lossless fallback for ordinary schemas.
    let mut property_order = property_order_resolver(path)
        .into_iter()
        .flat_map(|keys| keys.iter().copied())
        .filter(|key| properties.contains_key(*key))
        .map(RuntimeString::from)
        .collect::<Vec<_>>();
    for key in &required {
        if !property_order.contains(key) {
            property_order.push(key.clone());
        }
    }
    for key in properties.keys() {
        if !property_order.contains(&key) {
            property_order.push(key.clone());
        }
    }

    let mut output = RuntimeObject::new();
    for key in property_order {
        let Some(property_schema) = properties.get(key.clone()) else {
            if required.iter().any(|required| required == &key) && !input.contains_key(key.clone())
            {
                path.push(schema_string(&key));
                push_issue(path, "Invalid input: received undefined", issues);
                path.pop();
            }
            continue;
        };
        let value = input.get(key.clone());
        let has_default = property_schema
            .as_object()
            .is_some_and(|schema| schema.contains_key("default"));
        if value.is_none() && !has_default && !required.iter().any(|required| required == &key) {
            continue;
        }
        path.push(schema_string(&key));
        if let Some(parsed) = parse_schema_value(
            property_schema,
            value,
            path,
            issues,
            property_order_resolver,
        ) {
            output.insert(key, parsed);
        }
        path.pop();
    }

    let additional = schema.get("additionalProperties");
    for (key, value) in input {
        if properties.contains_key(key.clone()) {
            continue;
        }
        match additional {
            Some(Json::Bool(true)) => {
                output.insert(key.clone(), value.clone());
            }
            Some(additional_schema @ Json::Object(_)) => {
                path.push(runtime_path_segment(&key));
                if let Some(parsed) = parse_schema_value(
                    additional_schema,
                    Some(value),
                    path,
                    issues,
                    property_order_resolver,
                ) {
                    output.insert(key.clone(), parsed);
                }
                path.pop();
            }
            None if !schema.contains_key("properties") => {
                // No declared properties is not a `z.object` at all: both Zod
                // (`z.record`/loose) and Draft 2020-12 read it as an object
                // whose keys the schema never enumerates. They belong to
                // whoever authored the payload, so they pass through intact.
                output.insert(key.clone(), value.clone());
            }
            _ => {
                // `z.object` strips unknown keys. Draft schemas emitted from it
                // carry `additionalProperties:false`; absence is treated the
                // same way for a hand-authored schema that does declare its
                // properties.
            }
        }
    }

    validate_collection_size(schema, input.len(), "object", path, issues);
    (issues.len() == issue_start).then_some(Json::Object(output))
}

fn parse_array(
    schema: &RuntimeObject,
    input: &Json,
    path: &mut Vec<String>,
    issues: &mut Vec<ParseIssue>,
    property_order: &PropertyOrderResolver<'_>,
) -> Option<Json> {
    let Some(input) = input.as_array() else {
        push_type_issue(path, "array", input, issues);
        return None;
    };
    let issue_start = issues.len();
    let mut output = Vec::with_capacity(input.len());

    if let Some(prefix) = schema.get("prefixItems").and_then(Json::as_array) {
        for (index, value) in input.iter().enumerate() {
            let item_schema = prefix.get(index).or_else(|| schema.get("items"));
            match item_schema {
                Some(Json::Bool(false)) => {
                    path.push(index.to_string());
                    push_issue(path, "Invalid input", issues);
                    path.pop();
                }
                Some(item_schema) => {
                    path.push(index.to_string());
                    if let Some(parsed) =
                        parse_schema_value(item_schema, Some(value), path, issues, property_order)
                    {
                        output.push(parsed);
                    }
                    path.pop();
                }
                None => output.push(value.clone()),
            }
        }
    } else if let Some(item_schema) = schema.get("items") {
        for (index, value) in input.iter().enumerate() {
            path.push(index.to_string());
            if let Some(parsed) =
                parse_schema_value(item_schema, Some(value), path, issues, property_order)
            {
                output.push(parsed);
            }
            path.pop();
        }
    } else {
        output.clone_from(input);
    }

    validate_collection_size(schema, input.len(), "array", path, issues);
    if schema.get("uniqueItems").and_then(Json::as_bool) == Some(true)
        && input
            .iter()
            .enumerate()
            .any(|(index, value)| input[..index].contains(value))
    {
        push_issue(
            path,
            "Invalid input: expected array items to be unique",
            issues,
        );
    }
    (issues.len() == issue_start).then_some(Json::Array(output))
}

fn parse_string(
    schema: &RuntimeObject,
    input: &Json,
    path: &[String],
    issues: &mut Vec<ParseIssue>,
) -> Option<Json> {
    let Some(value) = input.as_str() else {
        push_type_issue(path, "string", input, issues);
        return None;
    };
    let issue_start = issues.len();
    let length = value.len_utf16() as u64;
    if let Some(minimum) = schema.get("minLength").and_then(Json::as_u64) {
        if length < minimum {
            push_issue(
                path,
                format!("Too small: expected string to have >={minimum} characters"),
                issues,
            );
        }
    }
    if let Some(maximum) = schema.get("maxLength").and_then(Json::as_u64) {
        if length > maximum {
            push_issue(
                path,
                format!("Too big: expected string to have <={maximum} characters"),
                issues,
            );
        }
    }
    if let Some(pattern) = schema.get("pattern").and_then(Json::as_str) {
        let pattern = schema_string(pattern);
        let value = node_projected_string(value);
        if Regex::new(&pattern).is_ok_and(|pattern| !pattern.is_match(&value)) {
            push_issue(
                path,
                format!("Invalid string: must match pattern /{pattern}/"),
                issues,
            );
        }
    }
    (issues.len() == issue_start).then_some(input.clone())
}

fn parse_number(
    schema: &RuntimeObject,
    input: &Json,
    path: &[String],
    issues: &mut Vec<ParseIssue>,
    integer: bool,
) -> Option<Json> {
    let Some(number) = input.as_number() else {
        push_type_issue(path, "number", input, issues);
        return None;
    };
    let value = number.as_f64();
    if !value.is_finite() {
        push_issue(
            path,
            "Invalid input: expected number, received number",
            issues,
        );
        return None;
    }
    if integer && value.fract() != 0.0 {
        push_issue(path, "Invalid input: expected int, received number", issues);
        return None;
    }
    let issue_start = issues.len();
    if integer && value < -JS_MAX_SAFE_INTEGER {
        push_issue(
            path,
            "Too small: expected int to be >=-9007199254740991",
            issues,
        );
    } else if integer && value > JS_MAX_SAFE_INTEGER {
        push_issue(
            path,
            "Too big: expected int to be <=9007199254740991",
            issues,
        );
    }

    validate_numeric_bound(schema, "minimum", value, true, false, integer, path, issues);
    validate_numeric_bound(
        schema,
        "exclusiveMinimum",
        value,
        false,
        false,
        integer,
        path,
        issues,
    );
    validate_numeric_bound(schema, "maximum", value, true, true, integer, path, issues);
    validate_numeric_bound(
        schema,
        "exclusiveMaximum",
        value,
        false,
        true,
        integer,
        path,
        issues,
    );

    if let Some(multiple) = schema.get("multipleOf").and_then(Json::as_f64) {
        let quotient = value / multiple;
        if multiple == 0.0 || (quotient - quotient.round()).abs() > f64::EPSILON * 16.0 {
            push_issue(
                path,
                format!(
                    "Invalid number: must be a multiple of {}",
                    render_number(schema.get("multipleOf").unwrap())
                ),
                issues,
            );
        }
    }

    (issues.len() == issue_start).then_some(canonical_number(number))
}

#[allow(clippy::too_many_arguments)]
fn validate_numeric_bound(
    schema: &RuntimeObject,
    keyword: &str,
    value: f64,
    inclusive: bool,
    upper: bool,
    integer: bool,
    path: &[String],
    issues: &mut Vec<ParseIssue>,
) {
    let Some(bound_json) = schema.get(keyword) else {
        return;
    };
    let Some(bound) = bound_json.as_f64() else {
        return;
    };
    // z.number().int() contributes these safe-int bounds itself. Avoid
    // reporting them twice when the erased schema carries the same keywords.
    if integer
        && ((keyword == "minimum" && bound == -JS_MAX_SAFE_INTEGER)
            || (keyword == "maximum" && bound == JS_MAX_SAFE_INTEGER))
    {
        return;
    }
    let invalid = if upper {
        if inclusive {
            value > bound
        } else {
            value >= bound
        }
    } else if inclusive {
        value < bound
    } else {
        value <= bound
    };
    if !invalid {
        return;
    }
    let comparison = match (upper, inclusive) {
        (true, true) => "<=",
        (true, false) => "<",
        (false, true) => ">=",
        (false, false) => ">",
    };
    let direction = if upper { "Too big" } else { "Too small" };
    push_issue(
        path,
        format!(
            "{direction}: expected number to be {comparison}{}",
            render_number(bound_json)
        ),
        issues,
    );
}

fn validate_collection_size(
    schema: &RuntimeObject,
    length: usize,
    origin: &str,
    path: &[String],
    issues: &mut Vec<ParseIssue>,
) {
    let (minimum_key, maximum_key, unit) = if origin == "array" {
        ("minItems", "maxItems", "items")
    } else {
        ("minProperties", "maxProperties", "properties")
    };
    if let Some(minimum) = schema.get(minimum_key).and_then(Json::as_u64) {
        if length < minimum as usize {
            push_issue(
                path,
                format!("Too small: expected {origin} to have >={minimum} {unit}"),
                issues,
            );
        }
    }
    if let Some(maximum) = schema.get(maximum_key).and_then(Json::as_u64) {
        if length > maximum as usize {
            push_issue(
                path,
                format!("Too big: expected {origin} to have <={maximum} {unit}"),
                issues,
            );
        }
    }
}

fn push_missing_issue(schema: &RuntimeObject, path: &[String], issues: &mut Vec<ParseIssue>) {
    if schema.contains_key("anyOf") || schema.contains_key("oneOf") {
        push_issue(path, "Invalid input", issues);
        return;
    }
    if let Some(expected) = schema.get("const") {
        push_issue(
            path,
            format!("Invalid input: expected {}", render_json(expected)),
            issues,
        );
        return;
    }
    if let Some(values) = schema.get("enum").and_then(Json::as_array) {
        push_issue(
            path,
            format!(
                "Invalid option: expected one of {}",
                values.iter().map(render_json).collect::<Vec<_>>().join("|")
            ),
            issues,
        );
        return;
    }
    let expected = schema
        .get("type")
        .and_then(Json::as_str)
        .map(|value| {
            if value == "integer" {
                "number".to_owned()
            } else {
                schema_string(value)
            }
        })
        .unwrap_or_else(|| "value".to_owned());
    push_issue(
        path,
        format!("Invalid input: expected {expected}, received undefined"),
        issues,
    );
}

fn push_type_issue(path: &[String], expected: &str, received: &Json, issues: &mut Vec<ParseIssue>) {
    push_issue(
        path,
        format!(
            "Invalid input: expected {expected}, received {}",
            json_type(received)
        ),
        issues,
    );
}

fn push_issue(path: &[String], message: impl Into<String>, issues: &mut Vec<ParseIssue>) {
    issues.push(ParseIssue {
        path: path.to_vec(),
        message: message.into(),
    });
}

fn value_matches_type(value: &Json, expected: &RuntimeString) -> bool {
    if expected == "object" {
        value.is_object()
    } else if expected == "array" {
        value.is_array()
    } else if expected == "string" {
        value.is_string()
    } else if expected == "number" {
        value.is_number()
    } else if expected == "integer" {
        value
            .as_f64()
            .is_some_and(|value| value.fract() == 0.0 && value.abs() <= JS_MAX_SAFE_INTEGER)
    } else if expected == "boolean" {
        value.is_boolean()
    } else if expected == "null" {
        value.is_null()
    } else {
        true
    }
}

fn json_type(value: &Json) -> &'static str {
    match value {
        Json::Null => "null",
        Json::Bool(_) => "boolean",
        Json::Number(_) => "number",
        Json::String(_) => "string",
        Json::Array(_) => "array",
        Json::Object(_) => "object",
    }
}

fn render_json(value: &Json) -> String {
    runtime_json::stringify(value)
        .try_to_utf8()
        .expect("JSON writer escapes non-scalar runtime strings")
}

fn render_number(value: &Json) -> String {
    value
        .as_number()
        .map(|number| number.to_json_token())
        .unwrap_or_else(|| render_json(value))
}

fn canonical_number(number: &RuntimeNumber) -> Json {
    Json::Number(*number)
}

fn schema_string(value: &RuntimeString) -> String {
    value
        .try_to_utf8()
        .expect("validated static tool schema strings are Unicode scalar values")
}

fn node_projected_string(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8()).expect("Node UTF-8 projection is valid UTF-8")
}

fn runtime_path_segment(value: &RuntimeString) -> String {
    value
        .try_to_utf8()
        .unwrap_or_else(|_| node_projected_string(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime(source: &str) -> Json {
        runtime_json::parse(&RuntimeString::from(source)).expect("valid exact JSON fixture")
    }

    #[test]
    fn render_json_preserves_runtime_lone_surrogate_without_marker_leak() {
        let surrogate = runtime_json::parse(&RuntimeString::from(r#""\ud800""#)).unwrap();
        assert_eq!(render_json(&surrogate), r#""\ud800""#);
        assert!(!render_json(&surrogate).contains('\u{fdd0}'));
    }

    #[test]
    fn string_length_uses_javascript_utf16_units_for_surrogates_and_literal_markers() {
        let max_one = runtime(r#"{"type":"string","maxLength":1}"#);
        let surrogate = runtime_json::parse(&RuntimeString::from(r#""\ud800""#)).unwrap();
        assert!(parse_native_input(&max_one, &surrogate, &|_| None).is_ok());

        let literal_marker = runtime_json::parse(&RuntimeString::from(r#""\ufdd0ud800""#)).unwrap();
        let max_five = runtime(r#"{"type":"string","maxLength":5}"#);
        assert!(parse_native_input(&max_five, &literal_marker, &|_| None).is_err());
        let max_six = runtime(r#"{"type":"string","maxLength":6}"#);
        assert!(parse_native_input(&max_six, &literal_marker, &|_| None).is_ok());
    }
}
