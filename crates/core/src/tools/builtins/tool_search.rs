//! The `tool_search` tool: surfaces deferred tools by keyword so a large tool
//! set (e.g. many MCP tools) needn't all ship in the initial prompt. Matched
//! tools become callable next turn via the context's `surface_tools` callback.
//!
//! Also home to [`with_deferred_tools`], which marks tools beyond a threshold as
//! deferred and prepends `tool_search`, so a host with many tools keeps the
//! initial prompt small.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::runtime_values::{
    runtime_codec, RuntimeJson, RuntimeNumber, RuntimeObject, RuntimeString,
};
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::permission::PermissionResult;
use crate::types::provider::ToolSpecKind;
use crate::types::tool::{
    PathTargets, PermissionPathKind, Tool, ToolCatalogEntry, ToolContext, ToolExposure,
    ToolInputKind, ToolProgress, ToolResult, ValidationResult,
};

/// Default number of results `tool_search` returns.
pub(crate) const DEFAULT_MAX_RESULTS: usize = 10;
/// Default tool-count threshold above which tools are deferred.
const DEFAULT_THRESHOLD: usize = 30;
const JS_MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

/// Build the tool's closed input JSON Schema.
fn build_input_schema(default_max_results: usize) -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Use select:<tool_name>[,<tool_name>] to load exact deferred tools, or keywords describing the capability you need."
            },
            "limit": {
                "type": "integer",
                "exclusiveMinimum": 0,
                "maximum": JS_MAX_SAFE_INTEGER,
                "description": format!(
                    "Maximum number of tools to return. Defaults to {}.",
                    default_max_results
                )
            },
            "max_results": {
                "type": "integer",
                "exclusiveMinimum": 0,
                "maximum": JS_MAX_SAFE_INTEGER,
                "description": format!(
                    "Alias for limit. Defaults to {}.",
                    default_max_results
                )
            }
        },
        "required": ["query"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap()
}

/// A tool that searches the deferred-tool catalog by capability keywords.
pub struct ToolSearchTool {
    input_schema: JsonSchema,
    max_results: usize,
}

impl ToolSearchTool {
    fn new(max_results: usize) -> Self {
        ToolSearchTool {
            input_schema: build_input_schema(max_results),
            max_results,
        }
    }
}

/// Construct the `tool_search` tool as an erased registry-side [`Tool`].
/// `max_results` defaults to 10 when `None`.
pub fn create_tool_search_tool(max_results: Option<usize>) -> Arc<dyn Tool> {
    Arc::new(ToolSearchTool::new(
        max_results.unwrap_or(DEFAULT_MAX_RESULTS),
    ))
}

/// Keep deferred tools reachable whenever a runtime surface is assembled or
/// refreshed. Existing search tools win so host-specific variants are retained.
pub(crate) fn ensure_tool_search(tools: Vec<Arc<dyn Tool>>) -> Vec<Arc<dyn Tool>> {
    let has_deferred = tools
        .iter()
        .any(|tool| tool.exposure() == ToolExposure::Deferred);
    let has_search = tools.iter().any(|tool| tool.name() == "tool_search");
    if !has_deferred || has_search {
        return tools;
    }

    let mut with_search = Vec::with_capacity(tools.len() + 1);
    with_search.push(create_tool_search_tool(None));
    with_search.extend(tools);
    with_search
}

fn json_input_type(value: &Json) -> &'static str {
    match value {
        Json::Null => "null",
        Json::Bool(_) => "boolean",
        Json::Number(_) => "number",
        Json::String(_) => "string",
        Json::Array(_) => "array",
        Json::Object(_) => "object",
    }
}

fn parse_zod_positive_integer(key: &str, value: &Json, issues: &mut Vec<String>) -> Option<u64> {
    let Json::Number(number) = value else {
        issues.push(format!(
            "{key}: Invalid input: expected number, received {}",
            json_input_type(value)
        ));
        return None;
    };

    let value = number.as_f64();
    if value.fract() != 0.0 {
        issues.push(format!(
            "{key}: Invalid input: expected int, received number"
        ));
    } else if value < -(JS_MAX_SAFE_INTEGER as f64) {
        issues.push(format!(
            "{key}: Too small: expected int to be >=-{}",
            JS_MAX_SAFE_INTEGER
        ));
        issues.push(format!("{key}: Too small: expected number to be >0"));
    } else if value <= 0.0 {
        issues.push(format!("{key}: Too small: expected number to be >0"));
    } else if value > JS_MAX_SAFE_INTEGER as f64 {
        issues.push(format!(
            "{key}: Too big: expected int to be <={JS_MAX_SAFE_INTEGER}"
        ));
    } else {
        return Some(value as u64);
    }
    None
}

fn parse_tool_search_input(input: &Json) -> Result<Json, ToolError> {
    let object = input.as_object().ok_or_else(|| {
        ToolError::new(format!(
            "Invalid arguments: (root): Invalid input: expected object, received {}",
            json_input_type(input)
        ))
    })?;

    let mut parsed = RuntimeObject::new();
    let mut issues = Vec::new();
    match object.get("query") {
        Some(value @ Json::String(_)) => {
            parsed.insert("query", value.clone());
        }
        Some(value) => {
            issues.push(format!(
                "Invalid arguments: query: Invalid input: expected string, received {}",
                json_input_type(value)
            ));
        }
        None => {
            issues.push(
                "Invalid arguments: query: Invalid input: expected string, received undefined"
                    .to_string(),
            );
        }
    }

    for key in ["limit", "max_results"] {
        if let Some(value) = object.get(key) {
            if let Some(value) = parse_zod_positive_integer(key, value, &mut issues) {
                parsed.insert(key, Json::Number(RuntimeNumber::from(value)));
            }
        }
    }

    if issues.is_empty() {
        Ok(Json::Object(parsed))
    } else {
        let issues = issues
            .into_iter()
            .map(|issue| {
                issue
                    .strip_prefix("Invalid arguments: ")
                    .unwrap_or(&issue)
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join("; ");
        Err(ToolError::new(format!("Invalid arguments: {issues}")))
    }
}

fn js_number(value: &Json) -> Result<f64, ToolError> {
    if js_string_conversion_throws(value) {
        return Err(ToolError::new("Cannot convert object to primitive value"));
    }
    Ok(match value {
        Json::Number(number) => number.as_f64(),
        Json::String(value) => parse_js_number_string(value),
        Json::Bool(true) => 1.0,
        Json::Bool(false) | Json::Null => 0.0,
        Json::Array(values) => parse_js_number_string(&js_array_to_string(values)),
        Json::Object(_) => f64::NAN,
    })
}

fn js_string_conversion_throws(value: &Json) -> bool {
    match value {
        // JSON objects inherit callable `valueOf`/`toString` methods. An own
        // non-callable `toString` shadows the latter, so ToPrimitive exhausts
        // both candidates and throws. Array#join recursively stringifies every
        // element before Number sees the joined string.
        Json::Object(value) => value.contains_key("toString"),
        Json::Array(values) => values.iter().any(js_string_conversion_throws),
        _ => false,
    }
}

fn number_to_string(value: f64) -> String {
    if value == 0.0 {
        return "0".to_owned();
    }
    if value.is_nan() {
        return "NaN".to_owned();
    }
    if value == f64::INFINITY {
        return "Infinity".to_owned();
    }
    if value == f64::NEG_INFINITY {
        return "-Infinity".to_owned();
    }
    ryu_js::Buffer::new().format_finite(value).to_owned()
}

fn is_ecmascript_whitespace_unit(value: u16) -> bool {
    matches!(
        value,
        0x0009..=0x000d
            | 0x0020
            | 0x00a0
            | 0x1680
            | 0x2000..=0x200a
            | 0x2028
            | 0x2029
            | 0x202f
            | 0x205f
            | 0x3000
            | 0xfeff
    )
}

fn trim_ecmascript_whitespace(value: &RuntimeString) -> RuntimeString {
    let units = value.as_utf16();
    let start = units
        .iter()
        .position(|unit| !is_ecmascript_whitespace_unit(*unit))
        .unwrap_or(units.len());
    let end = units
        .iter()
        .rposition(|unit| !is_ecmascript_whitespace_unit(*unit))
        .map_or(start, |index| index + 1);
    value.slice_utf16(start..end)
}

fn scalar_ascii_string(value: &RuntimeString) -> Option<String> {
    // JavaScript Number string grammar is ASCII-only. Reject every other exact
    // code unit before this closed numeric parsing boundary creates a Rust str.
    value
        .as_utf16()
        .iter()
        .copied()
        .map(|unit| (unit <= 0x7f).then_some(char::from(unit as u8)))
        .collect()
}

fn parse_js_number_string(value: &RuntimeString) -> f64 {
    let value = trim_ecmascript_whitespace(value);
    if value.is_empty() {
        return 0.0;
    }
    let Some(value) = scalar_ascii_string(&value) else {
        return f64::NAN;
    };
    let value = value.as_str();
    match value {
        "Infinity" | "+Infinity" => return f64::INFINITY,
        "-Infinity" => return f64::NEG_INFINITY,
        _ => {}
    }
    for (prefixes, radix) in [(["0x", "0X"], 16), (["0b", "0B"], 2), (["0o", "0O"], 8)] {
        if prefixes.iter().any(|prefix| value.starts_with(prefix)) {
            return parse_js_radix_integer(&value[2..], radix).unwrap_or(f64::NAN);
        }
    }
    if !is_js_decimal_literal(value) {
        return f64::NAN;
    }
    value.parse::<f64>().unwrap_or(f64::NAN)
}

fn is_ecmascript_line_terminator_unit(value: u16) -> bool {
    matches!(value, 0x000a | 0x000d | 0x2028 | 0x2029)
}

fn parse_js_radix_integer(digits: &str, radix: u32) -> Option<f64> {
    if digits.is_empty() {
        return None;
    }
    digits.chars().try_fold(0.0, |value, digit| {
        digit
            .to_digit(radix)
            .map(|digit| value * f64::from(radix) + f64::from(digit))
    })
}

fn is_js_decimal_literal(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut index = usize::from(matches!(bytes.first(), Some(b'+') | Some(b'-')));
    let integer_start = index;
    while bytes.get(index).is_some_and(u8::is_ascii_digit) {
        index += 1;
    }
    let integer_digits = index - integer_start;
    let mut fraction_digits = 0;
    if bytes.get(index) == Some(&b'.') {
        index += 1;
        let fraction_start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        fraction_digits = index - fraction_start;
    }
    if integer_digits == 0 && fraction_digits == 0 {
        return false;
    }
    if matches!(bytes.get(index), Some(b'e') | Some(b'E')) {
        index += 1;
        if matches!(bytes.get(index), Some(b'+') | Some(b'-')) {
            index += 1;
        }
        let exponent_start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if index == exponent_start {
            return false;
        }
    }
    index == bytes.len()
}

fn join_runtime(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let separator = RuntimeString::from(separator);
    let mut output = RuntimeString::default();
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output = output.concat(&separator);
        }
        output = output.concat(value);
    }
    output
}

fn js_array_to_string(values: &[Json]) -> RuntimeString {
    let values = values
        .iter()
        .map(|value| match value {
            Json::Null => RuntimeString::default(),
            Json::Bool(value) => RuntimeString::from(value.to_string()),
            Json::Number(value) => RuntimeString::from(number_to_string(value.as_f64())),
            Json::String(value) => value.clone(),
            Json::Array(values) => js_array_to_string(values),
            Json::Object(_) => RuntimeString::from("[object Object]"),
        })
        .collect::<Vec<_>>();
    join_runtime(&values, ",")
}

fn effective_max_results(input: &Json, default_max_results: usize) -> Result<f64, ToolError> {
    match ["limit", "max_results"]
        .into_iter()
        .filter_map(|key| input.get(key))
        .find(|value| !value.is_null())
    {
        Some(value) => js_number(value),
        None => Ok(default_max_results as f64),
    }
}

fn js_slice_end(len: usize, end: f64) -> usize {
    if end.is_nan() || end == f64::NEG_INFINITY {
        return 0;
    }
    if end == f64::INFINITY {
        return len;
    }
    let end = end.trunc();
    if end < 0.0 {
        ((len as f64 + end).max(0.0).min(len as f64)) as usize
    } else {
        end.min(len as f64) as usize
    }
}

fn starts_with_ascii_case_insensitive(value: &RuntimeString, prefix: &str) -> bool {
    let prefix = prefix.as_bytes();
    value.as_utf16().len() >= prefix.len()
        && value.as_utf16()[..prefix.len()]
            .iter()
            .zip(prefix)
            .all(|(unit, expected)| {
                u8::try_from(*unit).is_ok_and(|unit| unit.eq_ignore_ascii_case(expected))
            })
}

fn parse_select_query(query: &RuntimeString) -> Option<Vec<RuntimeString>> {
    const PREFIX: &str = "select:";
    if !starts_with_ascii_case_insensitive(query, PREFIX) {
        return None;
    }
    let body = query.slice_utf16(PREFIX.len()..);
    // The fixed expression is /^select:(.+)$/i without multiline mode. Its
    // dot cannot consume any ECMAScript line terminator, including a terminal
    // one, so the anchored match fails as soon as the body contains one.
    if body.is_empty()
        || body
            .as_utf16()
            .iter()
            .copied()
            .any(is_ecmascript_line_terminator_unit)
    {
        return None;
    }
    let mut names = Vec::new();
    let mut start = 0;
    for index in body
        .as_utf16()
        .iter()
        .enumerate()
        .filter_map(|(index, unit)| (*unit == u16::from(b',')).then_some(index))
        .chain(std::iter::once(body.len_utf16()))
    {
        names.push(trim_ecmascript_whitespace(&body.slice_utf16(start..index)));
        start = index.saturating_add(1);
    }
    let names = names
        .into_iter()
        .filter(|name| !name.is_empty())
        .collect::<Vec<_>>();
    (!names.is_empty()).then_some(names)
}

fn lowercase_runtime(value: &RuntimeString) -> RuntimeString {
    let mut output = Vec::with_capacity(value.len_utf16());
    let mut scalar_run = String::new();
    for point in value.code_points() {
        let Some(character) = char::from_u32(point) else {
            output.extend(scalar_run.to_lowercase().encode_utf16());
            scalar_run.clear();
            output.push(point as u16);
            continue;
        };
        scalar_run.push(character);
    }
    output.extend(scalar_run.to_lowercase().encode_utf16());
    RuntimeString::from_utf16(output)
}

fn contains_runtime(haystack: &RuntimeString, needle: &RuntimeString) -> bool {
    needle.is_empty()
        || haystack
            .as_utf16()
            .windows(needle.len_utf16())
            .any(|window| window == needle.as_utf16())
}

fn parse_keywords(query: &RuntimeString) -> (Vec<RuntimeString>, Vec<RuntimeString>) {
    let query = lowercase_runtime(query);
    let mut tokens = Vec::new();
    let mut start = None;
    for (index, unit) in query.as_utf16().iter().copied().enumerate() {
        if is_ecmascript_whitespace_unit(unit) {
            if let Some(token_start) = start.take() {
                tokens.push(query.slice_utf16(token_start..index));
            }
        } else if start.is_none() {
            start = Some(index);
        }
    }
    if let Some(token_start) = start {
        tokens.push(query.slice_utf16(token_start..));
    }

    tokens
        .into_iter()
        .fold((Vec::new(), Vec::new()), |mut acc, token| {
            if token.as_utf16().first() == Some(&u16::from(b'+')) && token.len_utf16() > 1 {
                acc.0.push(token.slice_utf16(1..));
            } else {
                acc.1.push(token);
            }
            acc
        })
}

fn find_catalog_tool<'a>(
    catalog: &'a [ToolCatalogEntry],
    name: &RuntimeString,
) -> Option<&'a ToolCatalogEntry> {
    let needle = lowercase_runtime(&trim_ecmascript_whitespace(name));
    if needle.is_empty() {
        return None;
    }
    catalog
        .iter()
        .find(|tool| lowercase_runtime(&tool.name) == needle)
}

/// Score a catalog entry: reject any missing required term, then add one point
/// for every required/optional token found in name, description, or search hint.
fn score(tool: &ToolCatalogEntry, required: &[RuntimeString], optional: &[RuntimeString]) -> i64 {
    let separator = RuntimeString::from(" ");
    let hint = tool.search_hint.clone().unwrap_or_default();
    let haystack = lowercase_runtime(
        &tool
            .name
            .concat(&separator)
            .concat(&tool.description)
            .concat(&separator)
            .concat(&hint),
    );
    if required
        .iter()
        .any(|token| !contains_runtime(&haystack, token))
    {
        return 0;
    }
    let mut s = 0i64;
    for tok in required.iter().chain(optional) {
        if contains_runtime(&haystack, tok) {
            s += 1;
        }
    }
    s
}

fn to_search_output_tool(tool: &ToolCatalogEntry) -> Json {
    let mut output = RuntimeObject::new();
    if let Some(spec) = &tool.spec {
        output.insert("name", RuntimeJson::String(spec.name.clone()));
        output.insert("description", RuntimeJson::String(spec.description.clone()));
        output.insert("inputSchema", spec.input_schema.clone());
        output.insert("deferLoading", RuntimeJson::Bool(true));
        if spec.kind != ToolSpecKind::Function {
            output.insert(
                "kind",
                runtime_codec::encode_value(&spec.kind)
                    .expect("ToolSpecKind exact codec is infallible"),
            );
        }
        if let Some(format) = &spec.format {
            output.insert(
                "format",
                runtime_codec::encode_value(format)
                    .expect("ToolSpecFormat exact codec is infallible"),
            );
        }
    } else {
        output.insert("name", RuntimeJson::String(tool.name.clone()));
        output.insert("description", RuntimeJson::String(tool.description.clone()));
        output.insert(
            "inputSchema",
            crate::types::provider::open_fallback_tool_schema(),
        );
        output.insert("deferLoading", RuntimeJson::Bool(true));
    }
    RuntimeJson::Object(output)
}

fn format_selection_result(
    deferred: &[&ToolCatalogEntry],
    already_available: &[RuntimeString],
    missing: &[RuntimeString],
) -> RuntimeString {
    let mut lines = Vec::new();
    if !deferred.is_empty() {
        lines.push(RuntimeString::from("These tools are now available:"));
        lines.extend(deferred.iter().map(|tool| {
            RuntimeString::from("- ")
                .concat(&tool.name)
                .concat(&RuntimeString::from(": "))
                .concat(&tool.description)
        }));
    }
    if !already_available.is_empty() {
        lines.push(RuntimeString::from("These tools were already available:"));
        lines.extend(
            already_available
                .iter()
                .map(|name| RuntimeString::from("- ").concat(name)),
        );
    }
    if !missing.is_empty() {
        lines.push(RuntimeString::from("No matching tools found for:"));
        lines.extend(
            missing
                .iter()
                .map(|name| RuntimeString::from("- ").concat(name)),
        );
    }
    if lines.is_empty() {
        RuntimeString::from("No matching tools found.")
    } else {
        join_runtime(&lines, "\n")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ToolSearchResolution {
    pub output: Json,
    pub text: RuntimeString,
    pub surface_callback: Option<Vec<RuntimeString>>,
}

const ATOMIC_SURFACE_GROUPS: &[&[&str]] = &[&["exec_command", "write_stdin"]];

fn expand_atomic_surface_groups<'a>(
    selected: Vec<&'a ToolCatalogEntry>,
    catalog: &'a [ToolCatalogEntry],
) -> Vec<&'a ToolCatalogEntry> {
    let mut expanded = selected;
    let selected_names = expanded
        .iter()
        .map(|tool| tool.name.clone())
        .collect::<Vec<_>>();
    for group in ATOMIC_SURFACE_GROUPS {
        if !group.iter().any(|name| {
            let name = RuntimeString::from(*name);
            selected_names.iter().any(|selected| selected == &name)
        }) {
            continue;
        }
        for companion in *group {
            let companion = RuntimeString::from(*companion);
            let Some(tool) = find_catalog_tool(catalog, &companion) else {
                continue;
            };
            if tool.exposure == ToolExposure::Deferred
                && !expanded.iter().any(|entry| entry.name == tool.name)
            {
                expanded.push(tool);
            }
        }
    }
    expanded
}

fn select_tools(
    requested: &[RuntimeString],
    catalog: &[ToolCatalogEntry],
    max_results: f64,
) -> ToolSearchResolution {
    let mut deferred = Vec::new();
    let mut already_available = Vec::new();
    let mut missing = Vec::new();
    for requested_name in requested {
        let Some(tool) = find_catalog_tool(catalog, requested_name) else {
            missing.push(requested_name.clone());
            continue;
        };
        if tool.exposure == ToolExposure::Deferred {
            if !deferred
                .iter()
                .any(|entry: &&ToolCatalogEntry| entry.name == tool.name)
            {
                deferred.push(tool);
            }
        } else if !already_available.contains(&tool.name) {
            already_available.push(tool.name.clone());
        }
    }

    deferred.truncate(js_slice_end(deferred.len(), max_results));
    let deferred = expand_atomic_surface_groups(deferred, catalog);
    let matched = deferred
        .iter()
        .map(|tool| tool.name.clone())
        .collect::<Vec<_>>();
    let tools = deferred
        .iter()
        .map(|tool| to_search_output_tool(tool))
        .collect::<Vec<_>>();
    let text = format_selection_result(&deferred, &already_available, &missing);
    let mut output_object = RuntimeObject::new();
    output_object.insert(
        "matched",
        RuntimeJson::Array(matched.iter().cloned().map(RuntimeJson::String).collect()),
    );
    output_object.insert("tools", RuntimeJson::Array(tools));
    if !already_available.is_empty() {
        output_object.insert(
            "alreadyAvailable",
            RuntimeJson::Array(
                already_available
                    .iter()
                    .cloned()
                    .map(RuntimeJson::String)
                    .collect(),
            ),
        );
    }
    if !missing.is_empty() {
        output_object.insert(
            "missing",
            RuntimeJson::Array(missing.iter().cloned().map(RuntimeJson::String).collect()),
        );
    }
    ToolSearchResolution {
        surface_callback: (!matched.is_empty()).then_some(matched),
        output: RuntimeJson::Object(output_object),
        text,
    }
}

pub(crate) fn resolve_tool_search(
    input: &Json,
    catalog: &[ToolCatalogEntry],
    default_max_results: usize,
) -> Result<ToolSearchResolution, ToolError> {
    if input.is_null() {
        return Err(ToolError::new(
            "Cannot read properties of null (reading 'limit')",
        ));
    }
    let query = match input.as_object().and_then(|input| input.get("query")) {
        Some(Json::String(query)) => query,
        Some(Json::Null) => {
            return Err(ToolError::new(
                "Cannot read properties of null (reading 'match')",
            ));
        }
        Some(_) => return Err(ToolError::new("query.match is not a function")),
        None => {
            return Err(ToolError::new(
                "Cannot read properties of undefined (reading 'match')",
            ));
        }
    };
    let max_results = effective_max_results(input, default_max_results)?;
    let catalog = catalog
        .iter()
        .filter(|tool| tool.exposure != ToolExposure::Hidden)
        .cloned()
        .collect::<Vec<_>>();

    if let Some(selected) = parse_select_query(query) {
        return Ok(select_tools(&selected, &catalog, max_results));
    }
    if let Some(exact) = find_catalog_tool(&catalog, query) {
        return Ok(select_tools(
            std::slice::from_ref(&exact.name),
            &catalog,
            max_results,
        ));
    }

    let (required, optional) = parse_keywords(query);
    let mut scored: Vec<(&ToolCatalogEntry, i64)> = catalog
        .iter()
        .filter(|tool| tool.exposure == ToolExposure::Deferred)
        .map(|tool| {
            let score = score(tool, &required, &optional);
            (tool, score)
        })
        .filter(|(_, score)| *score > 0)
        .collect();
    scored.sort_by(|a, b| b.1.cmp(&a.1));
    scored.truncate(js_slice_end(scored.len(), max_results));
    let selected =
        expand_atomic_surface_groups(scored.iter().map(|(tool, _)| *tool).collect(), &catalog);

    let matched = selected
        .iter()
        .map(|tool| tool.name.clone())
        .collect::<Vec<_>>();
    let tools = selected
        .iter()
        .map(|tool| to_search_output_tool(tool))
        .collect::<Vec<_>>();
    let text = if matched.is_empty() {
        RuntimeString::from("No matching tools found.")
    } else {
        let mut lines = vec![RuntimeString::from("These tools are now available:")];
        lines.extend(selected.iter().map(|tool| {
            RuntimeString::from("- ")
                .concat(&tool.name)
                .concat(&RuntimeString::from(": "))
                .concat(&tool.description)
        }));
        join_runtime(&lines, "\n")
    };
    let mut output = RuntimeObject::new();
    output.insert(
        "matched",
        RuntimeJson::Array(matched.iter().cloned().map(RuntimeJson::String).collect()),
    );
    output.insert("tools", RuntimeJson::Array(tools));
    Ok(ToolSearchResolution {
        surface_callback: Some(matched.clone()),
        output: RuntimeJson::Object(output),
        text,
    })
}

#[async_trait]
impl Tool for ToolSearchTool {
    fn static_name(&self) -> &str {
        "tool_search"
    }

    fn static_description(&self) -> &str {
        "Search for additional (deferred) tools by capability keywords, or load exact deferred tools with select:<tool_name>."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    fn parse_input(&self, input: &Json) -> Result<Json, ToolError> {
        parse_tool_search_input(input)
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let (tool_catalog, surface_tools) = match (&ctx.tool_catalog, &ctx.surface_tools) {
            (Some(tc), Some(st)) => (tc, st),
            _ => {
                let mut output = RuntimeObject::new();
                output.insert("matched", RuntimeJson::Array(Vec::new()));
                output.insert("tools", RuntimeJson::Array(Vec::new()));
                return Ok(text_result(
                    RuntimeJson::Object(output),
                    RuntimeString::from("Tool search is not available in this context."),
                    true,
                ));
            }
        };

        let catalog = tool_catalog()
            .into_iter()
            .filter(|tool| tool.exposure != ToolExposure::Hidden)
            .collect::<Vec<_>>();
        let resolution = resolve_tool_search(&input, &catalog, self.max_results)?;
        if let Some(names) = &resolution.surface_callback {
            surface_tools(names.clone()).await?;
        }
        Ok(text_result(resolution.output, resolution.text, false))
    }
}

/// A tool wrapper that overrides ONLY its `exposure` to `deferred`, forwarding
/// every other behavior to the wrapped tool.
struct DeferredTool {
    inner: Arc<dyn Tool>,
}

#[async_trait]
impl Tool for DeferredTool {
    fn native_mcp_binding(&self) -> Option<crate::mcp::tool_wrapper::NativeMcpToolBinding> {
        self.inner.native_mcp_binding()
    }
    fn rebind_native_mcp(&self, replacement: Arc<dyn Tool>) -> Arc<dyn Tool> {
        Arc::new(Self {
            inner: self.inner.rebind_native_mcp(replacement),
        })
    }
    fn approval_metadata(&self) -> Option<crate::types::permission::ApprovalActionMetadata> {
        self.inner.approval_metadata()
    }

    fn persistence_identity(&self) -> Option<RuntimeString> {
        self.inner.persistence_identity()
    }

    fn name(&self) -> RuntimeString {
        self.inner.name()
    }
    fn aliases(&self) -> Vec<RuntimeString> {
        self.inner.aliases()
    }
    fn description(&self) -> RuntimeString {
        self.inner.description()
    }
    fn input_schema(&self) -> Json {
        self.inner.input_schema()
    }
    fn input_kind(&self) -> ToolInputKind {
        self.inner.input_kind()
    }
    fn output_schema(&self) -> Option<Json> {
        self.inner.output_schema()
    }
    fn exposure(&self) -> ToolExposure {
        ToolExposure::Deferred
    }
    fn search_hint(&self) -> Option<RuntimeString> {
        self.inner.search_hint()
    }
    fn max_result_size_chars(&self) -> Option<u64> {
        self.inner.max_result_size_chars()
    }
    fn mcp_server(&self) -> Option<RuntimeString> {
        self.inner.mcp_server()
    }
    fn task_scope_host_capability(&self) -> bool {
        self.inner.task_scope_host_capability()
    }
    fn is_enabled(&self) -> bool {
        self.inner.is_enabled()
    }
    fn is_concurrency_safe(&self, input: &Json) -> bool {
        self.inner.is_concurrency_safe(input)
    }
    fn is_read_only(&self, input: &Json) -> bool {
        self.inner.is_read_only(input)
    }
    fn is_goal_state_control(&self) -> bool {
        self.inner.is_goal_state_control()
    }
    fn planning_capability(&self, input: &Json) -> crate::types::tool::PlanningToolCapability {
        self.inner.planning_capability(input)
    }
    fn is_destructive(&self, input: &Json) -> Option<bool> {
        self.inner.is_destructive(input)
    }
    fn input_schema_property_order(
        &self,
        object_path: &[String],
    ) -> Option<&'static [&'static str]> {
        self.inner.input_schema_property_order(object_path)
    }
    fn preprocess_input(&self, input: &Json) -> Json {
        self.inner.preprocess_input(input)
    }
    fn parse_input(&self, input: &Json) -> Result<Json, ToolError> {
        self.inner.parse_input(input)
    }
    async fn validate_input(&self, input: &Json, ctx: &ToolContext) -> Option<ValidationResult> {
        self.inner.validate_input(input, ctx).await
    }
    async fn check_permissions(&self, input: &Json, ctx: &ToolContext) -> Option<PermissionResult> {
        self.inner.check_permissions(input, ctx).await
    }
    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.inner.call(input, ctx, on_progress).await
    }
    async fn call_with_id(
        &self,
        call_id: &RuntimeString,
        input: Json,
        ctx: &ToolContext,
        on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.inner
            .call_with_id(call_id, input, ctx, on_progress)
            .await
    }
    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        self.inner.summarize(input)
    }
    fn to_command_string(&self, input: &Json) -> Option<RuntimeString> {
        self.inner.to_command_string(input)
    }
    fn get_path(&self, input: &Json) -> Option<PathTargets> {
        self.inner.get_path(input)
    }
    fn get_raw_path(&self, input: &Json) -> Option<Json> {
        self.inner.get_raw_path(input)
    }
    fn permission_path_kind(&self) -> Option<PermissionPathKind> {
        self.inner.permission_path_kind()
    }
}

/// Mark the tools beyond `threshold` as deferred and prepend `tool_search`, so a
/// host with many tools keeps the initial prompt small. Tools already carrying a
/// non-default exposure (deferred/hidden) are left as-is. `threshold` defaults to
/// 30 when `None`.
pub fn with_deferred_tools(
    tools: Vec<Arc<dyn Tool>>,
    threshold: Option<usize>,
) -> Vec<Arc<dyn Tool>> {
    let threshold = threshold.unwrap_or(DEFAULT_THRESHOLD);
    if tools.len() <= threshold {
        return tools;
    }
    let mut result: Vec<Arc<dyn Tool>> = Vec::with_capacity(tools.len() + 1);
    result.push(create_tool_search_tool(None));
    for (i, t) in tools.into_iter().enumerate() {
        if i >= threshold && t.exposure() == ToolExposure::Direct {
            result.push(Arc::new(DeferredTool { inner: t }));
        } else {
            result.push(t);
        }
    }
    result
}

/// Keep a large runtime inventory discoverable when its declarations cannot fit
/// the selected model. Reuse the existing exposure-only wrapper so permissions,
/// implementation identity and tool execution remain owned by the original tool.
pub(crate) fn defer_direct_inventory(tools: Vec<Arc<dyn Tool>>) -> Vec<Arc<dyn Tool>> {
    ensure_tool_search(
        tools
            .into_iter()
            .map(|tool| {
                if tool.name() != "tool_search" && tool.exposure() == ToolExposure::Direct {
                    Arc::new(DeferredTool { inner: tool }) as Arc<dyn Tool>
                } else {
                    tool
                }
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::{RuntimePath, RuntimeString};
    use crate::schemas::content::OutputBlock;
    use crate::types::abort::AbortSignal;
    use crate::types::errors::ToolErrorKind;
    use crate::types::file_state::{FileStateCache, FileStateEntry, RecordOptions};
    use crate::types::permission::PermissionContext;
    use crate::types::provider::{ToolSpec, ToolSpecFormat, ToolSpecGrammarSyntax, ToolSpecKind};
    use crate::types::sandbox::{
        ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
    };
    use crate::types::tool::{NamesFn, ToolCatalogFn};
    use serde_json::{Map, Value};
    use std::sync::Mutex;

    fn runtime_from_serde(value: Value) -> RuntimeJson {
        match value {
            Value::Null => RuntimeJson::Null,
            Value::Bool(value) => RuntimeJson::Bool(value),
            Value::Number(value) => RuntimeJson::Number(RuntimeNumber::from_f64(
                value
                    .to_string()
                    .parse::<f64>()
                    .expect("serde JSON number has JavaScript Number projection"),
            )),
            Value::String(value) => RuntimeJson::String(RuntimeString::from(value)),
            Value::Array(values) => {
                RuntimeJson::Array(values.into_iter().map(runtime_from_serde).collect())
            }
            Value::Object(values) => {
                let mut object = RuntimeObject::new();
                for (key, value) in values {
                    object.insert(key, runtime_from_serde(value));
                }
                RuntimeJson::Object(object)
            }
        }
    }

    macro_rules! rjson {
        ($($json:tt)+) => {
            runtime_from_serde(serde_json::json!($($json)+))
        };
    }

    fn runtime_object(
        fields: impl IntoIterator<Item = (&'static str, RuntimeJson)>,
    ) -> RuntimeJson {
        let mut object = RuntimeObject::new();
        for (key, value) in fields {
            object.insert(key, value);
        }
        RuntimeJson::Object(object)
    }

    fn rs(value: &str) -> RuntimeString {
        RuntimeString::from(value)
    }

    struct NoopFileState;
    impl FileStateCache for NoopFileState {
        fn get(&self, _path: &RuntimePath) -> Option<FileStateEntry> {
            None
        }
        fn record(&self, _path: &RuntimePath, _mtime_ms: f64, _opts: RecordOptions) {}
        fn has(&self, _path: &RuntimePath) -> bool {
            false
        }
        fn entries(&self) -> Vec<(RuntimePath, FileStateEntry)> {
            Vec::new()
        }
        fn delete(&self, _path: &RuntimePath) {}
        fn clear(&self) {}
        fn clone_box(&self) -> Box<dyn FileStateCache> {
            Box::new(NoopFileState)
        }
    }

    struct NoopSandbox;
    impl SandboxBackend for NoopSandbox {
        fn kind(&self) -> SandboxKind {
            SandboxKind::Process
        }
        fn is_available(&self) -> bool {
            false
        }
        fn transform(&self, req: &ExecRequest, _policy: &SandboxPolicy) -> SpawnSpec {
            SpawnSpec {
                file: req.argv.first().cloned().unwrap_or_default(),
                args: req.argv.iter().skip(1).cloned().collect(),
                cwd: req.cwd.clone(),
                env: crate::runtime_process::RuntimeEnv::new(),
            }
        }
    }

    fn mk_ctx() -> ToolContext {
        ToolContext {
            async_question: None,
            background_processes: Arc::new(
                crate::command_runtime::background::BackgroundCommandRegistry::new(),
            ),
            process_context: crate::runtime_process::test_runtime_process_context(),
            cwd: RuntimePath::from("."),
            signal: AbortSignal::new(),
            file_state: Arc::new(NoopFileState),
            permission: PermissionContext {
                allow_rules: Vec::new(),
                deny_rules: Vec::new(),
                ask_rules: Vec::new(),
                additional_writable_roots: Vec::new(),
                internal_writable_roots: Vec::new(),
                pinned_memory_root: None,
                should_avoid_prompts: false,
            },
            sandbox: Arc::new(NoopSandbox),
            planning_read_only: false,
            approval_policy: None,
            sandbox_mode: None,
            approvals_reviewer: None,
            web_search_access: None,
            network_allowed: None,
            sandbox_retry_on_denial: None,
            request_approval: None,
            request_network_access: None,
            enqueue_background_notice: None,
            record_rollout_event: None,
            emit: std::sync::Arc::new(|_| Box::pin(async {})),
            native_pdf: None,
            vision: None,
            shell_env: None,
            depth: None,
            forward_subagent_text: None,
            spawn_sub_agent: None,
            spawn_background_agent: None,
            spawn_workflow_agent: None,
            surface_tools: None,
            activate_skill_paths: None,
            tool_catalog: None,
            elicit: None,
            plan_file: None,
            apply_skill_context: None,
            run_bash: None,
            mcp_clients: None,
            background_tasks: None,
            task_list_id: None,
            task_plan: None,
            worktree: None,
            workflow_journal: None,
            swarm: None,
            cron: None,
            goal: None,
            goal_generation: None,
            goal_owned_resources: None,
            lsp: None,
            browser: None,
            browser_tool_mapping: None,
            run_hook: None,
            invoke_tool: None,
            advisor: None,
        }
    }

    fn entry(name: &str, description: &str, exposure: ToolExposure) -> ToolCatalogEntry {
        ToolCatalogEntry {
            name: name.into(),
            description: description.into(),
            search_hint: None,
            exposure,
            spec: None,
        }
    }

    fn entry_with_spec(
        internal_name: &str,
        rendered_name: &str,
        exposure: ToolExposure,
    ) -> ToolCatalogEntry {
        ToolCatalogEntry {
            name: internal_name.into(),
            description: format!("{internal_name} description").into(),
            search_hint: None,
            exposure,
            spec: Some(ToolSpec {
                name: rendered_name.into(),
                description: format!("{internal_name} description").into(),
                input_schema: rjson!({ "type": "object" }),
                kind: ToolSpecKind::Function,
                format: None,
            }),
        }
    }

    fn exact_entry(
        name: RuntimeString,
        description: RuntimeString,
        exposure: ToolExposure,
    ) -> ToolCatalogEntry {
        ToolCatalogEntry {
            name,
            description,
            search_hint: None,
            exposure,
            spec: None,
        }
    }

    async fn call_search(tool: &Arc<dyn Tool>, ctx: &ToolContext, input: Json) -> ToolResult {
        tool.call(input, ctx, None).await.unwrap()
    }

    fn text_of(res: &ToolResult) -> RuntimeString {
        match &res.serializable.model_content[0] {
            OutputBlock::Text(t) => t.text.clone(),
            _ => panic!("expected a text block"),
        }
    }

    /// Wire a fixed catalog and a recording `surface_tools` into the context.
    fn with_catalog(
        ctx: &mut ToolContext,
        catalog: Vec<ToolCatalogEntry>,
    ) -> Arc<Mutex<Vec<RuntimeString>>> {
        let cat: ToolCatalogFn = Box::new(move || catalog.clone());
        let surfaced = Arc::new(Mutex::new(Vec::new()));
        let sink = surfaced.clone();
        let surf: NamesFn = Box::new(move |names| {
            *sink.lock().unwrap() = names;
            Box::pin(async { Ok(()) })
        });
        ctx.tool_catalog = Some(cat);
        ctx.surface_tools = Some(surf);
        surfaced
    }

    /// A minimal tool used to exercise `with_deferred_tools`.
    struct DummyTool {
        name: String,
        schema: JsonSchema,
    }
    impl DummyTool {
        fn erased(name: &str) -> Arc<dyn Tool> {
            Arc::new(DummyTool {
                name: name.to_string(),
                schema: Map::new(),
            })
        }
    }
    #[async_trait]
    impl Tool for DummyTool {
        fn static_name(&self) -> &str {
            &self.name
        }
        fn static_description(&self) -> &str {
            "dummy"
        }
        fn static_input_schema(&self) -> &JsonSchema {
            &self.schema
        }
        fn is_enabled(&self) -> bool {
            true
        }
        fn is_concurrency_safe(&self, _input: &Json) -> bool {
            false
        }
        fn is_read_only(&self, _input: &Json) -> bool {
            false
        }
        async fn call(
            &self,
            _input: Json,
            _ctx: &ToolContext,
            _on_progress: Option<ToolProgress>,
        ) -> Result<ToolResult, ToolError> {
            unreachable!("dummy tool is never called")
        }
    }

    struct ForwardingProbeTool {
        name: RuntimeString,
        aliases: Vec<RuntimeString>,
        description: RuntimeString,
        input_schema: Json,
        output_schema: Json,
        search_hint: RuntimeString,
        mcp_server: RuntimeString,
        summary: RuntimeString,
        call_ids: Arc<Mutex<Vec<RuntimeString>>>,
    }

    #[async_trait]
    impl Tool for ForwardingProbeTool {
        fn name(&self) -> RuntimeString {
            self.name.clone()
        }
        fn aliases(&self) -> Vec<RuntimeString> {
            self.aliases.clone()
        }
        fn description(&self) -> RuntimeString {
            self.description.clone()
        }
        fn input_schema(&self) -> Json {
            self.input_schema.clone()
        }
        fn output_schema(&self) -> Option<Json> {
            Some(self.output_schema.clone())
        }
        fn input_kind(&self) -> ToolInputKind {
            ToolInputKind::Freeform {
                format: "probe-grammar".into(),
            }
        }
        fn search_hint(&self) -> Option<RuntimeString> {
            Some(self.search_hint.clone())
        }
        fn mcp_server(&self) -> Option<RuntimeString> {
            Some(self.mcp_server.clone())
        }
        fn input_schema_property_order(
            &self,
            object_path: &[String],
        ) -> Option<&'static [&'static str]> {
            object_path.is_empty().then_some(&["z", "a"])
        }
        fn preprocess_input(&self, _input: &Json) -> Json {
            rjson!("preprocessed")
        }
        fn is_enabled(&self) -> bool {
            true
        }
        fn is_concurrency_safe(&self, _input: &Json) -> bool {
            true
        }
        fn is_read_only(&self, _input: &Json) -> bool {
            true
        }
        async fn call(
            &self,
            _input: Json,
            _ctx: &ToolContext,
            _on_progress: Option<ToolProgress>,
        ) -> Result<ToolResult, ToolError> {
            Ok(text_result(
                rjson!({}),
                RuntimeString::from("called"),
                false,
            ))
        }
        async fn call_with_id(
            &self,
            call_id: &RuntimeString,
            input: Json,
            ctx: &ToolContext,
            on_progress: Option<ToolProgress>,
        ) -> Result<ToolResult, ToolError> {
            self.call_ids.lock().unwrap().push(call_id.clone());
            self.call(input, ctx, on_progress).await
        }
        fn summarize(&self, _input: &Json) -> Option<RuntimeString> {
            Some(self.summary.clone())
        }
    }

    #[test]
    fn deferred_native_tool_preserves_implementation_identity_for_approval() {
        let inner = crate::tools::builtins::write::create_write_tool();
        let identity = inner.persistence_identity();
        assert!(identity.is_some());
        let out = with_deferred_tools(vec![inner], Some(0));
        assert_eq!(out[1].persistence_identity(), identity);
    }

    #[test]
    fn window_deferral_keeps_the_existing_search_and_original_tool_identity() {
        let search = create_tool_search_tool(Some(3));
        let write = crate::tools::builtins::write::create_write_tool();
        let identity = write.persistence_identity();
        let tools = defer_direct_inventory(vec![search.clone(), write]);
        assert_eq!(
            tools
                .iter()
                .filter(|tool| tool.name() == "tool_search")
                .count(),
            1
        );
        assert!(Arc::ptr_eq(&tools[0], &search));
        assert_eq!(tools[0].exposure(), ToolExposure::Direct);
        assert_eq!(tools[1].exposure(), ToolExposure::Deferred);
        assert_eq!(tools[1].persistence_identity(), identity);
    }

    #[test]
    fn deferred_mcp_tool_preserves_registered_approval_metadata() {
        use crate::mcp::tool_wrapper::{wrap_mcp_tool, McpToolCall, WrapMcpToolOptions};
        use crate::mcp::types::{McpCallResult, McpToolDescriptor};
        let call: McpToolCall =
            Arc::new(|_, _, _, _| Box::pin(async { Ok(McpCallResult::default()) }));
        let inner = wrap_mcp_tool(
            "registered-server",
            McpToolDescriptor {
                name: "actual-tool".into(),
                title: Some("Registered title".into()),
                ..Default::default()
            },
            call,
            Some(WrapMcpToolOptions {
                deferred_by_default: Some(false),
                ..Default::default()
            }),
        );
        let metadata = inner.approval_metadata();
        assert!(metadata.is_some());
        let out = with_deferred_tools(vec![inner], Some(0));
        assert_eq!(out[1].exposure(), ToolExposure::Deferred);
        assert_eq!(out[1].approval_metadata(), metadata);
    }

    #[test]
    fn exposes_expected_metadata() {
        let tool = create_tool_search_tool(Some(7));
        assert_eq!(tool.name(), "tool_search");
        assert_eq!(
            tool.description(),
            "Search for additional (deferred) tools by capability keywords, or load exact deferred tools with select:<tool_name>."
        );
        let input = rjson!({ "query": "x" });
        assert!(tool.is_read_only(&input));
        assert!(tool.is_concurrency_safe(&input));
        assert_eq!(tool.is_destructive(&input), Some(false));
        assert_eq!(
            tool.input_schema(),
            rjson!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Use select:<tool_name>[,<tool_name>] to load exact deferred tools, or keywords describing the capability you need."
                    },
                    "limit": {
                        "type": "integer",
                        "exclusiveMinimum": 0,
                        "maximum": 9007199254740991_u64,
                        "description": "Maximum number of tools to return. Defaults to 7."
                    },
                    "max_results": {
                        "type": "integer",
                        "exclusiveMinimum": 0,
                        "maximum": 9007199254740991_u64,
                        "description": "Alias for limit. Defaults to 7."
                    }
                },
                "required": ["query"],
                "additionalProperties": false
            })
        );
    }

    #[test]
    fn limit_parser_matches_the_fixed_safe_positive_integer_contract() {
        assert_eq!(
            parse_tool_search_input(&rjson!({ "query": "q", "limit": 1.0 })).unwrap(),
            rjson!({ "query": "q", "limit": 1 })
        );
        assert_eq!(
            parse_tool_search_input(&rjson!({ "query": "q", "limit": 1e0 })).unwrap(),
            rjson!({ "query": "q", "limit": 1 })
        );
        for value in [rjson!(0), rjson!(-1), rjson!(9_007_199_254_740_992_u64)] {
            assert!(
                parse_tool_search_input(&runtime_object([
                    ("query", RuntimeJson::from("q")),
                    ("limit", value.clone()),
                ]))
                .is_err(),
                "accepted {value:?}"
            );
        }
    }

    #[tokio::test]
    async fn errors_without_catalog_or_surface() {
        let tool = create_tool_search_tool(None);
        let ctx = mk_ctx();
        let res = tool
            .call(rjson!({ "query": "web" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            res.serializable.output,
            rjson!({ "matched": [], "tools": [] })
        );
        assert_eq!(
            text_of(&res),
            "Tool search is not available in this context."
        );
    }

    #[tokio::test]
    async fn matches_deferred_tools_and_ranks_by_score() {
        let tool = create_tool_search_tool(None);
        let mut ctx = mk_ctx();
        let surfaced = with_catalog(
            &mut ctx,
            vec![
                entry(
                    "web_fetch",
                    "Fetch a web page over http",
                    ToolExposure::Deferred,
                ),
                entry("web_search", "Search the web", ToolExposure::Deferred),
                entry("calc", "Do math", ToolExposure::Deferred),
                // A direct tool is ignored even if it matches.
                entry("web_direct", "web web web", ToolExposure::Direct),
            ],
        );
        let res = tool
            .call(rjson!({ "query": "web fetch" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        // web_fetch scores 2 (web + fetch), web_search scores 1 (web).
        assert_eq!(
            res.serializable.output,
            rjson!({
                "matched": ["web_fetch", "web_search"],
                "tools": [
                    {
                        "name": "web_fetch",
                        "description": "Fetch a web page over http",
                        "inputSchema": {
                            "type": "object",
                            "properties": {},
                            "additionalProperties": true
                        },
                        "deferLoading": true
                    },
                    {
                        "name": "web_search",
                        "description": "Search the web",
                        "inputSchema": {
                            "type": "object",
                            "properties": {},
                            "additionalProperties": true
                        },
                        "deferLoading": true
                    }
                ]
            })
        );
        assert_eq!(*surfaced.lock().unwrap(), vec!["web_fetch", "web_search"]);
        assert_eq!(
            text_of(&res),
            "These tools are now available:\n- web_fetch: Fetch a web page over http\n- web_search: Search the web"
        );
    }

    #[tokio::test]
    async fn reports_no_matches() {
        let tool = create_tool_search_tool(None);
        let mut ctx = mk_ctx();
        let surfaced = with_catalog(
            &mut ctx,
            vec![entry("calc", "Do math", ToolExposure::Deferred)],
        );
        let res = tool
            .call(rjson!({ "query": "unrelated" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable.output,
            rjson!({ "matched": [], "tools": [] })
        );
        assert_eq!(text_of(&res), "No matching tools found.");
        assert!(surfaced.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn limit_precedes_max_results_then_configured_default() {
        let tool = create_tool_search_tool(Some(3));
        let mut ctx = mk_ctx();
        let surfaced = with_catalog(
            &mut ctx,
            vec![
                entry("web_a", "web", ToolExposure::Deferred),
                entry("web_b", "web", ToolExposure::Deferred),
                entry("web_c", "web", ToolExposure::Deferred),
                entry("web_d", "web", ToolExposure::Deferred),
            ],
        );

        let explicit = call_search(
            &tool,
            &ctx,
            rjson!({ "query": "web", "limit": 1, "max_results": 2 }),
        )
        .await;
        assert_eq!(explicit.serializable.output["matched"], rjson!(["web_a"]));
        assert_eq!(*surfaced.lock().unwrap(), vec!["web_a"]);

        let alias = call_search(&tool, &ctx, rjson!({ "query": "web", "max_results": 2 })).await;
        assert_eq!(
            alias.serializable.output["matched"],
            rjson!(["web_a", "web_b"])
        );
        assert_eq!(*surfaced.lock().unwrap(), vec!["web_a", "web_b"]);

        let configured = call_search(&tool, &ctx, rjson!({ "query": "web" })).await;
        assert_eq!(
            configured.serializable.output["matched"],
            rjson!(["web_a", "web_b", "web_c"])
        );
        assert_eq!(*surfaced.lock().unwrap(), vec!["web_a", "web_b", "web_c"]);
    }

    #[tokio::test]
    async fn select_reports_matched_available_missing_and_full_rendered_specs() {
        let tool = create_tool_search_tool(None);
        let mut ctx = mk_ctx();
        let surfaced = with_catalog(
            &mut ctx,
            vec![
                entry_with_spec("alpha", "visible_alpha", ToolExposure::Deferred),
                entry_with_spec("beta", "visible_beta", ToolExposure::Direct),
            ],
        );

        let res = call_search(
            &tool,
            &ctx,
            rjson!({ "query": "select:ALPHA,beta,missing", "limit": 5 }),
        )
        .await;

        assert_eq!(
            res.serializable.output,
            rjson!({
                "matched": ["alpha"],
                "tools": [{
                    "name": "visible_alpha",
                    "description": "alpha description",
                    "inputSchema": { "type": "object" },
                    "deferLoading": true
                }],
                "alreadyAvailable": ["beta"],
                "missing": ["missing"]
            })
        );
        assert_eq!(*surfaced.lock().unwrap(), vec!["alpha"]);
        assert_eq!(
            text_of(&res),
            "These tools are now available:\n- alpha: alpha description\nThese tools were already available:\n- beta\nNo matching tools found for:\n- missing"
        );
    }

    #[tokio::test]
    async fn select_limit_only_truncates_deferred_matches_and_rendered_tools() {
        let tool = create_tool_search_tool(None);
        let mut ctx = mk_ctx();
        let surfaced = with_catalog(
            &mut ctx,
            vec![
                entry_with_spec("d1", "visible_d1", ToolExposure::Deferred),
                entry_with_spec("d2", "visible_d2", ToolExposure::Deferred),
                entry("direct", "direct description", ToolExposure::Direct),
            ],
        );

        let res = call_search(
            &tool,
            &ctx,
            rjson!({ "query": "select:d1,d2,direct,missing", "limit": 1 }),
        )
        .await;

        assert_eq!(
            res.serializable.output,
            rjson!({
                "matched": ["d1"],
                "tools": [{
                    "name": "visible_d1",
                    "description": "d1 description",
                    "inputSchema": { "type": "object" },
                    "deferLoading": true
                }],
                "alreadyAvailable": ["direct"],
                "missing": ["missing"]
            })
        );
        assert_eq!(*surfaced.lock().unwrap(), vec!["d1"]);
    }

    #[tokio::test]
    async fn trusted_rewritten_limit_uses_fixed_ts_array_slice_coercion() {
        let tool = create_tool_search_tool(None);
        let mut ctx = mk_ctx();
        with_catalog(
            &mut ctx,
            vec![
                entry_with_spec("d1", "visible_d1", ToolExposure::Deferred),
                entry_with_spec("d2", "visible_d2", ToolExposure::Deferred),
                entry_with_spec("d3", "visible_d3", ToolExposure::Deferred),
            ],
        );

        for (limit, expected) in [
            (rjson!(0), Vec::<String>::new()),
            (rjson!(-1), vec!["d1".to_string(), "d2".to_string()]),
            (rjson!(1.5), vec!["d1".to_string()]),
            (rjson!("2"), vec!["d1".to_string(), "d2".to_string()]),
            (rjson!("0x2"), vec!["d1".to_string(), "d2".to_string()]),
            (rjson!("0b10"), vec!["d1".to_string(), "d2".to_string()]),
            (rjson!("0o2"), vec!["d1".to_string(), "d2".to_string()]),
            (rjson!([2]), vec!["d1".to_string(), "d2".to_string()]),
            (rjson!(["2"]), vec!["d1".to_string(), "d2".to_string()]),
            (rjson!([[2]]), vec!["d1".to_string(), "d2".to_string()]),
            (rjson!([]), Vec::<String>::new()),
            (rjson!([null]), Vec::<String>::new()),
            (rjson!([true]), Vec::<String>::new()),
            (rjson!([1, 2]), Vec::<String>::new()),
            (rjson!({}), Vec::<String>::new()),
            (
                rjson!("Infinity"),
                vec!["d1".to_string(), "d2".to_string(), "d3".to_string()],
            ),
            (rjson!("-Infinity"), Vec::<String>::new()),
            (
                rjson!("  \u{feff}2\u{a0} "),
                vec!["d1".to_string(), "d2".to_string()],
            ),
            (rjson!(false), Vec::<String>::new()),
            (rjson!(true), vec!["d1".to_string()]),
            (
                Json::Null,
                vec!["d1".to_string(), "d2".to_string(), "d3".to_string()],
            ),
        ] {
            let result = call_search(
                &tool,
                &ctx,
                runtime_object([
                    ("query", RuntimeJson::from("select:d1,d2,d3")),
                    ("limit", limit.clone()),
                ]),
            )
            .await;
            assert_eq!(
                result.serializable.output["matched"],
                rjson!(expected),
                "limit={limit:?}"
            );
        }
    }

    #[tokio::test]
    async fn trusted_rewritten_query_preserves_fixed_ts_runtime_type_errors() {
        let tool = create_tool_search_tool(None);
        let mut ctx = mk_ctx();
        with_catalog(&mut ctx, Vec::new());

        for (label, input, expected) in [
            (
                "null input",
                RuntimeJson::Null,
                "Cannot read properties of null (reading 'limit')",
            ),
            (
                "primitive input",
                rjson!("query"),
                "Cannot read properties of undefined (reading 'match')",
            ),
            (
                "missing query",
                rjson!({}),
                "Cannot read properties of undefined (reading 'match')",
            ),
            (
                "null query",
                rjson!({"query": null}),
                "Cannot read properties of null (reading 'match')",
            ),
            (
                "number query",
                rjson!({"query": 7}),
                "query.match is not a function",
            ),
            (
                "array query",
                rjson!({"query": ["select:d1"]}),
                "query.match is not a function",
            ),
            (
                "null query before throwing limit coercion",
                rjson!({"query": null, "limit": {"toString": "2"}}),
                "Cannot read properties of null (reading 'match')",
            ),
            (
                "number query before throwing limit coercion",
                rjson!({"query": 7, "limit": {"toString": "2"}}),
                "query.match is not a function",
            ),
        ] {
            let error = match tool.call(input, &ctx, None).await {
                Ok(_) => panic!("{label} unexpectedly succeeded"),
                Err(error) => error,
            };
            assert_eq!(error.message, expected, "{label}");
        }
    }

    #[tokio::test]
    async fn trusted_rewritten_limit_preserves_js_object_to_primitive_errors() {
        let tool = create_tool_search_tool(None);
        let mut ctx = mk_ctx();
        with_catalog(&mut ctx, Vec::new());

        for (label, limit) in [
            ("object", rjson!({"toString": "2"})),
            ("array object", rjson!([{"toString": "2"}])),
            ("nested array object", rjson!([[{"toString": "2"}]])),
            (
                "multi-item array still converts every element",
                rjson!([0, {"toString": "2"}]),
            ),
        ] {
            let error = match tool
                .call(
                    runtime_object([("query", RuntimeJson::from("anything")), ("limit", limit)]),
                    &ctx,
                    None,
                )
                .await
            {
                Ok(_) => panic!("{label} unexpectedly succeeded"),
                Err(error) => error,
            };
            assert_eq!(
                error.message, "Cannot convert object to primitive value",
                "{label}"
            );
        }
    }

    #[tokio::test]
    async fn select_prefix_and_items_are_case_insensitive_trimmed_nonempty_and_deduplicated() {
        let tool = create_tool_search_tool(None);
        let mut ctx = mk_ctx();
        let surfaced = with_catalog(
            &mut ctx,
            vec![
                entry_with_spec("alpha", "visible_alpha", ToolExposure::Deferred),
                entry(
                    "bridge",
                    "bridge description",
                    ToolExposure::DirectModelOnly,
                ),
            ],
        );

        let res = call_search(
            &tool,
            &ctx,
            rjson!({ "query": "SeLeCt: , ALPHA,alpha, bridge,BRIDGE," }),
        )
        .await;

        assert_eq!(
            res.serializable.output,
            rjson!({
                "matched": ["alpha"],
                "tools": [{
                    "name": "visible_alpha",
                    "description": "alpha description",
                    "inputSchema": { "type": "object" },
                    "deferLoading": true
                }],
                "alreadyAvailable": ["bridge"]
            })
        );
        assert_eq!(*surfaced.lock().unwrap(), vec!["alpha"]);
    }

    #[test]
    fn exact_select_surfaces_the_interactive_shell_group_atomically() {
        let catalog = vec![
            entry(
                "exec_command",
                "persistent interactive shell",
                ToolExposure::Deferred,
            ),
            entry(
                "write_stdin",
                "send input to an interactive shell",
                ToolExposure::Deferred,
            ),
        ];
        let resolution = resolve_tool_search(
            &rjson!({ "query": "select:exec_command", "limit": 1 }),
            &catalog,
            DEFAULT_MAX_RESULTS,
        )
        .unwrap();

        assert_eq!(
            resolution.output["matched"],
            rjson!(["exec_command", "write_stdin"])
        );
        assert_eq!(
            resolution.surface_callback,
            Some(vec![rs("exec_command"), rs("write_stdin")])
        );
    }

    #[test]
    fn keyword_search_surfaces_the_interactive_shell_group_atomically() {
        let catalog = vec![
            entry(
                "exec_command",
                "persistent interactive shell",
                ToolExposure::Deferred,
            ),
            entry(
                "write_stdin",
                "send input to a running process",
                ToolExposure::Deferred,
            ),
        ];
        let resolution = resolve_tool_search(
            &rjson!({ "query": "+persistent +interactive", "limit": 1 }),
            &catalog,
            DEFAULT_MAX_RESULTS,
        )
        .unwrap();

        assert_eq!(
            resolution.output["matched"],
            rjson!(["exec_command", "write_stdin"])
        );
    }

    #[test]
    fn interactive_shell_group_respects_the_filtered_catalog() {
        let catalog = vec![entry(
            "exec_command",
            "persistent interactive shell",
            ToolExposure::Deferred,
        )];
        let resolution = resolve_tool_search(
            &rjson!({ "query": "select:exec_command" }),
            &catalog,
            DEFAULT_MAX_RESULTS,
        )
        .unwrap();

        assert_eq!(resolution.output["matched"], rjson!(["exec_command"]));
        assert_eq!(resolution.surface_callback, Some(vec![rs("exec_command")]));
    }

    #[tokio::test]
    async fn exact_select_keeps_surrogate_and_replacement_catalog_names_distinct() {
        let high = RuntimeString::from_utf16([0xd800]);
        let low = RuntimeString::from_utf16([0xdc00]);
        let replacement = RuntimeString::from_utf16([0xfffd]);
        let query = rs("select:")
            .concat(&high)
            .concat(&rs(","))
            .concat(&low)
            .concat(&rs(","))
            .concat(&replacement);
        let tool = create_tool_search_tool(None);
        let mut ctx = mk_ctx();
        let surfaced = with_catalog(
            &mut ctx,
            vec![
                exact_entry(high.clone(), rs("high"), ToolExposure::Deferred),
                exact_entry(low.clone(), rs("low"), ToolExposure::Deferred),
                exact_entry(
                    replacement.clone(),
                    rs("replacement"),
                    ToolExposure::Deferred,
                ),
            ],
        );

        let result = call_search(
            &tool,
            &ctx,
            runtime_object([("query", RuntimeJson::String(query))]),
        )
        .await;
        let expected_names = vec![high.clone(), low.clone(), replacement.clone()];
        assert_eq!(
            result.serializable.output["matched"],
            RuntimeJson::Array(
                expected_names
                    .iter()
                    .cloned()
                    .map(RuntimeJson::String)
                    .collect()
            )
        );
        for (index, expected) in expected_names.iter().enumerate() {
            assert_eq!(
                result.serializable.output["tools"][index]["name"],
                RuntimeJson::String(expected.clone())
            );
        }
        assert_eq!(*surfaced.lock().unwrap(), expected_names);
        let expected_text = rs("These tools are now available:\n- ")
            .concat(&high)
            .concat(&rs(": high\n- "))
            .concat(&low)
            .concat(&rs(": low\n- "))
            .concat(&replacement)
            .concat(&rs(": replacement"));
        assert_eq!(text_of(&result), expected_text);
    }

    #[tokio::test]
    async fn rendered_spec_preserves_exact_name_description_schema_and_format() {
        let rendered_name = RuntimeString::from_utf16([b'n' as u16, 0xd810]);
        let rendered_description = RuntimeString::from_utf16([b'd' as u16, 0xdc10]);
        let schema_value = RuntimeString::from_utf16([b's' as u16, 0xd811]);
        let definition = RuntimeString::from_utf16([b'g' as u16, 0xdc11]);
        let input_schema = runtime_object([("title", RuntimeJson::String(schema_value.clone()))]);
        let mut catalog_entry = exact_entry(
            rs("internal_exact"),
            rs("catalog description"),
            ToolExposure::Deferred,
        );
        catalog_entry.spec = Some(ToolSpec {
            name: rendered_name.clone(),
            description: rendered_description.clone(),
            input_schema: input_schema.clone(),
            kind: ToolSpecKind::Freeform,
            format: Some(ToolSpecFormat::Grammar {
                syntax: ToolSpecGrammarSyntax::Lark,
                definition: definition.clone(),
            }),
        });
        let tool = create_tool_search_tool(None);
        let mut ctx = mk_ctx();
        with_catalog(&mut ctx, vec![catalog_entry]);

        let result = call_search(&tool, &ctx, rjson!({ "query": "select:internal_exact" })).await;
        let rendered = &result.serializable.output["tools"][0];
        assert_eq!(rendered["name"], RuntimeJson::String(rendered_name));
        assert_eq!(
            rendered["description"],
            RuntimeJson::String(rendered_description)
        );
        assert_eq!(rendered["inputSchema"], input_schema);
        assert_eq!(
            rendered["format"]["definition"],
            RuntimeJson::String(definition)
        );
        assert_eq!(rendered["kind"], RuntimeJson::from("freeform"));
    }

    #[test]
    fn trusted_select_and_exact_matching_use_javascript_trim_and_regex_boundaries() {
        assert_eq!(
            parse_select_query(&rs("SeLeCt:\u{feff}alpha\u{feff},\u{a0}beta\u{a0}")),
            Some(vec![rs("alpha"), rs("beta")])
        );
        assert_eq!(
            parse_select_query(&rs("select:\u{0085}alpha\u{0085}")),
            Some(vec![rs("\u{0085}alpha\u{0085}")])
        );
        assert_eq!(parse_select_query(&rs("select:alpha\n")), None);
        assert_eq!(parse_select_query(&rs("select:alpha\r\n")), None);
        assert_eq!(parse_select_query(&rs("select:alpha\u{2028}")), None);
        assert_eq!(parse_select_query(&rs("select:alpha\u{2029}")), None);
        assert_eq!(parse_select_query(&rs("select:alpha\nbeta")), None);

        let catalog = vec![entry("alpha", "alpha", ToolExposure::Deferred)];
        assert_eq!(
            find_catalog_tool(&catalog, &rs("\u{feff}ALPHA\u{feff}")).map(|tool| tool.name.clone()),
            Some(rs("alpha"))
        );
        assert_eq!(
            find_catalog_tool(&catalog, &rs("\u{0085}ALPHA\u{0085}")),
            None
        );
    }

    #[test]
    fn trusted_keyword_splitting_uses_ecmascript_whitespace() {
        assert_eq!(
            parse_keywords(&rs("+must\u{feff}name\u{a0}hint")),
            (vec![rs("must")], vec![rs("name"), rs("hint")])
        );
        assert_eq!(
            parse_keywords(&rs("+must\u{0085}hint")),
            (vec![rs("must\u{0085}hint")], Vec::new())
        );
    }

    #[test]
    fn exact_lowercase_preserves_surrogates_and_unicode_final_sigma_context() {
        assert_eq!(
            lowercase_runtime(&RuntimeString::from_utf16([b'A' as u16, 0x03a3, 0xd800,])),
            RuntimeString::from_utf16([b'a' as u16, 0x03c2, 0xd800])
        );
        assert_eq!(
            lowercase_runtime(&RuntimeString::from_utf16([b'A' as u16, 0xd800, 0x03a3,])),
            RuntimeString::from_utf16([b'a' as u16, 0xd800, 0x03c3])
        );
    }

    #[test]
    fn trusted_limit_overflow_preserves_javascript_infinity() {
        let overflow = crate::runtime_values::runtime_json::parse(&rs("1e400")).unwrap();
        assert_eq!(js_number(&overflow).unwrap(), f64::INFINITY);
        let negative = crate::runtime_values::runtime_json::parse(&rs("-1e400")).unwrap();
        assert_eq!(js_number(&negative).unwrap(), f64::NEG_INFINITY);
    }

    #[tokio::test]
    async fn bare_exact_name_is_case_insensitive_and_uses_internal_identity() {
        let tool = create_tool_search_tool(None);
        let mut ctx = mk_ctx();
        let surfaced = with_catalog(
            &mut ctx,
            vec![entry_with_spec(
                "alpha",
                "visible_alpha",
                ToolExposure::Deferred,
            )],
        );

        let res = call_search(&tool, &ctx, rjson!({ "query": "  ALPHA  " })).await;

        assert_eq!(res.serializable.output["matched"], rjson!(["alpha"]));
        assert_eq!(
            res.serializable.output["tools"][0]["name"],
            rjson!("visible_alpha")
        );
        assert_eq!(*surfaced.lock().unwrap(), vec!["alpha"]);
    }

    #[tokio::test]
    async fn bare_exact_direct_and_model_only_internal_names_are_already_available() {
        let tool = create_tool_search_tool(None);
        let mut ctx = mk_ctx();
        let surfaced = with_catalog(
            &mut ctx,
            vec![
                entry_with_spec("direct", "visible_direct", ToolExposure::Direct),
                entry_with_spec(
                    "model_bridge",
                    "visible_bridge",
                    ToolExposure::DirectModelOnly,
                ),
            ],
        );

        let direct = call_search(&tool, &ctx, rjson!({ "query": " DIRECT " })).await;
        assert_eq!(
            direct.serializable.output,
            rjson!({
                "matched": [],
                "tools": [],
                "alreadyAvailable": ["direct"]
            })
        );

        let model_only = call_search(&tool, &ctx, rjson!({ "query": "MODEL_BRIDGE" })).await;
        assert_eq!(
            model_only.serializable.output,
            rjson!({
                "matched": [],
                "tools": [],
                "alreadyAvailable": ["model_bridge"]
            })
        );
        assert!(surfaced.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn keyword_search_scores_name_description_and_hint_after_required_filtering() {
        let tool = create_tool_search_tool(None);
        let mut ctx = mk_ctx();
        let mut ranked_all = entry(
            "name_everywhere",
            "must description",
            ToolExposure::Deferred,
        );
        ranked_all.search_hint = Some(rs("hint"));
        let name_hit = entry("must_name", "plain", ToolExposure::Deferred);
        let description_hit = entry("desc_tool", "must description", ToolExposure::Deferred);
        let mut hint_hit = entry("search_tool", "must", ToolExposure::Deferred);
        hint_hit.search_hint = Some(rs("hint"));
        let mut missing_required = entry("name_rejected", "description", ToolExposure::Deferred);
        missing_required.search_hint = Some(rs("hint"));
        let surfaced = with_catalog(
            &mut ctx,
            vec![
                name_hit,
                description_hit,
                hint_hit,
                ranked_all,
                missing_required,
            ],
        );

        let res = call_search(
            &tool,
            &ctx,
            rjson!({ "query": "+MUST\tNAME\nDESCRIPTION HINT" }),
        )
        .await;

        assert_eq!(
            res.serializable.output["matched"],
            rjson!(["name_everywhere", "must_name", "desc_tool", "search_tool"])
        );
        assert_eq!(
            *surfaced.lock().unwrap(),
            vec!["name_everywhere", "must_name", "desc_tool", "search_tool"]
        );
    }

    #[tokio::test]
    async fn missing_spec_uses_exact_open_object_fallback() {
        let tool = create_tool_search_tool(None);
        let mut ctx = mk_ctx();
        with_catalog(
            &mut ctx,
            vec![entry(
                "fallback",
                "fallback description",
                ToolExposure::Deferred,
            )],
        );

        let res = call_search(&tool, &ctx, rjson!({ "query": "select:fallback" })).await;

        assert_eq!(
            res.serializable.output,
            rjson!({
                "matched": ["fallback"],
                "tools": [{
                    "name": "fallback",
                    "description": "fallback description",
                    "inputSchema": {
                        "type": "object",
                        "properties": {},
                        "additionalProperties": true
                    },
                    "deferLoading": true
                }]
            })
        );
    }

    #[tokio::test]
    async fn rendered_spec_is_carried_verbatim_with_non_default_kind_and_format() {
        let tool = create_tool_search_tool(None);
        let mut ctx = mk_ctx();
        let mut catalog_entry = entry(
            "internal_patch",
            "internal description",
            ToolExposure::Deferred,
        );
        catalog_entry.spec = Some(ToolSpec {
            name: rs("apply_patch"),
            description: rs("rendered patch description"),
            input_schema: rjson!({ "type": "string" }),
            kind: ToolSpecKind::Freeform,
            format: Some(ToolSpecFormat::Grammar {
                syntax: ToolSpecGrammarSyntax::Lark,
                definition: rs("start: /.+/"),
            }),
        });
        let surfaced = with_catalog(&mut ctx, vec![catalog_entry]);

        let res = call_search(&tool, &ctx, rjson!({ "query": "select:internal_patch" })).await;

        assert_eq!(
            res.serializable.output,
            rjson!({
                "matched": ["internal_patch"],
                "tools": [{
                    "name": "apply_patch",
                    "description": "rendered patch description",
                    "inputSchema": { "type": "string" },
                    "deferLoading": true,
                    "kind": "freeform",
                    "format": {
                        "type": "grammar",
                        "syntax": "lark",
                        "definition": "start: /.+/"
                    }
                }]
            })
        );
        assert_eq!(*surfaced.lock().unwrap(), vec!["internal_patch"]);
    }

    #[tokio::test]
    async fn hidden_exact_and_rendered_facade_name_are_missing() {
        let tool = create_tool_search_tool(None);
        let mut ctx = mk_ctx();
        let surfaced = with_catalog(
            &mut ctx,
            vec![
                entry("hidden", "hidden description", ToolExposure::Hidden),
                entry_with_spec("task", "spawn_agent", ToolExposure::Deferred),
            ],
        );

        let res = call_search(
            &tool,
            &ctx,
            rjson!({ "query": "select:hidden,spawn_agent" }),
        )
        .await;

        assert_eq!(
            res.serializable.output,
            rjson!({
                "matched": [],
                "tools": [],
                "missing": ["hidden", "spawn_agent"]
            })
        );
        assert!(surfaced.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn validation_does_not_reparse_tool_search_initial_input() {
        let tool = create_tool_search_tool(None);
        assert_eq!(
            tool.validate_input(&rjson!({ "query": 7 }), &mk_ctx())
                .await,
            None
        );
    }

    #[test]
    fn invalid_limits_are_model_errors_in_the_initial_parser() {
        let tool = create_tool_search_tool(None);
        let overflow = runtime_from_serde(
            serde_json::from_str("18446744073709551616").expect("valid scalar JSON number"),
        );
        assert!(overflow
            .as_f64()
            .is_some_and(|number| number > JS_MAX_SAFE_INTEGER as f64));
        let invalid_values = vec![
            (rjson!(0), "Too small: expected number to be >0"),
            (rjson!(-1), "Too small: expected number to be >0"),
            (rjson!(1.5), "Invalid input: expected int, received number"),
            (
                rjson!("2"),
                "Invalid input: expected number, received string",
            ),
            (Json::Null, "Invalid input: expected number, received null"),
            (overflow, "Too big: expected int to be <=9007199254740991"),
        ];

        for key in ["limit", "max_results"] {
            for (value, issue) in &invalid_values {
                let mut input = rjson!({ "query": "anything" });
                input
                    .as_object_mut()
                    .unwrap()
                    .insert(key.to_string(), value.clone());
                let message = format!("Invalid arguments: {key}: {issue}");

                let error = tool
                    .parse_input(&input)
                    .expect_err("parser accepted invalid input");
                assert_eq!(error.kind, ToolErrorKind::Model);
                assert_eq!(error.message, message, "parser accepted {key}={value:?}");
            }
        }

        let both = rjson!({ "limit": 0, "max_results": "2" });
        let compound = concat!(
            "Invalid arguments: ",
            "query: Invalid input: expected string, received undefined; ",
            "limit: Too small: expected number to be >0; ",
            "max_results: Invalid input: expected number, received string"
        );
        let error = tool
            .parse_input(&both)
            .expect_err("parser accepted compound invalid input");
        assert_eq!(error.message, compound);
    }

    #[test]
    fn invalid_query_shapes_fail_like_the_fixed_ts_schema_before_context_access() {
        let tool = create_tool_search_tool(None);
        let cases = [
            (
                rjson!({}),
                "Invalid arguments: query: Invalid input: expected string, received undefined",
            ),
            (
                rjson!({ "query": 7 }),
                "Invalid arguments: query: Invalid input: expected string, received number",
            ),
            (
                rjson!([]),
                "Invalid arguments: (root): Invalid input: expected object, received array",
            ),
            (
                Json::Null,
                "Invalid arguments: (root): Invalid input: expected object, received null",
            ),
            (
                rjson!("query"),
                "Invalid arguments: (root): Invalid input: expected object, received string",
            ),
        ];

        for (input, expected) in cases {
            let error = tool
                .parse_input(&input)
                .expect_err("parser accepted invalid query shape");
            assert_eq!(
                error.kind,
                ToolErrorKind::Model,
                "parser accepted {input:?}"
            );
            assert_eq!(error.message, expected, "parser accepted {input:?}");
        }
    }

    #[tokio::test]
    async fn integral_float_limits_are_valid_in_initial_parsing_and_call() {
        let tool = create_tool_search_tool(None);
        let mut ctx = mk_ctx();
        let surfaced = with_catalog(&mut ctx, Vec::new());
        let integral_float = rjson!(1.0);
        let exponent_float =
            runtime_from_serde(serde_json::from_str("1e0").expect("valid scalar JSON number"));

        for key in ["limit", "max_results"] {
            for (syntax, value) in [
                ("1.0", integral_float.clone()),
                ("1e0", exponent_float.clone()),
            ] {
                assert_eq!(value.as_f64(), Some(1.0));
                let mut input = rjson!({ "query": "anything" });
                input
                    .as_object_mut()
                    .unwrap()
                    .insert(key.to_string(), value);

                let parsed = tool
                    .parse_input(&input)
                    .unwrap_or_else(|error| panic!("parser rejected {key}={syntax}: {error:?}"));
                let result = call_search(&tool, &ctx, parsed).await;
                assert_eq!(result.serializable.is_error, Some(false));
                assert_eq!(
                    result.serializable.output,
                    rjson!({ "matched": [], "tools": [] })
                );
            }
        }
        assert!(surfaced.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn no_catalog_and_empty_catalog_return_the_exact_empty_shape() {
        let tool = create_tool_search_tool(None);
        let no_catalog = call_search(&tool, &mk_ctx(), rjson!({ "query": "anything" })).await;
        assert_eq!(
            no_catalog.serializable.output,
            rjson!({ "matched": [], "tools": [] })
        );

        let mut empty_ctx = mk_ctx();
        let surfaced = with_catalog(&mut empty_ctx, Vec::new());
        let empty = call_search(&tool, &empty_ctx, rjson!({ "query": "nothing" })).await;
        assert_eq!(
            empty.serializable.output,
            rjson!({ "matched": [], "tools": [] })
        );
        assert!(surfaced.lock().unwrap().is_empty());
    }

    #[test]
    fn with_deferred_tools_below_threshold_is_unchanged() {
        let tools: Vec<Arc<dyn Tool>> = (0..5)
            .map(|i| DummyTool::erased(&format!("t{i}")))
            .collect();
        let out = with_deferred_tools(tools, None);
        assert_eq!(out.len(), 5);
        assert_ne!(out[0].name(), "tool_search");
    }

    #[test]
    fn with_deferred_tools_defers_past_threshold_and_prepends_search() {
        let tools: Vec<Arc<dyn Tool>> = (0..31)
            .map(|i| DummyTool::erased(&format!("t{i}")))
            .collect();
        let out = with_deferred_tools(tools, None);
        // tool_search + 31 tools.
        assert_eq!(out.len(), 32);
        assert_eq!(out[0].name(), "tool_search");
        // Index 0..29 of the originals (out[1..=30]) stay direct.
        assert_eq!(out[1].exposure(), ToolExposure::Direct);
        assert_eq!(out[30].exposure(), ToolExposure::Direct);
        // The one tool at original index 30 (out[31]) is deferred.
        assert_eq!(out[31].exposure(), ToolExposure::Deferred);
        assert_eq!(out[31].name(), "t30");
    }

    #[test]
    fn with_deferred_tools_honors_a_custom_threshold() {
        let tools: Vec<Arc<dyn Tool>> = vec![DummyTool::erased("a"), DummyTool::erased("b")];
        let out = with_deferred_tools(tools, Some(1));
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].name(), "tool_search");
        // index 0 (< threshold 1) stays direct; index 1 (>= 1) is deferred.
        assert_eq!(out[1].exposure(), ToolExposure::Direct);
        assert_eq!(out[2].exposure(), ToolExposure::Deferred);
    }

    #[tokio::test]
    async fn deferred_wrapper_only_changes_exposure_and_forwards_erased_contracts() {
        let call_ids = Arc::new(Mutex::new(Vec::new()));
        let exact_name = RuntimeString::from_utf16([b'n' as u16, 0xd800]);
        let exact_alias = RuntimeString::from_utf16([b'a' as u16, 0xdc00]);
        let exact_description = RuntimeString::from_utf16([b'd' as u16, 0xd801]);
        let exact_hint = RuntimeString::from_utf16([b'h' as u16, 0xdc01]);
        let exact_server = RuntimeString::from_utf16([b'm' as u16, 0xd802]);
        let exact_summary = RuntimeString::from_utf16([b's' as u16, 0xdc02]);
        let exact_input_schema = runtime_object([(
            "title",
            RuntimeJson::String(RuntimeString::from_utf16([0xd803])),
        )]);
        let exact_output_schema = runtime_object([(
            "title",
            RuntimeJson::String(RuntimeString::from_utf16([0xdc03])),
        )]);
        let probe: Arc<dyn Tool> = Arc::new(ForwardingProbeTool {
            name: exact_name.clone(),
            aliases: vec![exact_alias.clone()],
            description: exact_description.clone(),
            input_schema: exact_input_schema.clone(),
            output_schema: exact_output_schema.clone(),
            search_hint: exact_hint.clone(),
            mcp_server: exact_server.clone(),
            summary: exact_summary.clone(),
            call_ids: call_ids.clone(),
        });
        let out = with_deferred_tools(vec![DummyTool::erased("direct"), probe], Some(1));
        let deferred = &out[2];

        assert_eq!(deferred.exposure(), ToolExposure::Deferred);
        assert_eq!(deferred.name(), exact_name);
        assert_eq!(deferred.aliases(), vec![exact_alias]);
        assert_eq!(deferred.description(), exact_description);
        assert_eq!(deferred.input_schema(), exact_input_schema);
        assert_eq!(deferred.output_schema(), Some(exact_output_schema));
        assert_eq!(deferred.search_hint(), Some(exact_hint));
        assert_eq!(deferred.mcp_server(), Some(exact_server));
        assert_eq!(deferred.summarize(&rjson!(null)), Some(exact_summary));
        assert_eq!(
            deferred.input_kind(),
            ToolInputKind::Freeform {
                format: rs("probe-grammar")
            }
        );
        assert_eq!(
            deferred.input_schema_property_order(&[]),
            Some(&["z", "a"][..])
        );
        assert_eq!(
            deferred.preprocess_input(&rjson!(null)),
            rjson!("preprocessed")
        );
        let call_identity = RuntimeString::from_utf16([b'c' as u16, 0xd804]);
        deferred
            .call_with_id(&call_identity, rjson!("raw"), &mk_ctx(), None)
            .await
            .unwrap();
        assert_eq!(*call_ids.lock().unwrap(), [call_identity]);
    }
}
