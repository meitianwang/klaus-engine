//! The `grep` [`Tool`]: regular-expression content search over the filesystem,
//! powered by ripgrep. Three output modes (files_with_matches[default] /
//! content / count), gitignore RESPECTED, hidden files searched, VCS dirs
//! excluded, `--max-columns 500`, a default head-limit of 250 with offset
//! pagination, and files-mode results sorted by mtime newest-first. The rg
//! flags / modes / sort / output structure are reproduced exactly; the
//! descriptions and chrome messages are original prose.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::fs::node_path::project_runtime_path;
use crate::fs::ripgrep::{
    default_search_roots, expand_path, node_stat_error_message, run_task_ripgrep, to_relative_path,
    RipgrepError,
};
use crate::runtime_values::{
    RuntimeJson, RuntimeNumber, RuntimeObject, RuntimePath, RuntimeString,
};
use crate::tools::build_tool::text_result;
use crate::tools::locale_sort::sort_runtime_by_key;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{
    PathTargets, PermissionPathKind, Tool, ToolContext, ToolProgress, ToolResult, ValidationResult,
};

/// VCS metadata dirs excluded from every search.
const VCS_DIRS: [&str; 6] = [".git", ".svn", ".hg", ".bzr", ".jj", ".sl"];
/// Result cap when head_limit is unspecified. Pass 0 for unlimited.
const DEFAULT_HEAD_LIMIT: f64 = 250.0;

const GREP_DESCRIPTION: &str = "Search file contents with a regular expression, powered by ripgrep. Three modes: \"files_with_matches\" (default; matching file paths, sorted newest-first), \"content\" (matching lines with optional -A/-B/-C context and -n line numbers), and \"count\" (per-file match counts). Respects .gitignore, searches hidden files, excludes VCS metadata. Prefer this tool when its search and output options fit; use shell rg when shell composition or other unsupported options are useful.";
const INPUT_SCHEMA_PROPERTY_ORDER: &[&str] = &[
    "pattern",
    "path",
    "glob",
    "output_mode",
    "-B",
    "-A",
    "-C",
    "context",
    "-n",
    "-i",
    "type",
    "head_limit",
    "offset",
    "multiline",
    "-o",
];

/// Whether `s` is a numeric literal matching `^-?\d+(\.\d+)?$`, evaluated
/// directly on JavaScript UTF-16 code units.
fn is_numeric_literal(s: &RuntimeString) -> bool {
    let b = s.as_utf16();
    if b.is_empty() {
        return false;
    }
    let mut i = 0;
    if b[i] == u16::from(b'-') {
        i += 1;
    }
    let int_start = i;
    while i < b.len() && (u16::from(b'0')..=u16::from(b'9')).contains(&b[i]) {
        i += 1;
    }
    if i == int_start {
        return false;
    }
    if i < b.len() && b[i] == u16::from(b'.') {
        i += 1;
        let frac_start = i;
        while i < b.len() && (u16::from(b'0')..=u16::from(b'9')).contains(&b[i]) {
            i += 1;
        }
        if i == frac_start {
            return false;
        }
    }
    i == b.len()
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

fn trim_ecmascript(value: &RuntimeString) -> RuntimeString {
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

fn scalar_string(value: &RuntimeString) -> Option<String> {
    let mut output = String::new();
    for point in value.code_points() {
        output.push(char::from_u32(point)?);
    }
    Some(output)
}

fn ui_string(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8()).expect("Node UTF-8 UI projection is valid UTF-8")
}

fn runtime_object(fields: impl IntoIterator<Item = (&'static str, RuntimeJson)>) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    for (key, value) in fields {
        object.insert(key, value);
    }
    RuntimeJson::Object(object)
}

fn runtime_number(value: f64) -> RuntimeJson {
    RuntimeJson::Number(RuntimeNumber::from_f64(value))
}

fn join_runtime(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let mut output = RuntimeString::default();
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output = output.concat(&RuntimeString::from(separator));
        }
        output = output.concat(value);
    }
    output
}

fn overflowing_semantic_numbers(input: &Json) -> Vec<&'static str> {
    const NUMBER_KEYS: [&str; 6] = ["-B", "-A", "-C", "context", "head_limit", "offset"];
    let Some(input) = input.as_object() else {
        return Vec::new();
    };
    NUMBER_KEYS
        .into_iter()
        .filter(|key| {
            let Some(Json::String(value)) = input.get(*key) else {
                return false;
            };
            let value = trim_ecmascript(value);
            is_numeric_literal(&value)
                && scalar_string(&value)
                    .and_then(|value| value.parse::<f64>().ok())
                    .is_some_and(|number| !number.is_finite())
        })
        .collect()
}

/// Preserve the Zod-style input preprocessors before the generic schema parser.
/// Only matching numeric strings and the exact boolean strings transform;
/// every other value is left for normal type validation.
fn preprocess_grep_input(input: &Json) -> Json {
    let Some(input) = input.as_object() else {
        return input.clone();
    };
    let mut output = input.clone();
    for key in ["-B", "-A", "-C", "context", "head_limit", "offset"] {
        let Some(Json::String(value)) = input.get(key) else {
            continue;
        };
        let trimmed = trim_ecmascript(value);
        if !is_numeric_literal(&trimmed) {
            continue;
        }
        if let Some(value) = scalar_string(&trimmed).and_then(|value| value.parse::<f64>().ok()) {
            output.insert(key, Json::Number(RuntimeNumber::from_f64(value)));
        }
    }
    for key in ["-n", "-i", "multiline", "-o"] {
        match input.get(key).and_then(Json::as_str) {
            Some(value) if value == "true" => {
                output.insert(key, Json::Bool(true));
            }
            Some(value) if value == "false" => {
                output.insert(key, Json::Bool(false));
            }
            _ => {}
        }
    }
    Json::Object(output)
}

/// Coerce a numeric string into a number (models sometimes pass "5"). Mirrors the
/// semanticNumber preprocess: a JSON number passes through; a string matching
/// `^-?\d+(\.\d+)?$` (after trim) is parsed; anything else is treated as absent.
#[cfg_attr(not(any(test, feature = "testing")), allow(dead_code))]
pub(crate) fn read_number(input: &Json, key: &str) -> Option<f64> {
    match input.get(key) {
        Some(Json::Number(n)) => Some(n.as_f64()),
        Some(Json::String(s)) => {
            let t = trim_ecmascript(s);
            if is_numeric_literal(&t) {
                scalar_string(&t).and_then(|value| value.parse::<f64>().ok())
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Coerce "true"/"false" strings into booleans (semanticBoolean). A real boolean
/// passes through; "true"/"false" strings coerce; anything else is absent.
#[cfg_attr(not(any(test, feature = "testing")), allow(dead_code))]
pub(crate) fn read_bool(input: &Json, key: &str) -> Option<bool> {
    match input.get(key) {
        Some(Json::Bool(b)) => Some(*b),
        Some(Json::String(s)) if s == "true" => Some(true),
        Some(Json::String(s)) if s == "false" => Some(false),
        _ => None,
    }
}

/// Format a number the way JS `String(n)` does for the values this tool emits:
/// an integral value renders without a fractional part.
fn number_to_string(n: f64) -> String {
    if n == 0.0 {
        return "0".to_string();
    }
    if n.is_nan() {
        return "NaN".to_string();
    }
    if n == f64::INFINITY {
        return "Infinity".to_string();
    }
    if n == f64::NEG_INFINITY {
        return "-Infinity".to_string();
    }
    ryu_js::Buffer::new().format_finite(n).to_owned()
}

fn js_number_value(number: &RuntimeNumber) -> f64 {
    number.as_f64()
}

fn js_property<'a>(input: &'a Json, property: &str) -> Result<Option<&'a Json>, ToolError> {
    match input {
        Json::Null => Err(ToolError::new(format!(
            "Cannot read properties of null (reading '{property}')"
        ))),
        Json::Object(object) => Ok(object.get(property)),
        Json::Bool(_) | Json::Number(_) | Json::String(_) | Json::Array(_) => Ok(None),
    }
}

fn js_nullish_or(value: Option<&Json>, default: Json) -> Json {
    match value {
        None | Some(Json::Null) => default,
        Some(value) => value.clone(),
    }
}

fn js_truthy(value: &Json) -> bool {
    match value {
        Json::Null | Json::Bool(false) => false,
        Json::Number(number) => {
            let number = js_number_value(number);
            number != 0.0 && !number.is_nan()
        }
        Json::String(value) => !value.is_empty(),
        Json::Bool(true) | Json::Array(_) | Json::Object(_) => true,
    }
}

fn js_strict_true(value: Option<&Json>) -> bool {
    matches!(value, Some(Json::Bool(true)))
}

fn js_string_is(value: &Json, expected: &str) -> bool {
    matches!(value, Json::String(value) if value == expected)
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

fn parse_js_number_string(value: &RuntimeString) -> f64 {
    let value = trim_ecmascript(value);
    if value.is_empty() {
        return 0.0;
    }
    let Some(value) = scalar_string(&value) else {
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

fn js_array_to_string(values: &[Json]) -> Result<RuntimeString, ToolError> {
    let values = values
        .iter()
        .map(|value| match value {
            Json::Null => Ok(RuntimeString::default()),
            _ => js_to_string(value),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(join_runtime(&values, ","))
}

fn js_to_string(value: &Json) -> Result<RuntimeString, ToolError> {
    match value {
        Json::Null => Ok(RuntimeString::from("null")),
        Json::Bool(value) => Ok(RuntimeString::from(value.to_string())),
        Json::Number(number) => Ok(RuntimeString::from(number_to_string(js_number_value(
            number,
        )))),
        Json::String(value) => Ok(value.clone()),
        Json::Array(values) => js_array_to_string(values),
        Json::Object(object) if object.contains_key("toString") => {
            Err(ToolError::new("Cannot convert object to primitive value"))
        }
        Json::Object(_) => Ok(RuntimeString::from("[object Object]")),
    }
}

fn node_inspect_string(value: &RuntimeString) -> String {
    let quote = if !value.contains("'") {
        '\''
    } else if !value.contains("\"") {
        '"'
    } else if !value.contains("`") && !value.contains("${") {
        '`'
    } else {
        '\''
    };
    let mut inspected = String::with_capacity(value.len_utf16() + 2);
    inspected.push(quote);
    for point in value.code_points() {
        let Some(character) = char::from_u32(point) else {
            inspected.push_str(&format!("\\u{point:04x}"));
            continue;
        };
        match character {
            '\0' => inspected.push_str("\\x00"),
            '\u{0008}' => inspected.push_str("\\b"),
            '\t' => inspected.push_str("\\t"),
            '\n' => inspected.push_str("\\n"),
            '\u{000b}' => inspected.push_str("\\v"),
            '\u{000c}' => inspected.push_str("\\f"),
            '\r' => inspected.push_str("\\r"),
            '\\' => inspected.push_str("\\\\"),
            value if value == quote => {
                inspected.push('\\');
                inspected.push(value);
            }
            value if value.is_control() && u32::from(value) <= 0xff => {
                inspected.push_str(&format!("\\x{:02X}", u32::from(value)));
            }
            value if value.is_control() => {
                inspected.push_str(&format!("\\u{{{:x}}}", u32::from(value)));
            }
            value => inspected.push(value),
        }
    }
    inspected.push(quote);
    inspected
}

fn truncate_node_error_message(message: String) -> RuntimeString {
    const MAX_UTF16_UNITS: usize = 200;
    const ELLIPSIS: &str = "...";
    let message = RuntimeString::from(message);
    if message.len_utf16() <= MAX_UTF16_UNITS {
        return message;
    }

    let content_limit = MAX_UTF16_UNITS - ELLIPSIS.len();
    message
        .slice_utf16(..content_limit)
        .concat(&RuntimeString::from(ELLIPSIS))
}

fn validate_node_child_process_args(args: &[RuntimeString]) -> Result<(), ToolError> {
    if let Some((index, value)) = args
        .iter()
        .enumerate()
        .find(|(_, value)| value.contains("\0"))
    {
        return Err(ToolError::new(truncate_node_error_message(format!(
            "The argument 'args[{index}]' must be a string without null bytes. Received {}",
            node_inspect_string(value)
        ))));
    }
    Ok(())
}

fn js_to_number(value: &Json) -> Result<f64, ToolError> {
    Ok(match value {
        Json::Null | Json::Bool(false) => 0.0,
        Json::Bool(true) => 1.0,
        Json::Number(number) => js_number_value(number),
        Json::String(value) => parse_js_number_string(value),
        Json::Array(values) => parse_js_number_string(&js_array_to_string(values)?),
        Json::Object(object) if object.contains_key("toString") => {
            return Err(ToolError::new("Cannot convert object to primitive value"));
        }
        Json::Object(_) => f64::NAN,
    })
}

fn expand_js_path(value: &Json, cwd: &RuntimePath) -> Result<RuntimePath, ToolError> {
    match value {
        Json::String(path) => expand_path(path, cwd),
        Json::Array(values) => {
            if values
                .iter()
                .any(|value| matches!(value, Json::String(value) if value == "\0"))
            {
                Err(ToolError::new("Path contains null bytes"))
            } else {
                Err(ToolError::new("p.trim is not a function"))
            }
        }
        Json::Null => Err(ToolError::new(
            "Cannot read properties of null (reading 'includes')",
        )),
        _ => Err(ToolError::new("p.includes is not a function")),
    }
}

fn system_time_millis(time: std::time::SystemTime) -> f64 {
    match time.duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_secs_f64() * 1000.0,
        Err(error) => -(error.duration().as_secs_f64() * 1000.0),
    }
}

async fn collect_mtimes_with<P, F, Fut>(paths: &[P], stat: F) -> Vec<f64>
where
    P: Clone,
    F: Fn(P) -> Fut,
    Fut: std::future::Future<Output = Option<std::time::SystemTime>>,
{
    let stats = paths.iter().cloned().map(stat).collect::<Vec<_>>();
    futures::future::join_all(stats)
        .await
        .into_iter()
        .map(|modified| modified.map(system_time_millis).unwrap_or(0.0))
        .collect()
}

async fn collect_mtimes(paths: &[RuntimeString]) -> Vec<f64> {
    collect_mtimes_with(paths, |path| async move {
        let path = project_runtime_path(&RuntimePath::from(path), "path").ok()?;
        tokio::fs::metadata(path)
            .await
            .ok()
            .and_then(|metadata| metadata.modified().ok())
    })
    .await
}

enum JsPrimitive {
    Null,
    Bool(bool),
    Number(f64),
    String(RuntimeString),
}

fn js_to_primitive_for_add(value: &Json) -> Result<JsPrimitive, ToolError> {
    Ok(match value {
        Json::Null => JsPrimitive::Null,
        Json::Bool(value) => JsPrimitive::Bool(*value),
        Json::Number(number) => JsPrimitive::Number(js_number_value(number)),
        Json::String(value) => JsPrimitive::String(value.clone()),
        Json::Array(values) => JsPrimitive::String(js_array_to_string(values)?),
        Json::Object(_) => JsPrimitive::String(js_to_string(value)?),
    })
}

fn js_primitive_to_string(value: &JsPrimitive) -> RuntimeString {
    match value {
        JsPrimitive::Null => RuntimeString::from("null"),
        JsPrimitive::Bool(value) => RuntimeString::from(value.to_string()),
        JsPrimitive::Number(value) => RuntimeString::from(number_to_string(*value)),
        JsPrimitive::String(value) => value.clone(),
    }
}

fn js_primitive_to_number(value: &JsPrimitive) -> f64 {
    match value {
        JsPrimitive::Null => 0.0,
        JsPrimitive::Bool(false) => 0.0,
        JsPrimitive::Bool(true) => 1.0,
        JsPrimitive::Number(value) => *value,
        JsPrimitive::String(value) => parse_js_number_string(value),
    }
}

fn js_add_as_number(left: &Json, right: &Json) -> Result<f64, ToolError> {
    let left = js_to_primitive_for_add(left)?;
    let right = js_to_primitive_for_add(right)?;
    if matches!(left, JsPrimitive::String(_)) || matches!(right, JsPrimitive::String(_)) {
        return Ok(parse_js_number_string(
            &js_primitive_to_string(&left).concat(&js_primitive_to_string(&right)),
        ));
    }
    Ok(js_primitive_to_number(&left) + js_primitive_to_number(&right))
}

fn js_strict_number_zero(value: Option<&Json>) -> bool {
    matches!(value, Some(Json::Number(number)) if number.as_f64() == 0.0)
}

fn javascript_number_value(number: f64) -> Json {
    Json::Number(RuntimeNumber::from_f64(number))
}

fn javascript_runtime_value(value: &Json) -> Json {
    match value {
        Json::Number(number) => javascript_number_value(js_number_value(number)),
        Json::Array(values) => Json::Array(values.iter().map(javascript_runtime_value).collect()),
        Json::Object(values) => {
            let mut output = RuntimeObject::new();
            for (key, value) in values.entries() {
                output.insert(key, javascript_runtime_value(value));
            }
            Json::Object(output)
        }
        _ => value.clone(),
    }
}

/// Base-10 `parseInt`: leading whitespace + optional sign, digits until the first
/// non-digit; empty digit run is `NaN` (`None`).
fn parse_int_10(s: &RuntimeString) -> Option<f64> {
    let units = s.as_utf16();
    let start = units
        .iter()
        .position(|unit| !is_ecmascript_whitespace_unit(*unit))
        .unwrap_or(units.len());
    let t = s.slice_utf16(start..);
    let t = scalar_string(&t)?;
    let b = t.as_bytes();
    let mut i = 0;
    let neg = if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
        let n = b[i] == b'-';
        i += 1;
        n
    } else {
        false
    };
    let start = i;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }
    if i == start {
        return None;
    }
    // Parse the complete decimal prefix in one operation. This uses the
    // correctly-rounded decimal-to-binary conversion that JavaScript applies
    // after collecting parseInt's digit run; incremental multiply/add can
    // round to a different adjacent IEEE-754 value.
    let digits = std::str::from_utf8(&b[start..i]).expect("ASCII digit prefix is valid UTF-8");
    let v = digits.parse::<f64>().ok()?;
    Some(if neg { -v } else { v })
}

fn js_slice_index(value: f64, len: usize) -> usize {
    if value.is_nan() || value == f64::NEG_INFINITY {
        return 0;
    }
    if value == f64::INFINITY {
        return len;
    }

    let integer = value.trunc();
    let len_number = len as f64;
    if integer < 0.0 {
        (len_number + integer).clamp(0.0, len_number) as usize
    } else {
        integer.min(len_number) as usize
    }
}

/// Apply JavaScript `Array.prototype.slice` limit + offset behavior.
/// `limit === 0` is the unlimited escape hatch; fractional and negative values
/// retain their original identity in the reported metadata while slice itself
/// applies JavaScript's `ToIntegerOrInfinity` conversion.
#[cfg_attr(not(any(test, feature = "testing")), allow(dead_code))]
pub(crate) fn apply_head_limit<T: Clone>(
    items: Vec<T>,
    limit: Option<f64>,
    offset: f64,
) -> (Vec<T>, Option<f64>) {
    let len = items.len();
    let start = js_slice_index(offset, len);
    if limit == Some(0.0) {
        return (items[start..].to_vec(), None);
    }
    let effective = limit.unwrap_or(DEFAULT_HEAD_LIMIT);
    let end = js_slice_index(offset + effective, len);
    let sliced = if end > start {
        items[start..end].to_vec()
    } else {
        Vec::new()
    };
    let was_truncated = len as f64 - offset > effective;
    (sliced, if was_truncated { Some(effective) } else { None })
}

struct JsPagination<T> {
    items: Vec<T>,
    applied_limit: Option<Json>,
    applied_offset: Option<Json>,
}

fn apply_js_head_limit<T: Clone>(
    items: Vec<T>,
    limit: Option<&Json>,
    offset: &Json,
) -> Result<JsPagination<T>, ToolError> {
    let len = items.len();
    let start = js_slice_index(js_to_number(offset)?, len);
    let applied_offset = (js_to_number(offset)? > 0.0).then(|| offset.clone());

    if js_strict_number_zero(limit) {
        return Ok(JsPagination {
            items: items[start..].to_vec(),
            applied_limit: None,
            applied_offset,
        });
    }

    let effective = match limit {
        None | Some(Json::Null) => runtime_number(DEFAULT_HEAD_LIMIT),
        Some(limit) => limit.clone(),
    };
    let end = js_slice_index(js_add_as_number(offset, &effective)?, len);
    let sliced = if end > start {
        items[start..end].to_vec()
    } else {
        Vec::new()
    };
    let was_truncated = len as f64 - js_to_number(offset)? > js_to_number(&effective)?;
    Ok(JsPagination {
        items: sliced,
        applied_limit: was_truncated.then_some(effective),
        applied_offset,
    })
}

/// "limit: N, offset: M" — omits a part when its value is absent.
#[cfg_attr(not(any(test, feature = "testing")), allow(dead_code))]
fn format_limit_info(applied_limit: Option<f64>, applied_offset: Option<f64>) -> RuntimeString {
    let mut parts: Vec<RuntimeString> = Vec::new();
    if let Some(l) = applied_limit {
        parts.push(RuntimeString::from(format!(
            "limit: {}",
            number_to_string(l)
        )));
    }
    if let Some(o) = applied_offset {
        if o != 0.0 {
            parts.push(RuntimeString::from(format!(
                "offset: {}",
                number_to_string(o)
            )));
        }
    }
    join_runtime(&parts, ", ")
}

fn format_js_limit_info(
    applied_limit: Option<&Json>,
    applied_offset: Option<&Json>,
) -> Result<RuntimeString, ToolError> {
    let mut parts = Vec::new();
    if let Some(limit) = applied_limit {
        parts.push(RuntimeString::from("limit: ").concat(&js_to_string(limit)?));
    }
    if let Some(offset) = applied_offset.filter(|offset| js_truthy(offset)) {
        parts.push(RuntimeString::from("offset: ").concat(&js_to_string(offset)?));
    }
    Ok(join_runtime(&parts, ", "))
}

fn plural(n: i64, word: &str) -> String {
    if n == 1 {
        word.to_string()
    } else {
        format!("{word}s")
    }
}

/// The three output modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GrepMode {
    Content,
    FilesWithMatches,
    Count,
}

/// The structured output of one search, before serialization.
#[derive(Debug)]
pub(crate) struct GrepOutput {
    pub mode: GrepMode,
    pub num_files: i64,
    pub filenames: Vec<RuntimeString>,
    pub content: Option<RuntimeString>,
    pub num_lines: Option<i64>,
    pub num_matches: Option<f64>,
    pub applied_limit: Option<f64>,
    pub applied_offset: Option<f64>,
}

fn render_grep_with_limit_info(o: &GrepOutput, limit_info: &RuntimeString) -> RuntimeString {
    match o.mode {
        GrepMode::Content => {
            let content = o.content.clone().unwrap_or_default();
            let body = if content.is_empty() {
                RuntimeString::from("No matches.")
            } else {
                content
            };
            if !limit_info.is_empty() {
                body.concat(&RuntimeString::from("\n\n(paginated — "))
                    .concat(limit_info)
                    .concat(&RuntimeString::from(")"))
            } else {
                body
            }
        }
        GrepMode::Count => {
            let content = o.content.clone().unwrap_or_default();
            let body = if content.is_empty() {
                RuntimeString::from("No matches.")
            } else {
                content
            };
            let matches = o.num_matches.unwrap_or(0.0);
            let files = o.num_files;
            let extra = if !limit_info.is_empty() {
                RuntimeString::from(" (")
                    .concat(limit_info)
                    .concat(&RuntimeString::from(")"))
            } else {
                RuntimeString::default()
            };
            body.concat(&RuntimeString::from("\n\n"))
                .concat(&RuntimeString::from(number_to_string(matches)))
                .concat(&RuntimeString::from(if matches == 1.0 {
                    " match in "
                } else {
                    " matches in "
                }))
                .concat(&RuntimeString::from(files.to_string()))
                .concat(&RuntimeString::from(" "))
                .concat(&RuntimeString::from(plural(files, "file")))
                .concat(&extra)
        }
        GrepMode::FilesWithMatches => {
            if o.num_files == 0 {
                return RuntimeString::from("No files matched.");
            }
            let extra = if !limit_info.is_empty() {
                RuntimeString::from(" (")
                    .concat(limit_info)
                    .concat(&RuntimeString::from(")"))
            } else {
                RuntimeString::default()
            };
            RuntimeString::from(o.num_files.to_string())
                .concat(&RuntimeString::from(" matching "))
                .concat(&RuntimeString::from(plural(o.num_files, "file")))
                .concat(&extra)
                .concat(&RuntimeString::from("\n"))
                .concat(&join_runtime(&o.filenames, "\n"))
        }
    }
}

/// Render the model-facing text for an output.
#[cfg_attr(not(any(test, feature = "testing")), allow(dead_code))]
pub(crate) fn render_grep(o: &GrepOutput) -> RuntimeString {
    render_grep_with_limit_info(o, &format_limit_info(o.applied_limit, o.applied_offset))
}

fn render_grep_with_js_pagination<T>(
    output: &GrepOutput,
    pagination: &JsPagination<T>,
) -> Result<RuntimeString, ToolError> {
    let limit_info = format_js_limit_info(
        pagination.applied_limit.as_ref(),
        pagination.applied_offset.as_ref(),
    )?;
    Ok(render_grep_with_limit_info(output, &limit_info))
}

/// Serialize a [`GrepOutput`] into the tool's structured output shape.
pub(crate) fn grep_output_json(o: &GrepOutput) -> Json {
    let mut m = RuntimeObject::new();
    match o.mode {
        GrepMode::Content => {
            m.insert("mode", Json::String(RuntimeString::from("content")));
            m.insert("numFiles", runtime_number(0.0));
            m.insert("filenames", Json::Array(Vec::new()));
            m.insert(
                "content",
                Json::String(o.content.clone().unwrap_or_default()),
            );
            m.insert("numLines", runtime_number(o.num_lines.unwrap_or(0) as f64));
        }
        GrepMode::Count => {
            m.insert("mode", Json::String(RuntimeString::from("count")));
            m.insert("numFiles", runtime_number(o.num_files as f64));
            m.insert("filenames", Json::Array(Vec::new()));
            m.insert(
                "content",
                Json::String(o.content.clone().unwrap_or_default()),
            );
            m.insert(
                "numMatches",
                javascript_number_value(o.num_matches.unwrap_or(0.0)),
            );
        }
        GrepMode::FilesWithMatches => {
            m.insert(
                "mode",
                Json::String(RuntimeString::from("files_with_matches")),
            );
            m.insert("numFiles", runtime_number(o.num_files as f64));
            m.insert(
                "filenames",
                Json::Array(o.filenames.iter().cloned().map(Json::String).collect()),
            );
        }
    }
    if let Some(l) = o.applied_limit {
        m.insert("appliedLimit", javascript_number_value(l));
    }
    if let Some(off) = o.applied_offset {
        m.insert("appliedOffset", javascript_number_value(off));
    }
    Json::Object(m)
}

fn grep_output_json_with_js_pagination<T>(
    output: &GrepOutput,
    pagination: &JsPagination<T>,
) -> Json {
    let mut value = grep_output_json(output);
    let object = value
        .as_object_mut()
        .expect("Grep output serialization always returns an object");
    if let Some(limit) = &pagination.applied_limit {
        object.insert("appliedLimit", javascript_runtime_value(limit));
    }
    if let Some(offset) = &pagination.applied_offset {
        object.insert("appliedOffset", javascript_runtime_value(offset));
    }
    value
}

/// Relativize the `path:` prefix of a content-mode rg line, using `to_rel`.
/// Splits at the field separator `:`, but starts PAST a leading Windows drive
/// letter (`C:\…`) so the split lands on the line-number colon, not the drive
/// colon. Context lines (no field-separator colon) pass through unchanged.
#[cfg(test)]
fn relativize_content_line_with(
    line: &str,
    cwd: &str,
    to_rel: impl Fn(&str, &str) -> String,
) -> String {
    let b = line.as_bytes();
    let start = if b.len() >= 2 && b[0].is_ascii_alphabetic() && b[1] == b':' {
        2
    } else {
        0
    };
    match line[start..].find(':') {
        Some(rel) => {
            let i = start + rel;
            if i > 0 {
                format!("{}{}", to_rel(&line[..i], cwd), &line[i..])
            } else {
                line.to_string()
            }
        }
        None => line.to_string(),
    }
}

/// Relativize a content-mode line against `cwd` without projecting either the
/// path prefix or the match payload out of exact UTF-16.
pub fn relativize_content_line(line: &RuntimeString, cwd: &RuntimePath) -> RuntimeString {
    let units = line.as_utf16();
    let start = if units.len() >= 2
        && ((u16::from(b'A')..=u16::from(b'Z')).contains(&units[0])
            || (u16::from(b'a')..=u16::from(b'z')).contains(&units[0]))
        && units[1] == u16::from(b':')
    {
        2
    } else {
        0
    };
    match units[start..]
        .iter()
        .position(|unit| *unit == u16::from(b':'))
    {
        Some(relative) if start + relative > 0 => {
            let index = start + relative;
            to_relative_path(&RuntimePath::from(line.slice_utf16(..index)), cwd)
                .into_runtime_string()
                .concat(&line.slice_utf16(index..))
        }
        _ => line.clone(),
    }
}

/// Split a `glob` parameter into individual ripgrep `--glob` patterns:
/// space-separated; a token containing both `{` and `}` is kept atomic,
/// otherwise it is further split on commas. Empty tokens are dropped.
pub(crate) fn split_glob_patterns(glob: &RuntimeString) -> Vec<RuntimeString> {
    let mut out = Vec::new();
    let units = glob.as_utf16();
    let mut token_start = 0;
    for token_end in 0..=units.len() {
        if token_end != units.len() && !is_ecmascript_whitespace_unit(units[token_end]) {
            continue;
        }
        let raw = glob.slice_utf16(token_start..token_end);
        token_start = token_end.saturating_add(1);
        if raw.contains("{") && raw.contains("}") {
            if !raw.is_empty() {
                out.push(raw);
            }
        } else {
            let raw_units = raw.as_utf16();
            let mut part_start = 0;
            for part_end in 0..=raw_units.len() {
                if part_end != raw_units.len() && raw_units[part_end] != u16::from(b',') {
                    continue;
                }
                let part = raw.slice_utf16(part_start..part_end);
                part_start = part_end.saturating_add(1);
                if !part.is_empty() {
                    out.push(part);
                }
            }
        }
    }
    out
}

/// The `grep` content-search tool.
pub struct GrepTool {
    input_schema: JsonSchema,
}

impl GrepTool {
    fn new() -> Self {
        GrepTool {
            input_schema: build_input_schema(),
        }
    }
}

/// Construct the `grep` tool as an erased registry-side [`Tool`].
pub fn create_grep_tool() -> Arc<dyn Tool> {
    Arc::new(GrepTool::new())
}

/// Build the input JSON Schema the model sees.
fn build_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "pattern": {
                "type": "string",
                "description": "Regular expression to search for in file contents (ripgrep / Rust-regex syntax)."
            },
            "path": {
                "description": "File or directory to search in (rg PATH). Omit to use the current working directory.",
                "type": "string"
            },
            "glob": {
                "description": "Glob filter for which files to search, e.g. \"*.ts\" or \"*.{ts,tsx}\" (rg --glob). Space/comma-separated.",
                "type": "string"
            },
            "output_mode": {
                "description": "\"files_with_matches\" (default) returns matching file paths; \"content\" returns matching lines (supports -A/-B/-C, -n, head_limit); \"count\" returns per-file match counts.",
                "type": "string",
                "enum": ["content", "files_with_matches", "count"]
            },
            "-B": {
                "description": "Lines of context BEFORE each match (content mode only).",
                "type": "number"
            },
            "-A": {
                "description": "Lines of context AFTER each match (content mode only).",
                "type": "number"
            },
            "-C": {
                "description": "Lines of context before AND after each match (alias of context).",
                "type": "number"
            },
            "context": {
                "description": "Lines of context before and after each match (content mode only).",
                "type": "number"
            },
            "-n": {
                "description": "Show line numbers (content mode only). Default true.",
                "type": "boolean"
            },
            "-i": {
                "description": "Case-insensitive search.",
                "type": "boolean"
            },
            "type": {
                "description": "Restrict to a ripgrep file type, e.g. \"js\", \"py\", \"rust\" (rg --type).",
                "type": "string"
            },
            "head_limit": {
                "description": "Cap output to the first N lines/entries (across all modes). Default 250. Pass 0 for unlimited.",
                "type": "number"
            },
            "offset": {
                "description": "Skip the first N lines/entries before head_limit. Default 0.",
                "type": "number"
            },
            "multiline": {
                "description": "Allow . to match newlines and patterns to span lines (rg -U --multiline-dotall). Default false.",
                "type": "boolean"
            },
            "-o": {
                "description": "Print only the matched part of each line instead of the whole line (content mode only). Default false.",
                "type": "boolean"
            }
        },
        "required": ["pattern"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .expect("grep schema has an object root")
}

#[async_trait]
impl Tool for GrepTool {
    fn static_name(&self) -> &str {
        "grep"
    }

    fn static_description(&self) -> &str {
        GREP_DESCRIPTION
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn input_schema_property_order(
        &self,
        object_path: &[String],
    ) -> Option<&'static [&'static str]> {
        object_path
            .is_empty()
            .then_some(INPUT_SCHEMA_PROPERTY_ORDER)
    }

    fn max_result_size_chars(&self) -> Option<u64> {
        Some(20_000)
    }

    fn preprocess_input(&self, input: &Json) -> Json {
        preprocess_grep_input(input)
    }

    fn parse_input(&self, input: &Json) -> Result<Json, ToolError> {
        let overflowing = overflowing_semantic_numbers(input);
        let preprocessed = self.preprocess_input(input);
        let schema = self.input_schema();
        match crate::tools::input_parser::parse_native_input(&schema, &preprocessed, &|path| {
            self.input_schema_property_order(path)
        }) {
            Ok(parsed) => Ok(parsed),
            Err(mut error) => {
                for key in overflowing {
                    error.message = error.message.replace(
                        &format!("{key}: Invalid input: expected number, received string"),
                        &format!("{key}: Invalid input: expected number, received number"),
                    );
                }
                Err(error)
            }
        }
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

    fn permission_path_kind(&self) -> Option<PermissionPathKind> {
        Some(PermissionPathKind::Read)
    }

    fn get_path(&self, input: &Json) -> Option<PathTargets> {
        match input.get("path")? {
            Json::String(path) => Some(PathTargets::One(RuntimePath::from(path.clone()))),
            Json::Array(paths) => paths
                .iter()
                .map(|path| path.as_str().cloned().map(RuntimePath::from))
                .collect::<Option<Vec<_>>>()
                .map(PathTargets::Many),
            _ => None,
        }
    }

    fn get_raw_path(&self, input: &Json) -> Option<Json> {
        input.get("path").cloned()
    }

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let pattern = input
            .get("pattern")
            .and_then(Json::as_str)
            .cloned()
            .unwrap_or_default();
        Some(RuntimeString::from("Searching for ").concat(&pattern))
    }

    async fn validate_input(&self, input: &Json, ctx: &ToolContext) -> Option<ValidationResult> {
        let path = match input.get("path").and_then(Json::as_str) {
            Some(p) => p,
            None => return Some(ValidationResult::Ok),
        };
        if path.contains("\0") {
            return Some(ValidationResult::Err {
                message: "Path contains null bytes".to_string(),
                error_code: Some(3),
            });
        }
        let abs = match expand_path(path, &ctx.cwd) {
            Ok(a) => a,
            Err(_) => {
                return Some(ValidationResult::Err {
                    message: "Path contains null bytes".to_string(),
                    error_code: Some(3),
                })
            }
        };
        if abs.as_runtime_string().starts_with("\\\\") || abs.as_runtime_string().starts_with("//")
        {
            return Some(ValidationResult::Ok);
        }
        let projected = match project_runtime_path(&abs, "path") {
            Ok(path) => path,
            Err(error) => {
                return Some(ValidationResult::Fatal {
                    error: ToolError::fatal(ui_string(error.message())),
                })
            }
        };
        let pinned_snapshot = match &ctx.permission.pinned_memory_root {
            Some(root) => match root.snapshot_search(&projected) {
                Ok(snapshot) => Some(snapshot),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Some(ValidationResult::Err {
                        message: format!(
                            "Path does not exist: {}. Paths are resolved against the current working directory: {}.",
                            ui_string(path),
                            ui_string(ctx.cwd.as_runtime_string())
                        ),
                        error_code: Some(1),
                    })
                }
                Err(error) => {
                    return Some(ValidationResult::Fatal {
                        error: ToolError::fatal(format!(
                            "pinned memory grep validation failed: {error}"
                        )),
                    })
                }
            },
            None => None,
        };
        let projected = pinned_snapshot
            .as_ref()
            .map(|snapshot| snapshot.path())
            .unwrap_or(projected.as_path());
        match tokio::fs::metadata(projected).await {
            Ok(_) => Some(ValidationResult::Ok),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Some(ValidationResult::Err {
                message: format!(
                    "Path does not exist: {}. Paths are resolved against the current working directory: {}.",
                    ui_string(path),
                    ui_string(ctx.cwd.as_runtime_string())
                ),
                error_code: Some(1),
            }),
            Err(error) => Some(ValidationResult::Fatal {
                error: ToolError::fatal(ui_string(&node_stat_error_message(&error, &abs))),
            }),
        }
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let output_mode = js_nullish_or(
            js_property(&input, "output_mode")?,
            Json::String(RuntimeString::from("files_with_matches")),
        );
        let show_line_numbers = match js_property(&input, "-n")? {
            None | Some(Json::Null) => true,
            Some(value) => js_truthy(value),
        };
        let case_insensitive = match js_property(&input, "-i")? {
            None | Some(Json::Null) => false,
            Some(value) => js_truthy(value),
        };
        let offset = js_nullish_or(js_property(&input, "offset")?, runtime_number(0.0));
        let head_limit = js_property(&input, "head_limit")?.cloned();
        let path_property = js_property(&input, "path")?;
        // An explicit path pins the search to it; NO path defaults to the
        // workspace roots (cwd + extras) below.
        let path_explicit = !matches!(path_property, None | Some(Json::Null));
        let absolute_path = match path_property {
            None | Some(Json::Null) => ctx.cwd.clone(),
            Some(path) => expand_js_path(path, &ctx.cwd)?,
        };
        let mut context_args = Vec::<(RuntimeString, RuntimeString)>::new();
        if js_string_is(&output_mode, "content") {
            let c = js_property(&input, "context")?;
            let c_dash = js_property(&input, "-C")?;
            let before = js_property(&input, "-B")?;
            let after = js_property(&input, "-A")?;
            if let Some(c) = c {
                context_args.push((RuntimeString::from("-C"), js_to_string(c)?));
            } else if let Some(cd) = c_dash {
                context_args.push((RuntimeString::from("-C"), js_to_string(cd)?));
            } else {
                if let Some(b) = before {
                    context_args.push((RuntimeString::from("-B"), js_to_string(b)?));
                }
                if let Some(a) = after {
                    context_args.push((RuntimeString::from("-A"), js_to_string(a)?));
                }
            }
        }
        let pattern = match js_property(&input, "pattern")? {
            Some(Json::String(pattern)) => pattern.clone(),
            None => {
                return Err(ToolError::new(
                    "Cannot read properties of undefined (reading 'startsWith')",
                ))
            }
            Some(Json::Null) => {
                return Err(ToolError::new(
                    "Cannot read properties of null (reading 'startsWith')",
                ))
            }
            Some(_) => return Err(ToolError::new("input.pattern.startsWith is not a function")),
        };

        let mut args: Vec<RuntimeString> = vec![RuntimeString::from("--hidden")];
        for dir in VCS_DIRS {
            args.push(RuntimeString::from("--glob"));
            args.push(RuntimeString::from(format!("!{dir}")));
        }
        // Drop long base64/minified lines instead of flooding the result.
        args.push(RuntimeString::from("--max-columns"));
        args.push(RuntimeString::from("500"));
        if js_strict_true(js_property(&input, "multiline")?) {
            args.push(RuntimeString::from("-U"));
            args.push(RuntimeString::from("--multiline-dotall"));
        }
        if case_insensitive {
            args.push(RuntimeString::from("-i"));
        }
        if js_string_is(&output_mode, "files_with_matches") {
            args.push(RuntimeString::from("-l"));
        } else if js_string_is(&output_mode, "count") {
            // `-H` forces the `path:` prefix even for a single-FILE search.
            args.push(RuntimeString::from("-c"));
            args.push(RuntimeString::from("-H"));
        }
        if show_line_numbers && js_string_is(&output_mode, "content") {
            args.push(RuntimeString::from("-n"));
        }
        if js_strict_true(js_property(&input, "-o")?) && js_string_is(&output_mode, "content") {
            args.push(RuntimeString::from("-o"));
        }
        for (flag, value) in context_args {
            args.push(flag);
            args.push(value);
        }
        if pattern.starts_with("-") {
            args.push(RuntimeString::from("-e"));
            args.push(pattern.clone());
        } else {
            args.push(pattern);
        }
        let mut delayed_type = None;
        if let Some(tool_type) = js_property(&input, "type")?.filter(|value| js_truthy(value)) {
            args.push(RuntimeString::from("--type"));
            match tool_type {
                Json::String(value) => args.push(value.clone()),
                value => {
                    let value_index = args.len();
                    args.push(RuntimeString::default());
                    delayed_type = Some((value_index, value.clone()));
                }
            }
        }
        if let Some(glob) = js_property(&input, "glob")?.filter(|glob| js_truthy(glob)) {
            let Json::String(glob) = glob else {
                return Err(ToolError::new(
                    "glob.split is not a function or its return value is not iterable",
                ));
            };
            for pattern in split_glob_patterns(glob) {
                args.push(RuntimeString::from("--glob"));
                args.push(pattern);
            }
        }
        // Node prevalidates every already-string child-process argument for
        // NUL before coercing non-string values. The trusted `type` value is
        // the only delayed coercion here, so validate the complete string
        // array first and the coerced final array once more afterward.
        validate_node_child_process_args(&args)?;
        if let Some((index, value)) = delayed_type {
            args[index] = js_to_string(&value)?;
        }
        validate_node_child_process_args(&args)?;

        let projected_search_path = project_runtime_path(&absolute_path, "path")
            .map_err(|error| ToolError::new(ui_string(error.message())))?;
        let pinned_search =
            match &ctx.permission.pinned_memory_root {
                Some(root) => Some(root.snapshot_search(&projected_search_path).map_err(
                    |error| ToolError::new(format!("pinned memory grep failed: {error}")),
                )?),
                None => None,
            };
        // Search targets: a memory-pinned session uses its snapshot root; an
        // explicit `path` uses just that path; otherwise the DEFAULT covers the
        // whole workspace (cwd + additional writable roots), so a multi-folder
        // workspace searches every folder in one ripgrep pass.
        let rg_targets: Vec<RuntimePath> = match &pinned_search {
            Some(snapshot) => vec![snapshot.runtime_path()],
            None if path_explicit => vec![absolute_path.clone()],
            None => default_search_roots(&ctx.cwd, &ctx.permission.additional_writable_roots),
        };
        let results = match run_task_ripgrep(ctx, &args, &rg_targets).await {
            Ok(r) => r,
            Err(RipgrepError::Unavailable(msg)) => {
                return Ok(text_result(
                    runtime_object([
                        ("numFiles", runtime_number(0.0)),
                        ("filenames", Json::Array(Vec::new())),
                    ]),
                    msg,
                    true,
                ))
            }
            Err(RipgrepError::Timeout { message, .. }) => {
                return Ok(text_result(
                    runtime_object([
                        ("numFiles", runtime_number(0.0)),
                        ("filenames", Json::Array(Vec::new())),
                    ]),
                    message,
                    true,
                ))
            }
            Err(RipgrepError::Spawn(e)) => return Err(ToolError::new(e.to_string())),
        };
        let results = match &pinned_search {
            Some(snapshot) => results
                .into_iter()
                .map(|result| snapshot.restore_search_output(&result))
                .collect(),
            None => results,
        };

        if js_string_is(&output_mode, "content") {
            let pagination = apply_js_head_limit(results, head_limit.as_ref(), &offset)?;
            let lines: Vec<RuntimeString> = pagination
                .items
                .iter()
                .map(|line| relativize_content_line(line, &ctx.cwd))
                .collect();
            let output = GrepOutput {
                mode: GrepMode::Content,
                num_files: 0,
                filenames: Vec::new(),
                content: Some(join_runtime(&lines, "\n")),
                num_lines: Some(lines.len() as i64),
                num_matches: None,
                applied_limit: None,
                applied_offset: None,
            };
            let text = render_grep_with_js_pagination(&output, &pagination)?;
            return Ok(text_result(
                grep_output_json_with_js_pagination(&output, &pagination),
                text,
                false,
            ));
        }

        if js_string_is(&output_mode, "count") {
            let pagination = apply_js_head_limit(results, head_limit.as_ref(), &offset)?;
            // Each entry is `path:count`; relativize by splitting at the LAST ':'.
            let lines: Vec<RuntimeString> = pagination
                .items
                .iter()
                .map(|line| {
                    match line
                        .as_utf16()
                        .iter()
                        .rposition(|unit| *unit == u16::from(b':'))
                    {
                        Some(index) if index > 0 => to_relative_path(
                            &RuntimePath::from(line.slice_utf16(..index)),
                            &ctx.cwd,
                        )
                        .into_runtime_string()
                        .concat(&line.slice_utf16(index..)),
                        _ => line.clone(),
                    }
                })
                .collect();
            let mut total_matches = 0.0;
            let mut file_count: i64 = 0;
            for line in &lines {
                if let Some(index) = line
                    .as_utf16()
                    .iter()
                    .rposition(|unit| *unit == u16::from(b':'))
                {
                    if index > 0 {
                        if let Some(n) = parse_int_10(&line.slice_utf16(index + 1..)) {
                            total_matches += n;
                            file_count += 1;
                        }
                    }
                }
            }
            let output = GrepOutput {
                mode: GrepMode::Count,
                num_files: file_count,
                filenames: Vec::new(),
                content: Some(join_runtime(&lines, "\n")),
                num_lines: None,
                num_matches: Some(total_matches),
                applied_limit: None,
                applied_offset: None,
            };
            let text = render_grep_with_js_pagination(&output, &pagination)?;
            return Ok(text_result(
                grep_output_json_with_js_pagination(&output, &pagination),
                text,
                false,
            ));
        }

        // files_with_matches (default): sort by mtime newest-first (filename
        // tiebreaker); a failed stat sorts as mtime 0.
        let mtimes = collect_mtimes(&results).await;
        // Deterministic filename order under test (keys off NODE_ENV).
        let node_test = std::env::var("NODE_ENV").ok().as_deref() == Some("test");
        let mut paired: Vec<(RuntimeString, f64)> = results.into_iter().zip(mtimes).collect();
        // Locale-sort first, then use a stable mtime sort. Equal mtimes retain
        // the locale order, exactly matching `b.mtime - a.mtime ||
        // a.file.localeCompare(b.file)` without crossing into V8 per compare.
        sort_runtime_by_key(&mut paired, |(path, _)| path);
        if !node_test {
            paired.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        }
        let sorted: Vec<RuntimeString> = paired.into_iter().map(|(p, _)| p).collect();
        let pagination = apply_js_head_limit(sorted, head_limit.as_ref(), &offset)?;
        let filenames: Vec<RuntimeString> = pagination
            .items
            .iter()
            .map(|path| {
                to_relative_path(&RuntimePath::from(path.clone()), &ctx.cwd).into_runtime_string()
            })
            .collect();
        let output = GrepOutput {
            mode: GrepMode::FilesWithMatches,
            num_files: filenames.len() as i64,
            filenames,
            content: None,
            num_lines: None,
            num_matches: None,
            applied_limit: None,
            applied_offset: None,
        };
        let text = render_grep_with_js_pagination(&output, &pagination)?;
        Ok(text_result(
            grep_output_json_with_js_pagination(&output, &pagination),
            text,
            false,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::runtime_json;
    use crate::tools::builtins::test_ctx::make_ctx;
    use serde_json::Value;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn runtime_from_serde(value: Value) -> RuntimeJson {
        match value {
            Value::Null => RuntimeJson::Null,
            Value::Bool(value) => RuntimeJson::Bool(value),
            Value::Number(value) => {
                RuntimeJson::Number(RuntimeNumber::from_f64(value.as_f64().unwrap()))
            }
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

    fn rs(value: impl Into<RuntimeString>) -> RuntimeString {
        value.into()
    }

    fn temp_dir() -> PathBuf {
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-grep-tool-{}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        dir.push(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn rg_available() -> bool {
        std::process::Command::new("rg")
            .arg("--version")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    fn text_of(res: &ToolResult) -> String {
        match &res.serializable.model_content[0] {
            crate::schemas::content::OutputBlock::Text(t) => ui_string(&t.text),
            _ => panic!("expected a text block"),
        }
    }

    fn tool_error_of(result: Result<ToolResult, ToolError>) -> ToolError {
        match result {
            Ok(_) => panic!("expected tool error"),
            Err(error) => error,
        }
    }

    fn assert_infinity_marker(value: &Json, negative: bool) {
        let value = value.as_f64().expect("marker must remain a number");
        assert!(!value.is_finite(), "marker became finite: {value}");
        assert_eq!(value.is_sign_negative(), negative, "{value}");
    }

    struct EnvVarGuard {
        name: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl EnvVarGuard {
        fn set(name: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(name);
            crate::runtime_process::set_process_environment_var(name, value);
            Self { name, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match self.previous.take() {
                Some(value) => {
                    crate::runtime_process::set_process_environment_var(self.name, value)
                }
                None => crate::runtime_process::remove_process_environment_var(self.name),
            }
        }
    }

    // ---- pure logic ----

    #[test]
    fn exposes_expected_metadata() {
        let tool = create_grep_tool();
        assert_eq!(tool.name(), "grep");
        assert_eq!(tool.description(), GREP_DESCRIPTION);
        assert_eq!(tool.permission_path_kind(), Some(PermissionPathKind::Read));
        assert_eq!(tool.max_result_size_chars(), Some(20_000));
        let input = rjson!({ "pattern": "x" });
        assert!(tool.is_read_only(&input));
        assert!(tool.is_concurrency_safe(&input));
        assert!(tool.is_enabled());
        assert_eq!(tool.is_destructive(&input), Some(false));
        assert_eq!(tool.summarize(&input), Some(rs("Searching for x")));
        assert_eq!(
            tool.get_path(&rjson!({ "pattern": "x", "path": "src" })),
            Some(PathTargets::One(RuntimePath::from("src")))
        );
        assert_eq!(tool.get_path(&rjson!({ "pattern": "x" })), None);
    }

    #[test]
    fn summary_preserves_lone_surrogate_pattern() {
        let input = runtime_json::parse(&RuntimeString::from(r#"{"pattern":"\ud800"}"#)).unwrap();
        let summary = create_grep_tool().summarize(&input).unwrap();
        assert_eq!(
            summary,
            RuntimeString::from("Searching for ").concat(&RuntimeString::from_utf16(vec![0xd800]))
        );
    }

    #[test]
    fn permission_path_targets_preserve_exact_string_arrays() {
        let tool = create_grep_tool();
        assert_eq!(
            tool.get_path(&rjson!({ "pattern": "x", "path": [] })),
            Some(PathTargets::Many(Vec::new()))
        );
        assert_eq!(
            tool.get_path(&rjson!({
                "pattern": "x",
                "path": ["inside", "/outside/root"]
            })),
            Some(PathTargets::Many(vec![
                RuntimePath::from("inside"),
                RuntimePath::from("/outside/root")
            ]))
        );
    }

    #[test]
    fn permission_paths_keep_surrogate_and_replacement_identities_distinct() {
        let tool = create_grep_tool();
        let input = |path: RuntimeString| {
            runtime_object([
                ("pattern", Json::String(rs("x"))),
                ("path", Json::String(path)),
            ])
        };
        let high = tool.get_path(&input(RuntimeString::from_utf16(vec![0xd800])));
        let low = tool.get_path(&input(RuntimeString::from_utf16(vec![0xdc00])));
        let replacement = tool.get_path(&input(rs("\u{fffd}")));
        assert_ne!(high, low);
        assert_ne!(high, replacement);
        assert_ne!(low, replacement);
    }

    #[test]
    fn permission_raw_path_preserves_trusted_non_string_values() {
        let tool = create_grep_tool();
        assert_eq!(tool.get_raw_path(&rjson!({ "pattern": "x" })), None);
        assert_eq!(
            tool.get_raw_path(&rjson!({ "pattern": "x", "path": null })),
            Some(Json::Null)
        );
        assert_eq!(
            tool.get_raw_path(&rjson!({
                "pattern": "x",
                "path": ["inside", null, 7, { "nested": true }]
            })),
            Some(rjson!(["inside", null, 7, { "nested": true }]))
        );
    }

    #[tokio::test]
    async fn metadata_collection_starts_every_stat_before_waiting_for_results() {
        let paths = vec![
            "first".to_string(),
            "second".to_string(),
            "third".to_string(),
        ];
        let barrier = Arc::new(tokio::sync::Barrier::new(paths.len() + 1));
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let task_barrier = Arc::clone(&barrier);
        let task = tokio::spawn(async move {
            collect_mtimes_with(&paths, move |path| {
                let barrier = Arc::clone(&task_barrier);
                let started_tx = started_tx.clone();
                async move {
                    started_tx.send(path).unwrap();
                    barrier.wait().await;
                    Some(std::time::UNIX_EPOCH)
                }
            })
            .await
        });

        let mut started = Vec::new();
        for _ in 0..3 {
            match tokio::time::timeout(std::time::Duration::from_secs(2), started_rx.recv()).await {
                Ok(Some(path)) => started.push(path),
                other => {
                    task.abort();
                    panic!("all stat futures must start concurrently, got {started:?}: {other:?}");
                }
            }
        }
        assert_eq!(started, ["first", "second", "third"]);
        barrier.wait().await;
        assert_eq!(task.await.unwrap(), [0.0, 0.0, 0.0]);
    }

    #[test]
    fn input_schema_shape() {
        let tool = create_grep_tool();
        let schema = tool.static_input_schema();
        assert_eq!(schema.get("type").and_then(Value::as_str), Some("object"));
        assert_eq!(schema.get("required").unwrap(), &json!(["pattern"]));
        assert_eq!(schema.get("additionalProperties").unwrap(), &json!(false));
        let props = schema.get("properties").unwrap();
        assert_eq!(
            props.get("output_mode").unwrap().get("enum").unwrap(),
            &json!(["content", "files_with_matches", "count"])
        );
        assert_eq!(
            props.get("-B").unwrap().get("type").and_then(Value::as_str),
            Some("number")
        );
        assert_eq!(
            props.get("-n").unwrap().get("type").and_then(Value::as_str),
            Some("boolean")
        );
    }

    #[test]
    fn relativize_content_line_cases() {
        let mark = |abs: &str, _cwd: &str| format!("<{abs}>");
        assert_eq!(
            relativize_content_line_with("src/a.ts:5:hello", "/cwd", mark),
            "<src/a.ts>:5:hello"
        );
        assert_eq!(
            relativize_content_line_with("C:\\proj\\a.ts:5:hello", "/cwd", mark),
            "<C:\\proj\\a.ts>:5:hello"
        );
        assert_eq!(
            relativize_content_line_with("src/a.ts-4-context", "/cwd", mark),
            "src/a.ts-4-context"
        );
    }

    #[test]
    fn split_glob_patterns_space_and_comma() {
        assert_eq!(split_glob_patterns(&rs("*.ts")), vec!["*.ts"]);
        assert_eq!(split_glob_patterns(&rs("*.ts *.js")), vec!["*.ts", "*.js"]);
        assert_eq!(split_glob_patterns(&rs("*.ts,*.js")), vec!["*.ts", "*.js"]);
        // A brace token is kept atomic.
        assert_eq!(split_glob_patterns(&rs("*.{ts,tsx}")), vec!["*.{ts,tsx}"]);
        assert_eq!(split_glob_patterns(&rs("a, ,b")), vec!["a", "b"]);
        assert_eq!(
            split_glob_patterns(&rs("*.rs\u{0085}*.md")),
            vec!["*.rs\u{0085}*.md"]
        );
        assert_eq!(
            split_glob_patterns(&rs("*.rs\u{feff}*.md")),
            vec!["*.rs", "*.md"]
        );
    }

    #[test]
    fn count_parse_int_uses_ecmascript_whitespace_and_javascript_overflow() {
        assert_eq!(parse_int_10(&rs("\u{feff}42tail")), Some(42.0));
        assert_eq!(parse_int_10(&rs("\u{0085}42tail")), None);
        let negative_zero = parse_int_10(&rs("  -0x10")).unwrap();
        assert_eq!(negative_zero, 0.0);
        assert!(negative_zero.is_sign_negative());

        let positive = parse_int_10(&rs("9".repeat(400))).unwrap();
        let negative = parse_int_10(&rs(format!("-{}", "9".repeat(400)))).unwrap();
        assert_eq!(number_to_string(positive), "Infinity");
        assert_eq!(number_to_string(negative), "-Infinity");

        // Number.parseInt parses the complete digit prefix as one decimal
        // number. Repeated floating-point multiply/add is not correctly
        // rounded for every integer of this width.
        assert_eq!(
            number_to_string(parse_int_10(&rs("85723949061807009")).unwrap()),
            "85723949061807000"
        );
    }

    #[test]
    fn head_limit_and_format() {
        let items: Vec<String> = (0..10).map(|i| format!("h{i}")).collect();
        let (out, applied) = apply_head_limit(items.clone(), Some(3.0), 0.0);
        assert_eq!(out, vec!["h0", "h1", "h2"]);
        assert_eq!(applied, Some(3.0));
        // offset
        let (out, applied) = apply_head_limit(items.clone(), Some(2.0), 2.0);
        assert_eq!(out, vec!["h2", "h3"]);
        assert_eq!(applied, Some(2.0));
        // unlimited escape hatch
        let (out, applied) = apply_head_limit(items.clone(), Some(0.0), 0.0);
        assert_eq!(out.len(), 10);
        assert_eq!(applied, None);
        // no truncation
        let (out, applied) = apply_head_limit(items, Some(100.0), 0.0);
        assert_eq!(out.len(), 10);
        assert_eq!(applied, None);
        assert_eq!(format_limit_info(Some(3.0), None), "limit: 3");
        assert_eq!(
            format_limit_info(Some(2.0), Some(2.0)),
            "limit: 2, offset: 2"
        );
        assert_eq!(format_limit_info(None, Some(0.0)), "");
    }

    #[test]
    fn head_limit_uses_javascript_slice_number_semantics() {
        let items: Vec<String> = (0..10).map(|i| format!("h{i}")).collect();

        let (out, applied) = apply_head_limit(items.clone(), Some(0.5), 0.0);
        assert!(out.is_empty());
        assert_eq!(applied, Some(0.5));

        let (out, applied) = apply_head_limit(items.clone(), Some(-1.0), 0.0);
        assert_eq!(out, items[..9]);
        assert_eq!(applied, Some(-1.0));

        let (out, applied) = apply_head_limit(items.clone(), Some(1.5), 1.5);
        assert_eq!(out, vec!["h1", "h2"]);
        assert_eq!(applied, Some(1.5));

        let (out, applied) = apply_head_limit(items.clone(), Some(0.0), -2.0);
        assert_eq!(out, vec!["h8", "h9"]);
        assert_eq!(applied, None);

        let (out, applied) = apply_head_limit(items, Some(f64::MAX), f64::MAX);
        assert!(out.is_empty());
        assert_eq!(applied, None);

        assert_eq!(
            format_limit_info(Some(0.5), Some(1.5)),
            "limit: 0.5, offset: 1.5"
        );
    }

    #[test]
    fn grep_output_metadata_uses_javascript_number_identity() {
        let output = GrepOutput {
            mode: GrepMode::FilesWithMatches,
            num_files: 1,
            filenames: vec![rs("a.rs")],
            content: None,
            num_lines: None,
            num_matches: None,
            applied_limit: Some(1.0),
            applied_offset: Some(1.5),
        };
        assert_eq!(grep_output_json(&output)["appliedLimit"], rjson!(1));
        assert_eq!(grep_output_json(&output)["appliedOffset"], rjson!(1.5));

        let positive_infinity = Json::Number(RuntimeNumber::from_f64(f64::INFINITY));
        let negative_infinity = Json::Number(RuntimeNumber::from_f64(f64::NEG_INFINITY));
        let projected = javascript_runtime_value(&runtime_object([
            ("string", Json::String(rs("1e400"))),
            (
                "array",
                Json::Array(vec![positive_infinity, negative_infinity]),
            ),
        ]));
        assert_eq!(projected["string"], rjson!("1e400"));
        assert!(projected["array"].is_array());
        assert!(projected["array"][0].is_number());
        assert_eq!(projected["array"][0].as_f64(), Some(f64::INFINITY));
        assert!(projected["array"][1].is_number());
        assert_eq!(projected["array"][1].as_f64(), Some(f64::NEG_INFINITY));
    }

    #[test]
    fn semantic_coercion_helpers() {
        let input = rjson!({ "n": "5", "b": "true", "f": "false", "raw": 7, "bad": "x" });
        assert_eq!(read_number(&input, "n"), Some(5.0));
        assert_eq!(read_number(&input, "raw"), Some(7.0));
        assert_eq!(read_number(&input, "bad"), None);
        assert_eq!(read_bool(&input, "b"), Some(true));
        assert_eq!(read_bool(&input, "f"), Some(false));
        assert_eq!(read_bool(&input, "raw"), None);
    }

    #[test]
    fn semantic_number_uses_ecmascript_trim() {
        let tool = create_grep_tool();
        let parsed = tool
            .parse_input(&rjson!({
                "pattern": "x",
                "head_limit": "\u{feff}0008.50\u{feff}"
            }))
            .unwrap();
        assert_eq!(parsed["head_limit"].as_f64(), Some(8.5));

        let error = tool
            .parse_input(&rjson!({
                "pattern": "x",
                "head_limit": "\u{0085}5\u{0085}"
            }))
            .unwrap_err();
        assert_eq!(
            error.message,
            "Invalid arguments: head_limit: Invalid input: expected number, received string"
        );
    }

    #[test]
    fn semantic_number_reports_overflow_as_received_number() {
        let tool = create_grep_tool();
        let error = tool
            .parse_input(&rjson!({
                "pattern": "x",
                "head_limit": "9".repeat(400)
            }))
            .unwrap_err();
        assert_eq!(
            error.message,
            "Invalid arguments: head_limit: Invalid input: expected number, received number"
        );
    }

    #[test]
    fn semantic_number_preserves_negative_zero() {
        let tool = create_grep_tool();
        let parsed = tool
            .parse_input(&rjson!({ "pattern": "x", "head_limit": "-0" }))
            .unwrap();
        let value = parsed["head_limit"].as_f64().unwrap();
        assert_eq!(value, 0.0);
        assert!(value.is_sign_negative());
    }

    #[test]
    fn semantic_preprocessors_preserve_zod_issue_order_and_types() {
        let tool = create_grep_tool();
        let error = tool
            .parse_input(&rjson!({
                "pattern": "x",
                "-B": "bad",
                "-A": "9".repeat(400),
                "-n": " TRUE "
            }))
            .unwrap_err();
        assert_eq!(
            error.message,
            concat!(
                "Invalid arguments: ",
                "-B: Invalid input: expected number, received string; ",
                "-A: Invalid input: expected number, received number; ",
                "-n: Invalid input: expected boolean, received string"
            )
        );
    }

    // ---- integration (skips when rg is absent) ----

    #[tokio::test]
    async fn files_with_matches_is_the_default_mode() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(dir.join("a.ts"), "needle here").unwrap();
        std::fs::write(dir.join("b.ts"), "nothing").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let tool = create_grep_tool();
        let r = tool
            .call(rjson!({ "pattern": "needle" }), &make_ctx(&cwd), None)
            .await
            .unwrap();
        assert_eq!(
            r.serializable.output.get("mode").unwrap(),
            &rjson!("files_with_matches")
        );
        assert_eq!(
            r.serializable.output.get("filenames").unwrap(),
            &rjson!(["a.ts"])
        );
        assert!(text_of(&r).contains("1 matching file"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn default_search_spans_all_workspace_roots() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        // A multi-folder workspace: cwd is the primary root, `extra` is a second
        // workspace folder carried as an additional writable root.
        let primary = temp_dir();
        let extra = temp_dir();
        std::fs::write(primary.join("a.ts"), "needle in primary").unwrap();
        std::fs::write(extra.join("b.ts"), "needle in extra").unwrap();
        let cwd = primary.to_string_lossy().into_owned();
        let mut ctx = make_ctx(&cwd);
        ctx.permission
            .additional_writable_roots
            .push(crate::runtime_values::RuntimePath::from(
                extra.to_string_lossy().into_owned().as_str(),
            ));
        // No `path`: the default now spans EVERY workspace root in one pass.
        let spanned = create_grep_tool()
            .call(rjson!({ "pattern": "needle" }), &ctx, None)
            .await
            .unwrap();
        let spanned_files = format!(
            "{:?}",
            spanned.serializable.output.get("filenames").unwrap()
        );
        assert!(
            spanned_files.contains("a.ts"),
            "primary-root hit missing: {spanned_files}"
        );
        assert!(
            spanned_files.contains("b.ts"),
            "extra workspace-root hit missing: {spanned_files}"
        );
        // An explicit `path` narrows back to a single root (no cross-root spread).
        let narrowed = create_grep_tool()
            .call(
                rjson!({ "pattern": "needle", "path": cwd.clone() }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        let narrowed_files = format!(
            "{:?}",
            narrowed.serializable.output.get("filenames").unwrap()
        );
        assert!(
            narrowed_files.contains("a.ts") && !narrowed_files.contains("b.ts"),
            "explicit path must not span roots: {narrowed_files}"
        );
        std::fs::remove_dir_all(&primary).ok();
        std::fs::remove_dir_all(&extra).ok();
    }

    #[tokio::test]
    async fn files_mode_uses_locale_style_order_for_ascii_case() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        let _node_env = EnvVarGuard::set("NODE_ENV", "test");
        let dir = temp_dir();
        std::fs::write(dir.join("a.rs"), "hit").unwrap();
        std::fs::write(dir.join("B.rs"), "hit").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let result = create_grep_tool()
            .call(rjson!({ "pattern": "hit" }), &make_ctx(&cwd), None)
            .await
            .unwrap();
        assert_eq!(
            result.serializable.output["filenames"],
            rjson!(["a.rs", "B.rs"])
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn files_mode_uses_node_en_us_unicode_collation() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        let _node_env = EnvVarGuard::set("NODE_ENV", "test");
        let dir = temp_dir();
        std::fs::write(dir.join("\u{10d6e}.rs"), "hit").unwrap();
        std::fs::write(dir.join("\u{2010}.rs"), "hit").unwrap();
        std::fs::write(dir.join("ä.rs"), "hit").unwrap();
        std::fs::write(dir.join("z.rs"), "hit").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let result = create_grep_tool()
            .call(rjson!({ "pattern": "hit" }), &make_ctx(&cwd), None)
            .await
            .unwrap();
        assert_eq!(
            result.serializable.output["filenames"],
            rjson!(["\u{10d6e}.rs", "\u{2010}.rs", "ä.rs", "z.rs"])
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn content_mode_reports_path_line_content() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(dir.join("a.ts"), "one\nneedle here\nthree").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let tool = create_grep_tool();
        let r = tool
            .call(
                rjson!({ "pattern": "needle", "output_mode": "content" }),
                &make_ctx(&cwd),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            r.serializable.output.get("mode").unwrap(),
            &rjson!("content")
        );
        assert!(text_of(&r).contains("a.ts:2:needle here"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn count_mode_reports_per_file_counts() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(dir.join("a.ts"), "x\nx\nx").unwrap();
        std::fs::write(dir.join("b.ts"), "x").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let tool = create_grep_tool();
        let r = tool
            .call(
                rjson!({ "pattern": "x", "output_mode": "count" }),
                &make_ctx(&cwd),
                None,
            )
            .await
            .unwrap();
        assert_eq!(r.serializable.output.get("numMatches").unwrap(), &rjson!(4));
        assert_eq!(r.serializable.output.get("numFiles").unwrap(), &rjson!(2));
        assert!(text_of(&r).contains("4 matches in 2 files"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn count_mode_on_a_single_file_reports_the_count() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(dir.join("only.ts"), "x\nx\nx").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let tool = create_grep_tool();
        let r = tool
            .call(
                rjson!({ "pattern": "x", "output_mode": "count", "path": "only.ts" }),
                &make_ctx(&cwd),
                None,
            )
            .await
            .unwrap();
        assert_eq!(r.serializable.output.get("numMatches").unwrap(), &rjson!(3));
        assert_eq!(r.serializable.output.get("numFiles").unwrap(), &rjson!(1));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn glob_filters_which_files_are_searched() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(dir.join("a.ts"), "hit").unwrap();
        std::fs::write(dir.join("b.js"), "hit").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let tool = create_grep_tool();
        let r = tool
            .call(
                rjson!({ "pattern": "hit", "glob": "*.ts" }),
                &make_ctx(&cwd),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            r.serializable.output.get("filenames").unwrap(),
            &rjson!(["a.ts"])
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn trusted_glob_uses_ecmascript_regex_whitespace_boundaries() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(dir.join("joined.rs\u{0085}part.md"), "hit").unwrap();
        std::fs::write(dir.join("plain.rs"), "hit").unwrap();
        std::fs::write(dir.join("plain.md"), "hit").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let tool = create_grep_tool();

        let nel = tool
            .call(
                rjson!({ "pattern": "hit", "glob": "*.rs\u{0085}*.md" }),
                &make_ctx(&cwd),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            nel.serializable.output["filenames"],
            rjson!(["joined.rs\u{0085}part.md"])
        );

        let bom = tool
            .call(
                rjson!({ "pattern": "hit", "glob": "*.rs\u{feff}*.md" }),
                &make_ctx(&cwd),
                None,
            )
            .await
            .unwrap();
        let bom_filenames = bom.serializable.output["filenames"].as_array().unwrap();
        assert_eq!(bom_filenames.len(), 3);
        assert!(bom_filenames.contains(&rjson!("plain.md")));
        assert!(bom_filenames.contains(&rjson!("plain.rs")));
        assert!(bom_filenames.contains(&rjson!("joined.rs\u{0085}part.md")));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn coerces_numeric_and_boolean_strings() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(dir.join("a.ts"), "NEEDLE").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let tool = create_grep_tool();
        let r = tool
            .call(
                rjson!({ "pattern": "needle", "output_mode": "content", "-i": "true", "head_limit": "5" }),
                &make_ctx(&cwd),
                None,
            )
            .await
            .unwrap();
        assert!(text_of(&r).contains("NEEDLE"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn trusted_string_offset_uses_javascript_addition_and_preserves_metadata() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(
            dir.join("a.ts"),
            (0..10)
                .map(|index| format!("hit{index}"))
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let result = create_grep_tool()
            .call(
                rjson!({
                    "pattern": "hit",
                    "output_mode": "content",
                    "-n": false,
                    "head_limit": 2,
                    "offset": "2"
                }),
                &make_ctx(&cwd),
                None,
            )
            .await
            .unwrap();

        assert_eq!(result.serializable.output["numLines"], rjson!(8));
        assert_eq!(result.serializable.output["appliedLimit"], rjson!(2));
        assert_eq!(result.serializable.output["appliedOffset"], rjson!("2"));
        assert!(result.serializable.output["content"]
            .as_str()
            .unwrap()
            .starts_with("a.ts:hit2"));
        assert!(text_of(&result).ends_with("(paginated — limit: 2, offset: 2)"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn trusted_non_numeric_head_limit_uses_nan_slice_end() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(dir.join("a.ts"), "hit0\nhit1\nhit2").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let result = create_grep_tool()
            .call(
                rjson!({
                    "pattern": "hit",
                    "output_mode": "content",
                    "head_limit": "bad"
                }),
                &make_ctx(&cwd),
                None,
            )
            .await
            .unwrap();

        assert_eq!(result.serializable.output["numLines"], rjson!(0));
        assert_eq!(result.serializable.output["content"], rjson!(""));
        assert!(result.serializable.output.get("appliedLimit").is_none());
        assert_eq!(text_of(&result), "No matches.");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn trusted_pagination_uses_javascript_to_primitive_for_json_values() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(
            dir.join("a.ts"),
            (0..10)
                .map(|index| format!("hit{index}"))
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let tool = create_grep_tool();

        let array_offset = tool
            .call(
                rjson!({
                    "pattern": "hit",
                    "output_mode": "content",
                    "-n": false,
                    "head_limit": 2,
                    "offset": [2]
                }),
                &make_ctx(&cwd),
                None,
            )
            .await
            .unwrap();
        assert_eq!(array_offset.serializable.output["numLines"], rjson!(8));
        assert_eq!(
            array_offset.serializable.output["appliedOffset"],
            rjson!([2])
        );

        let array_limit = tool
            .call(
                rjson!({
                    "pattern": "hit",
                    "output_mode": "content",
                    "-n": false,
                    "head_limit": [2],
                    "offset": 2
                }),
                &make_ctx(&cwd),
                None,
            )
            .await
            .unwrap();
        assert_eq!(array_limit.serializable.output["numLines"], rjson!(8));
        assert_eq!(array_limit.serializable.output["appliedLimit"], rjson!([2]));
        assert!(text_of(&array_limit).ends_with("(paginated — limit: 2, offset: 2)"));

        let object_offset = tool
            .call(
                rjson!({
                    "pattern": "hit",
                    "output_mode": "content",
                    "head_limit": 2,
                    "offset": {}
                }),
                &make_ctx(&cwd),
                None,
            )
            .await
            .unwrap();
        assert_eq!(object_offset.serializable.output["numLines"], rjson!(0));
        assert!(object_offset
            .serializable
            .output
            .get("appliedLimit")
            .is_none());
        assert!(object_offset
            .serializable
            .output
            .get("appliedOffset")
            .is_none());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn trusted_infinity_markers_survive_real_grep_in_all_modes() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(dir.join("a.rs"), "hit\nhit").unwrap();
        std::fs::write(dir.join("b.rs"), "hit").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let tool = create_grep_tool();
        let positive = Json::Number(RuntimeNumber::from_f64(f64::INFINITY));
        let negative = Json::Number(RuntimeNumber::from_f64(f64::NEG_INFINITY));

        let content = tool
            .call(
                runtime_object([
                    ("pattern", Json::String(rs("hit"))),
                    ("output_mode", Json::String(rs("content"))),
                    ("head_limit", negative.clone()),
                ]),
                &make_ctx(&cwd),
                None,
            )
            .await
            .unwrap();
        assert_eq!(content.serializable.output["numLines"], rjson!(0));
        assert_infinity_marker(&content.serializable.output["appliedLimit"], true);
        assert_eq!(
            text_of(&content),
            "No matches.\n\n(paginated — limit: -Infinity)"
        );

        let count = tool
            .call(
                runtime_object([
                    ("pattern", Json::String(rs("hit"))),
                    ("output_mode", Json::String(rs("count"))),
                    ("offset", positive.clone()),
                ]),
                &make_ctx(&cwd),
                None,
            )
            .await
            .unwrap();
        assert_eq!(count.serializable.output["numFiles"], rjson!(0));
        assert_eq!(count.serializable.output["numMatches"], rjson!(0));
        assert_infinity_marker(&count.serializable.output["appliedOffset"], false);
        assert_eq!(
            text_of(&count),
            "No matches.\n\n0 matches in 0 files (offset: Infinity)"
        );

        let files = tool
            .call(
                runtime_object([
                    ("pattern", Json::String(rs("hit"))),
                    ("head_limit", negative),
                ]),
                &make_ctx(&cwd),
                None,
            )
            .await
            .unwrap();
        assert_eq!(files.serializable.output["numFiles"], rjson!(0));
        assert_eq!(files.serializable.output["filenames"], rjson!([]));
        assert_infinity_marker(&files.serializable.output["appliedLimit"], true);
        assert_eq!(text_of(&files), "No files matched.");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn trusted_false_string_is_truthy_for_line_numbers() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(dir.join("a.ts"), "hit").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let result = create_grep_tool()
            .call(
                rjson!({
                    "pattern": "hit",
                    "output_mode": "content",
                    "-n": "false"
                }),
                &make_ctx(&cwd),
                None,
            )
            .await
            .unwrap();

        assert!(text_of(&result).contains("a.ts:1:hit"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn trusted_true_string_is_not_strict_true_for_multiline() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(dir.join("a.ts"), "start\nmiddle\nend").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let result = create_grep_tool()
            .call(
                rjson!({
                    "pattern": "start.*end",
                    "output_mode": "content",
                    "multiline": "true"
                }),
                &make_ctx(&cwd),
                None,
            )
            .await
            .unwrap();

        assert_eq!(result.serializable.output["numLines"], rjson!(0));
        assert_eq!(text_of(&result), "No matches.");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn trusted_context_uses_defined_precedence_and_javascript_string_conversion() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(dir.join("a.ts"), "before\nhit\nafter").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let tool = create_grep_tool();

        let null_context = tool
            .call(
                rjson!({
                    "pattern": "hit",
                    "output_mode": "content",
                    "context": null,
                    "-C": 1
                }),
                &make_ctx(&cwd),
                None,
            )
            .await
            .unwrap();
        assert_eq!(null_context.serializable.output["numLines"], rjson!(0));
        assert_eq!(text_of(&null_context), "No matches.");

        let conversion_error = tool_error_of(
            tool.call(
                rjson!({
                    "pattern": "hit",
                    "output_mode": "content",
                    "context": { "toString": "shadowed" },
                    "-C": 1
                }),
                &make_ctx(&cwd),
                None,
            )
            .await,
        );
        assert_eq!(
            conversion_error.message,
            "Cannot convert object to primitive value"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn trusted_non_string_pattern_fails_before_search() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(dir.join("a.ts"), "hit").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let error = tool_error_of(
            create_grep_tool()
                .call(rjson!({ "pattern": 7 }), &make_ctx(&cwd), None)
                .await,
        );
        assert_eq!(error.message, "input.pattern.startsWith is not a function");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn trusted_path_error_precedes_pattern_error() {
        let error = tool_error_of(
            create_grep_tool()
                .call(rjson!({ "pattern": 7, "path": 7 }), &make_ctx("."), None)
                .await,
        );
        assert_eq!(error.message, "p.includes is not a function");
    }

    #[tokio::test]
    async fn trusted_context_conversion_error_precedes_pattern_error() {
        let error = tool_error_of(
            create_grep_tool()
                .call(
                    rjson!({
                        "pattern": 7,
                        "output_mode": "content",
                        "context": { "toString": "x" }
                    }),
                    &make_ctx("."),
                    None,
                )
                .await,
        );
        assert_eq!(error.message, "Cannot convert object to primitive value");
    }

    #[tokio::test]
    async fn trusted_glob_error_precedes_delayed_type_conversion() {
        let error = tool_error_of(
            create_grep_tool()
                .call(
                    rjson!({
                        "pattern": "x",
                        "type": { "toString": "x" },
                        "glob": 7
                    }),
                    &make_ctx("."),
                    None,
                )
                .await,
        );
        assert_eq!(
            error.message,
            "glob.split is not a function or its return value is not iterable"
        );
    }

    #[tokio::test]
    async fn trusted_pattern_nul_validation_precedes_later_type_conversion() {
        let error = tool_error_of(
            create_grep_tool()
                .call(
                    rjson!({
                        "pattern": "x\0",
                        "type": { "toString": "x" }
                    }),
                    &make_ctx("."),
                    None,
                )
                .await,
        );
        assert_eq!(
            error.message,
            "The argument 'args[16]' must be a string without null bytes. Received 'x\\x00'"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn runtime_lone_surrogates_use_node_utf8_at_the_real_process_boundary() {
        use std::os::unix::fs::PermissionsExt;

        // Installs a fake rg via KLAUS_RIPGREP_PATH + a memoized-command reset,
        // then runs it. Both are process-global, so this needs the shared ripgrep
        // lock (not just the external-process one) or a concurrent case's reset
        // lands mid-test and the real rg runs instead, leaving no capture file.
        let (_external, _ripgrep) = crate::test_support::ripgrep_global_guard().await;
        let dir = temp_dir();
        let capture = dir.join("argv.txt");
        let fake_rg = dir.join("rg-capture");
        std::fs::write(
            &fake_rg,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\nexit 1\n",
                capture.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&fake_rg, std::fs::Permissions::from_mode(0o755)).unwrap();

        let _rg_path = EnvVarGuard::set("KLAUS_RIPGREP_PATH", fake_rg.to_str().unwrap());
        crate::fs::ripgrep::reset_ripgrep_command_for_test();
        let input = runtime_json::parse(&RuntimeString::from(
            r#"{"pattern":"\ud800","type":"\ud801","glob":"*\ud802*","path":"\ud803"}"#,
        ))
        .unwrap();
        create_grep_tool()
            .call(input, &make_ctx(dir.to_str().unwrap()), None)
            .await
            .unwrap();
        crate::fs::ripgrep::reset_ripgrep_command_for_test();

        let captured = std::fs::read_to_string(&capture).unwrap();
        let argv = captured.lines().collect::<Vec<_>>();
        assert_eq!(argv[16], "\u{fffd}", "pattern was not Node UTF-8 encoded");
        assert_eq!(argv[18], "\u{fffd}", "type was not Node UTF-8 encoded");
        assert_eq!(argv[20], "*\u{fffd}*", "glob was not Node UTF-8 encoded");
        assert_eq!(
            argv.last().copied(),
            Some(dir.join("\u{fffd}").to_str().unwrap()),
            "OS target path was not Node UTF-8 encoded"
        );
        assert!(
            !captured.contains('\u{fdd0}'),
            "internal lone-surrogate marker leaked into child-process data: {captured:?}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn runtime_lone_surrogate_path_uses_node_utf8_at_the_stat_boundary() {
        let dir = temp_dir();
        std::fs::write(dir.join("\u{fffd}"), b"present").unwrap();
        let input = runtime_json::parse(&RuntimeString::from(r#"{"pattern":"x","path":"\ud800"}"#))
            .unwrap();

        let validation = create_grep_tool()
            .validate_input(&input, &make_ctx(dir.to_str().unwrap()))
            .await;

        assert!(matches!(validation, Some(ValidationResult::Ok)));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn runtime_lone_surrogate_nul_error_uses_node_inspection_before_utf8_replacement() {
        let input = crate::runtime_values::runtime_json::parse(&RuntimeString::from(
            r#"{"pattern":"\ud800\u0000"}"#,
        ))
        .unwrap();
        let error = tool_error_of(create_grep_tool().call(input, &make_ctx("."), None).await);

        assert_eq!(
            error.message,
            "The argument 'args[16]' must be a string without null bytes. Received '\\ud800\\x00'"
        );
        assert!(!error.message.contains("\u{fdd0}"));
    }

    #[test]
    fn node_error_truncation_keeps_an_astral_high_surrogate_at_the_utf16_cut() {
        let message = format!("{}😀tail", "x".repeat(196));
        let truncated = truncate_node_error_message(message);

        assert_eq!(truncated.len_utf16(), 200);
        assert_eq!(
            crate::runtime_values::runtime_json::stringify(&Json::String(truncated)),
            format!("\"{}\\ud83d...\"", "x".repeat(196))
        );
    }

    #[tokio::test]
    async fn trusted_nul_error_uses_node_uppercase_hex_escapes() {
        let error = tool_error_of(
            create_grep_tool()
                .call(rjson!({ "pattern": "a\u{007f}\0b" }), &make_ctx("."), None)
                .await,
        );
        assert_eq!(
            error.message,
            "The argument 'args[16]' must be a string without null bytes. Received 'a\\x7F\\x00b'"
        );
    }

    #[tokio::test]
    async fn trusted_nul_error_avoids_node_template_interpolation_quotes() {
        let error = tool_error_of(
            create_grep_tool()
                .call(rjson!({ "pattern": "a'b\"c${d}\0z" }), &make_ctx("."), None)
                .await,
        );
        assert_eq!(
            error.message,
            "The argument 'args[16]' must be a string without null bytes. Received 'a\\'b\"c${d}\\x00z'"
        );
    }

    #[tokio::test]
    async fn trusted_nul_error_is_truncated_to_node_utf16_message_limit() {
        for pattern in [
            format!("{}\0", "x".repeat(500)),
            format!("{}\0", "😀".repeat(200)),
        ] {
            let error = tool_error_of(
                create_grep_tool()
                    .call(rjson!({ "pattern": pattern }), &make_ctx("."), None)
                    .await,
            );
            assert_eq!(error.message.len_utf16(), 200);
            assert!(error.message.ends_with("..."));
        }
    }

    #[tokio::test]
    async fn trusted_later_glob_nul_validation_precedes_type_conversion() {
        let error = tool_error_of(
            create_grep_tool()
                .call(
                    rjson!({
                        "pattern": "x",
                        "type": { "toString": "x" },
                        "glob": "*.rs\0"
                    }),
                    &make_ctx("."),
                    None,
                )
                .await,
        );
        assert_eq!(
            error.message,
            "The argument 'args[20]' must be a string without null bytes. Received '*.rs\\x00'"
        );
    }

    #[tokio::test]
    async fn trusted_type_nul_uses_its_final_child_process_argument_index() {
        let error = tool_error_of(
            create_grep_tool()
                .call(
                    rjson!({
                        "pattern": "x",
                        "type": "rust\0"
                    }),
                    &make_ctx("."),
                    None,
                )
                .await,
        );
        assert_eq!(
            error.message,
            "The argument 'args[18]' must be a string without null bytes. Received 'rust\\x00'"
        );
    }

    #[tokio::test]
    async fn trusted_earlier_type_nul_precedes_later_glob_nul() {
        let error = tool_error_of(
            create_grep_tool()
                .call(
                    rjson!({
                        "pattern": "x",
                        "type": "rust\0",
                        "glob": "*.rs\0"
                    }),
                    &make_ctx("."),
                    None,
                )
                .await,
        );
        assert_eq!(
            error.message,
            "The argument 'args[18]' must be a string without null bytes. Received 'rust\\x00'"
        );
    }

    #[tokio::test]
    async fn trusted_glob_nul_uses_its_final_child_process_argument_index() {
        let error = tool_error_of(
            create_grep_tool()
                .call(
                    rjson!({
                        "pattern": "x",
                        "glob": "*.rs\0"
                    }),
                    &make_ctx("."),
                    None,
                )
                .await,
        );
        assert_eq!(
            error.message,
            "The argument 'args[18]' must be a string without null bytes. Received '*.rs\\x00'"
        );
    }

    #[tokio::test]
    async fn trusted_non_string_path_fails_before_search() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(dir.join("a.ts"), "hit").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let error = tool_error_of(
            create_grep_tool()
                .call(
                    rjson!({ "pattern": "hit", "path": 7 }),
                    &make_ctx(&cwd),
                    None,
                )
                .await,
        );
        assert_eq!(error.message, "p.includes is not a function");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn trusted_array_path_runs_javascript_includes_before_trim() {
        let tool = create_grep_tool();
        let empty = tool_error_of(
            tool.call(rjson!({ "pattern": "x", "path": [] }), &make_ctx("."), None)
                .await,
        );
        assert_eq!(empty.message, "p.trim is not a function");

        let exact_nul = tool_error_of(
            tool.call(
                rjson!({ "pattern": "x", "path": ["\0"] }),
                &make_ctx("."),
                None,
            )
            .await,
        );
        assert_eq!(exact_nul.message, "Path contains null bytes");
    }

    #[tokio::test]
    async fn trusted_path_rewrite_uses_ecmascript_trim_without_expanding_scope() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(dir.join("root.rs"), "hit").unwrap();
        std::fs::create_dir(dir.join("\u{0085}.\u{0085}")).unwrap();
        std::fs::create_dir(dir.join("\u{feff}.\u{feff}")).unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let tool = create_grep_tool();

        let nel = tool
            .call(
                rjson!({ "pattern": "hit", "path": "\u{0085}.\u{0085}" }),
                &make_ctx(&cwd),
                None,
            )
            .await
            .unwrap();
        assert_eq!(nel.serializable.output["numFiles"], rjson!(0));
        assert_eq!(text_of(&nel), "No files matched.");

        let bom = tool
            .call(
                rjson!({ "pattern": "hit", "path": "\u{feff}.\u{feff}" }),
                &make_ctx(&cwd),
                None,
            )
            .await
            .unwrap();
        assert_eq!(bom.serializable.output["numFiles"], rjson!(1));
        assert_eq!(bom.serializable.output["filenames"], rjson!(["root.rs"]));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn trusted_non_string_glob_fails_before_search() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(dir.join("a.ts"), "hit").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let error = tool_error_of(
            create_grep_tool()
                .call(
                    rjson!({ "pattern": "hit", "glob": 7 }),
                    &make_ctx(&cwd),
                    None,
                )
                .await,
        );
        assert_eq!(
            error.message,
            "glob.split is not a function or its return value is not iterable"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn trusted_non_string_type_is_not_silently_ignored() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(dir.join("a.ts"), "hit").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let result = create_grep_tool()
            .call(
                rjson!({ "pattern": "hit", "type": true }),
                &make_ctx(&cwd),
                None,
            )
            .await
            .unwrap();
        assert_eq!(result.serializable.output["numFiles"], rjson!(0));
        assert_eq!(text_of(&result), "No files matched.");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn reports_no_matches() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(dir.join("a.ts"), "nothing").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let tool = create_grep_tool();
        let r = tool
            .call(rjson!({ "pattern": "zzz" }), &make_ctx(&cwd), None)
            .await
            .unwrap();
        assert_eq!(text_of(&r), "No files matched.");
        let r = tool
            .call(
                rjson!({ "pattern": "zzz", "output_mode": "content" }),
                &make_ctx(&cwd),
                None,
            )
            .await
            .unwrap();
        assert!(text_of(&r).contains("No matches."));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn validate_input_rejects_missing_path_and_accepts_existing() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let tool = create_grep_tool();
        let missing = dir.join("nope").to_string_lossy().into_owned();
        let v = tool
            .validate_input(
                &rjson!({ "pattern": "x", "path": missing }),
                &make_ctx(&cwd),
            )
            .await;
        match v {
            Some(ValidationResult::Err { message, .. }) => {
                assert!(message.contains("Path does not exist"))
            }
            other => panic!("expected err, got {other:?}"),
        }
        let ok = tool
            .validate_input(
                &rjson!({ "pattern": "x", "path": cwd.clone() }),
                &make_ctx(&cwd),
            )
            .await;
        assert_eq!(ok, Some(ValidationResult::Ok));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn files_mode_orders_signed_pre_epoch_mtimes_newest_first() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        if !rg_available() {
            return;
        }
        let _external = crate::test_support::external_process_lock().await;
        let _node_env = EnvVarGuard::set("NODE_ENV", "production");
        let dir = temp_dir();
        let older = dir.join("a.rs");
        let newer = dir.join("z.rs");
        std::fs::write(&older, "needle").unwrap();
        std::fs::write(&newer, "needle").unwrap();

        for (path, seconds) in [(&older, -315_619_200_i64), (&newer, -157_766_400_i64)] {
            let path = CString::new(path.as_os_str().as_bytes()).unwrap();
            let times = [
                libc::timespec {
                    tv_sec: seconds as _,
                    tv_nsec: 0,
                },
                libc::timespec {
                    tv_sec: seconds as _,
                    tv_nsec: 0,
                },
            ];
            // SAFETY: `path` is a live NUL-terminated byte string and `times`
            // contains the two initialized atime/mtime entries required by
            // `utimensat`.
            assert_eq!(
                unsafe { libc::utimensat(libc::AT_FDCWD, path.as_ptr(), times.as_ptr(), 0) },
                0
            );
        }
        assert!(std::fs::metadata(&older)
            .unwrap()
            .modified()
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .is_err());

        let cwd = dir.to_string_lossy().into_owned();
        let result = create_grep_tool()
            .call(rjson!({ "pattern": "needle" }), &make_ctx(&cwd), None)
            .await
            .unwrap();
        assert_eq!(
            result.serializable.output["filenames"],
            rjson!(["z.rs", "a.rs"])
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn validate_input_propagates_symlink_loop_as_fatal_error() {
        use std::os::unix::fs::symlink;

        let dir = temp_dir();
        let loop_path = dir.join("loop");
        symlink("loop", &loop_path).unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let path = loop_path.to_string_lossy().into_owned();
        let tool = create_grep_tool();
        let Some(ValidationResult::Fatal { error }) = tool
            .validate_input(&rjson!({ "pattern": "x", "path": path }), &make_ctx(&cwd))
            .await
        else {
            panic!("ELOOP must return a typed fatal grep validation result");
        };
        assert_eq!(error.kind, crate::types::errors::ToolErrorKind::Fatal);
        assert_eq!(
            error.message,
            format!(
                "ELOOP: too many symbolic links encountered, stat '{}'",
                loop_path.display()
            )
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
