//! Workflow `meta` — the static header every workflow script must begin with:
//! `export const meta = { name, description, ... }`. It MUST be a pure object
//! literal (no variables, calls, spreads, or template substitution) so it can be
//! read and validated WITHOUT executing the script body — the host uses it for
//! the permission prompt and progress grouping.
//!
//! The literal is located with a balanced-brace scan (string/comment/template
//! aware) and then evaluated NATIVELY as a restricted JSON-producing object
//! literal: a pure literal evaluates; any identifier reference, call, spread, or
//! template substitution is rejected — exactly the "pure literal" contract. The
//! native evaluator can only ever produce a JSON value, so a separate
//! "is this pure JSON?" walk is unnecessary (it is subsumed by the evaluator).

use crate::engine::workflow::WorkflowError;
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeEncoder, RuntimeJson,
    RuntimeNumber, RuntimeObject, RuntimeObjectEncoder, RuntimeString,
};

/// One phase declared in a workflow's `meta.phases`.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowPhase {
    pub title: RuntimeString,
    pub detail: Option<RuntimeString>,
    pub model: Option<RuntimeString>,
}

/// The validated `meta` header of a workflow script. Unknown keys are dropped
/// (only the declared fields survive validation).
#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowMeta {
    pub name: RuntimeString,
    pub description: RuntimeString,
    pub when_to_use: Option<RuntimeString>,
    pub phases: Option<Vec<WorkflowPhase>>,
    pub model: Option<RuntimeString>,
}

impl RuntimeEncode for WorkflowPhase {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("title", &self.title)?;
        if let Some(detail) = &self.detail {
            object.field("detail", detail)?;
        }
        if let Some(model) = &self.model {
            object.field("model", model)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for WorkflowPhase {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                title: object.required("title", &[])?,
                detail: object.optional("detail", &[])?,
                model: object.optional("model", &[])?,
            })
        })
    }
}

impl RuntimeEncode for WorkflowMeta {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        object.field("description", &self.description)?;
        if let Some(when_to_use) = &self.when_to_use {
            object.field("whenToUse", when_to_use)?;
        }
        if let Some(phases) = &self.phases {
            object.field("phases", phases)?;
        }
        if let Some(model) = &self.model {
            object.field("model", model)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for WorkflowMeta {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                name: object.required("name", &[])?,
                description: object.required("description", &[])?,
                when_to_use: object.optional("whenToUse", &[])?,
                phases: object.optional("phases", &[])?,
                model: object.optional("model", &[])?,
            })
        })
    }
}

/// A parsed workflow: its validated `meta`, plus the script body wrapped in an
/// async function expression ready for the host script runner to compile.
#[derive(Debug, Clone, PartialEq)]
pub struct ParsedWorkflow {
    pub meta: WorkflowMeta,
    /// The script body wrapped in an async function expression. `export const
    /// meta` is rewritten to `const meta` so it is valid inside the function;
    /// the script's top-level `await`/`return` work as-is.
    pub wrapped_source: RuntimeString,
    /// Lines prepended by the wrapper, so a thrown error's line can be
    /// corrected.
    pub line_offset: i64,
}

/// Parse + validate a workflow script: extract `meta`, validate it, wrap the
/// body.
pub fn parse_workflow(source: &RuntimeString) -> Result<ParsedWorkflow, WorkflowError> {
    let (literal, export_start) = locate_meta_literal(source)?;
    let raw = evaluate_meta_literal(&literal)?;
    let meta = validate_meta(&raw)?;
    let body = strip_meta_export(source, export_start);
    // One line is prepended (the async wrapper opener), so report lines off by
    // one.
    let wrapped_source = RuntimeString::from_utf8("(async function __workflow(){\n")
        .concat(&body)
        .concat(&RuntimeString::from_utf8("\n})"));
    Ok(ParsedWorkflow {
        meta,
        wrapped_source,
        line_offset: 1,
    })
}

/// Locate the `export const meta = { ... }` literal via a balanced-brace scan
/// that is aware of strings, template literals (incl. `${}` nesting), and
/// comments, so a brace inside a string never miscounts. Returns the literal
/// substring and the UTF-16 index of the `export` keyword.
fn locate_meta_literal(source: &RuntimeString) -> Result<(RuntimeString, usize), WorkflowError> {
    let units = source.as_utf16();
    let (export_start, open) = find_meta_assignment(units).ok_or_else(|| {
        WorkflowError::new("workflow script must begin with `export const meta = { ... }`")
    })?;
    if units.get(open) != Some(&u16::from(b'{')) {
        return Err(WorkflowError::new(
            "workflow `meta` must be an object literal `{ ... }`",
        ));
    }

    enum St {
        Code,
        Line,
        Block,
        Single,
        Double,
        Template,
    }
    let mut state = St::Code;
    let mut depth: i64 = 0;
    // Brace depth at which each open `${` template-expression began.
    let mut template_expr_stack: Vec<i64> = Vec::new();

    let mut i = open;
    while i < units.len() {
        let c = units[i];
        let next = units.get(i + 1).copied();
        match state {
            St::Line => {
                if c == u16::from(b'\n') {
                    state = St::Code;
                }
            }
            St::Block => {
                if c == u16::from(b'*') && next == Some(u16::from(b'/')) {
                    state = St::Code;
                    i += 1;
                }
            }
            St::Single => {
                if c == u16::from(b'\\') {
                    i += 1;
                } else if c == u16::from(b'\'') {
                    state = St::Code;
                }
            }
            St::Double => {
                if c == u16::from(b'\\') {
                    i += 1;
                } else if c == u16::from(b'"') {
                    state = St::Code;
                }
            }
            St::Template => {
                if c == u16::from(b'\\') {
                    i += 1;
                } else if c == u16::from(b'`') {
                    state = St::Code;
                } else if c == u16::from(b'$') && next == Some(u16::from(b'{')) {
                    template_expr_stack.push(depth);
                    depth += 1;
                    i += 1;
                    state = St::Code;
                }
            }
            St::Code => {
                if c == u16::from(b'/') && next == Some(u16::from(b'/')) {
                    state = St::Line;
                    i += 1;
                } else if c == u16::from(b'/') && next == Some(u16::from(b'*')) {
                    state = St::Block;
                    i += 1;
                } else if c == u16::from(b'\'') {
                    state = St::Single;
                } else if c == u16::from(b'"') {
                    state = St::Double;
                } else if c == u16::from(b'`') {
                    state = St::Template;
                } else if c == u16::from(b'{') {
                    depth += 1;
                } else if c == u16::from(b'}') {
                    depth -= 1;
                    let top = template_expr_stack.last().copied();
                    if !template_expr_stack.is_empty() && Some(depth) == top {
                        template_expr_stack.pop();
                        state = St::Template;
                    } else if depth == 0 {
                        return Ok((source.slice_utf16(open..=i), export_start));
                    }
                }
            }
        }
        i += 1;
    }
    Err(WorkflowError::new(
        "workflow `meta` object literal is never closed",
    ))
}

fn find_meta_assignment(units: &[u16]) -> Option<(usize, usize)> {
    let mut start = 0;
    while start < units.len() {
        if !starts_ascii(units, start, "export") {
            start += 1;
            continue;
        }
        let mut cursor = start + "export".len();
        if !consume_required_whitespace(units, &mut cursor) || !starts_ascii(units, cursor, "const")
        {
            start += 1;
            continue;
        }
        cursor += "const".len();
        if !consume_required_whitespace(units, &mut cursor) || !starts_ascii(units, cursor, "meta")
        {
            start += 1;
            continue;
        }
        cursor += "meta".len();
        consume_whitespace(units, &mut cursor);
        if units.get(cursor) != Some(&u16::from(b'=')) {
            start += 1;
            continue;
        }
        cursor += 1;
        consume_whitespace(units, &mut cursor);
        return Some((start, cursor));
    }
    None
}

fn starts_ascii(units: &[u16], start: usize, expected: &str) -> bool {
    units
        .get(start..start.saturating_add(expected.len()))
        .is_some_and(|candidate| {
            candidate
                .iter()
                .copied()
                .eq(expected.bytes().map(u16::from))
        })
}

fn consume_required_whitespace(units: &[u16], cursor: &mut usize) -> bool {
    let before = *cursor;
    consume_whitespace(units, cursor);
    *cursor != before
}

fn consume_whitespace(units: &[u16], cursor: &mut usize) {
    while let Some(length) = whitespace_len_at(units, *cursor) {
        *cursor += length;
    }
}

fn whitespace_len_at(units: &[u16], index: usize) -> Option<usize> {
    let (character, length) = scalar_at(units, index)?;
    character.is_whitespace().then_some(length)
}

fn scalar_at(units: &[u16], index: usize) -> Option<(char, usize)> {
    let first = *units.get(index)?;
    if (0xd800..=0xdbff).contains(&first) {
        let second = *units.get(index + 1)?;
        if !(0xdc00..=0xdfff).contains(&second) {
            return None;
        }
        let point = 0x10000 + ((u32::from(first) - 0xd800) << 10) + (u32::from(second) - 0xdc00);
        Some((char::from_u32(point).expect("valid surrogate pair"), 2))
    } else if (0xdc00..=0xdfff).contains(&first) {
        None
    } else {
        Some((
            char::from_u32(u32::from(first)).expect("non-surrogate UTF-16 unit"),
            1,
        ))
    }
}

/// Evaluate the located meta literal as a restricted JSON-producing object
/// literal. A pure literal evaluates; any identifier reference, call, spread, or
/// template substitution is rejected (that is exactly the "pure literal"
/// contract). An undefined identifier reports `X is not defined`, matching the
/// underlying script engine's reference error.
fn evaluate_meta_literal(literal: &RuntimeString) -> Result<RuntimeJson, WorkflowError> {
    let mut parser = MetaParser::new(literal);
    match parser.parse_document() {
        Ok(v) => Ok(v),
        Err(msg) => Err(WorkflowError::new(format!(
            "workflow `meta` must be a pure literal (no variables, calls, spreads, or substitutions): {msg}"
        ))),
    }
}

/// Rewrite only the leading `export ` of the meta export, by exact index.
fn strip_meta_export(source: &RuntimeString, export_start: usize) -> RuntimeString {
    let units = source.as_utf16();
    let mut rest = export_start + "export".len();
    consume_whitespace(units, &mut rest);
    let mut output = Vec::with_capacity(units.len().saturating_sub(rest - export_start));
    output.extend_from_slice(&units[..export_start]);
    output.extend_from_slice(&units[rest..]);
    RuntimeString::from_utf16(output)
}

// ---- schema validation ------------------------------------------------------

/// The type name a schema validator reports for a value in an
/// `Expected <x>, received <type>` message.
fn value_type(v: &RuntimeJson) -> &'static str {
    match v {
        RuntimeJson::Null => "null",
        RuntimeJson::Bool(_) => "boolean",
        RuntimeJson::Number(_) => "number",
        RuntimeJson::String(_) => "string",
        RuntimeJson::Array(_) => "array",
        RuntimeJson::Object(_) => "object",
    }
}

/// Validate one string field, appending any issue message(s). An absent value
/// on a required field reports `Required`; a present non-string reports
/// `Expected string, received <type>`; an empty string with `min1` set reports
/// the min-length message.
fn validate_string(
    obj: &RuntimeObject,
    key: &str,
    min1: bool,
    optional: bool,
    issues: &mut Vec<String>,
) {
    match obj.get(key) {
        None => {
            if !optional {
                issues.push("Required".to_string());
            }
        }
        Some(RuntimeJson::String(s)) => {
            if min1 && s.is_empty() {
                issues.push("String must contain at least 1 character(s)".to_string());
            }
        }
        Some(other) => {
            issues.push(format!("Expected string, received {}", value_type(other)));
        }
    }
}

/// Validate the optional `phases` array (each item is `{ title, detail?,
/// model? }`).
fn validate_phases(obj: &RuntimeObject, issues: &mut Vec<String>) {
    match obj.get("phases") {
        None => {}
        Some(RuntimeJson::Array(arr)) => {
            for item in arr {
                match item {
                    RuntimeJson::Object(po) => {
                        validate_string(po, "title", false, false, issues);
                        validate_string(po, "detail", false, true, issues);
                        validate_string(po, "model", false, true, issues);
                    }
                    other => {
                        issues.push(format!("Expected object, received {}", value_type(other)));
                    }
                }
            }
        }
        Some(other) => {
            issues.push(format!("Expected array, received {}", value_type(other)));
        }
    }
}

fn validate_meta(raw: &RuntimeJson) -> Result<WorkflowMeta, WorkflowError> {
    let obj = match raw {
        RuntimeJson::Object(o) => o,
        other => {
            return Err(WorkflowError::new(format!(
                "workflow `meta` is invalid: Expected object, received {}",
                value_type(other)
            )));
        }
    };
    let mut issues: Vec<String> = Vec::new();
    validate_string(obj, "name", true, false, &mut issues);
    validate_string(obj, "description", true, false, &mut issues);
    validate_string(obj, "whenToUse", false, true, &mut issues);
    validate_phases(obj, &mut issues);
    validate_string(obj, "model", false, true, &mut issues);
    if !issues.is_empty() {
        return Err(WorkflowError::new(format!(
            "workflow `meta` is invalid: {}",
            issues.join("; ")
        )));
    }
    Ok(WorkflowMeta {
        name: obj
            .get("name")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .unwrap_or_default(),
        description: obj
            .get("description")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .unwrap_or_default(),
        when_to_use: obj
            .get("whenToUse")
            .and_then(RuntimeJson::as_string)
            .cloned(),
        phases: build_phases(obj),
        model: obj.get("model").and_then(RuntimeJson::as_string).cloned(),
    })
}

fn build_phases(obj: &RuntimeObject) -> Option<Vec<WorkflowPhase>> {
    match obj.get("phases") {
        Some(RuntimeJson::Array(arr)) => Some(
            arr.iter()
                .filter_map(|item| {
                    item.as_object().map(|po| WorkflowPhase {
                        title: po
                            .get("title")
                            .and_then(RuntimeJson::as_string)
                            .cloned()
                            .unwrap_or_default(),
                        detail: po.get("detail").and_then(RuntimeJson::as_string).cloned(),
                        model: po.get("model").and_then(RuntimeJson::as_string).cloned(),
                    })
                })
                .collect(),
        ),
        _ => None,
    }
}

// ---- native object-literal evaluator ----------------------------------------

fn is_ident_start(c: char) -> bool {
    c.is_alphabetic() || c == '_' || c == '$'
}

fn is_ident_part(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '$'
}

fn json_number(number: f64) -> RuntimeJson {
    RuntimeJson::Number(RuntimeNumber::from_f64(number))
}

struct MetaParser<'a> {
    units: &'a [u16],
    pos: usize,
}

impl<'a> MetaParser<'a> {
    fn new(source: &'a RuntimeString) -> Self {
        Self {
            units: source.as_utf16(),
            pos: 0,
        }
    }

    fn peek(&self) -> Option<u16> {
        self.units.get(self.pos).copied()
    }

    fn peek_at(&self, n: usize) -> Option<u16> {
        self.units.get(self.pos + n).copied()
    }

    fn next(&mut self) -> Option<u16> {
        let unit = self.units.get(self.pos).copied();
        if unit.is_some() {
            self.pos += 1;
        }
        unit
    }

    fn peek_ascii(&self, expected: u8) -> bool {
        self.peek() == Some(u16::from(expected))
    }

    fn peek_at_ascii(&self, offset: usize, expected: u8) -> bool {
        self.peek_at(offset) == Some(u16::from(expected))
    }

    fn peek_scalar(&self) -> Option<(char, usize)> {
        scalar_at(self.units, self.pos)
    }

    fn token(&self) -> String {
        let length = self.peek_scalar().map(|(_, length)| length).unwrap_or(1);
        let end = (self.pos + length).min(self.units.len());
        runtime_diagnostic(&RuntimeString::from_utf16(
            self.units[self.pos..end].to_vec(),
        ))
    }

    fn skip_ws(&mut self) {
        loop {
            if let Some(length) = whitespace_len_at(self.units, self.pos) {
                self.pos += length;
                continue;
            }
            match self.peek() {
                Some(unit) if unit == u16::from(b'/') && self.peek_at_ascii(1, b'/') => {
                    self.pos += 2;
                    while let Some(unit) = self.peek() {
                        self.pos += 1;
                        if unit == u16::from(b'\n') {
                            break;
                        }
                    }
                }
                Some(unit) if unit == u16::from(b'/') && self.peek_at_ascii(1, b'*') => {
                    self.pos += 2;
                    loop {
                        match self.peek() {
                            None => break,
                            Some(unit)
                                if unit == u16::from(b'*') && self.peek_at_ascii(1, b'/') =>
                            {
                                self.pos += 2;
                                break;
                            }
                            Some(_) => {
                                self.pos += 1;
                            }
                        }
                    }
                }
                _ => break,
            }
        }
    }

    fn parse_document(&mut self) -> Result<RuntimeJson, String> {
        let value = self.parse_value()?;
        self.skip_ws();
        if self.pos < self.units.len() {
            return Err(format!("Unexpected token '{}'", self.token()));
        }
        Ok(value)
    }

    fn parse_value(&mut self) -> Result<RuntimeJson, String> {
        self.skip_ws();
        match (self.peek(), self.peek_scalar()) {
            (Some(unit), _) if unit == u16::from(b'{') => self.parse_object(),
            (Some(unit), _) if unit == u16::from(b'[') => self.parse_array(),
            (Some(unit), _)
                if [u16::from(b'"'), u16::from(b'\''), u16::from(b'`')].contains(&unit) =>
            {
                self.parse_string()
            }
            (Some(unit), _)
                if [u16::from(b'-'), u16::from(b'+'), u16::from(b'.')].contains(&unit)
                    || is_ascii_digit(unit) =>
            {
                self.parse_number()
            }
            (Some(_), Some((character, _))) if is_ident_start(character) => {
                let ident = self.read_ident();
                if ident == "true" {
                    Ok(RuntimeJson::Bool(true))
                } else if ident == "false" {
                    Ok(RuntimeJson::Bool(false))
                } else if ident == "null" {
                    Ok(RuntimeJson::Null)
                } else {
                    Err(format!("{} is not defined", runtime_diagnostic(&ident)))
                }
            }
            (Some(_), _) => Err(format!("Unexpected token '{}'", self.token())),
            (None, _) => Err("Unexpected end of input".to_string()),
        }
    }

    fn parse_object(&mut self) -> Result<RuntimeJson, String> {
        self.next(); // consume '{'
        let mut object = RuntimeObject::new();
        loop {
            self.skip_ws();
            match self.peek() {
                Some(unit) if unit == u16::from(b'}') => {
                    self.next();
                    return Ok(RuntimeJson::Object(object));
                }
                None => return Err("Unexpected end of input (unterminated object)".to_string()),
                _ => {}
            }
            let key = self.parse_key()?;
            self.skip_ws();
            match self.peek() {
                Some(unit) if unit == u16::from(b':') => {
                    self.next();
                }
                Some(_) => {
                    return Err(format!("Unexpected token '{}', expected ':'", self.token()))
                }
                None => return Err("Unexpected end of input, expected ':'".to_string()),
            }
            let value = self.parse_value()?;
            object.insert(key, value);
            self.skip_ws();
            match self.peek() {
                Some(unit) if unit == u16::from(b',') => {
                    self.next();
                }
                Some(unit) if unit == u16::from(b'}') => {
                    self.next();
                    return Ok(RuntimeJson::Object(object));
                }
                Some(_) => return Err(format!("Unexpected token '{}' in object", self.token())),
                None => return Err("Unexpected end of input (unterminated object)".to_string()),
            }
        }
    }

    fn parse_array(&mut self) -> Result<RuntimeJson, String> {
        self.next(); // consume '['
        let mut array = Vec::new();
        loop {
            self.skip_ws();
            match self.peek() {
                Some(unit) if unit == u16::from(b']') => {
                    self.next();
                    return Ok(RuntimeJson::Array(array));
                }
                None => return Err("Unexpected end of input (unterminated array)".to_string()),
                _ => {}
            }
            let value = self.parse_value()?;
            array.push(value);
            self.skip_ws();
            match self.peek() {
                Some(unit) if unit == u16::from(b',') => {
                    self.next();
                }
                Some(unit) if unit == u16::from(b']') => {
                    self.next();
                    return Ok(RuntimeJson::Array(array));
                }
                Some(_) => return Err(format!("Unexpected token '{}' in array", self.token())),
                None => return Err("Unexpected end of input (unterminated array)".to_string()),
            }
        }
    }

    fn parse_key(&mut self) -> Result<RuntimeString, String> {
        self.skip_ws();
        match (self.peek(), self.peek_scalar()) {
            (Some(unit), _)
                if [u16::from(b'"'), u16::from(b'\''), u16::from(b'`')].contains(&unit) =>
            {
                self.parse_string()?
                    .into_string()
                    .map_err(|_| "Invalid object key".to_string())
            }
            (Some(_), Some((character, _))) if is_ident_start(character) => Ok(self.read_ident()),
            (Some(unit), _) if is_ascii_digit(unit) || unit == u16::from(b'.') => {
                let start = self.pos;
                while self
                    .peek()
                    .is_some_and(|unit| is_ascii_digit(unit) || unit == u16::from(b'.'))
                {
                    self.pos += 1;
                }
                Ok(RuntimeString::from_utf16(
                    self.units[start..self.pos].to_vec(),
                ))
            }
            (Some(_), _) => Err(format!("Unexpected token '{}' in object key", self.token())),
            (None, _) => Err("Unexpected end of input in object key".to_string()),
        }
    }

    fn read_ident(&mut self) -> RuntimeString {
        let start = self.pos;
        while let Some((character, length)) = self.peek_scalar() {
            if !is_ident_part(character) {
                break;
            }
            self.pos += length;
        }
        RuntimeString::from_utf16(self.units[start..self.pos].to_vec())
    }

    fn parse_number(&mut self) -> Result<RuntimeJson, String> {
        let start = self.pos;
        let negative = self.peek_ascii(b'-');
        if self.peek_ascii(b'+') || self.peek_ascii(b'-') {
            self.pos += 1;
        }
        if self.peek_ascii(b'0') && (self.peek_at_ascii(1, b'x') || self.peek_at_ascii(1, b'X')) {
            self.pos += 2;
            let hex_start = self.pos;
            while self.peek().is_some_and(is_ascii_hex_digit) {
                self.pos += 1;
            }
            let value = parse_hex(&self.units[hex_start..self.pos])
                .ok_or_else(|| "Invalid hexadecimal literal".to_string())?;
            let value = value as f64;
            return Ok(json_number(if negative { -value } else { value }));
        }
        while self.peek().is_some_and(is_ascii_digit) {
            self.pos += 1;
        }
        if self.peek_ascii(b'.') {
            self.pos += 1;
            while self.peek().is_some_and(is_ascii_digit) {
                self.pos += 1;
            }
        }
        if self.peek_ascii(b'e') || self.peek_ascii(b'E') {
            self.pos += 1;
            if self.peek_ascii(b'+') || self.peek_ascii(b'-') {
                self.pos += 1;
            }
            while self.peek().is_some_and(is_ascii_digit) {
                self.pos += 1;
            }
        }
        let text = ascii_string(&self.units[start..self.pos])
            .ok_or_else(|| "Unexpected token in number".to_string())?;
        let number = text
            .parse::<f64>()
            .map_err(|_| format!("Unexpected token in number: {text}"))?;
        Ok(json_number(number))
    }

    fn parse_string(&mut self) -> Result<RuntimeJson, String> {
        let quote = self.next().expect("caller checked string quote");
        let template = quote == u16::from(b'`');
        let mut output = Vec::new();
        loop {
            match self.next() {
                None => return Err("Unterminated string literal".to_string()),
                Some(unit) if unit == u16::from(b'\\') => match self.next() {
                    None => return Err("Unterminated string literal".to_string()),
                    Some(unit) if unit == u16::from(b'n') => output.push(u16::from(b'\n')),
                    Some(unit) if unit == u16::from(b't') => output.push(u16::from(b'\t')),
                    Some(unit) if unit == u16::from(b'r') => output.push(u16::from(b'\r')),
                    Some(unit) if unit == u16::from(b'b') => output.push(0x0008),
                    Some(unit) if unit == u16::from(b'f') => output.push(0x000c),
                    Some(unit) if unit == u16::from(b'v') => output.push(0x000b),
                    Some(unit) if unit == u16::from(b'0') => output.push(0x0000),
                    Some(unit) if unit == u16::from(b'\\') => output.push(u16::from(b'\\')),
                    Some(unit) if unit == u16::from(b'\'') => output.push(u16::from(b'\'')),
                    Some(unit) if unit == u16::from(b'"') => output.push(u16::from(b'"')),
                    Some(unit) if unit == u16::from(b'`') => output.push(u16::from(b'`')),
                    Some(unit) if unit == u16::from(b'/') => output.push(u16::from(b'/')),
                    Some(unit) if unit == u16::from(b'$') => output.push(u16::from(b'$')),
                    Some(unit) if unit == u16::from(b'\n') => {}
                    Some(unit) if unit == u16::from(b'\r') => {
                        if self.peek_ascii(b'\n') {
                            self.pos += 1;
                        }
                    }
                    Some(unit) if unit == u16::from(b'u') => {
                        self.read_unicode_escape(&mut output)?;
                    }
                    Some(unit) if unit == u16::from(b'x') => {
                        self.read_hex_escape(&mut output)?;
                    }
                    Some(other) => output.push(other),
                },
                Some(unit) if unit == quote => {
                    return Ok(RuntimeJson::String(RuntimeString::from_utf16(output)))
                }
                Some(unit) if template && unit == u16::from(b'$') => {
                    if self.peek_ascii(b'{') {
                        return Err(
                            "template literals with substitutions are not allowed".to_string()
                        );
                    }
                    output.push(unit);
                }
                Some(unit) => output.push(unit),
            }
        }
    }

    fn read_unicode_escape(&mut self, output: &mut Vec<u16>) -> Result<(), String> {
        if self.peek_ascii(b'{') {
            self.next();
            let start = self.pos;
            while self.peek().is_some_and(|unit| unit != u16::from(b'}')) {
                self.pos += 1;
            }
            let end = self.pos;
            if self.next() != Some(u16::from(b'}')) {
                return Err("Invalid Unicode escape".to_string());
            }
            let code = parse_hex(&self.units[start..end])
                .filter(|code| *code <= 0x10ffff)
                .ok_or_else(|| "Invalid Unicode escape".to_string())?;
            if code <= 0xffff {
                output.push(code as u16);
            } else {
                let offset = code - 0x10000;
                output.push(0xd800 + ((offset >> 10) as u16));
                output.push(0xdc00 + ((offset & 0x03ff) as u16));
            }
        } else {
            let end = self.pos.saturating_add(4);
            let units = self
                .units
                .get(self.pos..end)
                .ok_or_else(|| "Invalid Unicode escape".to_string())?;
            let code = parse_hex(units).ok_or_else(|| "Invalid Unicode escape".to_string())?;
            output.push(code as u16);
            self.pos = end;
        }
        Ok(())
    }

    fn read_hex_escape(&mut self, output: &mut Vec<u16>) -> Result<(), String> {
        let end = self.pos.saturating_add(2);
        let units = self
            .units
            .get(self.pos..end)
            .ok_or_else(|| "Invalid hexadecimal escape".to_string())?;
        let code = parse_hex(units).ok_or_else(|| "Invalid hexadecimal escape".to_string())?;
        output.push(code as u16);
        self.pos = end;
        Ok(())
    }
}

fn is_ascii_digit(unit: u16) -> bool {
    (u16::from(b'0')..=u16::from(b'9')).contains(&unit)
}

fn is_ascii_hex_digit(unit: u16) -> bool {
    is_ascii_digit(unit)
        || (u16::from(b'a')..=u16::from(b'f')).contains(&unit)
        || (u16::from(b'A')..=u16::from(b'F')).contains(&unit)
}

fn parse_hex(units: &[u16]) -> Option<u64> {
    if units.is_empty() {
        return None;
    }
    let mut value = 0_u64;
    for unit in units.iter().copied() {
        let digit = match unit {
            unit if is_ascii_digit(unit) => u64::from(unit - u16::from(b'0')),
            unit if (u16::from(b'a')..=u16::from(b'f')).contains(&unit) => {
                10 + u64::from(unit - u16::from(b'a'))
            }
            unit if (u16::from(b'A')..=u16::from(b'F')).contains(&unit) => {
                10 + u64::from(unit - u16::from(b'A'))
            }
            _ => return None,
        };
        value = value.checked_mul(16)?.checked_add(digit)?;
    }
    Some(value)
}

fn ascii_string(units: &[u16]) -> Option<String> {
    units
        .iter()
        .copied()
        .map(|unit| u8::try_from(unit).ok().filter(u8::is_ascii).map(char::from))
        .collect()
}

/// Workflow errors are scalar host diagnostics. Preserve non-scalar identity by
/// escaping unpaired UTF-16 units instead of applying Display or replacement.
fn runtime_diagnostic(value: &RuntimeString) -> String {
    let mut output = String::new();
    let units = value.as_utf16();
    let mut index = 0;
    while index < units.len() {
        let unit = units[index];
        if (0xd800..=0xdbff).contains(&unit)
            && units
                .get(index + 1)
                .is_some_and(|low| (0xdc00..=0xdfff).contains(low))
        {
            let low = units[index + 1];
            let point = 0x10000 + ((u32::from(unit) - 0xd800) << 10) + (u32::from(low) - 0xdc00);
            output.push(char::from_u32(point).expect("valid surrogate pair"));
            index += 2;
            continue;
        }
        if (0xd800..=0xdfff).contains(&unit) {
            use std::fmt::Write as _;
            write!(&mut output, "\\u{unit:04x}").expect("writing to String cannot fail");
        } else {
            output.push(char::from_u32(u32::from(unit)).expect("non-surrogate UTF-16 unit"));
        }
        index += 1;
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(source: &str) -> Result<ParsedWorkflow, WorkflowError> {
        parse_workflow(&RuntimeString::from_utf8(source))
    }

    #[test]
    fn parses_a_full_valid_meta() {
        let src = concat!(
            "export const meta = {\n",
            "  name: 'demo',\n",
            "  description: 'A demo workflow',\n",
            "  whenToUse: 'when testing',\n",
            "  phases: [\n",
            "    { title: 'One' },\n",
            "    { title: 'Two', detail: 'second', model: 'm' },\n",
            "  ],\n",
            "  model: 'main',\n",
            "}\n",
            "return null\n"
        );
        let parsed = parse(src).unwrap();
        assert_eq!(parsed.meta.name, "demo");
        assert_eq!(parsed.meta.description, "A demo workflow");
        assert!(parsed
            .meta
            .when_to_use
            .as_ref()
            .is_some_and(|value| value == "when testing"));
        assert!(parsed
            .meta
            .model
            .as_ref()
            .is_some_and(|value| value == "main"));
        let phases = parsed.meta.phases.clone().unwrap();
        assert_eq!(phases.len(), 2);
        assert_eq!(phases[0].title, "One");
        assert!(phases[1]
            .detail
            .as_ref()
            .is_some_and(|value| value == "second"));
        assert!(parsed
            .wrapped_source
            .starts_with("(async function __workflow(){\nconst meta = {"));
        assert!(parsed.wrapped_source.ends_with("return null\n\n})"));
        assert_eq!(parsed.line_offset, 1);
    }

    #[test]
    fn missing_header_errors() {
        let e = parse("const x = 1").unwrap_err();
        assert_eq!(
            e.message,
            "workflow script must begin with `export const meta = { ... }`"
        );
    }

    #[test]
    fn non_object_meta_errors() {
        let e = parse("export const meta = 5").unwrap_err();
        assert_eq!(
            e.message,
            "workflow `meta` must be an object literal `{ ... }`"
        );
    }

    #[test]
    fn unclosed_meta_errors() {
        let e = parse("export const meta = { name: 'x'").unwrap_err();
        assert_eq!(e.message, "workflow `meta` object literal is never closed");
    }

    #[test]
    fn identifier_reference_is_not_defined() {
        let e = parse("export const meta = { name: foo }").unwrap_err();
        assert_eq!(
            e.message,
            "workflow `meta` must be a pure literal (no variables, calls, spreads, or substitutions): foo is not defined"
        );
    }

    #[test]
    fn missing_required_fields_report_zod_messages() {
        let e = parse("export const meta = {}").unwrap_err();
        assert_eq!(e.message, "workflow `meta` is invalid: Required; Required");
    }

    #[test]
    fn empty_and_wrong_typed_fields() {
        let e = parse("export const meta = { name: '', description: 5 }").unwrap_err();
        assert_eq!(
            e.message,
            "workflow `meta` is invalid: String must contain at least 1 character(s); Expected string, received number"
        );
    }

    #[test]
    fn null_optional_field_reports_received_null() {
        let e =
            parse("export const meta = { name: 'a', description: 'b', model: null }").unwrap_err();
        assert_eq!(
            e.message,
            "workflow `meta` is invalid: Expected string, received null"
        );
    }

    #[test]
    fn bad_phase_item_reports_object_expected() {
        let e = parse("export const meta = { name: 'a', description: 'b', phases: [ 3 ] }")
            .unwrap_err();
        assert_eq!(
            e.message,
            "workflow `meta` is invalid: Expected object, received number"
        );
    }

    #[test]
    fn brace_inside_a_string_does_not_miscount() {
        let parsed = parse("export const meta = { name: 'a}b', description: 'c{d' }\n").unwrap();
        assert_eq!(parsed.meta.name, "a}b");
        assert_eq!(parsed.meta.description, "c{d");
    }

    #[test]
    fn comments_and_trailing_commas_are_tolerated() {
        let src = concat!(
            "export const meta = {\n",
            "  // leading\n",
            "  name: 'a', /* inline */ description: 'b',\n",
            "}\n"
        );
        let parsed = parse(src).unwrap();
        assert_eq!(parsed.meta.name, "a");
        assert_eq!(parsed.meta.description, "b");
    }

    #[test]
    fn string_escapes_are_interpreted() {
        let parsed =
            parse("export const meta = { name: 'a', description: 'line1\\nline2' }\n").unwrap();
        assert_eq!(parsed.meta.description, "line1\nline2");
    }

    #[test]
    fn exact_source_meta_and_wrapper_preserve_arbitrary_utf16() {
        let raw_name = RuntimeString::from_utf16(vec![0xd800]);
        let raw_phase = RuntimeString::from_utf16(vec![0xdc01]);
        let raw_body = RuntimeString::from_utf16(vec![0xd804]);
        let source = RuntimeString::from_utf8("export const meta = { name: 'n")
            .concat(&raw_name)
            .concat(&RuntimeString::from_utf8(
                "', description: '\\uDC00\\u{D802}\\u{1f600}', phases: [{ title: 't",
            ))
            .concat(&raw_phase)
            .concat(&RuntimeString::from_utf8(
                "' }], model: '\\uD803' }\nreturn 'body",
            ))
            .concat(&raw_body)
            .concat(&RuntimeString::from_utf8("'\n"));

        let parsed = parse_workflow(&source).unwrap();
        assert_eq!(parsed.meta.name.as_utf16(), &[u16::from(b'n'), 0xd800]);
        assert_eq!(
            parsed.meta.description.as_utf16(),
            &[0xdc00, 0xd802, 0xd83d, 0xde00]
        );
        assert_eq!(
            parsed.meta.phases.as_ref().unwrap()[0].title.as_utf16(),
            &[u16::from(b't'), 0xdc01]
        );
        assert_eq!(parsed.meta.model.as_ref().unwrap().as_utf16(), &[0xd803]);
        assert!(parsed.wrapped_source.as_utf16().contains(&0xd800));
        assert!(parsed.wrapped_source.as_utf16().contains(&0xdc01));
        assert!(parsed.wrapped_source.as_utf16().contains(&0xd804));
        assert!(!parsed.wrapped_source.as_utf16().contains(&0xfffd));

        let encoded = runtime_codec::encode_value(&parsed.meta).unwrap();
        let encoded_object = encoded.as_object().unwrap();
        assert_eq!(
            encoded_object.keys().collect::<Vec<_>>(),
            vec![
                RuntimeString::from("name"),
                RuntimeString::from("description"),
                RuntimeString::from("phases"),
                RuntimeString::from("model"),
            ]
        );
        let decoded = WorkflowMeta::decode_runtime(&encoded).unwrap();
        assert_eq!(decoded, parsed.meta);
    }

    #[test]
    fn exact_literal_evaluator_keeps_duplicate_slot_and_property_order() {
        let literal = RuntimeString::from_utf8(
            r#"{"b":"\uD800","2":"two","1":"one","a":"\uDC00","b":"\uD801"}"#,
        );
        let value = evaluate_meta_literal(&literal).unwrap();
        let object = value.as_object().unwrap();
        assert_eq!(
            object
                .keys()
                .map(|key| key.as_utf16().to_vec())
                .collect::<Vec<_>>(),
            vec![
                vec![u16::from(b'1')],
                vec![u16::from(b'2')],
                vec![u16::from(b'b')],
                vec![u16::from(b'a')],
            ]
        );
        assert_eq!(
            object
                .get("b")
                .and_then(RuntimeJson::as_string)
                .unwrap()
                .as_utf16(),
            &[0xd801]
        );
        assert_eq!(
            object
                .get("a")
                .and_then(RuntimeJson::as_string)
                .unwrap()
                .as_utf16(),
            &[0xdc00]
        );
    }

    #[test]
    fn non_scalar_syntax_error_is_injective_and_not_replacement() {
        let source = RuntimeString::from_utf8("export const meta = { name: ")
            .concat(&RuntimeString::from_utf16(vec![0xd800]))
            .concat(&RuntimeString::from_utf8(" }"));
        let error = parse_workflow(&source).unwrap_err();
        assert!(error.message.contains("Unexpected token '\\ud800'"));
        assert!(!error.message.contains("�"));
    }

    #[test]
    fn nonfinite_number_stays_a_number_for_validation() {
        let error = parse("export const meta = { name: 'a', description: 1e999 }").unwrap_err();
        assert_eq!(
            error.message,
            "workflow `meta` is invalid: Expected string, received number"
        );
    }
}
