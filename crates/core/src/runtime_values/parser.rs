use std::fmt;

use super::{RuntimeJson, RuntimeNumber, RuntimeObject, RuntimeString};

/// Stable category for an exact runtime JSON parse failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeJsonErrorKind {
    UnexpectedEnd,
    UnexpectedToken,
    TrailingCharacters,
    InvalidEscape,
    InvalidUnicodeEscape,
    UnescapedControlCharacter,
    InvalidNumber,
}

/// Exact JSON parse failure at a UTF-16 code-unit offset.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeJsonError {
    offset: usize,
    kind: RuntimeJsonErrorKind,
}

impl RuntimeJsonError {
    pub fn offset(self) -> usize {
        self.offset
    }

    pub fn kind(self) -> RuntimeJsonErrorKind {
        self.kind
    }
}

impl fmt::Display for RuntimeJsonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "runtime JSON {:?} at UTF-16 offset {}",
            self.kind, self.offset
        )
    }
}

impl std::error::Error for RuntimeJsonError {}

/// Parse exact JavaScript JSON from UTF-16 without a serde intermediate.
pub fn parse(input: &RuntimeString) -> Result<RuntimeJson, RuntimeJsonError> {
    Parser::new(input.as_utf16()).parse()
}

struct Parser<'a> {
    units: &'a [u16],
    index: usize,
}

enum Frame {
    Array(Vec<RuntimeJson>),
    Object {
        value: RuntimeObject,
        pending_key: Option<RuntimeString>,
    },
}

enum AfterValue {
    NextArrayValue,
    NextObjectKey,
    Complete(RuntimeJson),
}

impl<'a> Parser<'a> {
    fn new(units: &'a [u16]) -> Self {
        Self { units, index: 0 }
    }

    fn parse(mut self) -> Result<RuntimeJson, RuntimeJsonError> {
        self.skip_whitespace();
        let mut frames = Vec::new();
        let mut value = self.begin_value(&mut frames)?;

        'attach: loop {
            if frames.is_empty() {
                self.skip_whitespace();
                if self.index != self.units.len() {
                    return Err(self.error(RuntimeJsonErrorKind::TrailingCharacters));
                }
                return Ok(value);
            }

            let after = match frames.last_mut().expect("non-empty frame stack") {
                Frame::Array(values) => {
                    values.push(value);
                    self.skip_whitespace();
                    match self.peek() {
                        Some(unit) if unit == ascii(b',') => {
                            self.index += 1;
                            AfterValue::NextArrayValue
                        }
                        Some(unit) if unit == ascii(b']') => {
                            self.index += 1;
                            AfterValue::Complete(RuntimeJson::Array(std::mem::take(values)))
                        }
                        Some(_) => return Err(self.error(RuntimeJsonErrorKind::UnexpectedToken)),
                        None => return Err(self.error(RuntimeJsonErrorKind::UnexpectedEnd)),
                    }
                }
                Frame::Object {
                    value: object,
                    pending_key,
                } => {
                    let key = pending_key
                        .take()
                        .expect("an object frame awaiting a value has a key");
                    object.insert(key, value);
                    self.skip_whitespace();
                    match self.peek() {
                        Some(unit) if unit == ascii(b',') => {
                            self.index += 1;
                            AfterValue::NextObjectKey
                        }
                        Some(unit) if unit == ascii(b'}') => {
                            self.index += 1;
                            AfterValue::Complete(RuntimeJson::Object(std::mem::take(object)))
                        }
                        Some(_) => return Err(self.error(RuntimeJsonErrorKind::UnexpectedToken)),
                        None => return Err(self.error(RuntimeJsonErrorKind::UnexpectedEnd)),
                    }
                }
            };

            match after {
                AfterValue::NextArrayValue => {
                    self.skip_whitespace();
                    if self.peek() == Some(ascii(b']')) {
                        return Err(self.error(RuntimeJsonErrorKind::UnexpectedToken));
                    }
                    value = self.begin_value(&mut frames)?;
                }
                AfterValue::NextObjectKey => {
                    self.skip_whitespace();
                    if self.peek() == Some(ascii(b'}')) {
                        return Err(self.error(RuntimeJsonErrorKind::UnexpectedToken));
                    }
                    let key = self.parse_string()?;
                    self.skip_whitespace();
                    self.expect(ascii(b':'))?;
                    if let Some(Frame::Object { pending_key, .. }) = frames.last_mut() {
                        *pending_key = Some(key);
                    } else {
                        unreachable!("object key belongs to the top object frame");
                    }
                    value = self.begin_value(&mut frames)?;
                }
                AfterValue::Complete(completed) => {
                    frames.pop();
                    value = completed;
                }
            }
            continue 'attach;
        }
    }

    fn begin_value(&mut self, frames: &mut Vec<Frame>) -> Result<RuntimeJson, RuntimeJsonError> {
        loop {
            self.skip_whitespace();
            match self.peek() {
                Some(unit) if unit == ascii(b'n') => {
                    self.consume_literal(b"null")?;
                    return Ok(RuntimeJson::Null);
                }
                Some(unit) if unit == ascii(b't') => {
                    self.consume_literal(b"true")?;
                    return Ok(RuntimeJson::Bool(true));
                }
                Some(unit) if unit == ascii(b'f') => {
                    self.consume_literal(b"false")?;
                    return Ok(RuntimeJson::Bool(false));
                }
                Some(unit) if unit == ascii(b'"') => {
                    return self.parse_string().map(RuntimeJson::String);
                }
                Some(unit) if unit == ascii(b'-') || is_digit(unit) => {
                    return self.parse_number().map(RuntimeJson::Number);
                }
                Some(unit) if unit == ascii(b'[') => {
                    self.index += 1;
                    self.skip_whitespace();
                    if self.peek() == Some(ascii(b']')) {
                        self.index += 1;
                        return Ok(RuntimeJson::Array(Vec::new()));
                    }
                    frames.push(Frame::Array(Vec::new()));
                }
                Some(unit) if unit == ascii(b'{') => {
                    self.index += 1;
                    self.skip_whitespace();
                    if self.peek() == Some(ascii(b'}')) {
                        self.index += 1;
                        return Ok(RuntimeJson::Object(RuntimeObject::new()));
                    }
                    let key = self.parse_string()?;
                    self.skip_whitespace();
                    self.expect(ascii(b':'))?;
                    frames.push(Frame::Object {
                        value: RuntimeObject::new(),
                        pending_key: Some(key),
                    });
                }
                Some(_) => return Err(self.error(RuntimeJsonErrorKind::UnexpectedToken)),
                None => return Err(self.error(RuntimeJsonErrorKind::UnexpectedEnd)),
            }
        }
    }

    fn parse_string(&mut self) -> Result<RuntimeString, RuntimeJsonError> {
        self.expect(ascii(b'"'))?;
        let mut output = Vec::new();
        loop {
            let unit_offset = self.index;
            let unit = self
                .next()
                .ok_or_else(|| self.error(RuntimeJsonErrorKind::UnexpectedEnd))?;
            match unit {
                unit if unit == ascii(b'"') => return Ok(RuntimeString::from_utf16(output)),
                unit if unit == ascii(b'\\') => {
                    let escape_offset = self.index;
                    let escape = self
                        .next()
                        .ok_or_else(|| self.error(RuntimeJsonErrorKind::UnexpectedEnd))?;
                    match escape {
                        unit if unit == ascii(b'"') => output.push(ascii(b'"')),
                        unit if unit == ascii(b'\\') => output.push(ascii(b'\\')),
                        unit if unit == ascii(b'/') => output.push(ascii(b'/')),
                        unit if unit == ascii(b'b') => output.push(0x0008),
                        unit if unit == ascii(b'f') => output.push(0x000c),
                        unit if unit == ascii(b'n') => output.push(0x000a),
                        unit if unit == ascii(b'r') => output.push(0x000d),
                        unit if unit == ascii(b't') => output.push(0x0009),
                        unit if unit == ascii(b'u') => output.push(self.parse_hex_escape()?),
                        _ => {
                            return Err(
                                self.error_at(escape_offset, RuntimeJsonErrorKind::InvalidEscape)
                            );
                        }
                    }
                }
                0x0000..=0x001f => {
                    return Err(
                        self.error_at(unit_offset, RuntimeJsonErrorKind::UnescapedControlCharacter)
                    );
                }
                unit => output.push(unit),
            }
        }
    }

    fn parse_hex_escape(&mut self) -> Result<u16, RuntimeJsonError> {
        let mut value = 0_u16;
        for _ in 0..4 {
            let digit_offset = self.index;
            let unit = self
                .next()
                .ok_or_else(|| self.error(RuntimeJsonErrorKind::UnexpectedEnd))?;
            let digit = hex_digit(unit).ok_or_else(|| {
                self.error_at(digit_offset, RuntimeJsonErrorKind::InvalidUnicodeEscape)
            })?;
            value = (value << 4) | digit;
        }
        Ok(value)
    }

    fn parse_number(&mut self) -> Result<RuntimeNumber, RuntimeJsonError> {
        let start = self.index;
        if self.peek() == Some(ascii(b'-')) {
            self.index += 1;
        }
        match self.peek() {
            Some(unit) if unit == ascii(b'0') => self.index += 1,
            Some(unit) if (ascii(b'1')..=ascii(b'9')).contains(&unit) => {
                self.index += 1;
                while self.peek().is_some_and(is_digit) {
                    self.index += 1;
                }
            }
            _ => return Err(self.error(RuntimeJsonErrorKind::InvalidNumber)),
        }

        if self.peek() == Some(ascii(b'.')) {
            self.index += 1;
            if !self.peek().is_some_and(is_digit) {
                return Err(self.error(RuntimeJsonErrorKind::InvalidNumber));
            }
            while self.peek().is_some_and(is_digit) {
                self.index += 1;
            }
        }
        if matches!(self.peek(), Some(unit) if unit == ascii(b'e') || unit == ascii(b'E')) {
            self.index += 1;
            if matches!(self.peek(), Some(unit) if unit == ascii(b'+') || unit == ascii(b'-')) {
                self.index += 1;
            }
            if !self.peek().is_some_and(is_digit) {
                return Err(self.error(RuntimeJsonErrorKind::InvalidNumber));
            }
            while self.peek().is_some_and(is_digit) {
                self.index += 1;
            }
        }

        let token = self.units[start..self.index]
            .iter()
            .map(|unit| char::from_u32(u32::from(*unit)).expect("number tokens are ASCII"))
            .collect::<String>();
        token
            .parse::<f64>()
            .map(RuntimeNumber::from_f64)
            .map_err(|_| self.error(RuntimeJsonErrorKind::InvalidNumber))
    }

    fn consume_literal(&mut self, expected: &[u8]) -> Result<(), RuntimeJsonError> {
        for byte in expected {
            match self.peek() {
                Some(actual) if actual == ascii(*byte) => self.index += 1,
                Some(_) => return Err(self.error(RuntimeJsonErrorKind::UnexpectedToken)),
                None => return Err(self.error(RuntimeJsonErrorKind::UnexpectedEnd)),
            }
        }
        Ok(())
    }

    fn expect(&mut self, expected: u16) -> Result<(), RuntimeJsonError> {
        match self.peek() {
            Some(actual) if actual == expected => {
                self.index += 1;
                Ok(())
            }
            Some(_) => Err(self.error(RuntimeJsonErrorKind::UnexpectedToken)),
            None => Err(self.error(RuntimeJsonErrorKind::UnexpectedEnd)),
        }
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(0x0009 | 0x000a | 0x000d | 0x0020)) {
            self.index += 1;
        }
    }

    fn peek(&self) -> Option<u16> {
        self.units.get(self.index).copied()
    }

    fn next(&mut self) -> Option<u16> {
        let unit = self.peek()?;
        self.index += 1;
        Some(unit)
    }

    fn error(&self, kind: RuntimeJsonErrorKind) -> RuntimeJsonError {
        self.error_at(self.index, kind)
    }

    fn error_at(&self, offset: usize, kind: RuntimeJsonErrorKind) -> RuntimeJsonError {
        RuntimeJsonError { offset, kind }
    }
}

const fn ascii(byte: u8) -> u16 {
    byte as u16
}

fn is_digit(unit: u16) -> bool {
    (ascii(b'0')..=ascii(b'9')).contains(&unit)
}

fn hex_digit(unit: u16) -> Option<u16> {
    match unit {
        unit if (ascii(b'0')..=ascii(b'9')).contains(&unit) => Some(unit - ascii(b'0')),
        unit if (ascii(b'a')..=ascii(b'f')).contains(&unit) => Some(unit - ascii(b'a') + 10),
        unit if (ascii(b'A')..=ascii(b'F')).contains(&unit) => Some(unit - ascii(b'A') + 10),
        _ => None,
    }
}
