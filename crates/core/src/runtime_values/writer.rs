use std::io::{self, Write};

use super::{RuntimeJson, RuntimeString};

/// JavaScript-compatible compact `JSON.stringify` output.
pub fn stringify(value: &RuntimeJson) -> RuntimeString {
    render(value, false)
}

/// JavaScript-compatible `JSON.stringify(value, null, 2)` output.
pub fn stringify_pretty(value: &RuntimeJson) -> RuntimeString {
    render(value, true)
}

/// Write compact exact JSON as scalar UTF-8 bytes.
pub fn write_to<W: Write + ?Sized>(value: &RuntimeJson, writer: &mut W) -> io::Result<()> {
    writer.write_all(&stringify(value).to_node_utf8())
}

enum WriteOp<'a> {
    Value(&'a RuntimeJson, usize),
    String(RuntimeString),
    Ascii(&'static str),
    Indent(usize),
}

fn render(value: &RuntimeJson, pretty: bool) -> RuntimeString {
    let mut output = Vec::new();
    let mut stack = vec![WriteOp::Value(value, 0)];
    while let Some(operation) = stack.pop() {
        match operation {
            WriteOp::Value(value, depth) => match value {
                RuntimeJson::Null => push_ascii(&mut output, "null"),
                RuntimeJson::Bool(value) => {
                    push_ascii(&mut output, if *value { "true" } else { "false" });
                }
                RuntimeJson::Number(value) => push_ascii(&mut output, &value.to_json_token()),
                RuntimeJson::String(value) => write_string(&mut output, value),
                RuntimeJson::Array(values) if values.is_empty() => push_ascii(&mut output, "[]"),
                RuntimeJson::Object(values) if values.is_empty() => push_ascii(&mut output, "{}"),
                RuntimeJson::Array(values) => {
                    if pretty {
                        push_ascii(&mut output, "[\n");
                        stack.push(WriteOp::Ascii("]"));
                        stack.push(WriteOp::Indent(depth));
                        for (index, value) in values.iter().enumerate().rev() {
                            stack.push(WriteOp::Ascii(if index + 1 == values.len() {
                                "\n"
                            } else {
                                ",\n"
                            }));
                            stack.push(WriteOp::Value(value, depth + 1));
                            stack.push(WriteOp::Indent(depth + 1));
                        }
                    } else {
                        push_ascii(&mut output, "[");
                        stack.push(WriteOp::Ascii("]"));
                        for (index, value) in values.iter().enumerate().rev() {
                            if index + 1 != values.len() {
                                stack.push(WriteOp::Ascii(","));
                            }
                            stack.push(WriteOp::Value(value, depth));
                        }
                    }
                }
                RuntimeJson::Object(values) => {
                    let entries = values.entries().collect::<Vec<_>>();
                    if pretty {
                        push_ascii(&mut output, "{\n");
                        stack.push(WriteOp::Ascii("}"));
                        stack.push(WriteOp::Indent(depth));
                        for (index, (key, value)) in entries.iter().enumerate().rev() {
                            stack.push(WriteOp::Ascii(if index + 1 == entries.len() {
                                "\n"
                            } else {
                                ",\n"
                            }));
                            stack.push(WriteOp::Value(value, depth + 1));
                            stack.push(WriteOp::Ascii(": "));
                            stack.push(WriteOp::String(key.clone()));
                            stack.push(WriteOp::Indent(depth + 1));
                        }
                    } else {
                        push_ascii(&mut output, "{");
                        stack.push(WriteOp::Ascii("}"));
                        for (index, (key, value)) in entries.iter().enumerate().rev() {
                            if index + 1 != entries.len() {
                                stack.push(WriteOp::Ascii(","));
                            }
                            stack.push(WriteOp::Value(value, depth));
                            stack.push(WriteOp::Ascii(":"));
                            stack.push(WriteOp::String(key.clone()));
                        }
                    }
                }
            },
            WriteOp::String(value) => write_string(&mut output, &value),
            WriteOp::Ascii(value) => push_ascii(&mut output, value),
            WriteOp::Indent(depth) => {
                for _ in 0..depth {
                    push_ascii(&mut output, "  ");
                }
            }
        }
    }
    RuntimeString::from_utf16(output)
}

fn write_string(output: &mut Vec<u16>, value: &RuntimeString) {
    output.push(u16::from(b'"'));
    let units = value.as_utf16();
    let mut index = 0;
    while index < units.len() {
        let unit = units[index];
        if (0xd800..=0xdbff).contains(&unit)
            && units
                .get(index + 1)
                .is_some_and(|low| (0xdc00..=0xdfff).contains(low))
        {
            output.push(unit);
            output.push(units[index + 1]);
            index += 2;
            continue;
        }
        match unit {
            unit if (0xd800..=0xdfff).contains(&unit) => push_hex_escape(output, unit),
            unit if unit == u16::from(b'"') => push_ascii(output, "\\\""),
            unit if unit == u16::from(b'\\') => push_ascii(output, "\\\\"),
            0x0008 => push_ascii(output, "\\b"),
            0x000c => push_ascii(output, "\\f"),
            0x000a => push_ascii(output, "\\n"),
            0x000d => push_ascii(output, "\\r"),
            0x0009 => push_ascii(output, "\\t"),
            0x0000..=0x001f => push_hex_escape(output, unit),
            unit => output.push(unit),
        }
        index += 1;
    }
    output.push(u16::from(b'"'));
}

fn push_hex_escape(output: &mut Vec<u16>, value: u16) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    push_ascii(output, "\\u");
    for shift in [12, 8, 4, 0] {
        output.push(u16::from(HEX[usize::from((value >> shift) & 0x0f)]));
    }
}

fn push_ascii(output: &mut Vec<u16>, value: &str) {
    output.extend(value.bytes().map(u16::from));
}
