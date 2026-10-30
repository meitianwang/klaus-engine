use crate::runtime_values::{RuntimePath, RuntimeString};

const CHAR_DOLLAR: u16 = b'$' as u16;
const CHAR_DOUBLE_QUOTE: u16 = b'"' as u16;
const CHAR_LEFT_BRACE: u16 = b'{' as u16;
const CHAR_SINGLE_QUOTE: u16 = b'\'' as u16;
const CHAR_BACKTICK: u16 = b'`' as u16;
const CHAR_BACKSLASH: u16 = b'\\' as u16;

/// Platform boundary selected only by the module's cross-platform tests.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum Node20FsPathTarget {
    /// The libuv Unix filesystem boundary consumes the projected bytes.
    Unix,
    /// The libuv Windows filesystem boundary converts the projected bytes to wide units.
    Windows,
}

/// One validated Unix pathname after the single Node/V8 UTF-8 projection.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct Node20UnixFsPath {
    bytes: Vec<u8>,
}

impl Node20UnixFsPath {
    /// Native pathname bytes, excluding the C terminator added by the syscall shim.
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// One validated Windows pathname after Node UTF-8 and libuv 1.46 conversion.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct Node20WindowsFsPath {
    node_utf8: Vec<u8>,
    wide: Vec<u16>,
}

impl Node20WindowsFsPath {
    /// The one Node/V8 UTF-8 image that libuv consumed.
    pub(crate) fn node_utf8_bytes(&self) -> &[u8] {
        &self.node_utf8
    }

    /// Final wide pathname units, excluding the C terminator added by libuv.
    pub(crate) fn as_wide(&self) -> &[u16] {
        &self.wide
    }
}

/// Closed native result of [`Node20FsPathProjection`].
#[cfg(test)]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum Node20FsProjectedPath {
    /// Unix consumes the exact Node UTF-8 bytes.
    Unix(Node20UnixFsPath),
    /// Windows consumes the libuv-produced wide units.
    Windows(Node20WindowsFsPath),
}

/// Exact Node 20 `ERR_INVALID_ARG_VALUE` raised for a logical path containing NUL.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct Node20FsPathProjectionError {
    code: RuntimeString,
    argument_label: RuntimeString,
    message: RuntimeString,
}

impl Node20FsPathProjectionError {
    /// Stable Node error code.
    pub(crate) fn code(&self) -> &RuntimeString {
        &self.code
    }

    /// Exact argument/property label supplied by the filesystem wrapper.
    pub(crate) fn argument_label(&self) -> &RuntimeString {
        &self.argument_label
    }

    /// Exact Node-formatted error message.
    pub(crate) fn message(&self) -> &RuntimeString {
        &self.message
    }
}

/// Pure Rust terminal adapter for Node 20.19.4 filesystem string paths.
///
/// This is a shadow foundation. It does not call a filesystem API and is not
/// wired into legacy production call sites. The adapter validates the exact
/// logical UTF-16 value, performs one V8-compatible UTF-8 projection, and then
/// applies the selected libuv 1.46 platform conversion without path rewriting.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub(crate) struct Node20FsPathProjection;

#[cfg(unix)]
pub(crate) type Node20NativeFsPath = Node20UnixFsPath;
#[cfg(windows)]
pub(crate) type Node20NativeFsPath = Node20WindowsFsPath;

impl Node20FsPathProjection {
    /// Pinned JavaScript runtime.
    pub(crate) const NODE_VERSION: &'static str = "20.19.4";
    /// Pinned V8 runtime.
    pub(crate) const V8_VERSION: &'static str = "11.3.244.8-node.29";
    /// Pinned libuv runtime.
    pub(crate) const LIBUV_VERSION: &'static str = "1.46.0";
    /// SHA-256 of the official `node-v20.19.4.tar.xz` source archive.
    pub(crate) const NODE_SOURCE_ARCHIVE_SHA256: &'static str =
        "b87fd7106013d3906706913ffc63a4403715fbb272c4f83ff4338527353eec0f";
    /// SHA-256 of `src/util.cc`, which owns `BufferValue` and V8 UTF-8 flags.
    pub(crate) const NODE_UTIL_CC_SHA256: &'static str =
        "00d53e30632236004d23a50f6b8fb77d53ad7656f48ae566d92b3ddfe7b1eb3d";
    /// SHA-256 of `lib/internal/fs/utils.js`, which validates string paths.
    pub(crate) const NODE_INTERNAL_FS_UTILS_SHA256: &'static str =
        "f246bfb979d956afc20b5d8c58f97da415cb556169391f5b451c28b74bd13c97";
    /// SHA-256 of `lib/internal/errors.js`, which formats the stable error.
    pub(crate) const NODE_INTERNAL_ERRORS_SHA256: &'static str =
        "05ca73078e2d6640545587e41f27fcd6e53847cf3141a24b0a768617228f591c";
    /// SHA-256 of `lib/internal/util/inspect.js`, which quotes the received path.
    pub(crate) const NODE_INTERNAL_UTIL_INSPECT_SHA256: &'static str =
        "1c313514349cfd251231e22869d3569c89edfe374f0bce6bb2b889ae990e33f6";
    /// SHA-256 of `deps/uv/src/win/fs.c` in the pinned Node archive.
    pub(crate) const LIBUV_WINDOWS_FS_SHA256: &'static str =
        "a156bf6a84116eb0a30cc313715b05ebc870a62320786058a09b83f9991d1fe2";
    /// SHA-256 of bare libuv tag `v1.46.0/src/win/fs.c`. Node's vendored
    /// source differs by the four-byte decoder mask captured by the source pin.
    pub(crate) const BARE_LIBUV_146_WINDOWS_FS_SHA256: &'static str =
        "b4b121080b05a4b091f58f570d979103bb7439553205aef3e2ffd22cef3b8a29";

    /// Validate and project one logical string path for the actual host syscall.
    ///
    /// The target is selected by compilation, never by a downstream caller.
    pub(crate) fn project(
        path: &RuntimePath,
        argument_label: &str,
    ) -> Result<Node20NativeFsPath, Node20FsPathProjectionError> {
        let logical = path.as_runtime_string();
        if logical.as_utf16().contains(&0) {
            return Err(node20_nul_path_error(argument_label, logical));
        }

        // This is the only logical-to-host projection in the adapter. The
        // resulting bytes are moved into the native carrier and, on Windows,
        // are also the exact bytes consumed by libuv's decoder.
        let node_utf8 = logical.to_node_utf8();
        #[cfg(unix)]
        {
            Ok(Node20UnixFsPath { bytes: node_utf8 })
        }
        #[cfg(windows)]
        {
            let wide = libuv_146_wtf8_to_wide(&node_utf8);
            Ok(Node20WindowsFsPath { node_utf8, wide })
        }
    }
}

#[cfg(test)]
fn project_for_target(
    target: Node20FsPathTarget,
    path: &RuntimePath,
    argument_label: &str,
) -> Result<Node20FsProjectedPath, Node20FsPathProjectionError> {
    let native = Node20FsPathProjection::project(path, argument_label)?;
    #[cfg(unix)]
    let node_utf8 = native.bytes;
    #[cfg(windows)]
    let node_utf8 = native.node_utf8;

    Ok(match target {
        Node20FsPathTarget::Unix => {
            Node20FsProjectedPath::Unix(Node20UnixFsPath { bytes: node_utf8 })
        }
        Node20FsPathTarget::Windows => {
            let wide = libuv_146_wtf8_to_wide(&node_utf8);
            Node20FsProjectedPath::Windows(Node20WindowsFsPath { node_utf8, wide })
        }
    })
}

fn node20_nul_path_error(
    argument_label: &str,
    logical: &RuntimeString,
) -> Node20FsPathProjectionError {
    let label = RuntimeString::from(argument_label);
    let inspected = node20_inspect_string(logical);
    let kind = if argument_label.contains('.') {
        "property"
    } else {
        "argument"
    };
    let mut message = Vec::new();
    push_ascii(&mut message, "The ");
    push_ascii(&mut message, kind);
    push_ascii(&mut message, " '");
    message.extend_from_slice(label.as_utf16());
    push_ascii(
        &mut message,
        "' must be a string, Uint8Array, or URL without null bytes. Received ",
    );
    message.extend_from_slice(&inspected);
    Node20FsPathProjectionError {
        code: RuntimeString::from("ERR_INVALID_ARG_VALUE"),
        argument_label: label,
        message: RuntimeString::from_utf16(message),
    }
}

/// Node 20's default `util.inspect()` string form used by
/// `ERR_INVALID_ARG_VALUE`, including its 128-code-unit error truncation.
fn node20_inspect_string(value: &RuntimeString) -> Vec<u16> {
    // `util.inspect` first applies its default `maxStringLength` in JavaScript
    // code units. Its omitted trailer can never enter the later 128-unit
    // `ERR_INVALID_ARG_VALUE` prefix, but quote selection must use this slice.
    let units = &value.as_utf16()[..value.len_utf16().min(10_000)];

    // `formatPrimitive` uses the default `compact = 3`, `breakLength = 80`,
    // and zero indentation here. Long strings are split *after* each LF and
    // each resulting line is escaped independently before concatenation. The
    // per-line escaping is observable because each line selects its own quote.
    let mut inspected = if units.len() > 16 && units.len() > 80 - 4 {
        let mut output = Vec::new();
        let mut start = 0;
        for (index, unit) in units.iter().copied().enumerate() {
            if unit != u16::from(b'\n') {
                continue;
            }
            if !output.is_empty() {
                push_ascii(&mut output, " +\n  ");
            }
            output.extend_from_slice(&node20_str_escape(&units[start..=index]));
            start = index + 1;
        }
        if start < units.len() || output.is_empty() {
            if !output.is_empty() {
                push_ascii(&mut output, " +\n  ");
            }
            output.extend_from_slice(&node20_str_escape(&units[start..]));
        }
        output
    } else {
        node20_str_escape(units)
    };

    if inspected.len() > 128 {
        inspected.truncate(128);
        push_ascii(&mut inspected, "...");
    }
    inspected
}

/// Node 20 `strEscape` for one already sliced string or multiline segment.
fn node20_str_escape(units: &[u16]) -> Vec<u16> {
    let contains_single_quote = units.contains(&CHAR_SINGLE_QUOTE);
    let quote = if !contains_single_quote {
        CHAR_SINGLE_QUOTE
    } else if !units.contains(&CHAR_DOUBLE_QUOTE) {
        CHAR_DOUBLE_QUOTE
    } else if !units.contains(&CHAR_BACKTICK) && !contains_dollar_left_brace(units) {
        CHAR_BACKTICK
    } else {
        CHAR_SINGLE_QUOTE
    };

    let mut escaped = Vec::with_capacity(units.len().saturating_add(2));
    escaped.push(quote);
    let mut index = 0;
    while index < units.len() {
        let unit = units[index];
        if (0xd800..=0xdbff).contains(&unit)
            && units
                .get(index + 1)
                .is_some_and(|next| (0xdc00..=0xdfff).contains(next))
        {
            escaped.extend_from_slice(&units[index..index + 2]);
            index += 2;
            continue;
        }
        if (0xd800..=0xdfff).contains(&unit) {
            push_ascii(&mut escaped, "\\u");
            push_lower_hex_u16(&mut escaped, unit);
        } else if unit == quote || unit == CHAR_BACKSLASH {
            escaped.push(CHAR_BACKSLASH);
            escaped.push(unit);
        } else if unit <= 0x1f || (0x7f..=0x9f).contains(&unit) {
            push_node_control_escape(&mut escaped, unit);
        } else {
            escaped.push(unit);
        }
        index += 1;
    }
    escaped.push(quote);
    escaped
}

fn contains_dollar_left_brace(units: &[u16]) -> bool {
    units
        .windows(2)
        .any(|pair| pair == [CHAR_DOLLAR, CHAR_LEFT_BRACE])
}

fn push_node_control_escape(output: &mut Vec<u16>, unit: u16) {
    match unit {
        0x08 => push_ascii(output, "\\b"),
        0x09 => push_ascii(output, "\\t"),
        0x0a => push_ascii(output, "\\n"),
        0x0c => push_ascii(output, "\\f"),
        0x0d => push_ascii(output, "\\r"),
        _ => {
            push_ascii(output, "\\x");
            output.push(upper_hex_digit((unit >> 4) as u8));
            output.push(upper_hex_digit((unit & 0x0f) as u8));
        }
    }
}

fn push_lower_hex_u16(output: &mut Vec<u16>, value: u16) {
    for shift in [12, 8, 4, 0] {
        output.push(lower_hex_digit(((value >> shift) & 0x0f) as u8));
    }
}

fn upper_hex_digit(value: u8) -> u16 {
    u16::from(if value < 10 {
        b'0' + value
    } else {
        b'A' + value - 10
    })
}

fn lower_hex_digit(value: u8) -> u16 {
    u16::from(if value < 10 {
        b'0' + value
    } else {
        b'a' + value - 10
    })
}

fn push_ascii(output: &mut Vec<u16>, value: &str) {
    output.extend(value.bytes().map(u16::from));
}

/// Exact `fs__wtf8_to_wide` conversion from libuv 1.46 `src/win/fs.c`.
///
/// The source bytes here are always the valid UTF-8 image produced by V8, but
/// the decoder intentionally retains libuv's wider WTF-8 acceptance and its
/// `code_point > 0x10000` boundary rather than using a Rust Unicode shortcut.
fn libuv_146_wtf8_to_wide(source: &[u8]) -> Vec<u16> {
    let mut wide = Vec::with_capacity(source.len());
    let mut index = 0;
    while index < source.len() {
        let (code_point, width) = libuv_146_decode_wtf8_char(&source[index..])
            .expect("RuntimeString::to_node_utf8 always emits valid UTF-8");
        index += width;
        if code_point > 0x10000 {
            let value = code_point - 0x10000;
            wide.push(0xd800 + ((value >> 10) as u16));
            wide.push(0xdc00 + ((value & 0x03ff) as u16));
        } else {
            // This preserves libuv 1.46's exact U+10000 boundary behavior.
            wide.push(code_point as u16);
        }
    }
    wide
}

fn libuv_146_decode_wtf8_char(source: &[u8]) -> Option<(u32, usize)> {
    let first = *source.first()?;
    if first <= 0x7f {
        return Some((u32::from(first), 1));
    }
    if first < 0xc2 {
        return None;
    }

    let second = *source.get(1)?;
    if second & 0xc0 != 0x80 {
        return None;
    }
    let mut code_point = (u32::from(first) << 6) | u32::from(second & 0x3f);
    if first <= 0xdf {
        return Some((code_point & 0x07ff, 2));
    }

    let third = *source.get(2)?;
    if third & 0xc0 != 0x80 {
        return None;
    }
    code_point = (code_point << 6) | u32::from(third & 0x3f);
    if first <= 0xef {
        return Some((code_point & 0xffff, 3));
    }

    let fourth = *source.get(3)?;
    if fourth & 0xc0 != 0x80 {
        return None;
    }
    code_point = ((code_point << 6) | u32::from(fourth & 0x3f)) & 0x1f_ffff;
    (first <= 0xf4 && code_point <= 0x10_ffff).then_some((code_point, 4))
}

#[cfg(test)]
#[path = "fs_path_projection_tests.rs"]
mod tests;
