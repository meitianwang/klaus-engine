//! Streaming JSONL metadata extraction. Ignored values never enter an owned buffer.
use serde::Deserialize;
use std::io::{self, BufRead, Read};

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Header {
    #[serde(default)]
    pub kind: String,
    pub ctx: Option<Context>,
    pub message: Option<Message>,
    pub event: Option<String>,
    pub turn_id: Option<String>,
    pub payload: Option<Payload>,
    pub num_turns: Option<u32>,
}
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Context {
    pub turn_id: Option<String>,
}
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Message {
    pub uuid: Option<String>,
    #[serde(rename = "type")]
    pub kind: Option<String>,
    pub subtype: Option<String>,
    pub is_meta: Option<bool>,
    pub tool_use_id: Option<String>,
}
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Payload {
    pub tool_use_id: Option<String>,
    pub turn_id: Option<String>,
    pub message_uuid: Option<String>,
    pub display_uuid: Option<String>,
    pub cursor_epoch: Option<String>,
}

/// A record cannot consume the next line, including when its JSON is corrupt.
pub(super) struct Line<'a, R> {
    reader: &'a mut R,
    ended: bool,
    pub len: u64,
    pub newline: bool,
    available: usize,
}
impl<'a, R: BufRead> Line<'a, R> {
    pub fn new(reader: &'a mut R) -> Self {
        Self {
            reader,
            ended: false,
            len: 0,
            newline: false,
            available: 0,
        }
    }
    pub fn finish(&mut self) -> io::Result<()> {
        while !self.fill_buf()?.is_empty() {
            let len = self.fill_buf()?.len();
            self.consume(len);
        }
        Ok(())
    }
}
impl<R: BufRead> Read for Line<'_, R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let bytes = self.fill_buf()?;
        let len = bytes.len().min(out.len());
        out[..len].copy_from_slice(&bytes[..len]);
        self.consume(len);
        Ok(len)
    }
}
impl<R: BufRead> BufRead for Line<'_, R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.ended {
            return Ok(&[]);
        }
        let bytes = self.reader.fill_buf()?;
        if self.available == 0 {
            self.available = bytes
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(bytes.len(), |at| at + 1);
        }
        Ok(&bytes[..self.available])
    }
    fn consume(&mut self, len: usize) {
        if len > 0
            && self
                .reader
                .fill_buf()
                .is_ok_and(|bytes| bytes[len - 1] == b'\n')
        {
            self.ended = true;
            self.newline = true;
        }
        self.len += len as u64;
        self.available -= len;
        self.reader.consume(len);
    }
}

fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid rollout JSON")
}
fn peek(reader: &mut impl BufRead) -> io::Result<u8> {
    loop {
        let bytes = reader.fill_buf()?;
        let Some(&byte) = bytes.first() else {
            return Err(invalid());
        };
        if !byte.is_ascii_whitespace() {
            return Ok(byte);
        }
        reader.consume(1);
    }
}
fn expect(reader: &mut impl BufRead, byte: u8) -> io::Result<()> {
    if peek(reader)? != byte {
        return Err(invalid());
    }
    reader.consume(1);
    Ok(())
}
fn string(reader: &mut impl BufRead, mut output: Option<&mut Vec<u8>>) -> io::Result<()> {
    expect(reader, b'"')?;
    if let Some(out) = &mut output {
        out.push(b'"');
    }
    loop {
        let bytes = reader.fill_buf()?;
        if bytes.is_empty() {
            return Err(invalid());
        }
        let at = bytes
            .iter()
            .position(|byte| matches!(*byte, b'"' | b'\\' | b'\n' | b'\r'));
        let len = at.map_or(bytes.len(), |at| at + 1);
        if let Some(out) = &mut output {
            if out.len().saturating_add(len) > super::MAX_ITEMS_PAGE_BYTES as usize * 6 + 64 * 1024
            {
                return Err(super::oversized(super::MAX_ITEMS_PAGE_BYTES + 1));
            }
            out.extend_from_slice(&bytes[..len]);
        }
        let end = bytes[len - 1];
        reader.consume(len);
        match (at, end) {
            (Some(_), b'"') => return Ok(()),
            (Some(_), b'\\') => {
                let bytes = reader.fill_buf()?;
                let byte = *bytes.first().ok_or_else(invalid)?;
                if byte == b'\n' {
                    return Err(invalid());
                }
                if let Some(out) = &mut output {
                    out.push(byte);
                }
                reader.consume(1);
            }
            (Some(_), _) => return Err(invalid()),
            _ => {}
        }
    }
}
fn skip(reader: &mut impl BufRead, depth: usize) -> io::Result<()> {
    if depth > 128 {
        return Err(invalid());
    }
    match peek(reader)? {
        b'"' => string(reader, None),
        b'{' | b'[' => {
            let open = peek(reader)?;
            reader.consume(1);
            let close = if open == b'{' { b'}' } else { b']' };
            if peek(reader)? == close {
                reader.consume(1);
                return Ok(());
            }
            loop {
                if open == b'{' {
                    string(reader, None)?;
                    expect(reader, b':')?;
                }
                skip(reader, depth + 1)?;
                let byte = peek(reader)?;
                reader.consume(1);
                if byte == close {
                    return Ok(());
                }
                if byte != b',' {
                    return Err(invalid());
                }
            }
        }
        _ => {
            let mut found = false;
            loop {
                let bytes = reader.fill_buf()?;
                let len = bytes
                    .iter()
                    .position(|byte| {
                        byte.is_ascii_whitespace() || matches!(*byte, b',' | b'}' | b']')
                    })
                    .unwrap_or(bytes.len());
                if len == 0 {
                    return if found { Ok(()) } else { Err(invalid()) };
                }
                found = true;
                reader.consume(len);
            }
        }
    }
}

/// Keep only routing fields. Whole message content, output objects and prompt
/// replacement snapshots are skipped in chunks, even when they precede UUIDs.
fn object(
    reader: &mut impl BufRead,
    scope: &str,
    out: &mut Vec<u8>,
    summary: bool,
) -> io::Result<()> {
    expect(reader, b'{')?;
    out.push(b'{');
    let mut first = true;
    if peek(reader)? == b'}' {
        reader.consume(1);
        out.push(b'}');
        return Ok(());
    }
    loop {
        let mut key = Vec::new();
        string(reader, Some(&mut key))?;
        let name: String = serde_json::from_slice(&key).map_err(|_| invalid())?;
        expect(reader, b':')?;
        let nested = scope.is_empty() && matches!(name.as_str(), "ctx" | "message" | "payload");
        let keep = match scope {
            "" => {
                matches!(name.as_str(), "kind" | "event" | "turnId" | "numTurns")
                    || (summary && name == "summary")
            }
            "ctx" => name == "turnId",
            "message" => matches!(
                name.as_str(),
                "uuid" | "type" | "subtype" | "isMeta" | "toolUseId"
            ),
            "payload" => matches!(
                name.as_str(),
                "toolUseId" | "messageUuid" | "displayUuid" | "turnId" | "cursorEpoch"
            ),
            _ => false,
        };
        if nested || keep {
            if !first {
                out.push(b',');
            }
            first = false;
            out.extend_from_slice(&key);
            out.push(b':');
            if nested && peek(reader)? == b'{' {
                object(reader, &name, out, summary)?;
            } else if peek(reader)? == b'"' {
                string(reader, Some(out))?;
            } else {
                // These controls are nullable scalar values, never content.
                loop {
                    let byte = peek(reader)?;
                    if matches!(byte, b',' | b'}') {
                        break;
                    }
                    if matches!(byte, b'{' | b'[' | b']') {
                        return Err(invalid());
                    }
                    out.push(byte);
                    reader.consume(1);
                }
            }
        } else {
            skip(reader, 0)?;
        }
        let byte = peek(reader)?;
        reader.consume(1);
        if byte == b'}' {
            out.push(b'}');
            return Ok(());
        }
        if byte != b',' {
            return Err(invalid());
        }
    }
}

pub(super) fn header(reader: &mut impl BufRead) -> io::Result<Option<Header>> {
    let mut bytes = Vec::new();
    match object(reader, "", &mut bytes, false) {
        Ok(()) => Ok(serde_json::from_slice(&bytes).ok()),
        Err(error) if error.kind() == io::ErrorKind::InvalidData => Ok(None),
        Err(error) => Err(error),
    }
}

pub(super) fn compaction(
    reader: &mut impl BufRead,
) -> io::Result<crate::runtime_values::RuntimeString> {
    let mut bytes = Vec::new();
    object(reader, "", &mut bytes, true)?;
    let source = std::str::from_utf8(&bytes).map_err(|_| invalid())?;
    let value =
        crate::runtime_values::runtime_json::parse(&source.into()).map_err(|_| invalid())?;
    value
        .get("summary")
        .and_then(|value| value.as_string())
        .cloned()
        .ok_or_else(invalid)
}

pub(super) struct ImageReplacement {
    pub offset: u64,
    pub len: u64,
    pub bytes: Vec<u8>,
}

struct ScannedImage {
    offset: u64,
    len: u64,
    mime: String,
    source_path: Option<String>,
}

struct Positioned<R> {
    reader: R,
    offset: u64,
}
impl<R: BufRead> Read for Positioned<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let count = self.reader.read(out)?;
        self.offset += count as u64;
        Ok(count)
    }
}
impl<R: BufRead> BufRead for Positioned<R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.reader.fill_buf()
    }
    fn consume(&mut self, count: usize) {
        self.offset += count as u64;
        self.reader.consume(count);
    }
}
fn key(reader: &mut impl BufRead) -> io::Result<String> {
    let mut bytes = Vec::new();
    string(reader, Some(&mut bytes))?;
    serde_json::from_slice(&bytes).map_err(|_| invalid())
}
fn image<R: BufRead>(
    reader: &mut Positioned<R>,
    images: &mut Vec<ScannedImage>,
    saved: &mut Vec<crate::store::thread_history::SavedPathReference>,
) -> io::Result<()> {
    let _ = peek(reader)?;
    let start = reader.offset;
    expect(reader, b'{')?;
    let mut kind = None;
    let mut path = None;
    let mut mime = None;
    let mut text = None;
    if peek(reader)? == b'}' {
        reader.consume(1);
        return Ok(());
    }
    loop {
        let name = key(reader)?;
        expect(reader, b':')?;
        if matches!(name.as_str(), "type" | "sourcePath" | "mimeType" | "text")
            && peek(reader)? == b'"'
        {
            let value = key(reader)?;
            match name.as_str() {
                "type" => kind = Some(value),
                "sourcePath" => path = Some(value),
                "mimeType" => mime = Some(value),
                _ => text = Some(value),
            }
        } else {
            skip(reader, 0)?;
        }
        let next = peek(reader)?;
        reader.consume(1);
        if next == b'}' {
            break;
        }
        if next != b',' {
            return Err(invalid());
        }
    }
    match kind.as_deref() {
        Some("image") => {
            if let Some(mime) = mime {
                images.push(ScannedImage {
                    offset: start,
                    len: reader.offset - start,
                    mime,
                    source_path: path,
                });
            }
        }
        Some("text") => {
            if let Some(text) = text {
                saved.extend(crate::store::thread_history::saved_path_references(&text));
            }
        }
        _ => {}
    }
    Ok(())
}
fn content<R: BufRead>(
    reader: &mut Positioned<R>,
    images: &mut Vec<ImageReplacement>,
) -> io::Result<()> {
    expect(reader, b'[')?;
    let mut scanned = Vec::new();
    let mut saved = Vec::new();
    if peek(reader)? == b']' {
        reader.consume(1);
        return Ok(());
    }
    loop {
        if peek(reader)? == b'{' {
            image(reader, &mut scanned, &mut saved)?;
        } else {
            skip(reader, 0)?;
        }
        let next = peek(reader)?;
        reader.consume(1);
        if next == b']' {
            break;
        }
        if next != b',' {
            return Err(invalid());
        }
    }
    let mut saved_index = 0;
    for image in scanned {
        let mut path = image.source_path;
        if path.is_none() {
            while saved_index < saved.len() {
                let candidate = &saved[saved_index];
                saved_index += 1;
                let exists = std::path::Path::new(&candidate.path).is_file();
                if exists || candidate.exact || saved.len() == 1 {
                    path = Some(candidate.path.clone());
                    break;
                }
            }
        }
        let Some(path) = path else {
            continue;
        };
        if std::path::Path::new(&path).is_file() {
            images.push(ImageReplacement {
                offset: image.offset,
                len: image.len,
                bytes: serde_json::to_vec(&serde_json::json!({"type":"image","mimeType":image.mime,"base64":"","sourcePath":path}))
                    .map_err(|_| invalid())?,
            });
        }
    }
    images.sort_by_key(|image| image.offset);
    Ok(())
}
fn image_object<R: BufRead>(
    reader: &mut Positioned<R>,
    message: bool,
    images: &mut Vec<ImageReplacement>,
) -> io::Result<()> {
    expect(reader, b'{')?;
    if peek(reader)? == b'}' {
        reader.consume(1);
        return Ok(());
    }
    loop {
        let name = key(reader)?;
        expect(reader, b':')?;
        if !message && name == "message" && peek(reader)? == b'{' {
            image_object(reader, true, images)?;
        } else if message && name == "content" && peek(reader)? == b'[' {
            content(reader, images)?;
        } else {
            skip(reader, 0)?;
        }
        let next = peek(reader)?;
        reader.consume(1);
        if next == b'}' {
            return Ok(());
        }
        if next != b',' {
            return Err(invalid());
        }
    }
}

pub(super) fn image_replacements(reader: &mut impl BufRead) -> io::Result<Vec<ImageReplacement>> {
    let mut reader = Positioned { reader, offset: 0 };
    let mut images = Vec::new();
    image_object(&mut reader, false, &mut images)?;
    Ok(images)
}

/// Locate a projected field without buffering the surrounding rollout record.
pub(super) fn field_range(reader: &mut impl BufRead, path: &[&str]) -> io::Result<(u64, u64)> {
    fn locate<R: BufRead>(reader: &mut Positioned<R>, path: &[&str]) -> io::Result<(u64, u64)> {
        expect(reader, b'{')?;
        loop {
            let name = key(reader)?;
            expect(reader, b':')?;
            if name == path[0] {
                if path.len() > 1 {
                    return locate(reader, &path[1..]);
                }
                peek(reader)?;
                let start = reader.offset;
                skip(reader, 0)?;
                return Ok((start, reader.offset - start));
            }
            skip(reader, 0)?;
            let next = peek(reader)?;
            reader.consume(1);
            if next != b',' {
                return Err(invalid());
            }
        }
    }
    locate(&mut Positioned { reader, offset: 0 }, path)
}
