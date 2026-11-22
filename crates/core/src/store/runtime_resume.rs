//! Continuation-only recovery. Original audit and fork reads retain their full contract.

use crate::runtime_values::{NodeBufferUtf8, RuntimeJson, RuntimeString};
use crate::schemas::message::{Message, Usage};
use crate::schemas::rollout::{RolloutItem, SessionMeta, UsageLedgerItem};
use crate::store::jsonl_store::RolloutMetadataAccumulator;
use crate::store::projection::project_for_prompt;
use crate::store::rollout::parse_rollout_line;
use crate::store::session_store::RuntimeResumeResult;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Only top-level original assistant rows contribute legacy billing. Replacement
/// history is deliberately absent from this type, preventing duplicate charges.
#[derive(Debug, Clone, PartialEq)]
pub enum RuntimeUsage {
    Typed(UsageLedgerItem),
    Legacy {
        assistant_message_id: RuntimeString,
        model: RuntimeString,
        usage: Option<Usage>,
    },
}

pub(crate) fn history_metadata(
    meta: &SessionMeta,
    items: &[RolloutItem],
) -> (HashSet<RuntimeString>, Vec<RuntimeUsage>) {
    let mut users = HashSet::new();
    let mut usage = Vec::new();
    let mut billable = meta.forked_from_id.is_none()
        && meta.role != Some(crate::schemas::rollout::SessionRole::SideTask);
    for item in items {
        collect_history_metadata(item, &mut billable, &mut users, &mut usage);
    }
    (users, usage)
}

fn collect_history_metadata(
    item: &RolloutItem,
    billable: &mut bool,
    users: &mut HashSet<RuntimeString>,
    usage: &mut Vec<RuntimeUsage>,
) {
    match item {
        RolloutItem::TurnContext { .. } => *billable = true,
        RolloutItem::Message {
            message: Message::User(user),
        } => {
            users.insert(user.uuid.clone());
        }
        RolloutItem::Message {
            message: Message::Assistant(assistant),
        } if *billable => {
            usage.push(RuntimeUsage::Legacy {
                assistant_message_id: assistant.uuid.clone(),
                model: assistant.model.clone(),
                usage: assistant.usage.clone(),
            });
        }
        RolloutItem::UsageLedger(record) if *billable => {
            usage.push(RuntimeUsage::Typed(record.clone()))
        }
        RolloutItem::Event(event) if event.event == "rollout_migration" => {
            if let Some(ids) = event
                .payload
                .as_ref()
                .and_then(|payload| payload.get("seenUserMessageIds"))
                .and_then(RuntimeJson::as_array)
            {
                users.extend(ids.iter().filter_map(RuntimeJson::as_string).cloned());
            }
        }
        _ => {}
    }
}

/// Dropping the awaiting future cancels the blocking scanner at its next bounded
/// read. The worker retains its writer lease until it has observed cancellation.
pub(crate) struct ReadCancellation(pub Arc<AtomicBool>);
impl ReadCancellation {
    pub(crate) fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }
}
impl Drop for ReadCancellation {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
fn check(cancelled: &AtomicBool) -> io::Result<()> {
    if cancelled.load(Ordering::Acquire) {
        Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "runtime resume cancelled",
        ))
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SkimmedMessageKind {
    User,
    Assistant,
    ToolResult,
    System,
    CompactBoundary,
    Other,
}

#[derive(Debug)]
struct Row {
    offset: u64,
    len: u64,
    kind: String,
    event: Option<String>,
    replacement: bool,
    approval: bool,
    message_id: Option<RuntimeString>,
    msg_kind: Option<SkimmedMessageKind>,
    has_question_answer: bool,
}

/// A bounded line reader prevents malformed JSON from consuming a following row.
/// Ignored JSON values are scanned in chunks without allocating their strings.
struct Line<'a> {
    reader: &'a mut BufReader<File>,
    cancelled: &'a AtomicBool,
    ended: bool,
    available: usize,
    len: u64,
}
impl Read for Line<'_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let bytes = self.fill_buf()?;
        let len = bytes.len().min(out.len());
        out[..len].copy_from_slice(&bytes[..len]);
        self.consume(len);
        Ok(len)
    }
}
impl BufRead for Line<'_> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        check(self.cancelled)?;
        if self.ended {
            return Ok(&[]);
        }
        let bytes = self.reader.fill_buf()?;
        if self.available == 0 {
            self.available = bytes
                .iter()
                .position(|b| *b == b'\n')
                .map_or(bytes.len(), |n| n + 1);
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
        }
        self.reader.consume(len);
        self.available -= len;
        self.len += len as u64;
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
fn expect(reader: &mut impl BufRead, expected: u8) -> io::Result<()> {
    if peek(reader)? != expected {
        return Err(invalid());
    }
    reader.consume(1);
    Ok(())
}
fn string(reader: &mut impl BufRead, mut out: Option<&mut Vec<u8>>) -> io::Result<()> {
    expect(reader, b'"')?;
    if let Some(out) = &mut out {
        out.push(b'"');
    }
    loop {
        let bytes = reader.fill_buf()?;
        if bytes.is_empty() {
            return Err(invalid());
        }
        let at = bytes
            .iter()
            .position(|b| matches!(*b, b'"' | b'\\' | b'\n' | b'\r'));
        let len = at.map_or(bytes.len(), |at| at + 1);
        if let Some(out) = &mut out {
            out.extend_from_slice(&bytes[..len]);
        }
        let last = bytes[len - 1];
        reader.consume(len);
        match (at, last) {
            (Some(_), b'"') => return Ok(()),
            (Some(_), b'\\') => {
                let bytes = reader.fill_buf()?;
                let byte = *bytes.first().ok_or_else(invalid)?;
                if matches!(byte, b'\n' | b'\r') {
                    return Err(invalid());
                }
                if let Some(out) = &mut out {
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
            let close = if open == b'{' { b'}' } else { b']' };
            reader.consume(1);
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
                    .position(|b| b.is_ascii_whitespace() || matches!(*b, b',' | b'}' | b']'))
                    .unwrap_or(bytes.len());
                if len == 0 {
                    return if found { Ok(()) } else { Err(invalid()) };
                }
                reader.consume(len);
                found = true;
            }
        }
    }
}
struct Header {
    kind: String,
    event: Option<String>,
    replacement: bool,
    approval: bool,
    message_id: Option<RuntimeString>,
    msg_kind: Option<SkimmedMessageKind>,
    has_question_answer: bool,
}
fn runtime_string_literal(reader: &mut impl BufRead) -> io::Result<RuntimeString> {
    let mut value = Vec::new();
    string(reader, Some(&mut value))?;
    let source = NodeBufferUtf8::decode(&value);
    crate::runtime_values::runtime_json::parse(&source)
        .ok()
        .and_then(|parsed| parsed.as_string().cloned())
        .ok_or_else(invalid)
}
fn message_meta(
    reader: &mut impl BufRead,
) -> io::Result<(Option<RuntimeString>, Option<SkimmedMessageKind>, bool)> {
    if peek(reader)? != b'{' {
        skip(reader, 0)?;
        return Ok((None, None, false));
    }
    expect(reader, b'{')?;
    if peek(reader)? == b'}' {
        reader.consume(1);
        return Ok((None, None, false));
    }
    let mut id = None;
    let mut msg_kind = None;
    let mut has_question_answer = false;
    loop {
        let mut key = Vec::new();
        string(reader, Some(&mut key))?;
        let key: String = serde_json::from_slice(&key).map_err(|_| invalid())?;
        expect(reader, b':')?;
        if key == "uuid" && peek(reader)? == b'"' {
            id = Some(runtime_string_literal(reader)?);
        } else if key == "type" && peek(reader)? == b'"' {
            let mut raw_type = Vec::new();
            string(reader, Some(&mut raw_type))?;
            let parsed_type: String = serde_json::from_slice(&raw_type).map_err(|_| invalid())?;
            msg_kind = Some(match parsed_type.as_str() {
                "user" => SkimmedMessageKind::User,
                "assistant" => SkimmedMessageKind::Assistant,
                "tool_result" => SkimmedMessageKind::ToolResult,
                "system" => SkimmedMessageKind::System,
                "compact_boundary" => SkimmedMessageKind::CompactBoundary,
                _ => SkimmedMessageKind::Other,
            });
        } else if key == "questionAnswer" {
            if peek(reader)? != b'n' {
                has_question_answer = true;
            }
            skip(reader, 0)?;
        } else {
            skip(reader, 0)?;
        }
        let byte = peek(reader)?;
        reader.consume(1);
        if byte == b'}' {
            return Ok((id, msg_kind, has_question_answer));
        }
        if byte != b',' {
            return Err(invalid());
        }
    }
}
fn header(reader: &mut impl BufRead) -> io::Result<Header> {
    expect(reader, b'{')?;
    let mut kind = String::new();
    let mut event = None;
    let mut replacement = false;
    let mut approval = false;
    let mut uuid = None;
    let mut msg_kind = None;
    let mut has_question_answer = false;
    if peek(reader)? == b'}' {
        reader.consume(1);
        return Ok(Header {
            kind,
            event,
            replacement,
            approval,
            message_id: uuid,
            msg_kind,
            has_question_answer,
        });
    }
    loop {
        let mut key = Vec::new();
        string(reader, Some(&mut key))?;
        let key: String = serde_json::from_slice(&key).map_err(|_| invalid())?;
        expect(reader, b':')?;
        if key == "kind" || key == "event" {
            if peek(reader)? == b'"' {
                let mut value = Vec::new();
                string(reader, Some(&mut value))?;
                let value: String = serde_json::from_slice(&value).map_err(|_| invalid())?;
                if key == "kind" {
                    kind = value;
                } else {
                    event = Some(value);
                }
            } else {
                skip(reader, 0)?;
            }
        } else if key == "message" {
            let (parsed_id, parsed_kind, parsed_qa) = message_meta(reader)?;
            uuid = parsed_id;
            msg_kind = parsed_kind;
            has_question_answer = parsed_qa;
        } else {
            if key == "replacementHistory" {
                replacement = peek(reader)? != b'n';
            }
            if key == "approvalParentHistory" {
                approval = peek(reader)? != b'n';
            }
            skip(reader, 0)?;
        }
        let byte = peek(reader)?;
        reader.consume(1);
        if byte == b'}' {
            break;
        }
        if byte != b',' {
            return Err(invalid());
        }
    }
    Ok(Header {
        kind,
        event,
        replacement,
        approval,
        message_id: uuid,
        msg_kind,
        has_question_answer,
    })
}

fn rows(source: &File, cancelled: &AtomicBool) -> io::Result<Vec<Row>> {
    let mut reader = BufReader::with_capacity(64 * 1024, source.try_clone()?);
    let mut rows = Vec::new();
    let mut offset = 0;
    loop {
        check(cancelled)?;
        if reader.fill_buf()?.is_empty() {
            break;
        }
        let mut line = Line {
            reader: &mut reader,
            cancelled,
            ended: false,
            available: 0,
            len: 0,
        };
        let header = header(&mut line);
        // Fully drain even corrupt rows, checking cancellation per buffer.
        io::copy(&mut line, &mut io::sink())?;
        let Header {
            kind,
            event,
            replacement,
            approval,
            message_id,
            msg_kind,
            has_question_answer,
        } = header.unwrap_or(Header {
            kind: String::new(),
            event: None,
            replacement: false,
            approval: false,
            message_id: None,
            msg_kind: None,
            has_question_answer: false,
        });
        // Unknown routing must reach the original tolerant decoder. A valid
        // complex row must never disappear solely because the skimmer refused it.
        rows.push(Row {
            offset,
            len: line.len,
            kind,
            event,
            replacement,
            approval,
            message_id,
            msg_kind,
            has_question_answer,
        });
        offset += line.len;
    }
    Ok(rows)
}

fn read_row(file: &mut File, row: &Row, cancelled: &AtomicBool) -> io::Result<Option<RolloutItem>> {
    check(cancelled)?;
    file.seek(SeekFrom::Start(row.offset))?;
    let capacity = usize::try_from(row.len)
        .map_err(|_| io::Error::other("rollout row exceeds address space"))?;
    let mut bytes = Vec::with_capacity(capacity);
    let mut remaining = row.len;
    let mut buffer = [0_u8; 64 * 1024];
    while remaining > 0 {
        check(cancelled)?;
        let wanted = remaining.min(buffer.len() as u64) as usize;
        let count = file.read(&mut buffer[..wanted])?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "rollout changed during resume",
            ));
        }
        bytes.extend_from_slice(&buffer[..count]);
        remaining -= count as u64;
    }
    check(cancelled)?;
    let text = NodeBufferUtf8::decode(&bytes)
        .try_to_utf8()
        .expect("Node UTF-8 decoding is scalar");
    Ok(parse_rollout_line(&text))
}

struct CancelTake<'a, R> {
    reader: io::Take<R>,
    cancelled: &'a AtomicBool,
}
impl<R: Read> Read for CancelTake<'_, R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        check(self.cancelled)?;
        self.reader.read(out)
    }
}

fn read_u64_scalar(reader: &mut impl BufRead) -> io::Result<u64> {
    let mut digits = Vec::new();
    let first = peek(reader)?;
    if !first.is_ascii_digit() {
        return Err(invalid());
    }
    loop {
        let bytes = reader.fill_buf()?;
        let take_len = bytes
            .iter()
            .position(|b| !b.is_ascii_digit())
            .unwrap_or(bytes.len());
        if take_len == 0 {
            break;
        }
        digits.extend_from_slice(&bytes[..take_len]);
        reader.consume(take_len);
    }
    std::str::from_utf8(&digits)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(invalid)
}

fn skim_obsolete_compacted(
    file: &mut File,
    row: &Row,
    cancelled: &AtomicBool,
    slots: &mut Vec<QuestionSlot>,
) -> io::Result<bool> {
    check(cancelled)?;
    file.seek(SeekFrom::Start(row.offset))?;
    let bounded = CancelTake {
        reader: Read::by_ref(file).take(row.len),
        cancelled,
    };
    let mut reader = BufReader::with_capacity(64 * 1024, bounded);
    struct SkimmedCompacted {
        replaced_range: (u64, u64),
        replacement_ids: Option<Vec<RuntimeString>>,
        source_ids: Option<HashSet<RuntimeString>>,
    }
    let result = (|| -> io::Result<Option<SkimmedCompacted>> {
        expect(&mut reader, b'{')?;
        let mut is_compacted = false;
        let mut has_summary = false;
        let mut replaced_range = None;
        let mut replacement_ids: Option<Vec<RuntimeString>> = None;
        let mut source_ids: Option<HashSet<RuntimeString>> = None;
        if peek(&mut reader)? == b'}' {
            return Ok(None);
        }
        loop {
            let mut key_bytes = Vec::new();
            string(&mut reader, Some(&mut key_bytes))?;
            let field: String = serde_json::from_slice(&key_bytes).map_err(|_| invalid())?;
            expect(&mut reader, b':')?;
            match field.as_str() {
                "kind" if peek(&mut reader)? == b'"' => {
                    let mut val = Vec::new();
                    string(&mut reader, Some(&mut val))?;
                    let kind: String = serde_json::from_slice(&val).map_err(|_| invalid())?;
                    is_compacted = kind == "compacted";
                }
                "summary" if peek(&mut reader)? == b'"' => {
                    string(&mut reader, None)?;
                    has_summary = true;
                }
                "replacedRange" if peek(&mut reader)? == b'[' => {
                    expect(&mut reader, b'[')?;
                    let start = read_u64_scalar(&mut reader)?;
                    expect(&mut reader, b',')?;
                    let end = read_u64_scalar(&mut reader)?;
                    expect(&mut reader, b']')?;
                    replaced_range = Some((start, end));
                }
                "sourceMessageIds" if peek(&mut reader)? == b'[' => {
                    expect(&mut reader, b'[')?;
                    let mut set = HashSet::new();
                    if peek(&mut reader)? == b']' {
                        reader.consume(1);
                    } else {
                        loop {
                            set.insert(runtime_string_literal(&mut reader)?);
                            let sep = peek(&mut reader)?;
                            reader.consume(1);
                            if sep == b']' {
                                break;
                            }
                            if sep != b',' {
                                return Err(invalid());
                            }
                        }
                    }
                    source_ids = Some(set);
                }
                "replacementHistory" if peek(&mut reader)? == b'[' => {
                    expect(&mut reader, b'[')?;
                    let mut ids = Vec::new();
                    if peek(&mut reader)? == b']' {
                        reader.consume(1);
                    } else {
                        loop {
                            let (msg_id, msg_kind, has_qa) = message_meta(&mut reader)?;
                            if has_qa {
                                // Fall back to full decode so questionAnswer messages are retained.
                                return Ok(None);
                            }
                            let (Some(msg_id), Some(kind)) = (msg_id, msg_kind) else {
                                return Ok(None);
                            };
                            if kind == SkimmedMessageKind::Other {
                                return Ok(None);
                            }
                            ids.push(msg_id);
                            let sep = peek(&mut reader)?;
                            reader.consume(1);
                            if sep == b']' {
                                break;
                            }
                            if sep != b',' {
                                return Err(invalid());
                            }
                        }
                    }
                    replacement_ids = Some(ids);
                }
                _ => skip(&mut reader, 0)?,
            }
            let sep = peek(&mut reader)?;
            reader.consume(1);
            if sep == b'}' {
                break;
            }
            if sep != b',' {
                return Err(invalid());
            }
        }
        let Some(range) = replaced_range else {
            return Ok(None);
        };
        if !is_compacted || !has_summary {
            return Ok(None);
        }
        Ok(Some(SkimmedCompacted {
            replaced_range: range,
            replacement_ids,
            source_ids,
        }))
    })();
    match result {
        Ok(Some(SkimmedCompacted {
            replaced_range,
            replacement_ids,
            source_ids,
        })) => {
            if let Some(ids) = replacement_ids {
                let mut next: Vec<QuestionSlot> = ids
                    .into_iter()
                    .map(|id| QuestionSlot { id, answer: None })
                    .collect();
                if let Some(source) = source_ids {
                    let mut retained: HashSet<RuntimeString> =
                        next.iter().map(|slot| slot.id.clone()).collect();
                    for slot in slots.drain(..) {
                        if slot.answer.is_some()
                            && !source.contains(&slot.id)
                            && retained.insert(slot.id.clone())
                        {
                            next.push(slot);
                        }
                    }
                }
                *slots = next;
            } else {
                let start = usize::try_from(replaced_range.0)
                    .unwrap_or(usize::MAX)
                    .min(slots.len());
                let end = usize::try_from(replaced_range.1)
                    .unwrap_or(usize::MAX)
                    .clamp(start, slots.len());
                slots.splice(
                    start..end,
                    [QuestionSlot {
                        id: format!("compact-{}-{}", replaced_range.0, replaced_range.1).into(),
                        answer: None,
                    }],
                );
            }
            Ok(true)
        }
        Ok(None) => Ok(false),
        Err(error) if error.kind() == io::ErrorKind::Interrupted => Err(error),
        Err(_) => Ok(false),
    }
}

/// Reconstruct all lifetime state while decoding at most one obsolete snapshot:
/// the newest authoritative review checkpoint. Rollbacks retain complete real
/// model input so review boundary resolution never receives a sparse prefix.
pub(crate) fn load(
    file: &str,
    thread_id: &str,
    cancelled: &AtomicBool,
) -> io::Result<RuntimeResumeResult> {
    let mut source =
        crate::store::compression::open_seekable_cancelled(std::path::Path::new(file), cancelled)?;
    let rows = rows(&source, cancelled)?;
    let has_rollback = rows
        .iter()
        .any(|row| row.kind == "rollback" || row.kind.is_empty());
    let mut approval_start = None;
    let mut model_start = None;
    let mut cached_rows: HashMap<usize, Option<RolloutItem>> = HashMap::new();
    for (index, row) in rows.iter().enumerate().rev() {
        check(cancelled)?;
        if approval_start.is_none()
            && (row.event.as_deref() == Some("approval_parent_history")
                || (row.kind == "compacted" && row.approval))
        {
            if let std::collections::hash_map::Entry::Vacant(entry) = cached_rows.entry(index) {
                let decoded = read_row(&mut source, row, cancelled)?;
                entry.insert(decoded);
            }
            if cached_rows.get(&index).and_then(Option::as_ref).is_some() {
                approval_start = Some(index);
            }
        }
        if model_start.is_none() && row.kind == "compacted" && row.replacement && row.approval {
            if let std::collections::hash_map::Entry::Vacant(entry) = cached_rows.entry(index) {
                let decoded = read_row(&mut source, row, cancelled)?;
                entry.insert(decoded);
            }
            if let Some(RolloutItem::Compacted(compacted)) =
                cached_rows.get(&index).and_then(Option::as_ref)
            {
                if let Some(checkpoint) = &compacted.approval_parent_history {
                    crate::engine::approval_capture::ReviewParentCheckpoint::from_runtime_value(
                        checkpoint,
                    )
                    .map_err(|error| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            String::from_utf16_lossy(error.as_utf16()),
                        )
                    })?;
                    model_start = Some(index);
                }
            }
        }
        if approval_start.is_some() && model_start.is_some() {
            break;
        }
    }
    if has_rollback {
        model_start = None;
    }
    // An approval rollback resolves against the complete pre-rollback model
    // projection, even when the newest checkpoint is much later in the file.
    let evidence_start = if has_rollback {
        0
    } else {
        approval_start.unwrap_or(0)
    };
    let model_start = model_start.unwrap_or(0);
    let mut state = RolloutMetadataAccumulator::default();
    let mut recovery_items = Vec::new();
    let mut model_items = Vec::new();
    let mut users = HashSet::new();
    let mut usage = Vec::new();
    let mut billable = true;
    let mut billing_meta_seen = false;
    let mut question_slots = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        check(cancelled)?;
        if row.event.as_deref() == Some("approval_parent_history") && Some(index) != approval_start
        {
            continue;
        }
        if index < model_start && index < evidence_start {
            if row.kind == "message" {
                if let (Some(id), Some(msg_kind)) = (&row.message_id, row.msg_kind) {
                    let skippable = matches!(
                        msg_kind,
                        SkimmedMessageKind::ToolResult
                            | SkimmedMessageKind::System
                            | SkimmedMessageKind::CompactBoundary
                    ) || (msg_kind == SkimmedMessageKind::User
                        && !row.has_question_answer);
                    if skippable {
                        state.push_message_id(id);
                        if msg_kind == SkimmedMessageKind::User {
                            users.insert(id.clone());
                        }
                        question_slots.push(QuestionSlot {
                            id: id.clone(),
                            answer: None,
                        });
                        continue;
                    }
                }
            } else if row.kind == "compacted"
                && skim_obsolete_compacted(&mut source, row, cancelled, &mut question_slots)?
            {
                continue;
            }
        }
        let decoded = match cached_rows.remove(&index) {
            Some(cached) => cached,
            None => read_row(&mut source, row, cancelled)?,
        };
        let Some(item) = decoded else {
            continue;
        };
        if !billing_meta_seen {
            if let RolloutItem::SessionMeta { meta, .. } = &item {
                billable = meta.forked_from_id.is_none()
                    && meta.role != Some(crate::schemas::rollout::SessionRole::SideTask);
                billing_meta_seen = true;
            }
        }
        state.push(&item);
        collect_history_metadata(&item, &mut billable, &mut users, &mut usage);
        if index == model_start && model_start > 0 {
            // These are the original typed answers still present immediately
            // before replacement, in their actual projection order.
            model_items.extend(
                question_slots
                    .iter()
                    .filter_map(|slot: &QuestionSlot| slot.answer.clone())
                    .map(|message| RolloutItem::Message { message }),
            );
        }
        if index < model_start {
            update_question_slots(&mut question_slots, &item);
        }
        match &item {
            RolloutItem::Message { .. } | RolloutItem::Compacted(_) | RolloutItem::Rollback(_) => {
                let keep_model = index >= model_start;
                let original_answer = matches!(&item, RolloutItem::Message { message: Message::User(user) } if user.question_answer.is_some());
                let keep_recovery = index >= evidence_start || original_answer;
                match (keep_model, keep_recovery) {
                    (true, true) => {
                        model_items.push(item.clone());
                        recovery_items.push(item);
                    }
                    (true, false) => model_items.push(item),
                    (false, true) => recovery_items.push(item),
                    (false, false) => {}
                }
            }
            _ => {
                // All small runtime state remains chronological. The model
                // projection also needs retained goal-instruction events.
                model_items.push(item.clone());
                recovery_items.push(item);
            }
        }
    }
    let (meta, last_turn_context, turn_index, file_changes) = state.finish();
    let meta = meta.ok_or_else(|| {
        io::Error::other(format!("session {thread_id} has no session_meta header"))
    })?;
    if meta.id != *thread_id {
        return Err(io::Error::other(format!(
            "session {thread_id} rollout header names a different session: {}",
            String::from_utf16_lossy(meta.id.as_utf16())
        )));
    }
    check(cancelled)?;
    let mut hidden_context_message_ids = Vec::new();
    if meta.role == Some(crate::schemas::rollout::SessionRole::SideTask) {
        for row in &rows {
            if row.kind == "session_meta" {
                continue;
            }
            if matches!(
                row.event.as_deref(),
                Some("side_task_boundary" | "approval_parent_history")
            ) || matches!(row.kind.as_str(), "checkpoint" | "turn_context")
                || matches!(
                    row.event.as_deref(),
                    Some("prediction_source" | "turn_goal_metadata")
                )
            {
                break;
            }
            if row.kind == "message" {
                if let Some(id) = &row.message_id {
                    hidden_context_message_ids.push(id.clone());
                }
            }
        }
    }
    let messages = project_for_prompt(&model_items);
    check(cancelled)?;
    Ok(RuntimeResumeResult {
        meta,
        messages,
        recovery_items,
        last_turn_context,
        turn_index,
        file_changes,
        persisted_user_uuids: users,
        hidden_context_message_ids,
        usage,
    })
}

struct QuestionSlot {
    id: RuntimeString,
    answer: Option<Message>,
}
fn question_slot(message: &Message) -> QuestionSlot {
    let id = match message {
        Message::User(message) => &message.uuid,
        Message::Assistant(message) => &message.uuid,
        Message::ToolResult(message) => &message.uuid,
        Message::System(message) => &message.uuid,
        Message::CompactBoundary(message) => &message.uuid,
    }
    .clone();
    QuestionSlot {
        id,
        answer: matches!(message, Message::User(user) if user.question_answer.is_some())
            .then(|| message.clone()),
    }
}
/// Maintain range positions without retaining unrelated historical bodies.
/// Rollback traces take the complete replay path before this helper is used.
fn update_question_slots(slots: &mut Vec<QuestionSlot>, item: &RolloutItem) {
    match item {
        RolloutItem::Message { message } => slots.push(question_slot(message)),
        RolloutItem::Compacted(compacted) => {
            if let Some(replacement) = &compacted.replacement_history {
                let mut next = replacement.iter().map(question_slot).collect::<Vec<_>>();
                if let Some(source) = &compacted.source_message_ids {
                    let source = source.iter().collect::<HashSet<_>>();
                    let mut retained = next
                        .iter()
                        .map(|slot| slot.id.clone())
                        .collect::<HashSet<_>>();
                    for slot in slots.drain(..) {
                        if slot.answer.is_some()
                            && !source.contains(&slot.id)
                            && retained.insert(slot.id.clone())
                        {
                            next.push(slot);
                        }
                    }
                }
                *slots = next;
            } else {
                let start = usize::try_from(compacted.replaced_range.0)
                    .unwrap_or(usize::MAX)
                    .min(slots.len());
                let end = usize::try_from(compacted.replaced_range.1)
                    .unwrap_or(usize::MAX)
                    .clamp(start, slots.len());
                slots.splice(
                    start..end,
                    [QuestionSlot {
                        id: format!(
                            "compact-{}-{}",
                            compacted.replaced_range.0, compacted.replaced_range.1
                        )
                        .into(),
                        answer: None,
                    }],
                );
            }
        }
        _ => {}
    }
}

pub(crate) fn message_uuid(message: &Message) -> &RuntimeString {
    match message {
        Message::User(message) => &message.uuid,
        Message::Assistant(message) => &message.uuid,
        Message::ToolResult(message) => &message.uuid,
        Message::System(message) => &message.uuid,
        Message::CompactBoundary(message) => &message.uuid,
    }
}

pub(crate) fn select_messages(items: &[RolloutItem], ids: &[RuntimeString]) -> Vec<Message> {
    let requested = ids.iter().collect::<HashSet<_>>();
    let mut found = HashMap::new();
    for item in items {
        if let RolloutItem::Message { message } = item {
            let id = message_uuid(message);
            if requested.contains(id) {
                found.entry(id.clone()).or_insert_with(|| message.clone());
            }
        }
    }
    ids.iter().filter_map(|id| found.get(id).cloned()).collect()
}

pub(crate) fn read_messages(
    file: &str,
    thread_id: &str,
    ids: &[RuntimeString],
    cancelled: &AtomicBool,
) -> io::Result<Vec<Message>> {
    let mut source =
        crate::store::compression::open_seekable_cancelled(std::path::Path::new(file), cancelled)?;
    let rows = rows(&source, cancelled)?;
    let mut validated_header = false;
    let requested = ids.iter().collect::<HashSet<_>>();
    let mut found = HashMap::new();
    for row in rows {
        check(cancelled)?;
        if (row.kind == "session_meta" || row.kind.is_empty()) && !validated_header {
            if let Some(RolloutItem::SessionMeta { meta, .. }) =
                read_row(&mut source, &row, cancelled)?
            {
                if meta.id != *thread_id {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "parent rollout header names a different session",
                    ));
                }
                validated_header = true;
            }
        }
        let uncertain = row.kind.is_empty();
        if !uncertain
            && (row.kind != "message"
                || !row
                    .message_id
                    .as_ref()
                    .is_some_and(|id| requested.contains(id) && !found.contains_key(id)))
        {
            continue;
        }
        if let Some(RolloutItem::Message { message }) = read_row(&mut source, &row, cancelled)? {
            let id = message_uuid(&message);
            if requested.contains(id) {
                found.entry(id.clone()).or_insert(message);
            }
        }
    }
    if !validated_header {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "parent rollout has no session_meta header",
        ));
    }
    Ok(ids.iter().filter_map(|id| found.get(id).cloned()).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Scratch(std::path::PathBuf);
    impl Scratch {
        fn new(bytes: &[u8]) -> Self {
            let path = std::env::temp_dir().join(format!(
                "klaus-runtime-resume-{}.jsonl",
                uuid::Uuid::new_v4()
            ));
            std::fs::write(&path, bytes).unwrap();
            Self(path)
        }
    }
    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    #[test]
    fn skimmer_preserves_offsets_and_non_scalar_message_ids_without_retaining_bodies() {
        let input = b"{\"kind\":\"event\",\"payload\":{\"source\":\"\\ud800\"},\"event\":\"approval_parent_history\"}\n{\"kind\":\"message\",\"message\":{\"content\":[],\"uuid\":\"\\ud800\",\"type\":\"user\"}}\n";
        let scratch = Scratch::new(input);
        let rows = rows(&File::open(&scratch.0).unwrap(), &AtomicBool::new(false)).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].event.as_deref(), Some("approval_parent_history"));
        assert_eq!(rows[1].offset, rows[0].len);
        assert_eq!(rows[0].len + rows[1].len, input.len() as u64);
        assert_eq!(
            rows[1].message_id,
            Some(RuntimeString::from_utf16(vec![0xd800]))
        );
    }

    #[test]
    fn cancellation_is_observed_while_a_large_record_is_still_being_skipped() {
        let scratch = Scratch::new(&vec![b'x'; 128 * 1024]);
        let mut reader = BufReader::with_capacity(1024, File::open(&scratch.0).unwrap());
        let flag = Arc::new(AtomicBool::new(false));
        let owner = ReadCancellation(flag.clone());
        let mut line = Line {
            reader: &mut reader,
            cancelled: &flag,
            ended: false,
            available: 0,
            len: 0,
        };
        let mut buffer = [0_u8; 1024];
        assert_eq!(line.read(&mut buffer).unwrap(), 1024);
        drop(owner);
        assert_eq!(
            line.read(&mut buffer).unwrap_err().kind(),
            io::ErrorKind::Interrupted
        );
        assert_eq!(line.len, 1024);
    }
}
