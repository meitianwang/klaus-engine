//! Complete-record projection batches with bounded producer backpressure.
use super::*;
use crate::runtime_values::{runtime_codec, runtime_json, RuntimeJson};
use std::io::Write;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::SyncSender,
    Arc,
};

pub(super) const BATCH_SOURCE_BYTES: u64 = 1024 * 1024;
pub(super) const BATCH_STEPS: usize = 256;
pub(super) const QUEUED_BATCHES: usize = 2;

pub(super) struct Step {
    pub offset: u64,
    pub len: u64,
    pub ordinal: i64,
    pub header: Option<scan::Header>,
    pub payload: Option<Payload>,
    pub summary: Option<RuntimeString>,
    pub control: Option<RuntimeJson>,
    /// A message record whose decoded form has no display item. The byte range
    /// still advances the checkpoint so one unprojectable record cannot wedge
    /// the whole history behind it.
    pub skip_item: bool,
}

pub(super) enum Payload {
    Inline(io::Cursor<Vec<u8>>),
    Staged(std::fs::File),
}
impl Read for Payload {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Inline(reader) => reader.read(out),
            Self::Staged(reader) => reader.read(out),
        }
    }
}
impl Seek for Payload {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        match self {
            Self::Inline(reader) => reader.seek(position),
            Self::Staged(reader) => reader.seek(position),
        }
    }
}
pub(super) struct Batch {
    pub start: u64,
    pub end: u64,
    pub next_ordinal: i64,
    pub steps: Vec<Step>,
    pub source_read_us: u64,
    pub normalize_us: u64,
}
pub(super) enum ProjectionMessage {
    Batch(Batch),
    Complete(u64),
    Failed(io::Error),
}

struct CancelReader<R> {
    reader: R,
    cancelled: Arc<AtomicBool>,
}
impl<R: Read> Read for CancelReader<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "history projection cancelled",
            ));
        }
        self.reader.read(out)
    }
}

fn temp_bytes(bytes: &[u8]) -> io::Result<Payload> {
    if bytes.len() <= 64 * 1024 {
        return Ok(Payload::Inline(io::Cursor::new(bytes.to_vec())));
    }
    let mut file = tempfile::tempfile()?;
    file.write_all(bytes)?;
    file.rewind()?;
    Ok(Payload::Staged(file))
}
fn field(
    source: &mut std::fs::File,
    offset: u64,
    len: u64,
    path: &[&str],
    cancelled: &Arc<AtomicBool>,
) -> io::Result<Payload> {
    source.seek(SeekFrom::Start(offset))?;
    let reader = CancelReader {
        reader: Read::by_ref(source).take(len),
        cancelled: cancelled.clone(),
    };
    let (start, count) = scan::field_range(&mut BufReader::with_capacity(64 * 1024, reader), path)?;
    source.seek(SeekFrom::Start(offset + start))?;
    if count <= 64 * 1024 {
        let mut bytes = Vec::with_capacity(count as usize);
        CancelReader {
            reader: Read::by_ref(source).take(count),
            cancelled: cancelled.clone(),
        }
        .read_to_end(&mut bytes)?;
        return Ok(Payload::Inline(io::Cursor::new(bytes)));
    }
    let mut file = tempfile::tempfile()?;
    io::copy(
        &mut CancelReader {
            reader: Read::by_ref(source).take(count),
            cancelled: cancelled.clone(),
        },
        &mut file,
    )?;
    file.rewind()?;
    Ok(Payload::Staged(file))
}
pub(super) fn is_side_task_prefix_end_before(header: &scan::Header) -> bool {
    matches!(header.kind.as_str(), "checkpoint" | "turn_context")
        || matches!(
            header.event.as_deref(),
            Some("prediction_source" | "turn_goal_metadata")
        )
}

pub(super) fn is_side_task_prefix_end_inclusive(header: &scan::Header) -> bool {
    matches!(
        header.event.as_deref(),
        Some("side_task_boundary" | "approval_parent_history")
    )
}

fn prepare(
    source: &mut std::fs::File,
    offset: u64,
    len: u64,
    ordinal: i64,
    header: Option<scan::Header>,
    skip_prefix: &mut bool,
    cancelled: &Arc<AtomicBool>,
) -> io::Result<Step> {
    let mut step = Step {
        offset,
        len,
        ordinal,
        header,
        payload: None,
        summary: None,
        control: None,
        skip_item: false,
    };
    let Some(header) = step.header.as_ref() else {
        return Ok(step);
    };
    if header.kind == "session_meta" {
        *skip_prefix =
            header.meta.as_ref().and_then(|meta| meta.role.as_deref()) == Some("side_task");
        return Ok(step);
    }
    if *skip_prefix {
        if is_side_task_prefix_end_inclusive(header) {
            *skip_prefix = false;
            return Ok(step);
        }
        if is_side_task_prefix_end_before(header) {
            *skip_prefix = false;
        } else {
            step.skip_item = true;
            return Ok(step);
        }
    }
    if header.kind == "message"
        && header
            .message
            .as_ref()
            .and_then(|message| message.uuid.as_ref())
            .is_some()
    {
        step.payload = Some(if len > MAX_ITEMS_PAGE_BYTES {
            field(source, offset, len, &["message"], cancelled)?
        } else {
            match load::record_raw(source, offset, len) {
                Ok(bytes) => {
                    let Some(item) = std::str::from_utf8(&bytes)
                        .ok()
                        .and_then(parse_rollout_line)
                    else {
                        // A record the canonical decoder does not recognize is
                        // skipped, never fatal: the byte range still advances.
                        tracing::warn!(offset, "skipping rollout message that does not decode");
                        step.skip_item = true;
                        return Ok(step);
                    };
                    let projected = project_thread_history(vec![item]);
                    let Some(message) = projected.history.first() else {
                        let uuid = header
                            .message
                            .as_ref()
                            .and_then(|m| m.uuid.as_deref())
                            .unwrap_or("?");
                        tracing::warn!(
                            offset,
                            uuid,
                            "skipping rollout message without a display projection"
                        );
                        step.skip_item = true;
                        return Ok(step);
                    };
                    let json = runtime_codec::encode_value(message).map_err(io::Error::other)?;
                    temp_bytes(
                        runtime_json::stringify(&json)
                            .try_to_utf8()
                            .map_err(io::Error::other)?
                            .as_bytes(),
                    )?
                }
                Err(error) if error.kind() == io::ErrorKind::InvalidInput => {
                    field(source, offset, len, &["message"], cancelled)?
                }
                Err(error) => return Err(error),
            }
        });
    } else if header.kind == "compacted" {
        source.seek(SeekFrom::Start(offset))?;
        let reader = CancelReader {
            reader: Read::by_ref(source).take(len),
            cancelled: cancelled.clone(),
        };
        let summary = scan::compaction(&mut BufReader::with_capacity(64 * 1024, reader))?;
        if summary.is_empty() {
            let bytes = load::record_raw(source, offset, len)?;
            let is_repair = std::str::from_utf8(&bytes)
                .ok()
                .and_then(parse_rollout_line)
                .is_some_and(
                    |item| matches!(item, RolloutItem::Compacted(c) if c.is_history_repair()),
                );
            if is_repair {
                step.skip_item = true;
            } else {
                step.summary = Some(summary);
            }
        } else {
            step.summary = Some(summary);
        }
    } else if header.event.as_deref() == Some("tool_result_output") {
        step.payload = Some(field(
            source,
            offset,
            len,
            &["payload", "output"],
            cancelled,
        )?);
    } else if header.kind == "turn_context"
        || matches!(
            header.event.as_deref(),
            Some(
                "turn_lifecycle"
                    | "turn_goal_metadata"
                    | "tool_lifecycle"
                    | "goal_submission_bound"
                    | "agent_notification"
                    | "goal_completed"
                    | "file_change"
            )
        )
    {
        let bytes = load::record_raw(source, offset, len)?;
        let item = std::str::from_utf8(&bytes)
            .ok()
            .and_then(parse_rollout_line)
            .ok_or_else(|| io::Error::other("invalid rollout metadata during projection"))?;
        step.control = Some(super::projection::normalize_control(item)?);
    }
    Ok(step)
}

fn produce(
    source: &mut std::fs::File,
    from: u64,
    end: u64,
    initial_ordinal: i64,
    mut skip_prefix: bool,
    sender: &SyncSender<ProjectionMessage>,
    cancelled: &Arc<AtomicBool>,
) -> io::Result<()> {
    let mut offset = from;
    let mut ordinal = initial_ordinal;
    let mut batch = Batch {
        start: from,
        end: from,
        next_ordinal: ordinal,
        steps: Vec::new(),
        source_read_us: 0,
        normalize_us: 0,
    };
    while offset < end {
        if cancelled.load(Ordering::Acquire) {
            return Ok(());
        }
        let read_started = std::time::Instant::now();
        source.seek(SeekFrom::Start(offset))?;
        let reader = CancelReader {
            reader: Read::by_ref(source).take(end - offset),
            cancelled: cancelled.clone(),
        };
        let mut reader = BufReader::with_capacity(64 * 1024, reader);
        let mut line = scan::Line::new(&mut reader);
        let header = scan::header(&mut line)?;
        line.finish()?;
        let len = line.len;
        let newline = line.newline;
        drop(reader);
        // A trailing partial record belongs to the next durable write boundary.
        if !newline {
            break;
        }
        if len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "rollout shrank during projection",
            ));
        }
        batch.source_read_us += read_started.elapsed().as_micros() as u64;
        let normalize_started = std::time::Instant::now();
        let step = prepare(
            source,
            offset,
            len,
            ordinal,
            header,
            &mut skip_prefix,
            cancelled,
        )?;
        batch.normalize_us += normalize_started.elapsed().as_micros() as u64;
        offset = offset
            .checked_add(len)
            .ok_or_else(|| io::Error::other("history byte offset overflow"))?;
        ordinal += 1;
        batch.end = offset;
        batch.next_ordinal = ordinal;
        batch.steps.push(step);
        // The source-byte target is a batch target; a single record remains intact.
        if batch.end - batch.start >= BATCH_SOURCE_BYTES || batch.steps.len() >= BATCH_STEPS {
            sender.send(ProjectionMessage::Batch(batch)).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::Interrupted,
                    "history projection consumer closed",
                )
            })?;
            batch = Batch {
                start: offset,
                end: offset,
                next_ordinal: ordinal,
                steps: Vec::new(),
                source_read_us: 0,
                normalize_us: 0,
            };
        }
    }
    if !batch.steps.is_empty() {
        sender.send(ProjectionMessage::Batch(batch)).map_err(|_| {
            io::Error::new(
                io::ErrorKind::Interrupted,
                "history projection consumer closed",
            )
        })?;
    }
    sender
        .send(ProjectionMessage::Complete(offset))
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::Interrupted,
                "history projection consumer closed",
            )
        })?;
    Ok(())
}

pub(super) fn producer(
    mut source: std::fs::File,
    from: u64,
    end: u64,
    initial_ordinal: i64,
    initial_skip_prefix: bool,
    sender: SyncSender<ProjectionMessage>,
    cancelled: Arc<AtomicBool>,
) {
    if let Err(error) = produce(
        &mut source,
        from,
        end,
        initial_ordinal,
        initial_skip_prefix,
        &sender,
        &cancelled,
    ) {
        if !cancelled.load(Ordering::Acquire) {
            let _ = sender.send(ProjectionMessage::Failed(error));
        }
    }
}
