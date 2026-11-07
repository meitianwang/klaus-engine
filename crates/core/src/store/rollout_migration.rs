//! Explicit, revision-guarded normalization of indexed JSONL rollouts.
//!
//! Planning never changes stored rollouts. Publishing requires an inactive writer lease; the
//! store owns lifecycle admission and refreshes derived indexes before acknowledgement.

use crate::engine::approval_capture::ReviewParentCheckpoint;
use crate::engine::approval_context::history_types::ResponseItemEnvelope;
use crate::engine::approval_context::model::ResponseItem;
use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::UNIX_EPOCH;

const VERSION: u32 = 1;
const MAX_ROW_BYTES: usize = 64 * 1024 * 1024;
const MARKER: &str = "rollout_migration";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MigrationKind {
    Main,
    Subagent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceRevision {
    pub length: u64,
    pub modified_nanos: String,
    pub identity: String,
    pub sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MigrationStatus {
    Preview,
    Applied,
    Unchanged,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrationReport {
    pub status: MigrationStatus,
    pub source_revision: SourceRevision,
    pub target_version: u32,
    pub cursor_epoch: String,
    pub before_bytes: u64,
    pub after_bytes: u64,
    pub removed_approval_snapshots: u64,
    pub removed_rollback_records: u64,
    pub removed_records: u64,
    pub truncated_audit: bool,
    pub fully_normalized: bool,
    pub retained_reasons: Vec<String>,
    pub needs_index_refresh: bool,
}

#[derive(Debug, Clone)]
pub struct MigrationPlan {
    pub report: MigrationReport,
    path: PathBuf,
    rows: Vec<Row>,
    retained: Vec<bool>,
    zero_rollbacks: HashSet<usize>,
    marker: Vec<u8>,
    proof: Proof,
    kind: MigrationKind,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct MessageRoute {
    #[serde(rename = "type")]
    kind: String,
    uuid: Option<String>,
    #[serde(rename = "isMeta", default)]
    is_meta: Option<bool>,
    #[serde(rename = "questionAnswer", default)]
    answer: Option<IgnoredAny>,
    #[serde(default)]
    usage: Option<IgnoredAny>,
}
impl MessageRoute {
    fn human(&self) -> bool {
        self.kind == "user" && self.is_meta != Some(true)
    }
    fn typed(&self) -> bool {
        self.human() && self.answer.is_some()
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
struct MetaRoute {
    id: Option<String>,
    #[serde(rename = "forkedFromId")]
    forked_from: Option<String>,
}
#[derive(Debug, Clone, Default, Deserialize)]
struct Route {
    kind: String,
    event: Option<String>,
    message: Option<MessageRoute>,
    meta: Option<MetaRoute>,
    #[serde(rename = "numTurns")]
    turns: Option<u32>,
    #[serde(rename = "replacementHistory")]
    replacement: Option<Vec<MessageRoute>>,
    #[serde(rename = "sourceMessageIds")]
    source_ids: Option<Vec<String>>,
    #[serde(rename = "replacedRange")]
    replaced_range: Option<(u64, u64)>,
    #[serde(rename = "approvalParentHistory")]
    approval: Option<IgnoredAny>,
    #[serde(rename = "assistantMessageId")]
    assistant_id: Option<String>,
}
#[derive(Debug, Clone)]
struct Row {
    offset: u64,
    len: u64,
    digest: String,
    route: Route,
    newline: bool,
    seen_user_message_ids: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MarkerPayload {
    version: u32,
    cursor_epoch: String,
    #[serde(default)]
    audit_truncated: bool,
    // Delivery deduplication only. These IDs never reconstruct authorization,
    // original messages, or evidence omitted by a conversation rollback.
    #[serde(default)]
    seen_user_message_ids: Option<Vec<String>>,
}
#[derive(Deserialize)]
struct MarkerRow {
    payload: MarkerPayload,
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
fn conflict(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, message.into())
}
fn check(cancelled: &AtomicBool) -> io::Result<()> {
    if cancelled.load(Ordering::Acquire) {
        Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "rollout migration cancelled before publication",
        ))
    } else {
        Ok(())
    }
}
fn hex(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
fn digest(bytes: &[u8]) -> String {
    hex(Sha256::digest(bytes))
}
fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}
fn parent(path: &Path) -> io::Result<&Path> {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "rollout path requires a parent directory",
            )
        })
}
fn identity(metadata: &std::fs::Metadata) -> String {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        format!("{}:{}", metadata.dev(), metadata.ino())
    }
    #[cfg(not(unix))]
    {
        format!("{:?}", metadata.created().ok())
    }
}
fn revision(path: &Path, cancelled: &AtomicBool) -> io::Result<SourceRevision> {
    check(cancelled)?;
    if std::fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Err(invalid("migration source must not be a symbolic link"));
    }
    let mut file = File::open(path)?;
    let before = file.metadata()?;
    if !before.is_file() {
        return Err(invalid("migration source is not a regular file"));
    }
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        check(cancelled)?;
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    let after = std::fs::metadata(path)?;
    if identity(&before) != identity(&after)
        || before.len() != after.len()
        || before.modified()? != after.modified()?
    {
        return Err(conflict("rollout changed during migration scan"));
    }
    Ok(SourceRevision {
        length: after.len(),
        modified_nanos: after
            .modified()?
            .duration_since(UNIX_EPOCH)
            .map_err(|_| invalid("rollout modification time precedes epoch"))?
            .as_nanos()
            .to_string(),
        identity: identity(&after),
        sha256: hex(hasher.finalize()),
    })
}

fn scan(file: File, cancelled: &AtomicBool) -> io::Result<Vec<Row>> {
    let mut reader = BufReader::with_capacity(64 * 1024, file);
    let mut rows = Vec::new();
    let mut bytes = Vec::new();
    let mut offset = 0;
    loop {
        check(cancelled)?;
        bytes.clear();
        loop {
            check(cancelled)?;
            let available = reader.fill_buf()?;
            if available.is_empty() {
                break;
            }
            let count = available
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(available.len(), |at| at + 1);
            if bytes.len().saturating_add(count) > MAX_ROW_BYTES {
                return Err(invalid(format!(
                    "rollout row at {offset} exceeds migration decoding limit; source retained"
                )));
            }
            let ended = available[count - 1] == b'\n';
            bytes.extend_from_slice(&available[..count]);
            reader.consume(count);
            if ended {
                break;
            }
        }
        if bytes.is_empty() {
            break;
        }
        let route: Route = serde_json::from_slice(&bytes).map_err(|error| {
            invalid(format!(
                "invalid rollout row at {offset}: {error}; source retained"
            ))
        })?;
        if route.kind.is_empty() {
            return Err(invalid(format!(
                "rollout row at {offset} has no kind; source retained"
            )));
        }
        let seen_user_message_ids =
            if route.kind == "event" && route.event.as_deref() == Some(MARKER) {
                let marker: MarkerRow = serde_json::from_slice(&bytes)
                    .map_err(|error| invalid(format!("invalid migration marker: {error}")))?;
                marker.payload.seen_user_message_ids.unwrap_or_default()
            } else {
                Vec::new()
            };
        rows.push(Row {
            offset,
            len: bytes.len() as u64,
            digest: digest(bytes.trim_ascii_end()),
            route,
            newline: bytes.ends_with(b"\n"),
            seen_user_message_ids,
        });
        offset += bytes.len() as u64;
    }
    Ok(rows)
}
fn read_row(file: &mut File, row: &Row, cancelled: &AtomicBool) -> io::Result<Vec<u8>> {
    check(cancelled)?;
    file.seek(SeekFrom::Start(row.offset))?;
    let mut bytes = vec![0; row.len as usize];
    for chunk in bytes.chunks_mut(64 * 1024) {
        check(cancelled)?;
        file.read_exact(chunk)?;
    }
    Ok(bytes)
}
fn known(kind: &str) -> bool {
    matches!(
        kind,
        "session_meta"
            | "turn_context"
            | "message"
            | "compacted"
            | "event"
            | "rollback"
            | "checkpoint"
            | "usage_ledger"
            | "subagent_lifecycle"
            | "background_process_lifecycle"
    )
}
fn snapshot(row: &Row) -> bool {
    row.route.event.as_deref() == Some("approval_parent_history")
        || row.route.kind == "compacted" && row.route.approval.is_some()
}
#[derive(Deserialize)]
struct SnapshotRow {
    payload: Option<ReviewParentCheckpoint>,
    #[serde(rename = "approvalParentHistory")]
    approval: Option<ReviewParentCheckpoint>,
}
#[derive(Deserialize)]
struct OriginalId {
    message_id: String,
}
#[derive(Deserialize)]
struct AuditCheckpoint {
    transcript: Vec<ResponseItemEnvelope>,
    originals: Vec<OriginalId>,
}
#[derive(Deserialize)]
struct AuditRow {
    payload: Option<AuditCheckpoint>,
    #[serde(rename = "approvalParentHistory")]
    approval: Option<AuditCheckpoint>,
}
fn checkpoint_audit(bytes: &[u8]) -> io::Result<AuditCheckpoint> {
    let decoded: SnapshotRow = serde_json::from_slice(bytes).map_err(|error| {
        invalid(format!(
            "invalid approval checkpoint: {error}; source retained"
        ))
    })?;
    if decoded.payload.or(decoded.approval).is_none() {
        return Err(invalid(
            "approval checkpoint has no evidence; source retained",
        ));
    }
    let audit: AuditRow =
        serde_json::from_slice(bytes).map_err(|error| invalid(error.to_string()))?;
    audit
        .payload
        .or(audit.approval)
        .ok_or_else(|| invalid("missing approval audit"))
}
fn manual_keys(audit: &AuditCheckpoint) -> io::Result<HashSet<String>> {
    audit
        .transcript
        .iter()
        .filter(|envelope| {
            matches!(&envelope.item,
        ResponseItem::Message { role, .. } if role == "developer")
        })
        .map(|envelope| {
            serde_json::to_vec(envelope)
                .map(|value| digest(&value))
                .map_err(|error| invalid(error.to_string()))
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Slot {
    id: Option<String>,
    human: bool,
    typed: bool,
    token: String,
}
fn slot(message: &MessageRoute, token: String) -> Slot {
    Slot {
        id: message.uuid.clone(),
        human: message.human(),
        typed: message.typed(),
        token,
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct Proof {
    prompt: Vec<Slot>,
    visible: Vec<String>,
    users: Vec<String>,
    usage: Vec<String>,
    lifetime: Vec<String>,
    queue: Vec<String>,
    approval: Vec<String>,
    last_context: Option<String>,
    checkpoints: Vec<String>,
    seen_users: BTreeSet<String>,
}
fn proof(
    rows: &[Row],
    retained: &[bool],
    zero_rollbacks: &HashSet<usize>,
    additional_seen_users: &BTreeSet<String>,
) -> Proof {
    let typed_usage: HashSet<_> = rows
        .iter()
        .filter(|row| row.route.kind == "usage_ledger")
        .filter_map(|row| row.route.assistant_id.as_deref())
        .collect();
    let approval_start = rows
        .iter()
        .enumerate()
        .rev()
        .find(|(index, row)| retained[*index] && snapshot(row))
        .map(|(index, _)| index);
    let mut prompt: Vec<Slot> = Vec::new();
    let mut visible_rows: Vec<usize> = Vec::new();
    let mut boundaries = Vec::new();
    let mut pending_context = None;
    let mut usage = Vec::new();
    let mut lifetime = Vec::new();
    let mut queue = Vec::new();
    let mut approval = Vec::new();
    let mut last_context = None;
    let mut checkpoints = Vec::new();
    let mut seen_users = additional_seen_users.clone();
    for (index, row) in rows.iter().enumerate() {
        if !retained[index] {
            continue;
        }
        let route = &row.route;
        seen_users.extend(row.seen_user_message_ids.iter().cloned());
        if let Some(message) = route
            .message
            .as_ref()
            .filter(|message| message.kind == "user")
        {
            seen_users.extend(message.uuid.iter().cloned());
        }
        if route.kind == "turn_context" {
            last_context = Some(row.digest.clone());
        }
        if route.kind == "checkpoint" {
            checkpoints.push(row.digest.clone());
        }
        if route.kind == "rollback" && !zero_rollbacks.contains(&index) {
            checkpoints.truncate(
                checkpoints
                    .len()
                    .saturating_sub(route.turns.unwrap_or(0) as usize),
            );
        }
        if route.kind == "usage_ledger"
            || route.message.as_ref().is_some_and(|message| {
                message.kind == "assistant"
                    && message.usage.is_some()
                    && !message
                        .uuid
                        .as_deref()
                        .is_some_and(|id| typed_usage.contains(id))
            })
        {
            usage.push(row.digest.clone());
        }
        // Turn clocks remain lifetime statistics after conversation rewind.
        if matches!(
            route.kind.as_str(),
            "subagent_lifecycle" | "background_process_lifecycle"
        ) || route.kind == "event"
            && !matches!(
                route.event.as_deref(),
                Some(
                    "approval_parent_history"
                        | "approval_message_admitted"
                        | "approval_manual_action"
                        | "approval_review"
                        | "tool_lifecycle"
                        | "tool_result_output"
                )
            )
            && route.event.as_deref() != Some(MARKER)
        {
            lifetime.push(row.digest.clone());
        }
        if route.event.as_deref() == Some("queue_state") {
            queue.push(row.digest.clone());
        }
        if approval_start.is_some_and(|start| index >= start)
            && route.event.as_deref() != Some(MARKER)
            && (snapshot(row)
                || matches!(
                    route.event.as_deref(),
                    Some("approval_message_admitted" | "approval_manual_action")
                )
                || route.kind == "message"
                || route.kind == "rollback")
        {
            approval.push(row.digest.clone());
        }
        match route.kind.as_str() {
            "turn_context" => {
                pending_context = Some(visible_rows.len());
                visible_rows.push(index);
            }
            "message" => {
                if let Some(message) = &route.message {
                    prompt.push(slot(message, row.digest.clone()));
                    if message.human() {
                        boundaries.push(pending_context.take().unwrap_or(visible_rows.len()));
                    }
                }
                visible_rows.push(index);
            }
            "compacted" => {
                if let Some(replacement) = &route.replacement {
                    let mut next: Vec<_> = replacement
                        .iter()
                        .enumerate()
                        .map(|(at, message)| slot(message, format!("{}:{at}", row.digest)))
                        .collect();
                    if let Some(source_ids) = &route.source_ids {
                        let sources: HashSet<_> = source_ids.iter().map(String::as_str).collect();
                        let mut ids: HashSet<_> = next
                            .iter()
                            .filter_map(|message| message.id.clone())
                            .collect();
                        next.extend(prompt.into_iter().filter(|message| {
                            message.typed
                                && message.id.as_ref().is_some_and(|id| {
                                    !sources.contains(id.as_str()) && ids.insert(id.clone())
                                })
                        }));
                    }
                    prompt = next;
                } else {
                    let (start, end) = route.replaced_range.unwrap_or((0, 0));
                    let start = (start as usize).min(prompt.len());
                    let end = (end as usize).clamp(start, prompt.len());
                    prompt.splice(
                        start..end,
                        [Slot {
                            id: None,
                            human: false,
                            typed: false,
                            token: row.digest.clone(),
                        }],
                    );
                }
                visible_rows.push(index);
            }
            "rollback" if !zero_rollbacks.contains(&index) => {
                let count = route.turns.unwrap_or(0) as usize;
                let users: Vec<_> = prompt
                    .iter()
                    .enumerate()
                    .filter(|(_, message)| message.human)
                    .map(|(at, _)| at)
                    .collect();
                if count > 0 && !users.is_empty() {
                    prompt.truncate(users[users.len().saturating_sub(count)]);
                }
                if count > 0 && !boundaries.is_empty() {
                    let keep = boundaries.len().saturating_sub(count);
                    visible_rows.truncate(boundaries[keep]);
                    boundaries.truncate(keep);
                    pending_context = None;
                }
            }
            _ => visible_rows.push(index),
        }
    }
    let visible = visible_rows
        .iter()
        .filter(|index| matches!(rows[**index].route.kind.as_str(), "message" | "compacted"))
        .map(|index| rows[*index].digest.clone())
        .collect();
    let users = visible_rows
        .iter()
        .filter_map(|index| rows[*index].route.message.as_ref())
        .filter(|message| message.human())
        .filter_map(|message| message.uuid.clone())
        .collect();
    Proof {
        prompt,
        visible,
        users,
        usage,
        lifetime,
        queue,
        approval,
        last_context,
        checkpoints,
        seen_users,
    }
}

fn prune_rollbacks(
    rows: &[Row],
    retained: &mut [bool],
    latest_checkpoint: Option<usize>,
    admission_ids: &HashMap<usize, String>,
    reasons: &mut Vec<String>,
) -> HashSet<usize> {
    let mut boundaries: Vec<usize> = Vec::new();
    let mut pending_context = None;
    let mut zero = HashSet::new();
    let typed_usage: HashSet<_> = rows
        .iter()
        .filter(|row| row.route.kind == "usage_ledger")
        .filter_map(|row| row.route.assistant_id.as_deref())
        .collect();
    let forked = rows.iter().any(|row| {
        row.route
            .meta
            .as_ref()
            .is_some_and(|meta| meta.forked_from.is_some())
    });
    for (index, row) in rows.iter().enumerate() {
        match row.route.kind.as_str() {
            "turn_context" => pending_context = Some(index),
            "message" if row.route.message.as_ref().is_some_and(MessageRoute::human) => {
                boundaries.push(pending_context.take().unwrap_or(index));
            }
            "rollback" => {
                let count = row.route.turns.unwrap_or(0) as usize;
                if count == 0 {
                    continue;
                }
                if boundaries.is_empty() {
                    reasons.push("retained_complex_rollback".into());
                    continue;
                }
                let keep = boundaries.len().saturating_sub(count);
                let start = boundaries[keep];
                let removed_ids: HashSet<_> = rows[start..index]
                    .iter()
                    .filter_map(|candidate| {
                        candidate
                            .route
                            .message
                            .as_ref()
                            .and_then(|message| message.uuid.as_deref())
                    })
                    .collect();
                let early_admission = admission_ids.iter().any(|(at, id)| {
                    (*at < start || *at >= index) && removed_ids.contains(id.as_str())
                });
                let safe = !forked
                    && latest_checkpoint.is_some_and(|checkpoint| checkpoint > index)
                    && !early_admission
                    && rows[index + 1..]
                        .iter()
                        .any(|candidate| candidate.route.kind == "turn_context")
                    && rows[start..index].iter().all(|candidate| {
                        match candidate.route.kind.as_str() {
                            "message" => candidate.route.message.as_ref().is_some_and(|message| {
                                !message.typed()
                                    && (message.kind != "assistant"
                                        || message.usage.is_none()
                                        || message
                                            .uuid
                                            .as_deref()
                                            .is_some_and(|id| typed_usage.contains(id)))
                            }),
                            "turn_context" | "checkpoint" | "usage_ledger" => true,
                            "event" => matches!(
                                candidate.route.event.as_deref(),
                                Some(
                                    "tool_lifecycle"
                                        | "tool_result_output"
                                        | "approval_message_admitted"
                                        | "approval_manual_action"
                                        | "approval_review"
                                )
                            ),
                            _ => false,
                        }
                    });
                if safe {
                    for at in start..index {
                        if rows[at].route.kind != "usage_ledger" {
                            retained[at] = false;
                        }
                    }
                    zero.insert(index);
                } else {
                    reasons.push("retained_complex_rollback".into());
                }
                boundaries.truncate(keep);
                pending_context = None;
            }
            _ => {}
        }
    }
    zero
}

/// Inspect a stable source without publishing indexes, locks, journals, or files.
pub fn preview(
    path: &Path,
    kind: MigrationKind,
    cancelled: &AtomicBool,
) -> io::Result<MigrationPlan> {
    let source = revision(path, cancelled)?;
    let decoded = crate::store::compression::open_seekable(path)?;
    let rows = scan(decoded.try_clone()?, cancelled)?;
    if kind == MigrationKind::Main
        && !rows.first().is_some_and(|row| {
            row.route.kind == "session_meta"
                && row
                    .route
                    .meta
                    .as_ref()
                    .is_some_and(|meta| meta.id.is_some())
        })
    {
        return Err(invalid(
            "main rollout has no identified session header; source retained",
        ));
    }
    let mut retained = vec![true; rows.len()];
    let mut reasons = Vec::new();
    if rows.iter().any(|row| !known(&row.route.kind)) {
        reasons.push("retained_unknown_record_kind".into());
    }
    let mut reader = decoded;
    let mut admission_ids = HashMap::new();
    for (index, row) in rows
        .iter()
        .enumerate()
        .filter(|(_, row)| row.route.event.as_deref() == Some("approval_message_admitted"))
    {
        #[derive(Deserialize)]
        struct AdmissionPayload {
            message_id: String,
        }
        #[derive(Deserialize)]
        struct AdmissionRow {
            payload: AdmissionPayload,
        }
        let admission: AdmissionRow =
            serde_json::from_slice(&read_row(&mut reader, row, cancelled)?)
                .map_err(|error| invalid(format!("invalid admission metadata: {error}")))?;
        admission_ids.insert(index, admission.payload.message_id);
    }
    let mut covered_manual = HashSet::new();
    let raw_ids: HashSet<_> = rows
        .iter()
        .filter_map(|row| {
            row.route
                .message
                .as_ref()
                .and_then(|message| message.uuid.as_deref())
        })
        .collect();
    let mut newest = None;
    let mut complete = None;
    let mut removed_snapshots = 0;
    for (index, row) in rows
        .iter()
        .enumerate()
        .rev()
        .filter(|(_, row)| snapshot(row))
    {
        let bytes = read_row(&mut reader, row, cancelled)?;
        let audit = checkpoint_audit(&bytes)?;
        if row.route.kind == "compacted"
            && crate::store::rollout::parse_rollout_line(
                std::str::from_utf8(&bytes).map_err(|_| invalid("invalid UTF-8 compaction"))?,
            )
            .is_none()
        {
            return Err(invalid(
                "invalid complete compaction record; source retained",
            ));
        }
        let manuals = manual_keys(&audit)?;
        if newest.is_none() {
            newest = Some(index);
        }
        if complete.is_none() && row.route.kind == "compacted" && row.route.replacement.is_some() {
            complete = Some(index);
        }
        if row.route.event.as_deref() == Some("approval_parent_history")
            && newest != Some(index)
            && audit
                .originals
                .iter()
                .all(|original| raw_ids.contains(original.message_id.as_str()))
            && manuals.is_subset(&covered_manual)
        {
            retained[index] = false;
            removed_snapshots += 1;
        } else {
            if newest != Some(index)
                && row.route.event.as_deref() == Some("approval_parent_history")
            {
                reasons.push("retained_approval_audit_anchor".into());
            }
            covered_manual.extend(manuals);
        }
    }
    let zero_rollbacks =
        prune_rollbacks(&rows, &mut retained, newest, &admission_ids, &mut reasons);
    let mut truncated_audit = false;
    if kind == MigrationKind::Subagent {
        if let Some(checkpoint) = complete.filter(|checkpoint| {
            !rows[*checkpoint + 1..]
                .iter()
                .any(|row| row.route.kind == "rollback" || !known(&row.route.kind))
        }) {
            let needed: HashSet<_> = admission_ids
                .iter()
                .filter(|(index, _)| **index > checkpoint)
                .map(|(_, id)| id.as_str())
                .collect();
            for (index, row) in rows[..checkpoint].iter().enumerate() {
                if row.route.kind == "compacted"
                    || row.route.message.as_ref().is_some_and(|message| {
                        !(message.human()
                            || message
                                .uuid
                                .as_deref()
                                .is_some_and(|id| needed.contains(id))
                            || message.kind == "assistant" && message.usage.is_some())
                    })
                {
                    retained[index] = false;
                    truncated_audit = true;
                }
            }
        } else {
            reasons.push("retained_subagent_without_complete_checkpoint".into());
        }
    }
    let original = proof(
        &rows,
        &vec![true; rows.len()],
        &HashSet::new(),
        &BTreeSet::new(),
    );
    let proposed = proof(&rows, &retained, &zero_rollbacks, &original.seen_users);
    let equivalent = original.prompt == proposed.prompt
        && original.users == proposed.users
        && original.usage == proposed.usage
        && original.lifetime == proposed.lifetime
        && original.queue == proposed.queue
        && original.approval == proposed.approval
        && original.last_context == proposed.last_context
        && original.checkpoints == proposed.checkpoints
        && original.seen_users == proposed.seen_users
        && (truncated_audit || original.visible == proposed.visible);
    if !equivalent {
        // An unfamiliar cross-boundary interaction remains readable and can be
        // retried after a later normalization implementation understands it.
        retained.fill(true);
        removed_snapshots = 0;
        truncated_audit = false;
        reasons.push("retained_unproven_projection_equivalence".into());
    }
    let zero_rollbacks = if equivalent {
        zero_rollbacks
    } else {
        HashSet::new()
    };
    for (index, row) in rows.iter().enumerate().filter(|(index, row)| {
        !retained[*index] && row.route.event.as_deref() != Some("approval_parent_history")
    }) {
        let bytes = read_row(&mut reader, row, cancelled)?;
        if crate::store::rollout::parse_rollout_line(
            std::str::from_utf8(&bytes).map_err(|_| invalid("invalid UTF-8 in removed record"))?,
        )
        .is_none()
        {
            return Err(invalid(format!(
                "invalid record at {} cannot be physically removed; source retained",
                rows[index].offset
            )));
        }
    }
    let verified = proof(&rows, &retained, &zero_rollbacks, &original.seen_users);
    reasons.sort();
    reasons.dedup();
    let mut existing_epoch = None;
    let mut marker_needs_seen_users = false;
    if let Some(row) = rows
        .iter()
        .rev()
        .find(|row| row.route.event.as_deref() == Some(MARKER))
    {
        let marker: MarkerRow = serde_json::from_slice(&read_row(&mut reader, row, cancelled)?)
            .map_err(|error| invalid(format!("invalid migration marker: {error}")))?;
        if marker.payload.version > VERSION {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "rollout uses a newer normalization version",
            ));
        }
        if marker.payload.version == VERSION {
            existing_epoch = Some(marker.payload.cursor_epoch);
        }
        marker_needs_seen_users = marker.payload.seen_user_message_ids.is_none();
        truncated_audit |= marker.payload.audit_truncated;
    }
    let existing_marker = existing_epoch.is_some();
    let changed = retained.iter().any(|keep| !keep)
        || !zero_rollbacks.is_empty()
        || !existing_marker
        || marker_needs_seen_users;
    let epoch = if changed {
        uuid::Uuid::new_v4().to_string()
    } else {
        existing_epoch.unwrap_or_default()
    };
    let fully_normalized = reasons
        .iter()
        .all(|reason| reason == "retained_approval_audit_anchor");
    let marker = if changed {
        let mut bytes = serde_json::to_vec(
            &serde_json::json!({"kind":"event","event":MARKER,"payload":{
                "version":VERSION,"cursorEpoch":epoch,"auditTruncated":truncated_audit,
                "fullyNormalized":fully_normalized,"retainedReasons":reasons,
                "sourceRevision":source,"seenUserMessageIds":&original.seen_users
            }}),
        )
        .map_err(|error| invalid(error.to_string()))?;
        bytes.push(b'\n');
        bytes
    } else {
        Vec::new()
    };
    let before_bytes = source.length;
    let after_bytes = rows
        .iter()
        .enumerate()
        .filter(|(index, _)| retained[*index])
        .map(|(index, row)| {
            if zero_rollbacks.contains(&index) {
                b"{\"kind\":\"rollback\",\"numTurns\":0}\n".len() as u64
            } else {
                row.len
            }
        })
        .sum::<u64>()
        + marker.len() as u64
        + u64::from(
            changed
                && rows
                    .iter()
                    .enumerate()
                    .rev()
                    .find(|(index, _)| retained[*index])
                    .is_some_and(|(index, row)| !zero_rollbacks.contains(&index) && !row.newline),
        );
    if revision(path, cancelled)? != source {
        return Err(conflict("rollout changed while migration was planned"));
    }
    let removed_records = retained.iter().filter(|keep| !**keep).count() as u64;
    Ok(MigrationPlan {
        report: MigrationReport {
            status: if changed {
                MigrationStatus::Preview
            } else {
                MigrationStatus::Unchanged
            },
            source_revision: source,
            target_version: VERSION,
            cursor_epoch: epoch,
            before_bytes,
            after_bytes,
            removed_approval_snapshots: removed_snapshots,
            removed_rollback_records: zero_rollbacks.len() as u64,
            removed_records,
            truncated_audit,
            fully_normalized,
            retained_reasons: reasons,
            needs_index_refresh: pending(path)?,
        },
        path: path.to_path_buf(),
        rows,
        retained,
        zero_rollbacks,
        marker,
        proof: verified,
        kind,
    })
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Journal {
    version: u32,
    source: SourceRevision,
    output_sha256: String,
    output_length: u64,
    temporary_file: String,
    cursor_epoch: String,
}
struct Scratch(PathBuf);
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
impl Scratch {
    fn keep(mut self) {
        self.0.clear();
    }
}
fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(windows)]
    let file = {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_FLAG_BACKUP_SEMANTICS, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
        };
        OpenOptions::new()
            .read(true)
            .write(true)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(path)?
    };
    #[cfg(not(windows))]
    let file = File::open(path)?;
    file.sync_all()
}
fn load_journal(path: &Path) -> io::Result<Option<Journal>> {
    match File::open(sidecar(path, ".migration.pending")) {
        Ok(file) => {
            let journal: Journal = serde_json::from_reader(file.take(1024 * 1024))
                .map_err(|error| invalid(format!("invalid migration journal: {error}")))?;
            if journal.version != VERSION {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "migration journal has an unsupported version",
                ));
            }
            Ok(Some(journal))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}
/// A pending published transaction requires index refresh; a prepared transaction
/// whose source is unchanged can be abandoned under the maintenance lease.
pub fn pending(path: &Path) -> io::Result<bool> {
    Ok(load_journal(path)?.is_some())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryState {
    None,
    PreparedAbandoned,
    NeedsIndexRefresh,
}

/// Reconcile an interrupted publication while the caller holds the maintenance
/// and inactive writer leases. Published bytes are never rolled back.
pub fn recover(path: &Path, cancelled: &AtomicBool) -> io::Result<RecoveryState> {
    let Some(journal) = load_journal(path)? else {
        return Ok(RecoveryState::None);
    };
    let current = revision(path, cancelled)?;
    if current.sha256 == journal.output_sha256 && current.length == journal.output_length {
        return Ok(RecoveryState::NeedsIndexRefresh);
    }
    recover_prepared(path, cancelled)?;
    Ok(RecoveryState::PreparedAbandoned)
}

/// Complete the derived-index half of a published migration. The store calls
/// this only after rebuilding header, turn, and profile indexes for the new epoch.
pub fn acknowledge(path: &Path) -> io::Result<()> {
    let Some(journal) = load_journal(path)? else {
        return Ok(());
    };
    let current = revision(path, &AtomicBool::new(false))?;
    if current.sha256 != journal.output_sha256 || current.length != journal.output_length {
        return Err(conflict(
            "migration journal does not describe the published rollout",
        ));
    }
    std::fs::remove_file(sidecar(path, ".migration.pending"))?;
    sync_directory(parent(path)?)
}
fn recover_prepared(path: &Path, cancelled: &AtomicBool) -> io::Result<()> {
    let Some(journal) = load_journal(path)? else {
        return Ok(());
    };
    let current = revision(path, cancelled)?;
    if current.sha256 == journal.output_sha256 && current.length == journal.output_length {
        return Err(conflict(
            "migration was published; refresh indexes and acknowledge before another migration",
        ));
    }
    if current != journal.source {
        return Err(conflict(
            "migration journal source revision no longer matches",
        ));
    }
    let temporary = Path::new(&journal.temporary_file);
    if temporary.components().count() != 1
        || !journal.temporary_file.starts_with(".rollout-migration-")
    {
        return Err(invalid(
            "migration journal temporary file is outside its transaction directory",
        ));
    }
    match std::fs::remove_file(parent(path)?.join(temporary)) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    std::fs::remove_file(sidecar(path, ".migration.pending"))?;
    sync_directory(parent(path)?)
}

/// Publish a plan while the store holds maintenance, parent lifecycle, and writer
/// leases. Cancellation is observed until rename; after publication durability is drained.
pub(crate) fn apply_under_lease(
    path: &Path,
    plan: &MigrationPlan,
    cancelled: &AtomicBool,
) -> io::Result<MigrationReport> {
    apply_with_commit_guard(path, plan, cancelled, || Ok(()))
}

fn apply_with_commit_guard(
    path: &Path,
    plan: &MigrationPlan,
    cancelled: &AtomicBool,
    before_commit: impl FnOnce() -> io::Result<()>,
) -> io::Result<MigrationReport> {
    if path != plan.path {
        return Err(invalid("migration plan belongs to another path"));
    }
    if path.extension().is_some_and(|extension| extension == "zst") {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "materialize the compressed rollout under its writer lease before migration",
        ));
    }
    recover_prepared(path, cancelled)?;
    if revision(path, cancelled)? != plan.report.source_revision {
        return Err(conflict("migration source revision changed; preview again"));
    }
    if plan.report.status == MigrationStatus::Unchanged {
        return Ok(plan.report.clone());
    }
    let directory = parent(path)?;
    let temporary_name = format!(".rollout-migration-{}.tmp", uuid::Uuid::new_v4());
    let temporary = Scratch(directory.join(&temporary_name));
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary.0)?;
    output.set_permissions(std::fs::metadata(path)?.permissions())?;
    let mut input = File::open(path)?;
    let mut output_hash = Sha256::new();
    let mut output_length = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    let mut last = Some(b'\n');
    for (index, row) in plan.rows.iter().enumerate() {
        check(cancelled)?;
        if !plan.retained[index] {
            continue;
        }
        if plan.zero_rollbacks.contains(&index) {
            let bytes = b"{\"kind\":\"rollback\",\"numTurns\":0}\n";
            output.write_all(bytes)?;
            output_hash.update(bytes);
            output_length += bytes.len() as u64;
            last = Some(b'\n');
            continue;
        }
        input.seek(SeekFrom::Start(row.offset))?;
        let mut remaining = row.len;
        while remaining > 0 {
            check(cancelled)?;
            let wanted = remaining.min(buffer.len() as u64) as usize;
            input.read_exact(&mut buffer[..wanted])?;
            output.write_all(&buffer[..wanted])?;
            output_hash.update(&buffer[..wanted]);
            output_length += wanted as u64;
            remaining -= wanted as u64;
            last = Some(buffer[wanted - 1]);
        }
    }
    if last != Some(b'\n') {
        output.write_all(b"\n")?;
        output_hash.update(b"\n");
        output_length += 1;
    }
    output.write_all(&plan.marker)?;
    output_hash.update(&plan.marker);
    output_length += plan.marker.len() as u64;
    output.set_times(std::fs::FileTimes::new().set_modified(input.metadata()?.modified()?))?;
    output.sync_all()?;
    let staged_rows = scan(File::open(&temporary.0)?, cancelled)?;
    let staged_proof = proof(
        &staged_rows,
        &vec![true; staged_rows.len()],
        &HashSet::new(),
        &BTreeSet::new(),
    );
    if staged_proof != plan.proof {
        return Err(invalid(
            "staged migration failed projection equivalence validation; source retained",
        ));
    }
    let journal = Journal {
        version: VERSION,
        source: plan.report.source_revision.clone(),
        output_sha256: hex(output_hash.finalize()),
        output_length,
        temporary_file: temporary_name,
        cursor_epoch: plan.report.cursor_epoch.clone(),
    };
    let pending_path = sidecar(path, ".migration.pending");
    let journal_candidate = Scratch(directory.join(format!(
        ".rollout-migration-{}.journal",
        uuid::Uuid::new_v4()
    )));
    let mut pending_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&journal_candidate.0)?;
    serde_json::to_writer(&mut pending_file, &journal)
        .map_err(|error| invalid(error.to_string()))?;
    pending_file.write_all(b"\n")?;
    pending_file.sync_all()?;
    std::fs::hard_link(&journal_candidate.0, &pending_path)?;
    let pending_guard = Scratch(pending_path);
    sync_directory(directory)?;
    before_commit()?;
    check(cancelled)?;
    if revision(path, cancelled)? != plan.report.source_revision {
        return Err(conflict(
            "migration source changed before publication; source retained",
        ));
    }
    check(cancelled)?;
    std::fs::rename(&temporary.0, path)?;
    // Once rename succeeds the journal must survive every later error, including
    // directory-sync failure, so a restarted store can rebuild its derived indexes.
    pending_guard.keep();
    sync_directory(directory)?;
    let mut report = plan.report.clone();
    report.status = MigrationStatus::Applied;
    report.after_bytes = output_length;
    report.needs_index_refresh = true;
    Ok(report)
}

/// Standalone apply acquires the same atomically stamped writer sidecar used by
/// session writers. Sub-agent owners must instead use the store's parent lease.
pub fn apply(
    path: &Path,
    plan: &MigrationPlan,
    cancelled: &AtomicBool,
) -> io::Result<MigrationReport> {
    check(cancelled)?;
    if plan.kind == MigrationKind::Subagent {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "sub-agent migration requires the store's inactive parent writer lease",
        ));
    }
    let lock_path = sidecar(path, ".lock");
    let candidate = Scratch(sidecar(
        path,
        &format!(".migration-lock-{}", uuid::Uuid::new_v4()),
    ));
    let mut stamped = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&candidate.0)?;
    writeln!(stamped, "{}", std::process::id())?;
    stamped.sync_all()?;
    std::fs::hard_link(&candidate.0, &lock_path).map_err(|error| {
        if error.kind() == io::ErrorKind::AlreadyExists {
            conflict("rollout has an active writer or an unreconciled writer lease")
        } else {
            error
        }
    })?;
    let _lease = Scratch(lock_path);
    apply_under_lease(path, plan, cancelled)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_or_cancellation_after_staging_and_journal_publication_retains_original() {
        let directory =
            std::env::temp_dir().join(format!("migration-publish-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let source = directory.join("source.jsonl");
        let bytes=b"{\"kind\":\"session_meta\",\"schemaVersion\":3,\"meta\":{\"id\":\"source\",\"createdAt\":1,\"cwd\":\"/tmp\",\"model\":\"fixture\"}}\n";
        std::fs::write(&source, bytes).unwrap();
        let cancelled = AtomicBool::new(false);
        let plan = preview(&source, MigrationKind::Main, &cancelled).unwrap();
        let error = apply_with_commit_guard(&source, &plan, &cancelled, || {
            assert!(pending(&source)?);
            Err(io::Error::other("injected commit barrier failure"))
        })
        .unwrap_err();
        assert!(error.to_string().contains("injected commit barrier"));
        assert_eq!(std::fs::read(&source).unwrap(), bytes);
        assert!(!pending(&source).unwrap());
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 1);
        let error = apply_with_commit_guard(&source, &plan, &cancelled, || {
            assert!(pending(&source)?);
            cancelled.store(true, Ordering::Release);
            Ok(())
        })
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert_eq!(std::fs::read(&source).unwrap(), bytes);
        assert!(!pending(&source).unwrap());
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 1);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
