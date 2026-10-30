//! Normalized durable queue storage. Permanent identities never own payloads.
use super::queue_wire::*;
use super::runtime_wire::{RawJson, RuntimeValue};
use klaus_agent_core::runtime_values::{RuntimeJson, RuntimeObject};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

#[derive(Clone)]
pub(super) struct Store(Arc<StoreInner>);
struct StoreInner {
    path: PathBuf,
    writer: Mutex<Option<Connection>>,
    initialized: AtomicBool,
    closed: AtomicBool,
    identity: OnceLock<(u64, u64)>,
    #[cfg(test)]
    writer_opens: std::sync::atomic::AtomicUsize,
}
#[derive(Debug, thiserror::Error)]
pub(super) enum Error {
    #[error("{0}")]
    Invalid(String),
    #[error("queue revision conflict: expected {expected}, current {actual}")]
    Conflict { expected: u64, actual: u64 },
    #[error("queue entry changed")]
    EntryChanged,
    #[error("{0}")]
    Storage(String),
}
type Result<T> = std::result::Result<T, Error>;
pub(super) const INTERRUPTED_REASON: &str = "Interrupted before the turn ended.";
const PROOF_BYTES: u64 = 16 * 1024 * 1024;
const PROOF_AGE: u64 = 7 * 24 * 60 * 60 * 1000;
const MAX_QUEUE_ROWS: usize = 128;
const MAX_ITEM_BYTES: u64 = 8 * 1024 * 1024;
const MAX_QUEUE_BYTES: u64 = 64 * 1024 * 1024;

// Normal transactions load current headers and explicitly selected identities.
// The historical document format is accepted only by the one-time migration.
#[derive(Default, Serialize, Deserialize, JsonSchema)]
pub(super) struct Document {
    pub revision: u64,
    pub rows: Vec<QueuedSubmission>,
    pub receipts: HashMap<String, Receipt>,
    pub migrations: HashSet<String>,
    #[serde(default)]
    pub legacy_receipts: HashMap<String, QueueImportedReceipt>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct Receipt {
    pub id: String,
    pub kind: String,
    pub queued_submission: QueuedSubmission,
    pub revision: u64,
    pub submission_id: Option<String>,
    pub turn_id: Option<String>,
    #[serde(default)]
    pub fingerprint: Option<String>,
    #[serde(default)]
    pub birth_input_fingerprint: Option<String>,
    #[serde(skip)]
    pub proof_available: bool,
}
#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct Identity {
    id: String,
    kind: String,
    queued_submission_id: String,
    client_user_message_id: Option<String>,
    revision: u64,
    submission_id: Option<String>,
    turn_id: Option<String>,
    fingerprint: Option<String>,
    input_fingerprint: String,
    #[serde(default)]
    birth_input_fingerprint: Option<String>,
}
impl Receipt {
    fn identity(&self) -> Identity {
        Identity {
            id: self.id.clone(),
            kind: self.kind.clone(),
            queued_submission_id: self.queued_submission.id.clone(),
            client_user_message_id: self.queued_submission.client_user_message_id.clone(),
            revision: self.revision,
            submission_id: self.submission_id.clone(),
            turn_id: self.turn_id.clone(),
            fingerprint: self.fingerprint.clone(),
            input_fingerprint: self.queued_submission.input_fingerprint.clone(),
            birth_input_fingerprint: self.birth_input_fingerprint.clone(),
        }
    }
    pub fn raw(&self) -> RawJson {
        RawJson::from_serialize(&self.identity()).expect("serializable queue identity")
    }
}
impl Document {
    pub fn check_revision(&self, expected: Option<u64>) -> Result<()> {
        if let Some(expected) = expected {
            if expected != self.revision {
                return Err(Error::Conflict {
                    expected,
                    actual: self.revision,
                });
            }
        }
        Ok(())
    }
    pub fn row_index(&self, id: &str) -> Result<usize> {
        self.rows
            .iter()
            .position(|r| r.id == id)
            .ok_or_else(|| Error::Invalid(format!("unknown queued submission: {id}")))
    }
    pub fn receipt(&self, row: QueuedSubmission, kind: &str) -> Receipt {
        Receipt {
            id: uuid::Uuid::new_v4().to_string(),
            kind: kind.into(),
            queued_submission: row,
            revision: self.revision + 1,
            submission_id: None,
            turn_id: None,
            fingerprint: None,
            birth_input_fingerprint: None,
            proof_available: true,
        }
    }
    pub fn remember(&mut self, r: Receipt, mutation: Option<&str>) {
        if let Some(id) = &r.queued_submission.client_user_message_id {
            self.receipts.insert(format!("client:{id}"), r.clone());
        }
        if let Some(id) = mutation {
            self.receipts.insert(format!("mutation:{id}"), r);
        }
    }
    pub fn require_mutable(&self, index: usize) -> Result<()> {
        if self.rows[index].dispatching.is_some() {
            return Err(Error::Invalid(
                "admission is unconfirmed; explicitly retry or delete this entry".into(),
            ));
        }
        Ok(())
    }
}
fn storage(e: impl std::fmt::Display) -> Error {
    Error::Storage(e.to_string())
}
fn encode<T: Serialize + ?Sized>(v: &T) -> Result<String> {
    serde_json::to_string(v).map_err(storage)
}
fn decode<T: serde::de::DeserializeOwned>(s: &str) -> Result<T> {
    serde_json::from_str(s).map_err(storage)
}
pub(super) fn nonempty(v: &str, name: &str) -> Result<()> {
    if v.trim().is_empty() || v.len() > 4096 || v.contains('\0') {
        return Err(Error::Invalid(format!(
            "{name} must be nonempty and at most 4096 bytes"
        )));
    }
    Ok(())
}
pub(super) fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
fn fingerprint(input: &[RuntimeValue]) -> Result<String> {
    Ok(format!(
        "sha256:{:x}",
        Sha256::digest(encode(input)?.as_bytes())
    ))
}
fn item(m: &Option<RuntimeValue>) -> Option<&RuntimeJson> {
    m.as_ref()?.0.get("item")
}
fn item_bytes(row: &QueuedSubmission) -> Result<u64> {
    item(&row.client_metadata)
        .map(|value| encode(&RuntimeValue(value.clone())).map(|text| text.len() as u64))
        .unwrap_or(Ok(0))
}
fn check_capacity(document: &Document, row: &QueuedSubmission) -> Result<()> {
    if document.rows.len() >= MAX_QUEUE_ROWS {
        return Err(Error::Invalid(
            "queue cannot contain more than 128 entries".into(),
        ));
    }
    let bytes = item_bytes(row)?;
    if bytes > MAX_ITEM_BYTES {
        return Err(Error::Invalid(
            "queue UI item exceeds the 8 MiB limit".into(),
        ));
    }
    let total = document.rows.iter().try_fold(bytes, |total, row| {
        item_bytes(row).map(|bytes| total.saturating_add(bytes))
    })?;
    if total > MAX_QUEUE_BYTES {
        return Err(Error::Invalid(
            "queue UI items exceed the 64 MiB limit".into(),
        ));
    }
    Ok(())
}
fn validate_restore(p: &ThreadQueueAddParams, r: &QueuedSubmission) -> Result<()> {
    if r.dispatching.is_some()
        || item(&p.client_metadata) != item(&r.client_metadata)
        || (p.paused_reason != r.paused_reason
            && !(p.paused_reason.is_none()
                && r.paused_reason.as_deref() == Some(INTERRUPTED_REASON)))
        || p.created_at != Some(r.created_at)
    {
        return Err(Error::Invalid(
            "restore payload does not match a confirmed removed submission".into(),
        ));
    }
    Ok(())
}
fn legacy_removed(d: &Document, p: &ThreadQueueAddParams) -> Option<Receipt> {
    let (receipt, removed) = d
        .legacy_receipts
        .values()
        .filter(|r| r.kind == "removal")
        .filter_map(|r| {
            let raw = r.result.as_ref()?.to_runtime().ok()?.0;
            let removed = raw.get("removed")?.clone();
            let id = removed.get("id")?.as_string()?.try_to_utf8().ok()?;
            (id == p.client_user_message_id).then_some((r, removed))
        })
        .max_by_key(|(r, _)| r.created_at)?;
    let created_at = removed.get("createdAt")?.as_number()?.as_f64() as u64;
    let mut metadata = p
        .client_metadata
        .as_ref()
        .and_then(|v| v.0.as_object())
        .cloned()
        .unwrap_or_else(RuntimeObject::new);
    metadata.insert("item", removed.get("item")?.clone());
    metadata.insert("legacyUnprepared", RuntimeJson::Bool(true));
    let paused_reason = removed
        .get("pausedReason")
        .and_then(RuntimeJson::as_string)
        .and_then(|v| v.try_to_utf8().ok());
    let unknown = removed
        .get("dispatching")
        .is_some_and(|v| !matches!(v, RuntimeJson::Null | RuntimeJson::Bool(false)));
    Some(Receipt {
        id: format!("legacy-{}", receipt.mutation_id),
        kind: "deleted".into(),
        queued_submission: QueuedSubmission {
            id: uuid::Uuid::new_v4().to_string(),
            input: p.input.clone(),
            input_omitted: false,
            input_fingerprint: fingerprint(&p.input).ok()?,
            row_version: 1,
            client_user_message_id: Some(p.client_user_message_id.clone()),
            client_metadata: Some(RuntimeValue(RuntimeJson::Object(metadata))),
            paused_reason,
            created_at,
            dispatching: unknown.then(|| QueueDispatch {
                submission_id: "legacy-unconfirmed".into(),
                mode: super::wire::TurnSubmitMode::StartIfIdle,
            }),
        },
        revision: d.revision,
        submission_id: None,
        turn_id: None,
        fingerprint: None,
        birth_input_fingerprint: None,
        proof_available: true,
    })
}
const SCHEMA:&str="
CREATE TABLE IF NOT EXISTS thread_queues_v1(thread_id TEXT PRIMARY KEY NOT NULL,revision INTEGER NOT NULL,document TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS queue_threads(thread_id TEXT PRIMARY KEY NOT NULL,revision INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS queue_payloads(id TEXT PRIMARY KEY NOT NULL,input TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS queue_entries(thread_id TEXT NOT NULL,id TEXT NOT NULL,position INTEGER NOT NULL,payload_id TEXT NOT NULL,header TEXT NOT NULL,PRIMARY KEY(thread_id,id));
CREATE INDEX IF NOT EXISTS queue_entries_order ON queue_entries(thread_id,position,id);
CREATE INDEX IF NOT EXISTS queue_entries_payload ON queue_entries(payload_id);
CREATE TABLE IF NOT EXISTS queue_identities(thread_id TEXT NOT NULL,key TEXT NOT NULL,receipt TEXT NOT NULL,PRIMARY KEY(thread_id,key));
CREATE TABLE IF NOT EXISTS queue_proofs(id TEXT PRIMARY KEY NOT NULL,thread_id TEXT NOT NULL,row_id TEXT NOT NULL,header TEXT NOT NULL,payload_id TEXT,bytes INTEGER NOT NULL,created_at INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS queue_proofs_age ON queue_proofs(created_at,id);
CREATE INDEX IF NOT EXISTS queue_proofs_row ON queue_proofs(thread_id,row_id);
CREATE INDEX IF NOT EXISTS queue_proofs_payload ON queue_proofs(payload_id);
CREATE TABLE IF NOT EXISTS queue_migrations(thread_id TEXT NOT NULL,id TEXT NOT NULL,PRIMARY KEY(thread_id,id));
CREATE TABLE IF NOT EXISTS queue_legacy(thread_id TEXT NOT NULL,id TEXT NOT NULL,kind TEXT NOT NULL,fingerprint TEXT NOT NULL,created_at INTEGER NOT NULL,PRIMARY KEY(thread_id,id));";
fn revision(c: &Connection, t: &str) -> Result<u64> {
    Ok(c.query_row(
        "SELECT revision FROM queue_threads WHERE thread_id=?1",
        [t],
        |r| r.get(0),
    )
    .optional()
    .map_err(storage)?
    .unwrap_or(0))
}
#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct StoredHeader<'a> {
    id: &'a str,
    input_omitted: bool,
    input_fingerprint: &'a str,
    row_version: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_user_message_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_metadata: Option<&'a RuntimeValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    paused_reason: Option<&'a str>,
    created_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    dispatching: Option<&'a QueueDispatch>,
}

fn header(row: &QueuedSubmission) -> Result<String> {
    encode(&StoredHeader {
        id: &row.id,
        input_omitted: true,
        input_fingerprint: &row.input_fingerprint,
        row_version: row.row_version,
        client_user_message_id: row.client_user_message_id.as_deref(),
        client_metadata: row.client_metadata.as_ref(),
        paused_reason: row.paused_reason.as_deref(),
        created_at: row.created_at,
        dispatching: row.dispatching.as_ref(),
    })
}
fn hydrate(
    c: &Connection,
    mut r: QueuedSubmission,
    p: &str,
    include: bool,
) -> Result<QueuedSubmission> {
    if include {
        let text: String = c
            .query_row("SELECT input FROM queue_payloads WHERE id=?1", [p], |r| {
                r.get(0)
            })
            .map_err(storage)?;
        r.input = decode(&text)?;
        r.input_omitted = false;
    }
    Ok(r)
}
#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ListPage<T> {
    data: Vec<T>,
    next_cursor: Option<String>,
    revision: u64,
}

#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ProjectedSubmission {
    id: String,
    #[serde(default)]
    input_omitted: bool,
    #[serde(default)]
    input_fingerprint: String,
    #[serde(default)]
    row_version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    client_user_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    client_metadata: Option<RawJson>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    paused_reason: Option<String>,
    created_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dispatching: Option<QueueDispatch>,
}

fn projected_rows(
    c: &Connection,
    thread: &str,
    _include: bool,
    limit: usize,
    offset: usize,
) -> Result<Vec<ProjectedSubmission>> {
    let mut query = c.prepare("SELECT id,header FROM queue_entries WHERE thread_id=?1 ORDER BY position,id LIMIT ?2 OFFSET ?3").map_err(storage)?;
    let data = query
        .query_map(params![thread, limit as i64, offset as i64], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(storage)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(storage)?;
    data.into_iter()
        .map(|(id, header)| {
            let mut row: ProjectedSubmission = decode(&header)?;
            if row.id != id {
                return Err(storage(
                    "queue entry identity does not match its stored key",
                ));
            }
            row.input_omitted = true;
            Ok(row)
        })
        .collect()
}

struct StoredEntry {
    position: usize,
    header: String,
    payload_id: String,
}

fn transaction_rows(
    c: &Connection,
    thread: &str,
) -> Result<(Vec<QueuedSubmission>, HashMap<String, StoredEntry>)> {
    let mut query = c
        .prepare("SELECT id,header,payload_id FROM queue_entries WHERE thread_id=?1 ORDER BY position,id")
        .map_err(storage)?;
    let stored = query
        .query_map([thread], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(storage)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(storage)?;
    let mut rows = Vec::with_capacity(stored.len());
    let mut before = HashMap::with_capacity(stored.len());
    for (position, (id, header, payload_id)) in stored.into_iter().enumerate() {
        let row: QueuedSubmission = decode(&header)?;
        if row.id != id {
            return Err(storage(
                "queue entry identity does not match its stored key",
            ));
        }
        before.insert(
            id,
            StoredEntry {
                position,
                header,
                payload_id,
            },
        );
        rows.push(row);
    }
    Ok((rows, before))
}

fn stored_header_matches(stored: &str, current: &str) -> Result<bool> {
    if stored == current {
        return Ok(true);
    }
    // Persisted JSON spelling must not turn an unchanged entry into a mutation.
    let prior: QueuedSubmission = decode(stored)?;
    Ok(header(&prior)? == current)
}

fn rows(
    c: &Connection,
    t: &str,
    include: bool,
    limit: usize,
    offset: usize,
) -> Result<Vec<QueuedSubmission>> {
    let mut q=c.prepare("SELECT header,payload_id FROM queue_entries WHERE thread_id=?1 ORDER BY position,id LIMIT ?2 OFFSET ?3").map_err(storage)?;
    let data = q
        .query_map(params![t, limit as i64, offset as i64], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })
        .map_err(storage)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(storage)?;
    data.into_iter()
        .map(|(h, p)| hydrate(c, decode(&h)?, &p, include))
        .collect()
}
fn get_row(c: &Connection, t: &str, id: &str, include: bool) -> Result<Option<QueuedSubmission>> {
    let data = c
        .query_row(
            "SELECT header,payload_id FROM queue_entries WHERE thread_id=?1 AND id=?2",
            params![t, id],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(storage)?;
    data.map(|(h, p)| hydrate(c, decode(&h)?, &p, include))
        .transpose()
}
fn payload_id(c: &Connection, t: &str, id: &str) -> Result<Option<String>> {
    c.query_row(
        "SELECT payload_id FROM queue_entries WHERE thread_id=?1 AND id=?2",
        params![t, id],
        |r| r.get(0),
    )
    .optional()
    .map_err(storage)
}
fn gc(c: &Connection) -> Result<()> {
    c.execute("DELETE FROM queue_payloads WHERE NOT EXISTS(SELECT 1 FROM queue_entries WHERE payload_id=queue_payloads.id) AND NOT EXISTS(SELECT 1 FROM queue_proofs WHERE payload_id=queue_payloads.id)",[]).map_err(storage)?;
    Ok(())
}
fn prune(c: &Connection) -> Result<()> {
    c.execute(
        "DELETE FROM queue_proofs WHERE created_at<?1",
        [now().saturating_sub(PROOF_AGE)],
    )
    .map_err(storage)?;
    let mut q = c
        .prepare("SELECT id,bytes FROM queue_proofs ORDER BY created_at DESC,id DESC")
        .map_err(storage)?;
    let proofs = q
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, u64>(1)?)))
        .map_err(storage)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(storage)?;
    let mut total = 0u64;
    for (i, (id, size)) in proofs.into_iter().enumerate() {
        total = total.saturating_add(size);
        if i >= 512 || total > PROOF_BYTES {
            c.execute("DELETE FROM queue_proofs WHERE id=?1", [id])
                .map_err(storage)?;
        }
    }
    gc(c)
}
fn proof_bytes(r: &QueuedSubmission) -> Result<u64> {
    let mut removed = RuntimeObject::new();
    removed.insert(
        "id",
        r.client_user_message_id
            .clone()
            .unwrap_or_else(|| r.id.clone())
            .into(),
    );
    removed.insert(
        "item",
        item(&r.client_metadata)
            .cloned()
            .unwrap_or(RuntimeJson::Null),
    );
    removed.insert("createdAt", r.created_at.into());
    if let Some(reason) = &r.paused_reason {
        removed.insert("pausedReason", reason.clone().into());
    }
    if let Some(d) = &r.dispatching {
        removed.insert(
            "dispatching",
            RawJson::from_serialize(d)
                .map_err(storage)?
                .to_runtime()
                .map_err(storage)?
                .0,
        );
    }
    let mut result = RuntimeObject::new();
    result.insert("removed", RuntimeJson::Object(removed));
    Ok(encode(&RuntimeValue(RuntimeJson::Object(result)))?.len() as u64)
}
fn save_proof(c: &Connection, t: &str, r: &Receipt, p: Option<&str>, created: u64) -> Result<()> {
    let bytes = proof_bytes(&r.queued_submission)?;
    if bytes > PROOF_BYTES {
        return Err(Error::Invalid(
            "removed UI proof exceeds the 16 MiB undo budget".into(),
        ));
    }
    c.execute("INSERT OR IGNORE INTO queue_proofs(id,thread_id,row_id,header,payload_id,bytes,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7)",params![r.id,t,r.queued_submission.id,header(&r.queued_submission)?,p,bytes,created]).map_err(storage)?;
    Ok(())
}
fn save_identity(c: &Connection, t: &str, key: &str, r: &Receipt) -> Result<()> {
    let mut identity = r.identity();
    let previous: Option<String> = c
        .query_row(
            "SELECT receipt FROM queue_identities WHERE thread_id=?1 AND key=?2",
            params![t, key],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage)?;
    identity.birth_input_fingerprint = Some(if let Some(previous) = previous {
        let previous: Identity = decode(&previous)?;
        previous
            .birth_input_fingerprint
            .unwrap_or(previous.input_fingerprint)
    } else {
        identity
            .birth_input_fingerprint
            .unwrap_or_else(|| identity.input_fingerprint.clone())
    });
    c.execute("INSERT INTO queue_identities(thread_id,key,receipt) VALUES(?1,?2,?3) ON CONFLICT(thread_id,key) DO UPDATE SET receipt=excluded.receipt",params![t,key,encode(&identity)?]).map_err(storage)?;
    Ok(())
}
fn load_identity(c: &Connection, t: &str, key: &str) -> Result<Option<Receipt>> {
    let text: Option<String> = c
        .query_row(
            "SELECT receipt FROM queue_identities WHERE thread_id=?1 AND key=?2",
            params![t, key],
            |r| r.get(0),
        )
        .optional()
        .map_err(storage)?;
    let Some(text) = text else {
        return Ok(None);
    };
    let i: Identity = decode(&text)?;
    let birth_input_fingerprint = Some(
        i.birth_input_fingerprint
            .clone()
            .unwrap_or_else(|| i.input_fingerprint.clone()),
    );
    let proof = c
        .query_row(
            "SELECT header,payload_id FROM queue_proofs WHERE id=?1 AND created_at>=?2",
            params![i.id, now().saturating_sub(PROOF_AGE)],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?)),
        )
        .optional()
        .map_err(storage)?;
    let current = if i.kind == "deleted" {
        None
    } else {
        get_row(c, t, &i.queued_submission_id, false)?
    };
    let proof_available = proof.is_some();
    let row = if let Some(r) = current {
        r
    } else if let Some((h, _)) = proof {
        decode(&h)?
    } else {
        QueuedSubmission {
            id: i.queued_submission_id,
            input: vec![],
            input_omitted: true,
            input_fingerprint: i.input_fingerprint,
            row_version: 0,
            client_user_message_id: i.client_user_message_id,
            client_metadata: None,
            paused_reason: None,
            created_at: 0,
            dispatching: None,
        }
    };
    Ok(Some(Receipt {
        id: i.id,
        kind: i.kind,
        queued_submission: row,
        revision: i.revision,
        submission_id: i.submission_id,
        turn_id: i.turn_id,
        fingerprint: i.fingerprint,
        birth_input_fingerprint,
        proof_available,
    }))
}
fn load_legacy(c: &Connection, t: &str, id: &str) -> Result<Option<QueueImportedReceipt>> {
    let data = c
        .query_row(
            "SELECT kind,fingerprint,created_at FROM queue_legacy WHERE thread_id=?1 AND id=?2",
            params![t, id],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, u64>(2)?,
                ))
            },
        )
        .optional()
        .map_err(storage)?;
    data.map(|(kind, fingerprint, created_at)| {
        let result: Option<String> = c
            .query_row(
                "SELECT header FROM queue_proofs WHERE id=?1 AND created_at>=?2",
                params![format!("legacy:{t}:{id}"), now().saturating_sub(PROOF_AGE)],
                |r| r.get(0),
            )
            .optional()
            .map_err(storage)?;
        Ok(QueueImportedReceipt {
            kind,
            mutation_id: id.into(),
            fingerprint,
            created_at,
            result: result
                .map(|s| RawJson::from_serialize(&decode::<RuntimeValue>(&s)?).map_err(storage))
                .transpose()?,
        })
    })
    .transpose()
}
impl Store {
    pub fn new(path: PathBuf) -> Self {
        Self(Arc::new(StoreInner {
            path,
            writer: Mutex::new(None),
            initialized: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            identity: OnceLock::new(),
            #[cfg(test)]
            writer_opens: std::sync::atomic::AtomicUsize::new(0),
        }))
    }
    fn file_identity(&self) -> Result<(u64, u64)> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let metadata = std::fs::metadata(&self.0.path).map_err(storage)?;
            Ok((metadata.dev(), metadata.ino()))
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            use std::os::windows::io::AsRawHandle;
            use windows_sys::Win32::Storage::FileSystem::{
                GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION, FILE_SHARE_DELETE,
                FILE_SHARE_READ, FILE_SHARE_WRITE,
            };
            let file = std::fs::OpenOptions::new()
                .read(true)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
                .open(&self.0.path)
                .map_err(storage)?;
            let mut information = BY_HANDLE_FILE_INFORMATION::default();
            if unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, &mut information) }
                == 0
            {
                return Err(storage(std::io::Error::last_os_error()));
            }
            Ok((
                information.dwVolumeSerialNumber as u64,
                ((information.nFileIndexHigh as u64) << 32) | information.nFileIndexLow as u64,
            ))
        }
        #[cfg(not(any(unix, windows)))]
        {
            Err(storage(
                "queue file identity is unsupported on this platform",
            ))
        }
    }
    fn check_active(&self) -> Result<()> {
        if self.0.closed.load(Ordering::Acquire) {
            return Err(storage("queue endpoint is closed"));
        }
        if let Some(expected) = self.0.identity.get() {
            if &self.file_identity()? != expected {
                return Err(storage(
                    "queue database file was replaced; reconnect the endpoint",
                ));
            }
        }
        Ok(())
    }
    pub fn close(&self) {
        self.0.closed.store(true, Ordering::Release);
        let mut writer = self
            .0
            .writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        writer.take();
    }
    fn with_connection<T>(&self, f: impl FnOnce(&mut Connection) -> Result<T>) -> Result<T> {
        self.check_active()?;
        let mut writer = match self.0.writer.lock() {
            Ok(writer) => writer,
            Err(poisoned) => {
                let mut writer = poisoned.into_inner();
                writer.take();
                self.0.writer.clear_poison();
                return Err(storage(
                    "queue writer mutex was poisoned; connection retired",
                ));
            }
        };
        if let Err(error) = self.check_active() {
            writer.take();
            return Err(error);
        }
        if writer.is_none() {
            *writer = Some(self.open_writer()?);
            self.0.initialized.store(true, Ordering::Release);
        }
        let connection = writer.as_mut().expect("initialized queue writer");
        let result = f(connection);
        // Never retain a transaction/snapshot after an error or callback exit.
        // An unexpected unfinished transaction is rolled back and its physical
        // connection is retired, even if the rollback succeeds.
        let unfinished = !connection.is_autocommit();
        if unfinished {
            let _ = connection.execute_batch("ROLLBACK");
        }
        let active = self.check_active();
        if unfinished || active.is_err() || matches!(&result, Err(Error::Storage(_))) {
            writer.take();
        }
        active?;
        if unfinished {
            return Err(storage(
                "queue operation left an unfinished transaction; connection retired",
            ));
        }
        result
    }
    fn with_writer<T>(
        &self,
        thread: &str,
        f: impl FnOnce(&mut Connection) -> Result<T>,
    ) -> Result<T> {
        nonempty(thread, "threadId")?;
        self.with_connection(|connection| {
            let legacy: bool = connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM thread_queues_v1 WHERE thread_id=?1)",
                    [thread],
                    |row| row.get(0),
                )
                .map_err(storage)?;
            if legacy {
                let tx = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(storage)?;
                Self::migrate(&tx, thread)?;
                self.check_active()?;
                tx.commit().map_err(storage)?;
            }
            f(connection)
        })
    }
    pub async fn lock(&self, t: &str) -> Result<std::fs::File> {
        nonempty(t, "threadId")?;
        self.check_active()?;
        let locks = self.0.path.with_extension("locks");
        let name = format!("{:x}", Sha256::digest(t.as_bytes()));
        tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&locks).map_err(storage)?;
            let mut o = std::fs::OpenOptions::new();
            o.create(true).read(true).write(true).truncate(false);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                o.mode(0o600);
            }
            let file = o.open(locks.join(name)).map_err(storage)?;
            fs2::FileExt::lock_exclusive(&file).map_err(storage)?;
            Ok(file)
        })
        .await
        .map_err(storage)?
    }
    fn open_writer(&self) -> Result<Connection> {
        self.check_active()?;
        if let Some(p) = self.0.path.parent() {
            std::fs::create_dir_all(p).map_err(storage)?;
        }
        let previous_identity = if self.0.path.try_exists().map_err(storage)? {
            Some(self.file_identity()?)
        } else {
            None
        };
        // SQLite must own every database descriptor. Closing an unmanaged
        // descriptor releases the process's locks held by other connections.
        let c = Connection::open(&self.0.path).map_err(storage)?;
        let identity = self.file_identity()?;
        if previous_identity.is_some_and(|previous| previous != identity) {
            return Err(storage("queue database changed while opening"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&self.0.path, std::fs::Permissions::from_mode(0o600))
                .map_err(storage)?;
        }
        c.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(storage)?;
        c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
            .map_err(storage)?;
        c.execute_batch(SCHEMA).map_err(storage)?;
        if self.file_identity()? != identity {
            return Err(storage("queue database changed while opening"));
        }
        if self
            .0
            .identity
            .get()
            .is_some_and(|expected| *expected != identity)
        {
            return Err(storage(
                "queue database file was replaced; reconnect the endpoint",
            ));
        }
        let _ = self.0.identity.set(identity);
        self.check_active()?;
        #[cfg(test)]
        self.0.writer_opens.fetch_add(1, Ordering::Relaxed);
        Ok(c)
    }
    fn open_read(&self) -> Result<Connection> {
        self.check_active()?;
        if !self.0.initialized.load(Ordering::Acquire) {
            self.with_connection(|_| Ok(()))?;
        }
        self.check_active()?;
        let connection =
            Connection::open_with_flags(&self.0.path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .map_err(storage)?;
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(storage)?;
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(storage)?;
        self.check_active()?;
        Ok(connection)
    }
    #[cfg(test)]
    fn open(&self) -> Result<Connection> {
        if !self.0.initialized.load(Ordering::Acquire) {
            self.with_connection(|_| Ok(()))?;
        }
        self.open_writer()
    }
    fn migrate(c: &Connection, t: &str) -> Result<()> {
        let source: Option<(u64, String)> = c
            .query_row(
                "SELECT revision,document FROM thread_queues_v1 WHERE thread_id=?1",
                [t],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(storage)?;
        let Some((stored, text)) = source else {
            return Ok(());
        };
        let mut d: Document = decode(&text)?;
        if d.revision != stored || revision(c, t)? != 0 {
            return Err(Error::Storage("inconsistent v1 queue migration".into()));
        }
        let mut seen = HashSet::new();
        for (position, r) in d.rows.iter_mut().enumerate() {
            if !seen.insert(r.id.clone()) || r.input_omitted {
                return Err(Error::Storage(
                    "invalid duplicate or projected v1 entry".into(),
                ));
            }
            r.input_fingerprint = fingerprint(&r.input)?;
            r.row_version = r.row_version.max(1);
            let p = uuid::Uuid::new_v4().to_string();
            c.execute(
                "INSERT INTO queue_payloads(id,input) VALUES(?1,?2)",
                params![p, encode(&r.input)?],
            )
            .map_err(storage)?;
            c.execute("INSERT INTO queue_entries(thread_id,id,position,payload_id,header) VALUES(?1,?2,?3,?4,?5)",params![t,r.id,position as i64,p,header(r)?]).map_err(storage)?;
        }
        let mut births = HashMap::<String, (u64, String)>::new();
        for receipt in d
            .receipts
            .values()
            .filter(|receipt| receipt.kind == "added")
        {
            if let Some(client) = &receipt.queued_submission.client_user_message_id {
                let candidate = (
                    receipt.revision,
                    fingerprint(&receipt.queued_submission.input)?,
                );
                if births.get(client).is_none_or(|old| candidate.0 < old.0) {
                    births.insert(client.clone(), candidate);
                }
            }
        }
        let mut proof_payloads = HashMap::<String, String>::new();
        for (key, mut r) in d.receipts {
            r.queued_submission.input_fingerprint = fingerprint(&r.queued_submission.input)?;
            r.queued_submission.row_version = r.queued_submission.row_version.max(1);
            r.birth_input_fingerprint = r
                .queued_submission
                .client_user_message_id
                .as_ref()
                .and_then(|client| {
                    births
                        .get(client)
                        .map(|(_, fingerprint)| fingerprint.clone())
                });
            if r.kind == "deleted" {
                let row = &r.queued_submission;
                let p = if let Some(p) = proof_payloads.get(&r.id) {
                    p.clone()
                } else if let Some(p) = payload_id(c, t, &row.id)? {
                    p
                } else {
                    let p = uuid::Uuid::new_v4().to_string();
                    c.execute(
                        "INSERT INTO queue_payloads(id,input) VALUES(?1,?2)",
                        params![p, encode(&row.input)?],
                    )
                    .map_err(storage)?;
                    p
                };
                proof_payloads.insert(r.id.clone(), p.clone());
                save_proof(c, t, &r, Some(&p), now())?;
            }
            save_identity(c, t, &key, &r)?;
        }
        for id in d.migrations {
            c.execute(
                "INSERT OR IGNORE INTO queue_migrations(thread_id,id) VALUES(?1,?2)",
                params![t, id],
            )
            .map_err(storage)?;
        }
        for r in d.legacy_receipts.into_values() {
            save_legacy(c, t, &r)?;
        }
        c.execute("INSERT INTO queue_threads(thread_id,revision) VALUES(?1,?2) ON CONFLICT(thread_id) DO UPDATE SET revision=excluded.revision",params![t,d.revision]).map_err(storage)?;
        c.execute("DELETE FROM thread_queues_v1 WHERE thread_id=?1", [t])
            .map_err(storage)?;
        prune(c)
    }
    fn ready(&self, t: &str) -> Result<Connection> {
        nonempty(t, "threadId")?;
        // Healthy readers never take the writer mutex. A legacy migration is
        // the only reason to release this read connection and visit the writer.
        for _ in 0..2 {
            let c = self.open_read()?;
            let exists: bool = c
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM thread_queues_v1 WHERE thread_id=?1)",
                    [t],
                    |r| r.get(0),
                )
                .map_err(storage)?;
            if !exists {
                return Ok(c);
            }
            drop(c);
            self.with_writer(t, |_| Ok(()))?;
        }
        Err(storage("legacy queue changed repeatedly during read"))
    }
    pub fn read(&self, t: &str) -> Result<Document> {
        self.read_projected(t, true)
    }
    pub fn read_projected(&self, t: &str, include: bool) -> Result<Document> {
        let mut c = self.ready(t)?;
        let tx = c.transaction().map_err(storage)?;
        let d = Document {
            revision: revision(&tx, t)?,
            rows: rows(&tx, t, include, i64::MAX as usize, 0)?,
            ..Default::default()
        };
        tx.commit().map_err(storage)?;
        Ok(d)
    }
    pub fn entry(&self, t: &str, id: &str, include: bool) -> Result<Option<QueuedSubmission>> {
        let mut connection = self.ready(t)?;
        let tx = connection.transaction().map_err(storage)?;
        let row = get_row(&tx, t, id, include)?;
        tx.commit().map_err(storage)?;
        Ok(row)
    }
    fn list_page<T>(
        &self,
        p: ThreadQueueListParams,
        read: fn(&Connection, &str, bool, usize, usize) -> Result<Vec<T>>,
    ) -> Result<ListPage<T>> {
        let mut c = self.ready(&p.thread_id)?;
        let tx = c.transaction().map_err(storage)?;
        let revision = revision(&tx, &p.thread_id)?;
        let offset = if let Some(cursor) = p.cursor {
            let (v, n) = cursor
                .split_once(':')
                .ok_or_else(|| Error::Invalid("invalid queue cursor".into()))?;
            let v: u64 = v
                .parse()
                .map_err(|_| Error::Invalid("invalid queue cursor revision".into()))?;
            if v != revision {
                return Err(Error::Conflict {
                    expected: v,
                    actual: revision,
                });
            }
            n.parse::<usize>()
                .map_err(|_| Error::Invalid("invalid queue cursor offset".into()))?
        } else {
            0
        };
        let limit = p.limit.unwrap_or(100);
        if !(1..=1000).contains(&limit) {
            return Err(Error::Invalid("queue limit must be 1..1000".into()));
        }
        let count: usize = tx
            .query_row(
                "SELECT count(*) FROM queue_entries WHERE thread_id=?1",
                [&p.thread_id],
                |r| r.get(0),
            )
            .map_err(storage)?;
        if offset > count {
            return Err(Error::Invalid("queue cursor must be in range".into()));
        }
        let data = read(&tx, &p.thread_id, p.include_input, limit, offset)?;
        let end = offset.saturating_add(data.len());
        let r = ListPage {
            data,
            next_cursor: (end < count).then(|| format!("{revision}:{end}")),
            revision,
        };
        tx.commit().map_err(storage)?;
        Ok(r)
    }
    pub fn list(&self, p: ThreadQueueListParams) -> Result<ThreadQueueListResult> {
        let page = self.list_page(p, rows)?;
        Ok(ThreadQueueListResult {
            data: page.data,
            next_cursor: page.next_cursor,
            revision: page.revision,
        })
    }
    pub fn list_projected(&self, p: ThreadQueueListParams) -> Result<(RawJson, u64)> {
        if p.include_input {
            return Err(storage("projected queue reads must omit native input"));
        }
        let page = self.list_page(p, projected_rows)?;
        let revision = page.revision;
        let result = RawJson::from_serialize(&page).map_err(storage)?;
        Ok((result, revision))
    }
    pub fn transact<T>(
        &self,
        t: &str,
        f: impl FnOnce(&mut Document) -> Result<(T, bool)>,
    ) -> Result<(T, u64, bool)> {
        self.transact_keys(t, &[], false, f)
    }
    fn transact_keys<T>(
        &self,
        t: &str,
        keys: &[String],
        imports: bool,
        f: impl FnOnce(&mut Document) -> Result<(T, bool)>,
    ) -> Result<(T, u64, bool)> {
        self.transact_connection(t, keys, imports, false, |_, document| f(document))
    }
    // Add/delete only change membership. Existing headers retain their fields;
    // changed positions still receive the normal versioned SQL update.
    fn transact_membership_keys<T>(
        &self,
        t: &str,
        keys: &[String],
        imports: bool,
        f: impl FnOnce(&mut Document) -> Result<(T, bool)>,
    ) -> Result<(T, u64, bool)> {
        self.transact_connection(t, keys, imports, true, |_, document| f(document))
    }
    fn transact_connection<T>(
        &self,
        t: &str,
        keys: &[String],
        imports: bool,
        unchanged_existing_headers: bool,
        f: impl FnOnce(&Connection, &mut Document) -> Result<(T, bool)>,
    ) -> Result<(T, u64, bool)> {
        self.with_writer(t, |c| {
        let tx = c
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let (current_rows, before) = transaction_rows(&tx, t)?;
        let mut d = Document {
            revision: revision(&tx, t)?,
            rows: current_rows,
            ..Default::default()
        };
        for key in keys {
            if let Some(r) = load_identity(&tx, t, key)? {
                d.receipts.insert(key.clone(), r);
            }
            if let Some(id) = key.strip_prefix("mutation:") {
                if let Some(r) = load_legacy(&tx, t, id)? {
                    d.legacy_receipts.insert(id.into(), r);
                    let target_key = format!("legacy-target:{id}");
                    if let Some(target) = load_identity(&tx, t, &target_key)? {
                        d.receipts.insert(target_key, target);
                    }
                }
            }
        }
        if imports {
            let mut q = tx
                .prepare("SELECT id FROM queue_migrations WHERE thread_id=?1")
                .map_err(storage)?;
            d.migrations = q
                .query_map([t], |r| r.get(0))
                .map_err(storage)?
                .collect::<rusqlite::Result<_>>()
                .map_err(storage)?;
            let mut q = tx
                .prepare("SELECT id FROM queue_legacy WHERE thread_id=?1")
                .map_err(storage)?;
            let ids = q
                .query_map([t], |r| r.get::<_, String>(0))
                .map_err(storage)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(storage)?;
            for id in ids {
                if let Some(r) = load_legacy(&tx, t, &id)? {
                    d.legacy_receipts.insert(id, r);
                }
            }
        }
        let prior: HashMap<_, _> = d
            .receipts
            .iter()
            .map(|(k, r)| Ok((k.clone(), encode(&r.identity())?)))
            .collect::<Result<_>>()?;
        let (result, changed) = f(&tx, &mut d)?;
        if changed {
            d.revision = d
                .revision
                .checked_add(1)
                .ok_or_else(|| Error::Storage("queue revision exhausted".into()))?;
            let ids: HashSet<_> = d.rows.iter().map(|r| r.id.as_str()).collect();
            for (key, r) in &d.receipts {
                if prior.get(key) == Some(&encode(&r.identity())?) {
                    continue;
                }
                if r.kind == "deleted" {
                    save_proof(
                        &tx,
                        t,
                        r,
                        before
                            .get(&r.queued_submission.id)
                            .map(|entry| entry.payload_id.as_str()),
                        now(),
                    )?;
                } else if matches!(r.kind.as_str(), "started" | "steered" | "cleared") {
                    tx.execute(
                        "DELETE FROM queue_proofs WHERE thread_id=?1 AND row_id=?2",
                        params![t, r.queued_submission.id],
                    )
                    .map_err(storage)?;
                }
                save_identity(&tx, t, key, r)?;
            }
            for id in before.keys() {
                if !ids.contains(id.as_str()) {
                    tx.execute(
                        "DELETE FROM queue_entries WHERE thread_id=?1 AND id=?2",
                        params![t, id],
                    )
                    .map_err(storage)?;
                }
            }
            let mut write_entry = tx.prepare_cached("INSERT INTO queue_entries(thread_id,id,position,payload_id,header) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(thread_id,id) DO UPDATE SET position=excluded.position,payload_id=excluded.payload_id,header=excluded.header").map_err(storage)?;
            let mut update_header = tx.prepare_cached("UPDATE queue_entries SET position=?3,header=?4 WHERE thread_id=?1 AND id=?2 AND payload_id=?5").map_err(storage)?;
            for (position, r) in d.rows.iter_mut().enumerate() {
                let old = before.get(&r.id);
                if r.input_omitted {
                    if let Some(previous) = old {
                        if previous.position == position
                            && (unchanged_existing_headers
                                || stored_header_matches(&previous.header, &header(r)?)?)
                        {
                            continue;
                        }
                    }
                }
                let reused: Option<String> = if old.is_none() {
                    tx.query_row("SELECT payload_id FROM queue_proofs WHERE thread_id=?1 AND row_id=?2 AND payload_id IS NOT NULL AND json_extract(header,'$.inputFingerprint')=?3 ORDER BY created_at DESC LIMIT 1",params![t,r.id,r.input_fingerprint],|r|r.get(0)).optional().map_err(storage)?
                } else {
                    None
                };
                let payload = if let Some(p) = reused {
                    p
                } else if r.input_omitted {
                    old.map(|entry| entry.payload_id.clone())
                        .ok_or_else(|| Error::Storage("missing prepared queue payload".into()))?
                } else {
                    r.input_fingerprint = fingerprint(&r.input)?;
                    let p = uuid::Uuid::new_v4().to_string();
                    tx.execute(
                        "INSERT INTO queue_payloads(id,input) VALUES(?1,?2)",
                        params![p, encode(&r.input)?],
                    )
                    .map_err(storage)?;
                    p
                };
                r.row_version = if old.is_some() {
                    r.row_version
                        .checked_add(1)
                        .ok_or_else(|| Error::Storage("queue row version exhausted".into()))?
                } else {
                    r.row_version.max(1)
                };
                if old.is_some() && r.input_omitted {
                    let updated = update_header
                        .execute(params![t, r.id, position as i64, header(r)?, payload])
                        .map_err(storage)?;
                    if updated != 1 {
                        return Err(storage("queue payload identity changed during update"));
                    }
                } else {
                    write_entry.execute(params![t,r.id,position as i64,payload,header(r)?]).map_err(storage)?;
                }
            }
            drop(update_header);
            drop(write_entry);
            for id in &d.migrations {
                tx.execute(
                    "INSERT OR IGNORE INTO queue_migrations(thread_id,id) VALUES(?1,?2)",
                    params![t, id],
                )
                .map_err(storage)?;
            }
            for r in d.legacy_receipts.values() {
                save_legacy(&tx, t, r)?;
            }
            tx.execute("INSERT INTO queue_threads(thread_id,revision) VALUES(?1,?2) ON CONFLICT(thread_id) DO UPDATE SET revision=excluded.revision",params![t,d.revision]).map_err(storage)?;
            prune(&tx)?;
        }
        self.check_active()?;
        tx.commit().map_err(storage)?;
        Ok((result, d.revision, changed))
        })
    }
    pub fn import(&self, p: ThreadQueueImportParams) -> Result<(u64, bool)> {
        let keys = p
            .queued_submissions
            .iter()
            .map(|r| format!("client:{}", r.client_user_message_id))
            .collect::<Vec<_>>();
        let (_, revision, changed) = self.transact_keys(&p.thread_id, &keys, true, |d| {
            nonempty(&p.migration_id, "migrationId")?;
            if d.migrations.contains(&p.migration_id) {
                return Ok(((), false));
            }
            for i in p.queued_submissions {
                nonempty(&i.client_user_message_id, "clientUserMessageId")?;
                if d.receipts
                    .contains_key(&format!("client:{}", i.client_user_message_id))
                {
                    continue;
                }
                let unknown = i
                    .dispatching
                    .as_ref()
                    .is_some_and(|v| !matches!(v.0, RuntimeJson::Null | RuntimeJson::Bool(false)));
                let r = QueuedSubmission {
                    id: uuid::Uuid::new_v4().to_string(),
                    input_fingerprint: fingerprint(&i.input)?,
                    input: i.input,
                    input_omitted: false,
                    row_version: 1,
                    client_user_message_id: Some(i.client_user_message_id),
                    client_metadata: i.client_metadata,
                    paused_reason: if unknown {
                        Some("admission-unknown".into())
                    } else {
                        i.paused_reason
                    },
                    created_at: i.created_at.unwrap_or_else(now),
                    dispatching: unknown.then(|| QueueDispatch {
                        submission_id: format!("imported-{}", uuid::Uuid::new_v4()),
                        mode: super::wire::TurnSubmitMode::StartIfIdle,
                    }),
                };
                let receipt = d.receipt(r.clone(), "added");
                d.rows.push(r);
                d.remember(receipt, None);
            }
            for r in p.receipts {
                nonempty(&r.mutation_id, "receipt mutationId")?;
                if !matches!(r.kind.as_str(), "addition" | "removal") {
                    return Err(Error::Invalid("invalid imported receipt kind".into()));
                }
                if let Some(old) = d.legacy_receipts.get(&r.mutation_id) {
                    if old.fingerprint != r.fingerprint || old.kind != r.kind {
                        return Err(Error::Invalid(
                            "conflicting imported mutation receipt".into(),
                        ));
                    }
                } else {
                    d.legacy_receipts.insert(r.mutation_id.clone(), r);
                }
            }
            d.migrations.insert(p.migration_id);
            d.revision = d.revision.max(p.minimum_revision);
            Ok(((), true))
        })?;
        Ok((revision, changed))
    }
    pub fn clear_rows(&self, p: ThreadQueueClearParams) -> Result<(ThreadQueueClearResult, bool)> {
        let mut cleared = Vec::new();
        let (_, revision, changed) =
            self.transact_connection(&p.thread_id, &[], false, false, |tx, d| {
                d.check_revision(p.expected_revision)?;
                if p.include_input {
                    for r in &mut d.rows {
                        *r = get_row(tx, &p.thread_id, &r.id, true)?
                            .ok_or_else(|| Error::Storage("missing cleared row".into()))?;
                    }
                }
                cleared = std::mem::take(&mut d.rows);
                for row in &cleared {
                    let receipt = d.receipt(row.clone(), "cleared");
                    d.remember(receipt, None);
                }
                Ok(((), !cleared.is_empty()))
            })?;
        Ok((ThreadQueueClearResult { cleared, revision }, changed))
    }
    pub fn clear(&self, t: &str) -> Result<u64> {
        self.discard(t, None).map(|(r, _)| r)
    }
    pub fn discard(&self, t: &str, expected: Option<u64>) -> Result<(u64, bool)> {
        self.with_writer(t, |c| {
        let tx = c
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let current = revision(&tx, t)?;
        if let Some(expected) = expected {
            if expected != current {
                return Err(Error::Conflict {
                    expected,
                    actual: current,
                });
            }
        }
        let next = current
            .checked_add(1)
            .ok_or_else(|| Error::Storage("queue revision exhausted".into()))?;
        for table in [
            "queue_entries",
            "queue_identities",
            "queue_proofs",
            "queue_legacy",
            "queue_migrations",
        ] {
            tx.execute(&format!("DELETE FROM {table} WHERE thread_id=?1"), [t])
                .map_err(storage)?;
        }
        tx.execute("INSERT INTO queue_threads(thread_id,revision) VALUES(?1,?2) ON CONFLICT(thread_id) DO UPDATE SET revision=excluded.revision",params![t,next]).map_err(storage)?;
        gc(&tx)?;
        self.check_active()?;
        tx.commit().map_err(storage)?;
        Ok((next, true))
        })
    }
    pub fn watch_connection(&self) -> Result<Option<Connection>> {
        self.check_active()?;
        if !self.0.path.exists() {
            return Ok(None);
        }
        let identity = self.file_identity()?;
        let c =
            Connection::open_with_flags(&self.0.path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .map_err(storage)?;
        c.busy_timeout(std::time::Duration::from_secs(1))
            .map_err(storage)?;
        if self.file_identity()? != identity {
            return Err(storage("queue database changed while opening observer"));
        }
        let _ = self.0.identity.set(identity);
        self.check_active()?;
        Ok(Some(c))
    }
    pub fn data_version(c: &Connection) -> Result<i64> {
        c.query_row("PRAGMA data_version", [], |r| r.get(0))
            .map_err(storage)
    }
    pub fn revisions(c: &Connection) -> Result<Vec<(String, u64)>> {
        let mut q=c.prepare("SELECT thread_id,revision FROM queue_threads UNION ALL SELECT thread_id,revision FROM thread_queues_v1").map_err(storage)?;
        let data = q
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .map_err(storage)?
            .collect::<rusqlite::Result<_>>()
            .map_err(storage)?;
        Ok(data)
    }
    pub fn lookup(&self, p: &ThreadQueueAddParams) -> Result<ThreadQueueItemResult> {
        nonempty(&p.client_user_message_id, "clientUserMessageId")?;
        let mut connection = self.ready(&p.thread_id)?;
        let tx = connection.transaction().map_err(storage)?;
        let revision = revision(&tx, &p.thread_id)?;
        let missing = || ThreadQueueItemResult {
            queued_submission: None,
            revision,
            replayed: false,
            receipt: None,
            found: Some(false),
        };
        if let Some(id) = &p.mutation_id {
            if let Some(r) = load_legacy(&tx, &p.thread_id, id)? {
                if r.kind != "addition" || p.mutation_fingerprint.as_deref() != Some(&r.fingerprint)
                {
                    return Err(Error::Invalid(
                        "mutation fingerprint does not match its imported receipt".into(),
                    ));
                }
                return Ok(ThreadQueueItemResult {
                    replayed: true,
                    receipt: r.result,
                    found: Some(true),
                    ..missing()
                });
            }
            if let Some(r) = load_identity(&tx, &p.thread_id, &format!("mutation:{id}"))? {
                validate_add_identity(p, &r)?;
            }
        }
        let Some(r) = load_identity(
            &tx,
            &p.thread_id,
            &format!("client:{}", p.client_user_message_id),
        )?
        else {
            return Ok(missing());
        };
        let row = get_row(&tx, &p.thread_id, &r.queued_submission.id, p.include_input)?;
        Ok(ThreadQueueItemResult {
            queued_submission: row,
            revision,
            replayed: true,
            receipt: Some(r.raw()),
            found: Some(true),
        })
    }
    pub fn add(&self, p: ThreadQueueAddParams) -> Result<(ThreadQueueItemResult, bool)> {
        if p.lookup_only {
            return self.lookup(&p).map(|r| (r, false));
        }
        nonempty(&p.client_user_message_id, "clientUserMessageId")?;
        if p.restore && !p.require_removed_receipt {
            return Err(Error::Invalid(
                "restore requires requireRemovedReceipt=true".into(),
            ));
        }
        if let Some(id) = &p.mutation_id {
            nonempty(id, "mutationId")?;
        }
        let t = p.thread_id.clone();
        let mut keys = vec![format!("client:{}", p.client_user_message_id)];
        if let Some(id) = &p.mutation_id {
            keys.push(format!("mutation:{id}"));
        }
        let ((row_id, replayed, receipt), revision, changed) =
            self.transact_membership_keys(&t, &keys, p.restore, |d| {
                if let Some(r) = p
                    .mutation_id
                    .as_ref()
                    .and_then(|id| d.legacy_receipts.get(id))
                {
                    if r.kind != "addition"
                        || p.mutation_fingerprint.as_deref() != Some(&r.fingerprint)
                    {
                        return Err(Error::Invalid(
                            "mutation fingerprint does not match its imported receipt".into(),
                        ));
                    }
                    if !p.restore {
                        return Ok(((None, true, r.result.clone()), false));
                    }
                }
                let by_mutation = p
                    .mutation_id
                    .as_ref()
                    .and_then(|id| d.receipts.get(&format!("mutation:{id}")))
                    .cloned();
                if let Some(r) = &by_mutation {
                    validate_add_identity(&p, r)?;
                }
                let prior = d
                    .receipts
                    .get(&format!("client:{}", p.client_user_message_id))
                    .cloned()
                    .or(by_mutation)
                    .or_else(|| p.restore.then(|| legacy_removed(d, &p)).flatten());
                let restored = if let Some(r) = prior {
                    validate_birth_input(&p, &r)?;
                    if p.restore && matches!(r.kind.as_str(), "started" | "steered" | "cleared") {
                        return Err(Error::Invalid(
                            "consumed queued input cannot be restored".into(),
                        ));
                    }
                    if !p.restore || r.kind != "deleted" {
                        let id = d
                            .rows
                            .iter()
                            .find(|row| row.id == r.queued_submission.id)
                            .map(|r| r.id.clone());
                        return Ok(((id, true, Some(r.raw())), false));
                    }
                    if !r.proof_available {
                        return Err(Error::Invalid("deletion proof has expired".into()));
                    }
                    validate_restore(&p, &r.queued_submission)?;
                    Some(r.queued_submission)
                } else if p.restore || p.require_removed_receipt {
                    return Err(Error::Invalid(
                        "restore requires a durable deletion receipt".into(),
                    ));
                } else {
                    None
                };
                d.check_revision(p.expected_revision)?;
                if p.before_id.is_some() && p.after_id.is_some() {
                    return Err(Error::Invalid("choose beforeId or afterId".into()));
                }
                let index = if let Some(id) = &p.before_id {
                    d.row_index(id)?
                } else if let Some(id) = &p.after_id {
                    d.row_index(id)? + 1
                } else {
                    d.rows.len()
                };
                let row = if let Some(mut r) = restored {
                    r.row_version = r
                        .row_version
                        .checked_add(1)
                        .ok_or_else(|| Error::Storage("queue row version exhausted".into()))?;
                    r
                } else {
                    QueuedSubmission {
                        id: uuid::Uuid::new_v4().to_string(),
                        input_fingerprint: fingerprint(&p.input)?,
                        input: p.input.clone(),
                        input_omitted: false,
                        row_version: 1,
                        client_user_message_id: Some(p.client_user_message_id.clone()),
                        client_metadata: p.client_metadata.clone(),
                        paused_reason: p.paused_reason.clone(),
                        created_at: p.created_at.unwrap_or_else(now),
                        dispatching: None,
                    }
                };
                check_capacity(d, &row)?;
                let mut r = d.receipt(row.clone(), "added");
                r.fingerprint = p.mutation_fingerprint.clone();
                let id = row.id.clone();
                d.rows.insert(index, row);
                d.remember(r.clone(), p.mutation_id.as_deref());
                Ok(((Some(id), false, Some(r.raw())), true))
            })?;
        let row = row_id
            .map(|id| self.entry(&t, &id, p.include_input))
            .transpose()?
            .flatten();
        Ok((
            ThreadQueueItemResult {
                queued_submission: row,
                revision,
                replayed,
                receipt,
                found: None,
            },
            changed,
        ))
    }
    pub fn delete(&self, p: ThreadQueueDeleteParams) -> Result<(ThreadQueueDeleteResult, bool)> {
        let keys = p
            .mutation_id
            .as_ref()
            .map(|id| vec![format!("mutation:{id}")])
            .unwrap_or_default();
        let ((row, receipt, deleted), revision, changed) =
            self.transact_membership_keys(&p.thread_id, &keys, false, |d| {
                if let Some(r) = p
                    .mutation_id
                    .as_ref()
                    .and_then(|id| d.legacy_receipts.get(id))
                {
                    if r.kind != "removal"
                        || p.mutation_fingerprint.as_deref() != Some(&r.fingerprint)
                    {
                        return Err(Error::Invalid(
                            "mutation fingerprint does not match its imported receipt".into(),
                        ));
                    }
                    if d.receipts
                        .get(&format!("legacy-target:{}", r.mutation_id))
                        .map(|target| target.queued_submission.id.as_str())
                        .is_some_and(|target| target != p.queued_submission_id)
                    {
                        return Err(Error::Invalid(
                            "mutationId belongs to another removed input".into(),
                        ));
                    }
                    return Ok(((None, r.result.clone(), true), false));
                }
                if let Some(r) = p
                    .mutation_id
                    .as_ref()
                    .and_then(|id| d.receipts.get(&format!("mutation:{id}")))
                {
                    if r.kind != "deleted"
                        || (r.queued_submission.id != p.queued_submission_id
                            && r.queued_submission.client_user_message_id.as_deref()
                                != Some(&p.queued_submission_id))
                        || r.fingerprint != p.mutation_fingerprint
                    {
                        return Err(Error::Invalid(
                            "mutationId belongs to another mutation or fingerprint".into(),
                        ));
                    }
                    let row = r.proof_available.then(|| r.queued_submission.clone());
                    return Ok(((row, Some(r.raw()), true), false));
                }
                d.check_revision(p.expected_revision)?;
                let Some(index) = d.rows.iter().position(|r| r.id == p.queued_submission_id) else {
                    return Ok(((None, None, false), false));
                };
                let row = d.rows.remove(index);
                let mut r = d.receipt(row.clone(), "deleted");
                r.fingerprint = p.mutation_fingerprint.clone();
                d.remember(r.clone(), p.mutation_id.as_deref());
                Ok(((Some(row), Some(r.raw()), true), true))
            })?;
        let row = if let Some(mut row) = row {
            if p.include_input && row.input_omitted {
                let c = self.ready(&p.thread_id)?;
                let proof_id = receipt
                    .as_ref()
                    .map(|raw| raw.deserialize::<Identity>().map_err(storage))
                    .transpose()?
                    .map(|identity| identity.id);
                let payload:Option<String>=c.query_row("SELECT payload_id FROM queue_proofs WHERE thread_id=?1 AND id=?2 AND payload_id IS NOT NULL",params![p.thread_id,proof_id],|r|r.get(0)).optional().map_err(storage)?;
                if let Some(payload) = payload {
                    row = hydrate(&c, row, &payload, true)?;
                }
            }
            if !p.include_input {
                row.input.clear();
                row.input_omitted = true;
            }
            Some(row)
        } else {
            None
        };
        Ok((
            ThreadQueueDeleteResult {
                deleted,
                queued_submission: row,
                revision,
                receipt,
                replayed: !changed && deleted,
            },
            changed,
        ))
    }
}
fn validate_add_identity(p: &ThreadQueueAddParams, r: &Receipt) -> Result<()> {
    if r.kind != "added"
        || r.queued_submission.client_user_message_id.as_deref() != Some(&p.client_user_message_id)
        || r.fingerprint != p.mutation_fingerprint
    {
        return Err(Error::Invalid(
            "mutationId belongs to another mutation".into(),
        ));
    }
    validate_birth_input(p, r)?;
    Ok(())
}
fn validate_birth_input(p: &ThreadQueueAddParams, r: &Receipt) -> Result<()> {
    if !p.lookup_only && !p.restore {
        let expected = r
            .birth_input_fingerprint
            .as_deref()
            .unwrap_or(&r.queued_submission.input_fingerprint);
        if fingerprint(&p.input)? != expected {
            return Err(Error::Invalid(
                "clientUserMessageId belongs to different native input".into(),
            ));
        }
    }
    Ok(())
}
fn save_legacy(c: &Connection, t: &str, r: &QueueImportedReceipt) -> Result<()> {
    c.execute("INSERT OR IGNORE INTO queue_legacy(thread_id,id,kind,fingerprint,created_at) VALUES(?1,?2,?3,?4,?5)",params![t,r.mutation_id,r.kind,r.fingerprint,r.created_at]).map_err(storage)?;
    if let Some(result) = &r.result {
        let value = result.to_runtime().map_err(storage)?;
        if r.kind == "removal" {
            if let Some(id) = value
                .0
                .get("removed")
                .and_then(|row| row.get("id"))
                .and_then(RuntimeJson::as_string)
                .and_then(|id| id.try_to_utf8().ok())
            {
                let target = Receipt {
                    id: format!("legacy-{}", r.mutation_id),
                    kind: "legacy-removal".into(),
                    queued_submission: QueuedSubmission {
                        id,
                        input: vec![],
                        input_omitted: true,
                        input_fingerprint: fingerprint(&[])?,
                        row_version: 0,
                        client_user_message_id: None,
                        client_metadata: None,
                        paused_reason: None,
                        created_at: 0,
                        dispatching: None,
                    },
                    revision: 0,
                    submission_id: None,
                    turn_id: None,
                    fingerprint: Some(r.fingerprint.clone()),
                    birth_input_fingerprint: None,
                    proof_available: false,
                };
                save_identity(c, t, &format!("legacy-target:{}", r.mutation_id), &target)?;
            }
        }
        let text = encode(&value)?;
        let bytes = text.len() as u64;
        if bytes > PROOF_BYTES {
            return Err(Error::Invalid(
                "imported UI proof exceeds the 16 MiB undo budget".into(),
            ));
        }
        c.execute("INSERT OR IGNORE INTO queue_proofs(id,thread_id,row_id,header,payload_id,bytes,created_at) VALUES(?1,?2,?3,?4,NULL,?5,?6)",params![format!("legacy:{t}:{}",r.mutation_id),t,format!("legacy:{}",r.mutation_id),text,bytes,r.created_at]).map_err(storage)?;
    }
    Ok(())
}
#[cfg(test)]
#[path = "queue_store_tests.rs"]
mod tests;
