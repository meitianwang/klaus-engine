//! Durable thread metadata authority, independent of rollout-derived indexes.
//!
//! Rollout headers seed absent rows only. Once present, the complete metadata
//! row is authoritative even when a header or its derived cache is stale.

use std::io;
use std::path::Path;
use std::time::Duration;

use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

use crate::runtime_values::{
    runtime_codec, CanonicalRuntimeJsonUtf8, RuntimeJson, RuntimeNumber, RuntimeObject,
    RuntimeString,
};
use crate::schemas::rollout::{SessionMeta, SessionRole};
use crate::store::session_store::ThreadSummary;

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS thread_metadata_v1 (
    id BLOB PRIMARY KEY NOT NULL,
    meta BLOB NOT NULL,
    host_metadata BLOB
) WITHOUT ROWID;";

/// A synchronous SQLite authority. Callers own the database path and schedule
/// blocking work; this module never discovers a home directory or opens rollouts.
#[derive(Debug)]
pub struct ThreadMetadataDb {
    connection: Connection,
}

impl ThreadMetadataDb {
    pub fn open(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(path).map_err(sql_error)?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(sql_error)?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(sql_error)?;
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(sql_error)?;
        connection.execute_batch(SCHEMA).map_err(sql_error)?;
        Ok(Self { connection })
    }

    pub fn get(&self, id: &RuntimeString) -> io::Result<Option<SessionMeta>> {
        read_meta(&self.connection, id)
    }

    /// Import a legacy header without replacing any existing authority row.
    /// Return the current authority, which may differ from the supplied header.
    pub fn seed_if_absent(&self, meta: &SessionMeta) -> io::Result<SessionMeta> {
        let id = encode_key(&meta.id)?;
        let (body, host_metadata) = encode_meta(meta)?;
        self.connection
            .execute(
                "INSERT INTO thread_metadata_v1 (id, meta, host_metadata) VALUES (?1, ?2, ?3)
                 ON CONFLICT(id) DO NOTHING",
                params![id, body, host_metadata],
            )
            .map_err(sql_error)?;
        self.get(&meta.id)?.ok_or_else(missing_thread)
    }

    /// Seed an unseen legacy thread, then replace its complete header metadata.
    pub fn overlay_meta(&self, meta: &mut SessionMeta) -> io::Result<()> {
        *meta = self.seed_if_absent(meta)?;
        Ok(())
    }

    /// Overlay only metadata fields. Activity time and archive location belong
    /// to the rollout catalog. A summary alone cannot seed a complete header.
    pub fn overlay_summary(&self, summary: &mut ThreadSummary) -> io::Result<()> {
        if let Some(meta) = self.get(&summary.id)? {
            summary.created_at = meta.created_at;
            summary.cwd = meta.cwd;
            summary.model = meta.model;
            summary.title = meta.title;
            summary.role = meta.role.unwrap_or(SessionRole::Main);
            summary.parent_thread_id = meta.parent_thread_id;
            summary.forked_from_id = meta.forked_from_id;
            summary.host_metadata = meta.host_metadata;
        }
        Ok(())
    }

    /// Replace the title after acquiring the SQLite write lease. An absent
    /// title clears it; all other metadata comes from the latest committed row.
    pub fn update_title(
        &mut self,
        id: &RuntimeString,
        title: Option<RuntimeString>,
    ) -> io::Result<()> {
        self.update(id, |meta| meta.title = title)
    }

    /// Replace the opaque host value, preserving explicit null versus absence.
    pub fn update_host_metadata(
        &mut self,
        id: &RuntimeString,
        host_metadata: Option<RuntimeJson>,
    ) -> io::Result<()> {
        self.update(id, |meta| meta.host_metadata = host_metadata)
    }

    fn update(
        &mut self,
        id: &RuntimeString,
        edit: impl FnOnce(&mut SessionMeta),
    ) -> io::Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let mut meta = read_meta(&transaction, id)?.ok_or_else(missing_thread)?;
        edit(&mut meta);
        let (body, host_metadata) = encode_meta(&meta)?;
        transaction
            .execute(
                "UPDATE thread_metadata_v1 SET meta=?2, host_metadata=?3 WHERE id=?1",
                params![encode_key(id)?, body, host_metadata],
            )
            .map_err(sql_error)?;
        transaction.commit().map_err(sql_error)
    }

    /// Remove the authority row before an identifier is reused. Missing rows
    /// are harmless; rollout deletion and lifecycle coordination are caller-owned.
    pub fn delete(&self, id: &RuntimeString) -> io::Result<()> {
        self.connection
            .execute(
                "DELETE FROM thread_metadata_v1 WHERE id=?1",
                params![encode_key(id)?],
            )
            .map_err(sql_error)?;
        Ok(())
    }

    /// Atomically change both the key and the complete metadata's identifier.
    /// A destination row is never replaced, including a stale retained row.
    pub fn rename(&mut self, old: &RuntimeString, new: &RuntimeString) -> io::Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let mut meta = read_meta(&transaction, old)?.ok_or_else(missing_thread)?;
        if old == new {
            return transaction.commit().map_err(sql_error);
        }
        if read_meta(&transaction, new)?.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "thread metadata destination already exists",
            ));
        }
        meta.id = new.clone();
        let (body, host_metadata) = encode_meta(&meta)?;
        transaction
            .execute(
                "UPDATE thread_metadata_v1 SET id=?2, meta=?3, host_metadata=?4 WHERE id=?1",
                params![encode_key(old)?, encode_key(new)?, body, host_metadata],
            )
            .map_err(sql_error)?;
        transaction.commit().map_err(sql_error)
    }
}

fn encode_key(id: &RuntimeString) -> io::Result<Vec<u8>> {
    runtime_codec::encode_bytes(id).map_err(invalid_data)
}

fn encode_meta(meta: &SessionMeta) -> io::Result<(Vec<u8>, Option<Vec<u8>>)> {
    // The opaque host tree has a separate exact representation: JSON text alone
    // cannot retain negative zero, non-finite numbers, or optional explicit null.
    let mut header = meta.clone();
    header.host_metadata = None;
    let body = runtime_codec::encode_bytes(&header).map_err(invalid_data)?;
    let host_metadata = meta
        .host_metadata
        .as_ref()
        .map(|value| runtime_codec::encode_bytes(&pack_host(value)).map_err(invalid_data))
        .transpose()?;
    Ok((body, host_metadata))
}

fn read_meta(connection: &Connection, id: &RuntimeString) -> io::Result<Option<SessionMeta>> {
    let row: Option<(Vec<u8>, Option<Vec<u8>>)> = connection
        .query_row(
            "SELECT meta, host_metadata FROM thread_metadata_v1 WHERE id=?1",
            params![encode_key(id)?],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(sql_error)?;
    row.map(|(body, host_metadata)| {
        let value = CanonicalRuntimeJsonUtf8::decode(&body).map_err(invalid_data)?;
        let mut meta: SessionMeta = runtime_codec::decode_value(&value).map_err(invalid_data)?;
        if &meta.id != id {
            return Err(invalid_data(
                "thread metadata key does not match its record",
            ));
        }
        meta.host_metadata = host_metadata
            .map(|bytes| {
                let value = CanonicalRuntimeJsonUtf8::decode(&bytes).map_err(invalid_data)?;
                let value: RuntimeJson =
                    runtime_codec::decode_value(&value).map_err(invalid_data)?;
                unpack_host(&value)
            })
            .transpose()?;
        Ok(meta)
    })
    .transpose()
}

// Every node is tagged, so host objects and strings cannot collide with number
// encodings. Object entries remain in runtime order and keys stay exact UTF-16.
fn pack_host(value: &RuntimeJson) -> RuntimeJson {
    let (tag, payload) = match value {
        RuntimeJson::Null => return RuntimeJson::Array(vec![RuntimeJson::from("null")]),
        RuntimeJson::Bool(value) => ("bool", RuntimeJson::Bool(*value)),
        RuntimeJson::Number(value) => (
            "number",
            RuntimeJson::from(format!("{:016x}", value.to_bits())),
        ),
        RuntimeJson::String(value) => ("string", RuntimeJson::String(value.clone())),
        RuntimeJson::Array(values) => (
            "array",
            RuntimeJson::Array(values.iter().map(pack_host).collect()),
        ),
        RuntimeJson::Object(values) => (
            "object",
            RuntimeJson::Array(
                values
                    .entries()
                    .map(|(key, value)| {
                        RuntimeJson::Array(vec![RuntimeJson::String(key), pack_host(value)])
                    })
                    .collect(),
            ),
        ),
    };
    RuntimeJson::Array(vec![RuntimeJson::from(tag), payload])
}

fn unpack_host(value: &RuntimeJson) -> io::Result<RuntimeJson> {
    let invalid = || invalid_data("invalid exact host metadata encoding");
    let values = value.as_array().ok_or_else(invalid)?;
    match values.as_slice() {
        [tag] if tag.as_str().is_some_and(|tag| tag == "null") => Ok(RuntimeJson::Null),
        [tag, value] => {
            let tag = tag.as_str().ok_or_else(invalid)?;
            if tag == "bool" {
                return value.as_bool().map(RuntimeJson::Bool).ok_or_else(invalid);
            }
            if tag == "number" {
                let text = value.as_str().ok_or_else(invalid)?;
                let text = text.try_to_utf8().map_err(invalid_data)?;
                if text.len() != 16 {
                    return Err(invalid());
                }
                let bits = u64::from_str_radix(&text, 16).map_err(invalid_data)?;
                return Ok(RuntimeJson::Number(RuntimeNumber::from_f64(
                    f64::from_bits(bits),
                )));
            }
            if tag == "string" {
                return value
                    .as_str()
                    .cloned()
                    .map(RuntimeJson::String)
                    .ok_or_else(invalid);
            }
            let entries = value.as_array().ok_or_else(invalid)?;
            if tag == "array" {
                return entries
                    .iter()
                    .map(unpack_host)
                    .collect::<io::Result<Vec<_>>>()
                    .map(RuntimeJson::Array);
            }
            if tag == "object" {
                let mut object = RuntimeObject::new();
                for entry in entries {
                    let pair = entry.as_array().ok_or_else(invalid)?;
                    let [key, value] = pair.as_slice() else {
                        return Err(invalid());
                    };
                    let key = key.as_str().ok_or_else(invalid)?.clone();
                    if object.insert(key, unpack_host(value)?).is_some() {
                        return Err(invalid());
                    }
                }
                return Ok(RuntimeJson::Object(object));
            }
            Err(invalid())
        }
        _ => Err(invalid()),
    }
}

fn missing_thread() -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, "thread metadata not found")
}

fn invalid_data(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

fn sql_error(error: rusqlite::Error) -> io::Error {
    use rusqlite::ErrorCode;
    let kind = match error.sqlite_error_code() {
        Some(
            ErrorCode::DatabaseCorrupt
            | ErrorCode::NotADatabase
            | ErrorCode::TypeMismatch
            | ErrorCode::ConstraintViolation,
        ) => io::ErrorKind::InvalidData,
        Some(
            ErrorCode::PermissionDenied
            | ErrorCode::ReadOnly
            | ErrorCode::AuthorizationForStatementDenied,
        ) => io::ErrorKind::PermissionDenied,
        Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked) => io::ErrorKind::WouldBlock,
        _ => match &error {
            rusqlite::Error::InvalidColumnType(..)
            | rusqlite::Error::IntegralValueOutOfRange(..) => io::ErrorKind::InvalidData,
            _ => io::ErrorKind::Other,
        },
    };
    io::Error::new(kind, error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::rollout::GitSnapshot;

    fn meta(id: &str) -> SessionMeta {
        SessionMeta {
            id: RuntimeString::from(id),
            created_at: 42,
            cwd: RuntimeString::from("/project"),
            model: RuntimeString::from("model"),
            title: Some(RuntimeString::from("original title")),
            originator: Some(RuntimeString::from("test")),
            cli_version: Some(RuntimeString::from("1")),
            source: Some(RuntimeString::from("interactive")),
            role: Some(SessionRole::SideTask),
            parent_thread_id: Some(RuntimeString::from("parent")),
            forked_from_id: Some(RuntimeString::from("fork")),
            git: Some(GitSnapshot {
                commit: Some(RuntimeString::from("commit")),
                branch: Some(RuntimeString::from("branch")),
                origin_url: Some(RuntimeString::from("origin")),
            }),
            host_metadata: None,
        }
    }

    fn summary(id: &str) -> ThreadSummary {
        ThreadSummary {
            id: RuntimeString::from(id),
            created_at: 1,
            updated_at: 999,
            cwd: RuntimeString::from("/stale"),
            model: RuntimeString::from("stale"),
            title: None,
            role: SessionRole::Main,
            parent_thread_id: None,
            forked_from_id: None,
            archived: Some(true),
            host_metadata: None,
        }
    }

    #[test]
    fn cold_seed_never_replaces_existing_authority_and_overlay_replaces_full_meta() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("metadata.sqlite3");
        let authority = meta("thread");
        let db = ThreadMetadataDb::open(&path).unwrap();
        assert_eq!(db.seed_if_absent(&authority).unwrap(), authority);
        let mut stale = meta("thread");
        stale.created_at = 0;
        stale.cwd = RuntimeString::from("stale");
        stale.model = RuntimeString::from("stale");
        stale.title = None;
        stale.originator = None;
        stale.cli_version = None;
        stale.source = None;
        stale.role = None;
        stale.parent_thread_id = None;
        stale.forked_from_id = None;
        stale.git = None;
        db.overlay_meta(&mut stale).unwrap();
        assert_eq!(stale, authority);
        drop(db);
        let reopened = ThreadMetadataDb::open(&path).unwrap();
        assert_eq!(reopened.get(&authority.id).unwrap(), Some(authority));
    }

    #[test]
    fn summary_overlay_uses_authority_and_retains_activity_and_archive_fields() {
        let dir = tempfile::tempdir().unwrap();
        let db = ThreadMetadataDb::open(&dir.path().join("metadata.sqlite3")).unwrap();
        let mut expected = meta("thread");
        expected.host_metadata = Some(RuntimeJson::from("host"));
        db.seed_if_absent(&expected).unwrap();
        let mut row = summary("thread");
        db.overlay_summary(&mut row).unwrap();
        assert_eq!(row.created_at, expected.created_at);
        assert_eq!(row.cwd, expected.cwd);
        assert_eq!(row.model, expected.model);
        assert_eq!(row.title, expected.title);
        assert_eq!(row.role, SessionRole::SideTask);
        assert_eq!(row.parent_thread_id, expected.parent_thread_id);
        assert_eq!(row.forked_from_id, expected.forked_from_id);
        assert_eq!(row.host_metadata, expected.host_metadata);
        assert_eq!(row.updated_at, 999);
        assert_eq!(row.archived, Some(true));
        let mut absent = summary("absent");
        let before = absent.clone();
        db.overlay_summary(&mut absent).unwrap();
        assert_eq!(absent, before);
        assert_eq!(db.get(&absent.id).unwrap(), None);
    }

    #[test]
    fn durable_updates_preserve_exact_strings_host_values_and_other_connection_edits() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("metadata.sqlite3");
        let mut db = ThreadMetadataDb::open(&path).unwrap();
        let mut expected = meta("thread");
        expected.id = RuntimeString::from_utf16(vec![0xd800, 0x61]);
        expected.cwd = RuntimeString::from_utf16(vec![0xdc00]);
        db.seed_if_absent(&expected).unwrap();
        let mut other = ThreadMetadataDb::open(&path).unwrap();
        expected.title = Some(RuntimeString::from_utf16(vec![0xd800, 0x00, 0xdc00]));
        other
            .update_title(&expected.id, expected.title.clone())
            .unwrap();
        let mut host = RuntimeObject::new();
        host.insert(
            RuntimeString::from_utf16(vec![0xd800]),
            RuntimeJson::String(RuntimeString::from_utf16(vec![0xdc00, 0x00])),
        );
        host.insert(
            "numbers",
            RuntimeJson::Array(vec![
                RuntimeJson::from(-0.0),
                RuntimeJson::from(f64::NAN),
                RuntimeJson::from(f64::INFINITY),
                RuntimeJson::from(f64::NEG_INFINITY),
                RuntimeJson::from(1.5),
            ]),
        );
        host.insert("string", RuntimeJson::from("number"));
        host.insert("null", RuntimeJson::Null);
        host.insert("bool", RuntimeJson::Bool(true));
        expected.host_metadata = Some(RuntimeJson::Object(host));
        db.update_host_metadata(&expected.id, expected.host_metadata.clone())
            .unwrap();
        drop(db);
        drop(other);
        let mut db = ThreadMetadataDb::open(&path).unwrap();
        assert_eq!(db.get(&expected.id).unwrap(), Some(expected.clone()));
        db.update_host_metadata(&expected.id, Some(RuntimeJson::Null))
            .unwrap();
        assert_eq!(
            db.get(&expected.id).unwrap().unwrap().host_metadata,
            Some(RuntimeJson::Null)
        );
        db.update_host_metadata(&expected.id, None).unwrap();
        db.update_title(&expected.id, None).unwrap();
        expected.title = None;
        expected.host_metadata = None;
        assert_eq!(db.get(&expected.id).unwrap(), Some(expected.clone()));
        let mut stale = expected.clone();
        stale.title = Some(RuntimeString::from("stale title"));
        stale.host_metadata = Some(RuntimeJson::from("stale host"));
        db.overlay_meta(&mut stale).unwrap();
        assert_eq!(stale, expected);
    }

    #[test]
    fn rename_moves_complete_authority_and_collision_rolls_back() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = ThreadMetadataDb::open(&dir.path().join("metadata.sqlite3")).unwrap();
        let mut original = meta("old");
        original.host_metadata = Some(RuntimeJson::Null);
        db.seed_if_absent(&original).unwrap();
        let occupied = meta("occupied");
        db.seed_if_absent(&occupied).unwrap();
        assert_eq!(
            db.rename(&original.id, &occupied.id).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        assert_eq!(db.get(&original.id).unwrap(), Some(original.clone()));
        assert_eq!(db.get(&occupied.id).unwrap(), Some(occupied));
        let old = original.id.clone();
        let new = RuntimeString::from("new");
        db.rename(&old, &new).unwrap();
        original.id = new;
        assert_eq!(db.get(&old).unwrap(), None);
        assert_eq!(db.get(&original.id).unwrap(), Some(original.clone()));
        db.rename(&original.id, &original.id).unwrap();
        db.delete(&original.id).unwrap();
        db.delete(&original.id).unwrap();
        assert_eq!(db.get(&original.id).unwrap(), None);
        let replacement = meta("new");
        assert_eq!(db.seed_if_absent(&replacement).unwrap(), replacement);
    }

    #[test]
    fn mutations_require_existing_authority_and_corruption_does_not_fall_back() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = ThreadMetadataDb::open(&dir.path().join("metadata.sqlite3")).unwrap();
        let id = RuntimeString::from("missing");
        assert_eq!(
            db.update_title(&id, None).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            db.update_host_metadata(&id, None).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            db.rename(&id, &id).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        let original = meta("thread");
        db.seed_if_absent(&original).unwrap();
        db.connection
            .execute(
                "UPDATE thread_metadata_v1 SET meta=?2 WHERE id=?1",
                params![encode_key(&original.id).unwrap(), b"invalid".as_slice()],
            )
            .unwrap();
        assert_eq!(
            db.seed_if_absent(&original).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}
