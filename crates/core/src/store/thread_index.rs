//! Durable, keyed transcript projection. Rollout byte positions are recovery
//! checkpoints; page readers consume committed turn and item payloads.
mod load;
mod pipeline;
mod presentation;
mod projection;
mod scan;
#[cfg(test)]
mod tests;

use crate::runtime_values::RuntimeString;
use crate::schemas::message::{CompactBoundaryMessage, Message};
use crate::schemas::rollout::RolloutItem;
use crate::store::rollout::parse_rollout_line;
use crate::store::thread_history::{project_thread_history, ThreadHistory};
use rusqlite::{params, Connection, OptionalExtension};
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::Path;

/// The same refusal budget used by the message-page facade.
pub const MAX_ITEMS_PAGE_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnShell {
    pub id: String,
    pub ord: i64,
    pub ordinal: i64,
    pub byte_offset: i64,
    pub byte_len: i64,
    pub status: Option<String>,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    pub duration_ms: Option<i64>,
    pub error: Option<String>,
    pub error_kind: Option<String>,
    pub final_message_id: Option<String>,
    pub cancelled_tools: Vec<String>,
    pub first_user_item_id: Option<String>,
    pub final_agent_item_id: Option<String>,
    pub items_view: String,
    pub updated_at_ordinal: i64,
}
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PageEnvelope {
    pub order: String,
    pub limit: usize,
    pub next_cursor: Option<String>,
    pub has_more: bool,
    pub backwards_cursor: Option<String>,
}
#[derive(Debug, Clone)]
pub struct ThreadPageSnapshot {
    pub history: ThreadHistory,
    /// Display dependencies for returned users, outside raw item pagination.
    pub presentation_records: Vec<crate::runtime_values::RuntimeJson>,
    pub page: PageEnvelope,
    pub shells: Vec<TurnShell>,
    pub file_changes: crate::engine::session::SessionFileChanges,
}

/// Controls shared by turn and message windows. Ascending cursors are inclusive;
/// descending cursors name the first row excluded from the older page.
#[derive(Debug, Clone, Copy)]
pub struct MessageWindow<'a> {
    pub cursor: Option<&'a str>,
    pub limit: usize,
    pub desc: bool,
}

pub struct TurnIndexDb {
    conn: Connection,
    // Serialize materializers and explicit compatibility imports. Ordinary
    // page readers use SQLite snapshots without taking this writer lease.
    _lock: std::fs::File,
}
struct ProjectionLease(std::fs::File);
impl Drop for ProjectionLease {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.0);
    }
}

#[derive(Debug)]
struct Record {
    turn: String,
    message_id: Option<String>,
}

impl TurnIndexDb {
    pub fn open(path: &str) -> io::Result<Self> {
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(format!("{path}.lock"))?;
        let conn = Connection::open(path).map_err(sql_error)?;
        conn.busy_timeout(std::time::Duration::from_millis(3000))
            .map_err(sql_error)?;
        let journal: String = conn
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .map_err(sql_error)?;
        if !journal.eq_ignore_ascii_case("wal") {
            conn.pragma_update(None, "journal_mode", "WAL")
                .map_err(sql_error)?;
        }
        conn.pragma_update(None, "synchronous", "FULL")
            .map_err(sql_error)?;
        let version: i64 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .map_err(sql_error)?;
        if version < 4 {
            // Upgrade derived caches without changing the source rollout. Established
            // readers skip DDL so a live projection transaction cannot block opening.
            conn.execute_batch("BEGIN IMMEDIATE").map_err(sql_error)?;
            conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS page_epochs_v1 (thread TEXT PRIMARY KEY, epoch TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS page_threads_v2 (
                id TEXT PRIMARY KEY, path TEXT NOT NULL, indexed_len INTEGER NOT NULL,
                stamp INTEGER NOT NULL, newline INTEGER NOT NULL,
                record_count INTEGER NOT NULL, turn_count INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS page_turns_v2 (
                thread TEXT NOT NULL, id TEXT NOT NULL, offset INTEGER NOT NULL,
                PRIMARY KEY(thread,id)
             );
             CREATE INDEX IF NOT EXISTS page_turn_order_v2 ON page_turns_v2(thread,offset);
             CREATE TABLE IF NOT EXISTS page_records_v2 (
                thread TEXT NOT NULL, offset INTEGER NOT NULL, len INTEGER NOT NULL,
                position INTEGER NOT NULL, turn TEXT NOT NULL, kind TEXT NOT NULL,
                message_id TEXT, message_type TEXT, tool TEXT, event TEXT, event_turn TEXT,
                boundary INTEGER, retained INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY(thread,offset)
             );
             CREATE INDEX IF NOT EXISTS page_record_turn_v2 ON page_records_v2(thread,turn,retained,offset);
             CREATE INDEX IF NOT EXISTS page_record_message_v2 ON page_records_v2(thread,turn,message_id,retained);
             CREATE INDEX IF NOT EXISTS page_record_position_v2 ON page_records_v2(thread,retained,position);
             CREATE INDEX IF NOT EXISTS page_record_eventturn_v2 ON page_records_v2(thread,event_turn,retained);
             CREATE INDEX IF NOT EXISTS page_record_output_v2 ON page_records_v2(thread,tool,event,retained,offset);"
        ).map_err(sql_error)?;
            projection::create_schema(&conn)?;
            conn.pragma_update(None, "user_version", 4)
                .map_err(sql_error)?;
            conn.execute_batch("COMMIT").map_err(sql_error)?;
        }
        if version < 5 {
            presentation::upgrade(&conn)?;
        }
        if version < 6 {
            conn.execute_batch("CREATE INDEX IF NOT EXISTS history_turn_order ON history_turn_projection(thread,ordinal); PRAGMA user_version=6;").map_err(sql_error)?;
        }
        Ok(Self { conn, _lock: lock })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn window(
        &mut self,
        thread: &str,
        path: &str,
        len: u64,
        stamp: i64,
        cursor: Option<&str>,
        limit: usize,
        desc: bool,
        view: &str,
    ) -> io::Result<ThreadPageSnapshot> {
        if !matches!(view, "full" | "summary" | "notLoaded") {
            return Err(invalid_input("unknown itemsView"));
        }
        self.import_if_missing(thread, path, len, stamp)?;
        self.backfill_presentations(thread)?;
        self.read_snapshot(|db| db.window_snapshot(thread, cursor, limit, desc, view))
    }
    fn window_snapshot(
        &self,
        thread: &str,
        cursor: Option<&str>,
        limit: usize,
        desc: bool,
        view: &str,
    ) -> io::Result<ThreadPageSnapshot> {
        let cursor = self.decode_cursor(thread, cursor)?;
        let (ids, start, mut page) = self.turn_window(thread, cursor.as_deref(), limit, desc)?;
        self.encode_page(thread, &mut page)?;
        let mut records = Vec::new();
        for turn in &ids {
            if view == "full" {
                records.extend(self.messages(thread, turn, None)?);
            }
            if view == "summary" {
                records.extend(self.summary_records(thread, turn)?);
            }
        }
        let (history, file_changes) = self.read_projection(thread, &ids, records, false)?;
        let presentation_records = if view == "summary" {
            self.presentation_records(thread, &history, false)?
        } else {
            Vec::new()
        };
        let mut shells = self.shells(thread, &ids, start, &history)?;
        for shell in &mut shells {
            shell.items_view = view.into();
        }
        Ok(ThreadPageSnapshot {
            history,
            presentation_records,
            page,
            shells,
            file_changes,
        })
    }

    /// Select message UUIDs before reading or decoding any message body.
    #[allow(clippy::too_many_arguments)]
    pub fn turn_slice(
        &mut self,
        thread: &str,
        path: &str,
        len: u64,
        stamp: i64,
        turn: &str,
        window: MessageWindow<'_>,
    ) -> io::Result<Option<ThreadPageSnapshot>> {
        self.import_if_missing(thread, path, len, stamp)?;
        self.backfill_presentations(thread)?;
        self.read_snapshot(|db| db.turn_slice_snapshot(thread, turn, window))
    }
    /// Resolve a stable item identity inside the same snapshot used to read its older page.
    #[allow(clippy::too_many_arguments)]
    pub fn turn_slice_before(
        &mut self,
        thread: &str,
        path: &str,
        len: u64,
        stamp: i64,
        turn: &str,
        before_item_id: &str,
        limit: usize,
    ) -> io::Result<Option<ThreadPageSnapshot>> {
        if before_item_id.trim().is_empty() {
            return Err(invalid_input("beforeItemId must be nonempty"));
        }
        self.import_if_missing(thread, path, len, stamp)?;
        self.backfill_presentations(thread)?;
        self.read_snapshot(|db| {
            let cursor = match db.epoch(thread)? {
                Some(epoch) => format!("migration:{epoch}:{before_item_id}"),
                None => before_item_id.to_owned(),
            };
            db.turn_slice_snapshot(
                thread,
                turn,
                MessageWindow {
                    cursor: Some(&cursor),
                    limit,
                    desc: true,
                },
            )
        })
    }
    fn turn_slice_snapshot(
        &self,
        thread: &str,
        turn: &str,
        window: MessageWindow<'_>,
    ) -> io::Result<Option<ThreadPageSnapshot>> {
        let ordinal = self
            .turn_ordinal(thread, turn)?
            .ok_or_else(|| invalid_input("unknown history turn"))?;
        let ord = self.turn_rank(thread, ordinal)?;
        let ids = [turn.to_owned()];
        let total: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM history_items WHERE thread=?1 AND turn=?2",
                params![thread, turn],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        let decoded_cursor = self.decode_cursor(thread, window.cursor)?;
        let anchor = match decoded_cursor.as_deref() {
            Some(cursor) => Some(self.conn.query_row(
                "SELECT rollout_ordinal FROM history_items WHERE thread=?1 AND turn=?2 AND item_id=?3",
                params![thread,turn,cursor], |row| row.get::<_,i64>(0)).optional().map_err(sql_error)?
                .ok_or_else(|| invalid_input("unknown history cursor"))?),
            None => None,
        };
        let start = if let Some(anchor) = anchor {
            self.conn
                .query_row(
                    "SELECT COUNT(*) FROM history_items WHERE thread=?1 AND turn=?2 AND rollout_ordinal<?3",
                    params![thread, turn, anchor],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(sql_error)? as usize
        } else if window.desc {
            total as usize
        } else {
            0
        };
        let (start, end) = if window.desc {
            (start.saturating_sub(window.limit), start)
        } else {
            (
                start,
                start.saturating_add(window.limit).min(total as usize),
            )
        };
        let message_at = |at: usize| -> io::Result<String> {
            self.conn.query_row(
                "SELECT item_id FROM history_items WHERE thread=?1 AND turn=?2 ORDER BY rollout_ordinal LIMIT 1 OFFSET ?3",
                params![thread,turn,at as i64], |row| row.get(0)).map_err(sql_error)
        };
        let has_more = if window.desc {
            start > 0
        } else {
            end < total as usize
        };
        let mut page = PageEnvelope {
            order: if window.desc {
                "newest_first"
            } else {
                "oldest_first"
            }
            .into(),
            limit: window.limit,
            next_cursor: if has_more {
                Some(message_at(if window.desc { start } else { end })?)
            } else {
                None
            },
            has_more,
            backwards_cursor: if window.desc && end < total as usize {
                Some(message_at(end)?)
            } else if !window.desc && start > 0 {
                Some(message_at(start - 1)?)
            } else {
                None
            },
        };
        self.encode_page(thread, &mut page)?;
        let records = self.messages(thread, turn, Some((start, end - start)))?;
        let (history, file_changes) = self.read_projection(thread, &ids, records, true)?;
        let presentation_records = self.presentation_records(thread, &history, true)?;
        let shells = self.shells(thread, &ids, ord, &history)?;
        Ok(Some(ThreadPageSnapshot {
            history,
            presentation_records,
            page,
            shells,
            file_changes,
        }))
    }

    /// Read complete change metadata without paging or materializing message bodies.
    pub fn file_changes(
        &mut self,
        thread: &str,
        path: &str,
        len: u64,
        stamp: i64,
    ) -> io::Result<crate::engine::session::SessionFileChanges> {
        self.import_if_missing(thread, path, len, stamp)?;
        self.read_snapshot(|db| {
            let mut statement = db
                .conn
                .prepare(
                    "SELECT payload FROM history_turn_projection WHERE thread=?1 ORDER BY ordinal",
                )
                .map_err(sql_error)?;
            let mut rows = statement.query(params![thread]).map_err(sql_error)?;
            let mut changes = crate::engine::session::SessionFileChanges::default();
            while let Some(row) = rows.next().map_err(sql_error)? {
                let text: String = row.get(0).map_err(sql_error)?;
                let value = crate::runtime_values::runtime_json::parse(&text.into())
                    .map_err(io::Error::other)?;
                for change in value
                    .get("fileChanges")
                    .and_then(crate::runtime_values::RuntimeJson::as_array)
                    .into_iter()
                    .flatten()
                {
                    let turn: Option<RuntimeString> =
                        crate::runtime_values::runtime_codec::decode_value(
                            change
                                .get("turnId")
                                .ok_or_else(|| io::Error::other("missing file turn"))?,
                        )
                        .map_err(io::Error::other)?;
                    let change: crate::types::events::FileChange =
                        crate::runtime_values::runtime_codec::decode_value(change)
                            .map_err(io::Error::other)?;
                    changes.record_change(turn.as_ref(), &change);
                }
            }
            Ok(changes)
        })
    }

    fn projection_lease(&self) -> io::Result<ProjectionLease> {
        let file = self._lock.try_clone()?;
        fs2::FileExt::lock_exclusive(&file)?;
        Ok(ProjectionLease(file))
    }
    fn try_projection_lease(&self) -> io::Result<ProjectionLease> {
        let file = self._lock.try_clone()?;
        fs2::FileExt::try_lock_exclusive(&file)?;
        Ok(ProjectionLease(file))
    }
    fn read_snapshot<T>(&mut self, read: impl FnOnce(&Self) -> io::Result<T>) -> io::Result<T> {
        self.conn
            .execute_batch("BEGIN DEFERRED")
            .map_err(sql_error)?;
        let result = read(self);
        match result {
            Ok(value) => match self.conn.execute_batch("COMMIT") {
                Ok(()) => Ok(value),
                Err(error) => {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    Err(sql_error(error))
                }
            },
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }
    pub(crate) fn invalidate_all(&mut self) -> io::Result<()> {
        self.conn.execute_batch("BEGIN IMMEDIATE; DELETE FROM history_user_presentations; DELETE FROM history_presentation_backfill; DELETE FROM history_item_chunks; DELETE FROM history_turn_projection; DELETE FROM history_projection_state; DELETE FROM history_rollout_ordinals; DELETE FROM history_control_projection; DELETE FROM history_message_turns; DELETE FROM history_goal_projection; DELETE FROM history_items; DELETE FROM history_legacy_imports; DELETE FROM page_records_v2; DELETE FROM page_turns_v2; DELETE FROM page_threads_v2; DELETE FROM page_epochs_v1; COMMIT;").map_err(sql_error)
    }
    fn epoch(&self, thread: &str) -> io::Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT epoch FROM page_epochs_v1 WHERE thread=?1",
                params![thread],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)
    }
    fn decode_cursor(&self, thread: &str, cursor: Option<&str>) -> io::Result<Option<String>> {
        let Some(cursor) = cursor else {
            return Ok(None);
        };
        match self.epoch(thread)? {
            Some(epoch) => cursor
                .strip_prefix(&format!("migration:{epoch}:"))
                .map(|value| Some(value.to_string()))
                .ok_or_else(|| {
                    invalid_input("history cursor belongs to a previous storage revision")
                }),
            None => Ok(Some(cursor.to_string())),
        }
    }
    fn encode_page(&self, thread: &str, page: &mut PageEnvelope) -> io::Result<()> {
        if let Some(epoch) = self.epoch(thread)? {
            for cursor in [&mut page.next_cursor, &mut page.backwards_cursor] {
                if let Some(value) = cursor.take() {
                    *cursor = Some(format!("migration:{epoch}:{value}"));
                }
            }
        }
        Ok(())
    }
    fn turn_ordinal(&self, thread: &str, turn: &str) -> io::Result<Option<i64>> {
        self.conn
            .query_row(
                "SELECT ordinal FROM history_turn_projection WHERE thread=?1 AND turn=?2",
                params![thread, turn],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)
    }
    fn turn_rank(&self, thread: &str, ordinal: i64) -> io::Result<usize> {
        self.conn
            .query_row(
                "SELECT COUNT(*) FROM history_turn_projection WHERE thread=?1 AND ordinal<?2",
                params![thread, ordinal],
                |row| row.get::<_, i64>(0),
            )
            .map(|rank| rank as usize)
            .map_err(sql_error)
    }
    /// Select only the requested identities and one look-ahead row from the ordinal index.
    fn turn_window(
        &self,
        thread: &str,
        cursor: Option<&str>,
        limit: usize,
        desc: bool,
    ) -> io::Result<(Vec<String>, usize, PageEnvelope)> {
        let anchor = cursor
            .map(|id| {
                self.turn_ordinal(thread, id)?
                    .ok_or_else(|| invalid_input("unknown history cursor"))
            })
            .transpose()?;
        let sql = if desc {
            "SELECT turn,ordinal FROM history_turn_projection WHERE thread=?1 AND ordinal<?2 ORDER BY ordinal DESC LIMIT ?3"
        } else {
            "SELECT turn,ordinal FROM history_turn_projection WHERE thread=?1 AND ordinal>=?2 ORDER BY ordinal ASC LIMIT ?3"
        };
        let mut statement = self.conn.prepare(sql).map_err(sql_error)?;
        let mut rows = statement
            .query_map(
                params![
                    thread,
                    anchor.unwrap_or(if desc { i64::MAX } else { i64::MIN }),
                    limit.saturating_add(1) as i64
                ],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        let has_more = rows.len() > limit;
        let extra = if has_more {
            rows.pop().map(|row| row.0)
        } else {
            None
        };
        if desc {
            rows.reverse();
        }
        let start = rows
            .first()
            .map(|row| self.turn_rank(thread, row.1))
            .transpose()?
            .unwrap_or(0);
        let ids: Vec<String> = rows.into_iter().map(|row| row.0).collect();
        let backwards_cursor = if desc {
            cursor.map(str::to_owned)
        } else if let Some(anchor) = anchor {
            self.conn.query_row("SELECT turn FROM history_turn_projection WHERE thread=?1 AND ordinal<?2 ORDER BY ordinal DESC LIMIT 1",params![thread,anchor],|row|row.get(0)).optional().map_err(sql_error)?
        } else {
            None
        };
        let next_cursor = if has_more {
            if desc {
                ids.first().cloned()
            } else {
                extra
            }
        } else {
            None
        };
        Ok((
            ids,
            start,
            PageEnvelope {
                order: if desc { "newest_first" } else { "oldest_first" }.into(),
                limit,
                next_cursor,
                has_more,
                backwards_cursor,
            },
        ))
    }
    fn messages(
        &self,
        thread: &str,
        turn: &str,
        window: Option<(usize, usize)>,
    ) -> io::Result<Vec<Record>> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT turn,item_id FROM history_items
             WHERE thread=?1 AND turn=?2 ORDER BY rollout_ordinal LIMIT ?3 OFFSET ?4",
            )
            .map_err(sql_error)?;
        let (start, limit) = window.unwrap_or((0, i64::MAX as usize));
        let rows = statement
            .query_map(
                params![thread, turn, limit as i64, start as i64],
                record_from_row,
            )
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error);
        rows
    }
    fn shells(
        &self,
        thread: &str,
        ids: &[String],
        start: usize,
        history: &ThreadHistory,
    ) -> io::Result<Vec<TurnShell>> {
        let index = &history.turn_index;
        ids.iter().enumerate().map(|(local,id)| {
            let (ordinal,offset,len,updated):(i64,i64,i64,i64)=self.conn.query_row("SELECT ordinal,byte_offset,byte_len,updated_ordinal FROM history_turn_projection WHERE thread=?1 AND turn=?2",params![thread,id],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).map_err(sql_error)?;
            let key=RuntimeString::from(id.as_str());
            let text=|value:Option<&RuntimeString>|value.and_then(|value|value.try_to_utf8().ok());
            let first=self.conn.query_row("SELECT item_id FROM history_items WHERE thread=?1 AND turn=?2 AND item_type='user' ORDER BY rollout_ordinal LIMIT 1",params![thread,id],|row|row.get(0)).optional().map_err(sql_error)?;
            let last=self.conn.query_row("SELECT item_id FROM history_items WHERE thread=?1 AND turn=?2 AND item_type='assistant' ORDER BY rollout_ordinal DESC LIMIT 1",params![thread,id],|row|row.get(0)).optional().map_err(sql_error)?;
            Ok(TurnShell {id:id.clone(),ord:(start+local) as i64,ordinal,byte_offset:offset,byte_len:len,
                status:text(index.turn_status.get(&key)),started_at_ms:index.turn_started_at_ms.get(&key).copied(),
                finished_at_ms:index.turn_finished_at_ms.get(&key).copied(),duration_ms:index.turn_duration_ms.get(&key).map(|value|*value as i64),
                error:text(index.turn_errors.get(&key)),error_kind:text(index.turn_error_kinds.get(&key)),
                final_message_id:text(index.turn_final_message.get(&key)),
                cancelled_tools:index.turn_cancelled_tools.get(&key).map(|tools|tools.iter().filter_map(|tool|tool.try_to_utf8().ok()).collect()).unwrap_or_default(),
                first_user_item_id:first,final_agent_item_id:text(index.turn_final_message.get(&key)).or(last),items_view:"full".into(),updated_at_ordinal:updated})
        }).collect()
    }

    fn ensure(
        &mut self,
        thread: &str,
        path: &str,
        len: u64,
        stamp: i64,
        source: &std::fs::File,
    ) -> io::Result<()> {
        let stored: Option<(String, i64, i64, bool,i64,i64)> = self
            .conn
            .query_row(
                "SELECT path,indexed_len,stamp,newline,record_count,turn_count FROM page_threads_v2 WHERE id=?1",
                params![thread],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?,row.get(4)?,row.get(5)?)),
            )
            .optional()
            .map_err(sql_error)?;
        let projected: bool = self
            .conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM history_projection_state WHERE thread=?1)",
                params![thread],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        let rebuild = !projected;
        if projected {
            let indexed = stored
                .as_ref()
                .ok_or_else(|| io::Error::other("history projection control ledger is missing"))?
                .1;
            if indexed > len as i64 {
                return Err(io::Error::other("durable rollout shrank before projection"));
            }
            if indexed == len as i64 {
                return Ok(());
            }
        }
        let from = if rebuild {
            0
        } else {
            stored.as_ref().unwrap().1 as u64
        };
        let initial_ordinal = if rebuild {
            0
        } else {
            self.conn
                .query_row(
                    "SELECT next_rollout_ordinal FROM history_projection_state WHERE thread=?1",
                    params![thread],
                    |row| row.get(0),
                )
                .map_err(sql_error)?
        };
        let source = source.try_clone()?;
        let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        std::thread::scope(|scope| {
            let (sender, receiver) = std::sync::mpsc::sync_channel(pipeline::QUEUED_BATCHES);
            let producer_cancelled = cancelled.clone();
            let producer = scope.spawn(move || {
                pipeline::producer(
                    source,
                    from,
                    len,
                    initial_ordinal,
                    sender,
                    producer_cancelled,
                )
            });
            let mut first = true;
            let mut expected = from;
            let result = (|| {
                while let Ok(message) = receiver.recv() {
                    match message {
                        pipeline::ProjectionMessage::Batch(mut batch) => {
                            if batch.start != expected {
                                return Err(io::Error::other(
                                    "history projection batch is out of order",
                                ));
                            }
                            // Receive and normalize the complete batch before taking the SQLite writer lease.
                            let transaction_started = std::time::Instant::now();
                            self.conn
                                .execute_batch("BEGIN IMMEDIATE")
                                .map_err(sql_error)?;
                            let apply = (|| {
                                if rebuild && first {
                                    self.conn
                                        .execute(
                                            "DELETE FROM page_epochs_v1 WHERE thread=?1",
                                            params![thread],
                                        )
                                        .map_err(sql_error)?;
                                    self.conn
                                        .execute(
                                            "DELETE FROM page_records_v2 WHERE thread=?1",
                                            params![thread],
                                        )
                                        .map_err(sql_error)?;
                                    self.conn
                                        .execute(
                                            "DELETE FROM page_turns_v2 WHERE thread=?1",
                                            params![thread],
                                        )
                                        .map_err(sql_error)?;
                                    self.clear_projection(thread)?;
                                }
                                let checkpoint:i64=self.conn.query_row("SELECT COALESCE((SELECT next_rollout_byte_offset FROM history_projection_state WHERE thread=?1),0)",params![thread],|row|row.get(0)).map_err(sql_error)?;
                                if checkpoint != batch.start as i64 {
                                    return Err(io::Error::other(
                                        "history projection checkpoint changed",
                                    ));
                                }
                                let counts: (i64, i64) = if rebuild && first {
                                    (0, 0)
                                } else {
                                    self.conn.query_row("SELECT record_count,turn_count FROM page_threads_v2 WHERE id=?1",params![thread],|row|Ok((row.get(0)?,row.get(1)?))).optional().map_err(sql_error)?.unwrap_or((0,0))
                                };
                                let stats = self.apply_batch(thread, &mut batch)?;
                                self.conn.execute("INSERT INTO history_projection_state(thread,next_rollout_byte_offset,next_rollout_ordinal) VALUES(?1,?2,?3) ON CONFLICT(thread) DO UPDATE SET next_rollout_byte_offset=excluded.next_rollout_byte_offset,next_rollout_ordinal=excluded.next_rollout_ordinal",params![thread,batch.end as i64,batch.next_ordinal]).map_err(sql_error)?;
                                self.conn.execute("INSERT INTO page_threads_v2(id,path,indexed_len,stamp,newline,record_count,turn_count) VALUES(?1,?2,?3,?4,1,?5,?6) ON CONFLICT(id) DO UPDATE SET path=?2,indexed_len=?3,stamp=?4,newline=1,record_count=excluded.record_count,turn_count=excluded.turn_count",params![thread,path,batch.end as i64,stamp,counts.0+stats.records_added as i64,counts.1+stats.turn_delta]).map_err(sql_error)?;
                                Ok(stats)
                            })();
                            let stats = match apply {
                                Ok(stats) => stats,
                                Err(error) => {
                                    let _ = self.conn.execute_batch("ROLLBACK");
                                    return Err(error);
                                }
                            };
                            if let Err(error) = self.conn.execute_batch("COMMIT") {
                                let _ = self.conn.execute_batch("ROLLBACK");
                                return Err(sql_error(error));
                            }
                            #[cfg(debug_assertions)]
                            if std::env::var_os("KLAUS_HISTORY_PROJECTION_PROFILE").is_some() {
                                eprintln!(
                                    "history_projection_batch {}",
                                    serde_json::json!({"start":batch.start,"end":batch.end,"nextOrdinal":batch.next_ordinal,"steps":batch.steps.len(),"sourceReadUs":batch.source_read_us,"normalizeUs":batch.normalize_us,"recordsUs":stats.records_us,"cleanupUs":stats.refresh.cleanup_us,"metadataUs":stats.refresh.metadata_us,"controlRows":stats.refresh.control_rows,"transactionUs":transaction_started.elapsed().as_micros() as u64})
                                );
                            }
                            #[cfg(not(debug_assertions))]
                            let _ = (
                                stats.records_us,
                                stats.refresh.cleanup_us,
                                stats.refresh.metadata_us,
                                stats.refresh.control_rows,
                                batch.source_read_us,
                                batch.normalize_us,
                                transaction_started,
                            );
                            expected = batch.end;
                            first = false;
                        }
                        pipeline::ProjectionMessage::Complete(end) => {
                            if end != expected {
                                return Err(io::Error::other(
                                    "history projection completion mismatch",
                                ));
                            }
                            return Ok(());
                        }
                        pipeline::ProjectionMessage::Failed(error) => return Err(error),
                    }
                }
                Err(io::Error::other(
                    "history projection producer ended without completion",
                ))
            })();
            cancelled.store(true, std::sync::atomic::Ordering::Release);
            drop(receiver);
            producer
                .join()
                .map_err(|_| io::Error::other("history projection producer panicked"))?;
            result
        })
    }

    /// Compatibility import for saved rollouts predating materialized history.
    /// Once published, normal page reads trust SQLite until an explicit
    /// migration invalidates the projection.
    /// Best-effort catch-up for a published projection whose source grew.
    ///
    /// A read must never wait for the projection writer or fail because the
    /// source moved: it serves the committed pages and only repairs the index
    /// when the writer lease is free. A projection that is behind is a cache
    /// miss, never a reason to hide committed history.
    fn try_catch_up_projection(
        &mut self,
        thread: &str,
        path: &str,
        len: u64,
        stamp: i64,
    ) -> io::Result<()> {
        let indexed: Option<i64> = self
            .conn
            .query_row(
                "SELECT indexed_len FROM page_threads_v2 WHERE id=?1",
                params![thread],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        if indexed.is_none_or(|indexed| indexed >= len as i64) {
            return Ok(());
        }
        let Ok(_lease) = self.try_projection_lease() else {
            // A writer owns the projection; it will publish the new prefix.
            return Ok(());
        };
        let indexed: Option<i64> = self
            .conn
            .query_row(
                "SELECT indexed_len FROM page_threads_v2 WHERE id=?1",
                params![thread],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        if indexed.is_none_or(|indexed| indexed >= len as i64) {
            return Ok(());
        }
        let source = match crate::store::compression::open_seekable(Path::new(path)) {
            Ok(source) => source,
            Err(error) => {
                tracing::warn!(%error, %path, "history projection catch-up could not open source");
                return Ok(());
            }
        };
        if let Err(error) = self.ensure(thread, path, len, stamp, &source) {
            let _ = self.record_projection_failure(thread, &error.to_string());
            tracing::warn!(%error, %thread, "history projection catch-up failed");
        }
        Ok(())
    }

    /// Persist the first error of a failed projection pass together with the
    /// checkpoint it stopped at. These rows survive restarts, so a projection
    /// that keeps failing explains itself instead of leaving only a stderr warn.
    pub(crate) fn record_projection_failure(&self, thread: &str, error: &str) -> io::Result<()> {
        let (offset, ordinal): (i64, i64) = self
            .conn
            .query_row(
                "SELECT next_rollout_byte_offset,next_rollout_ordinal FROM history_projection_state WHERE thread=?1",
                params![thread],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql_error)?
            .unwrap_or((0, 0));
        let at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as i64)
            .unwrap_or(0);
        self.conn
            .execute(
                "INSERT INTO history_projection_failures(thread,at_ms,byte_offset,ordinal,message) VALUES(?1,?2,?3,?4,?5)",
                params![thread, at, offset, ordinal, error],
            )
            .map_err(sql_error)?;
        self.conn
            .execute(
                "DELETE FROM history_projection_failures WHERE thread=?1 AND id NOT IN (SELECT id FROM history_projection_failures WHERE thread=?1 ORDER BY id DESC LIMIT 20)",
                params![thread],
            )
            .map_err(sql_error)?;
        Ok(())
    }

    fn import_if_missing(
        &mut self,
        thread: &str,
        path: &str,
        len: u64,
        stamp: i64,
    ) -> io::Result<()> {
        let exists: bool = self
            .conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM history_projection_state WHERE thread=?1)",
                params![thread],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        let pending: Option<(i64, i64)> = self
            .conn
            .query_row(
                "SELECT source_len,source_stamp FROM history_legacy_imports WHERE thread=?1",
                params![thread],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql_error)?;
        if exists && pending.is_none() {
            self.try_catch_up_projection(thread, path, len, stamp)?;
            return Ok(());
        }
        {
            let _lease = self.projection_lease()?;
            let exists: bool = self
                .conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM history_projection_state WHERE thread=?1)",
                    params![thread],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            let pending: Option<(i64, i64)> = self
                .conn
                .query_row(
                    "SELECT source_len,source_stamp FROM history_legacy_imports WHERE thread=?1",
                    params![thread],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(sql_error)?;
            if exists && pending.is_none() {
                return Ok(());
            }
            let raw = std::fs::metadata(path)?;
            let revision = (
                raw.len() as i64,
                crate::store::jsonl_store::file_stamp(&raw),
            );
            if pending.is_some_and(|previous| previous != revision) {
                self.delete_thread(thread)?;
            }
            self.conn.execute("INSERT INTO history_legacy_imports(thread,source_len,source_stamp) VALUES(?1,?2,?3) ON CONFLICT(thread) DO UPDATE SET source_len=excluded.source_len,source_stamp=excluded.source_stamp",params![thread,revision.0,revision.1]).map_err(sql_error)?;
            let source = crate::store::compression::open_seekable(Path::new(path))?;
            let len = if path.ends_with(".zst") {
                source.metadata()?.len()
            } else {
                len
            };
            if let Err(error) = self.ensure(thread, path, len, stamp, &source) {
                let _ = self.record_projection_failure(thread, &error.to_string());
                return Err(error);
            }
            let after = std::fs::metadata(path)?;
            if (
                after.len() as i64,
                crate::store::jsonl_store::file_stamp(&after),
            ) != revision
            {
                return Err(io::Error::other(
                    "saved history changed during compatibility import",
                ));
            }
            self.conn
                .execute(
                    "DELETE FROM history_legacy_imports WHERE thread=?1",
                    params![thread],
                )
                .map_err(sql_error)?;
        }
        Ok(())
    }

    fn apply_batch(
        &self,
        thread: &str,
        batch: &mut pipeline::Batch,
    ) -> io::Result<projection::ApplyStats> {
        let records_started = std::time::Instant::now();
        let mut rewound = false;
        let mut goals_changed = false;
        let mut records_added = 0usize;
        let mut turn_delta = 0i64;
        let preamble = format!("{thread}-preamble");
        let mut turn = self.conn.query_row("SELECT turn FROM page_records_v2 WHERE thread=?1 AND retained=1 ORDER BY offset DESC LIMIT 1",params![thread],|row| row.get::<_,String>(0)).optional().map_err(sql_error)?.unwrap_or(preamble);
        let mut position: i64 = self.conn.query_row("SELECT COALESCE(MAX(position),-1)+1 FROM page_records_v2 WHERE thread=?1 AND retained=1",params![thread],|row| row.get(0)).map_err(sql_error)?;
        let mut pending = self.conn.query_row(
            "SELECT c.offset FROM page_records_v2 c WHERE c.thread=?1 AND c.retained=1 AND c.kind='turn_context'
             AND NOT EXISTS (SELECT 1 FROM page_records_v2 u WHERE u.thread=c.thread AND u.retained=1 AND u.boundary IS NOT NULL AND u.offset>c.offset)
             ORDER BY c.offset DESC LIMIT 1",
            params![thread],|row|row.get::<_,i64>(0)).optional().map_err(sql_error)?;
        let mut active_context = self.active_context(thread)?;
        let mut affected = std::collections::BTreeSet::new();
        for step in &mut batch.steps {
            let offset = step.offset;
            let len = step.len;
            self.conn.execute("INSERT INTO history_rollout_ordinals(thread,byte_offset,ordinal) VALUES(?1,?2,?3)",params![thread,offset as i64,step.ordinal]).map_err(sql_error)?;
            if let Some(header) = step.header.as_ref() {
                goals_changed |= header.event.as_deref() == Some("goal_completed");
                if header.kind == "rollback" {
                    rewound = true;
                    let count = header.num_turns.unwrap_or(0) as i64;
                    if count > 0 {
                        let cut: Option<i64>=self.conn.query_row("SELECT MIN(boundary) FROM (SELECT boundary FROM page_records_v2 WHERE thread=?1 AND retained=1 AND boundary IS NOT NULL ORDER BY offset DESC LIMIT ?2)",params![thread,count],|row| row.get(0)).map_err(sql_error)?;
                        if let Some(cut) = cut {
                            self.conn.execute("UPDATE page_records_v2 SET retained=0 WHERE thread=?1 AND offset>=?2",params![thread,cut]).map_err(sql_error)?;
                            turn_delta -=
                                self.conn
                                    .execute(
                                        "DELETE FROM page_turns_v2 WHERE thread=?1 AND offset>=?2",
                                        params![thread, cut],
                                    )
                                    .map_err(sql_error)? as i64;
                            turn=self.conn.query_row("SELECT turn FROM page_records_v2 WHERE thread=?1 AND retained=1 ORDER BY offset DESC LIMIT 1",params![thread],|row| row.get(0)).optional().map_err(sql_error)?.unwrap_or_else(|| format!("{thread}-preamble"));
                            position=self.conn.query_row("SELECT COALESCE(MAX(position),-1)+1 FROM page_records_v2 WHERE thread=?1 AND retained=1",params![thread],|row| row.get(0)).map_err(sql_error)?;
                            pending = None;
                        }
                    }
                    active_context = self.active_context(thread)?;
                    let mut statement = self
                        .conn
                        .prepare("SELECT id FROM page_turns_v2 WHERE thread=?1 ORDER BY offset")
                        .map_err(sql_error)?;
                    affected.extend(
                        statement
                            .query_map(params![thread], |row| row.get::<_, String>(0))
                            .map_err(sql_error)?
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(sql_error)?,
                    );
                    continue;
                }
                if header.event.as_deref() == Some("rollout_migration") {
                    if let Some(epoch) = header
                        .payload
                        .as_ref()
                        .and_then(|payload| payload.cursor_epoch.as_ref())
                    {
                        self.conn.execute("INSERT INTO page_epochs_v1(thread,epoch) VALUES(?1,?2) ON CONFLICT(thread) DO UPDATE SET epoch=?2",params![thread,epoch]).map_err(sql_error)?;
                    }
                }
                if header.kind == "turn_context" {
                    active_context = header.ctx.as_ref().and_then(|ctx| ctx.turn_id.clone());
                    if let Some(id) = header.ctx.as_ref().and_then(|ctx| ctx.turn_id.clone()) {
                        turn = id;
                    }
                    pending = Some(offset as i64);
                }
                turn_delta += self
                    .conn
                    .execute(
                        "INSERT OR IGNORE INTO page_turns_v2(thread,id,offset) VALUES(?1,?2,?3)",
                        params![thread, turn, offset as i64],
                    )
                    .map_err(sql_error)? as i64;
                // An undecodable message keeps its byte range and ordinal but
                // contributes no display item. Advancing the checkpoint past it
                // is what keeps one bad record from hiding every later turn.
                let message = if step.skip_item {
                    None
                } else {
                    header.message.as_ref()
                };
                let message_id = if header.kind == "compacted" && !step.skip_item {
                    Some(format!("history-compaction-{position}"))
                } else {
                    message.and_then(|message| message.uuid.clone())
                };
                let boundary = if header.kind == "message"
                    && message.is_some_and(|message| {
                        message.kind.as_deref() == Some("user") && message.is_meta != Some(true)
                    }) {
                    Some(pending.take().unwrap_or(offset as i64))
                } else {
                    None
                };
                let tool = message
                    .and_then(|message| message.tool_use_id.clone())
                    .or_else(|| {
                        header
                            .payload
                            .as_ref()
                            .and_then(|payload| payload.tool_use_id.clone())
                    });
                let event_turn = header.turn_id.as_ref().or_else(|| {
                    header
                        .payload
                        .as_ref()
                        .and_then(|payload| payload.turn_id.as_ref())
                });
                self.conn.execute("INSERT INTO page_records_v2(thread,offset,len,position,turn,kind,message_id,message_type,tool,event,boundary,event_turn) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
                    params![thread,offset as i64,len as i64,position,turn,header.kind,message_id,message.and_then(|message|message.kind.as_ref()),tool,header.event,boundary,event_turn]).map_err(sql_error)?;
                records_added += 1;
                if header.event.as_deref() == Some("goal_submission_bound") {
                    if let (Some(bound), Some(payload)) =
                        (header.turn_id.as_deref(), header.payload.as_ref())
                    {
                        for id in [&payload.message_uuid, &payload.display_uuid]
                            .into_iter()
                            .flatten()
                        {
                            let previous:Option<String>=self.conn.query_row("SELECT turn FROM page_records_v2 WHERE thread=?1 AND message_id=?2 AND retained=1 ORDER BY offset LIMIT 1",params![thread,id],|row|row.get(0)).optional().map_err(sql_error)?;
                            if let Some(previous) = previous {
                                affected.insert(previous);
                            }
                            self.conn.execute("UPDATE page_records_v2 SET turn=?3 WHERE thread=?1 AND message_id=?2 AND retained=1",params![thread,id,bound]).map_err(sql_error)?;
                            self.conn.execute("UPDATE history_item_chunks SET turn=?3 WHERE thread=?1 AND (item_id='message:'||?2 OR item_id='output:'||?2)",params![thread,id,bound]).map_err(sql_error)?;
                            self.conn.execute("INSERT INTO history_message_turns(thread,item_id,turn) VALUES(?1,?2,?3) ON CONFLICT(thread,item_id) DO UPDATE SET turn=excluded.turn",params![thread,id,bound]).map_err(sql_error)?;
                            self.conn.execute("UPDATE history_items SET turn=?3 WHERE thread=?1 AND item_id=?2",params![thread,id,bound]).map_err(sql_error)?;
                        }
                    }
                }
                if let Some(id) = &message_id {
                    if let Some(mapped) = &active_context {
                        self.conn.execute("INSERT INTO history_message_turns(thread,item_id,turn) VALUES(?1,?2,?3) ON CONFLICT(thread,item_id) DO UPDATE SET turn=excluded.turn",params![thread,id,mapped]).map_err(sql_error)?;
                    }
                }
                affected.insert(turn.clone());
                if let Some(target) = event_turn {
                    affected.insert(target.clone());
                }
                self.persist_step(thread, &turn, message_id.as_deref(), step)?;
                position += 1;
            }
        }
        let records_us = records_started.elapsed().as_micros() as u64;
        let refresh = self.refresh_turns(thread, &affected, batch.start, rewound, goals_changed)?;
        Ok(projection::ApplyStats {
            records_us,
            records_added,
            turn_delta,
            refresh,
        })
    }
}

fn record_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Record> {
    Ok(Record {
        turn: row.get(0)?,
        message_id: row.get(1)?,
    })
}

#[cfg(test)]
fn page_window(
    ids: &[String],
    cursor: Option<&str>,
    limit: usize,
    desc: bool,
) -> io::Result<(usize, usize, PageEnvelope)> {
    let anchor = cursor
        .map(|cursor| {
            ids.iter()
                .position(|id| id == cursor)
                .ok_or_else(|| invalid_input("unknown history cursor"))
        })
        .transpose()?;
    let (start, end) = if desc {
        let end = anchor.unwrap_or(ids.len());
        (end.saturating_sub(limit), end)
    } else {
        let start = anchor.unwrap_or(0);
        (start, start.saturating_add(limit).min(ids.len()))
    };
    let has_more = if desc { start > 0 } else { end < ids.len() };
    Ok((
        start,
        end,
        PageEnvelope {
            order: if desc { "newest_first" } else { "oldest_first" }.into(),
            limit,
            next_cursor: has_more.then(|| ids[if desc { start } else { end }].clone()),
            has_more,
            backwards_cursor: if desc && end < ids.len() {
                Some(ids[end].clone())
            } else if !desc && start > 0 {
                Some(ids[start - 1].clone())
            } else {
                None
            },
        },
    ))
}
fn oversized(bytes: u64) -> io::Error {
    invalid_input(&format!(
        "decoded message length too large: found at least {bytes} bytes"
    ))
}
fn invalid_input(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
fn sql_error(error: rusqlite::Error) -> io::Error {
    io::Error::other(error.to_string())
}
