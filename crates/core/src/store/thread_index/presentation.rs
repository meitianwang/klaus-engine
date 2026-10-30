//! Keyed UI dependencies. Raw message membership, limits and cursors stay intact.
use super::*;
use crate::runtime_values::{runtime_codec, runtime_json, RuntimeJson};
use crate::schemas::message::Message;
use std::collections::BTreeMap;

pub(super) fn upgrade(conn: &Connection) -> io::Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE").map_err(sql_error)?;
    let result = (|| {
        let version: i64 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .map_err(sql_error)?;
        if version >= 5 {
            return Ok(());
        }
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS history_user_presentations (
                thread TEXT NOT NULL,target_id TEXT NOT NULL,display_id TEXT NOT NULL,
                turn TEXT NOT NULL,ordinal INTEGER NOT NULL,
                PRIMARY KEY(thread,display_id)
             );
             CREATE INDEX IF NOT EXISTS history_presentation_target
                ON history_user_presentations(thread,target_id,ordinal DESC);
             CREATE INDEX IF NOT EXISTS history_presentation_systems
                ON history_items(thread,item_type,rollout_ordinal);
             CREATE INDEX IF NOT EXISTS history_presentation_item_identity
                ON history_items(thread,item_id);
             CREATE TABLE IF NOT EXISTS history_presentation_backfill (
                thread TEXT PRIMARY KEY,next_ordinal INTEGER NOT NULL,end_ordinal INTEGER NOT NULL
             );
             INSERT OR IGNORE INTO history_presentation_backfill
                SELECT thread,-1,next_rollout_ordinal-1 FROM history_projection_state;",
        )
        .map_err(sql_error)?;
        conn.pragma_update(None, "user_version", 5)
            .map_err(sql_error)?;
        Ok(())
    })();
    match result {
        Ok(()) => conn.execute_batch("COMMIT").map_err(sql_error),
        Err(error) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(error)
        }
    }
}

impl TurnIndexDb {
    /// Index only valid envelopes. An invalid later envelope cannot hide an
    /// earlier valid one, matching the Host's last-valid reducer.
    pub(super) fn index_presentation(
        &self,
        thread: &str,
        turn: &str,
        id: &str,
        ordinal: i64,
        payload: &mut impl Read,
    ) -> io::Result<()> {
        // The payload is already staged by the bounded projection producer.
        // Parse one system record, never unrelated user/image payloads.
        let mut bytes = Vec::new();
        payload
            .take(MAX_ITEMS_PAGE_BYTES * 6 + 64 * 1024 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_ITEMS_PAGE_BYTES * 6 + 64 * 1024 {
            return Err(oversized(MAX_ITEMS_PAGE_BYTES + 1));
        }
        let text = std::str::from_utf8(&bytes).map_err(io::Error::other)?;
        let message = runtime_json::parse(&text.into()).map_err(io::Error::other)?;
        self.conn
            .execute(
                "DELETE FROM history_user_presentations WHERE thread=?1 AND display_id=?2",
                params![thread, id],
            )
            .map_err(sql_error)?;
        if message
            .get("subtype")
            .and_then(RuntimeJson::as_str)
            .is_none_or(|s| s != "user_display")
        {
            return Ok(());
        }
        let Some(text) = message.get("text").and_then(RuntimeJson::as_str) else {
            return Ok(());
        };
        let Ok(envelope) = runtime_json::parse(text) else {
            return Ok(());
        };
        let Some(target) = envelope
            .get("messageUuid")
            .and_then(RuntimeJson::as_str)
            .and_then(|id| id.try_to_utf8().ok())
        else {
            return Ok(());
        };
        if envelope
            .get("localRender")
            .and_then(RuntimeJson::as_object)
            .is_none()
        {
            return Ok(());
        }
        self.conn
            .execute(
                "INSERT INTO history_user_presentations(thread,target_id,display_id,turn,ordinal)
             VALUES(?1,?2,?3,?4,?5)",
                params![thread, target, id, turn, ordinal],
            )
            .map_err(sql_error)?;
        Ok(())
    }

    /// A v4 cache is upgraded once, in resumable batches of existing staged
    /// system records. Its source checkpoint and cursor epoch are untouched.
    pub(super) fn backfill_presentations(&mut self, thread: &str) -> io::Result<()> {
        let pending: bool = self
            .conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM history_presentation_backfill WHERE thread=?1)",
                params![thread],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if !pending {
            return Ok(());
        }
        let _lease = self.projection_lease()?;
        loop {
            self.conn
                .execute_batch("BEGIN IMMEDIATE")
                .map_err(sql_error)?;
            let result: io::Result<bool> = (|| {
                let progress: Option<(i64,i64)> = self.conn.query_row(
                    "SELECT next_ordinal,end_ordinal FROM history_presentation_backfill WHERE thread=?1",
                    params![thread], |row| Ok((row.get(0)?,row.get(1)?)),
                ).optional().map_err(sql_error)?;
                let Some((after, end)) = progress else {
                    return Ok(true);
                };
                let mut statement = self
                    .conn
                    .prepare(
                        "SELECT turn,item_id,rollout_ordinal FROM history_items
                     WHERE thread=?1 AND item_type='system' AND rollout_ordinal>?2
                     AND rollout_ordinal<=?3 ORDER BY rollout_ordinal LIMIT 256",
                    )
                    .map_err(sql_error)?;
                let rows = statement
                    .query_map(params![thread, after, end], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                        ))
                    })
                    .map_err(sql_error)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(sql_error)?;
                drop(statement);
                for (turn, id, ordinal) in &rows {
                    let mut file =
                        self.payload_file(thread, turn, &format!("message:{id}"), true)?;
                    let header =
                        scan::header(&mut BufReader::new(&mut file))?.ok_or_else(|| {
                            io::Error::other(
                                "invalid cached system record during presentation migration",
                            )
                        })?;
                    if header
                        .message
                        .is_some_and(|message| message.subtype.as_deref() == Some("user_display"))
                    {
                        let mut payload =
                            self.payload_file(thread, turn, &format!("message:{id}"), false)?;
                        self.index_presentation(thread, turn, id, *ordinal, &mut payload)?;
                    }
                }
                if let Some((_, _, ordinal)) = rows.last() {
                    self.conn.execute(
                        "UPDATE history_presentation_backfill SET next_ordinal=?2 WHERE thread=?1",
                        params![thread,ordinal],
                    ).map_err(sql_error)?;
                    Ok(false)
                } else {
                    self.conn
                        .execute(
                            "DELETE FROM history_presentation_backfill WHERE thread=?1",
                            params![thread],
                        )
                        .map_err(sql_error)?;
                    Ok(true)
                }
            })();
            match result {
                Ok(done) => {
                    if let Err(error) = self.conn.execute_batch("COMMIT") {
                        let _ = self.conn.execute_batch("ROLLBACK");
                        return Err(sql_error(error));
                    }
                    if done {
                        return Ok(());
                    }
                }
                Err(error) => {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(error);
                }
            }
        }
    }

    pub(super) fn prune_presentations(&self, thread: &str) -> io::Result<()> {
        self.conn.execute(
            "DELETE FROM history_user_presentations AS p WHERE thread=?1 AND
             (NOT EXISTS(SELECT 1 FROM history_items i WHERE i.thread=p.thread AND i.item_id=p.display_id)
             OR NOT EXISTS(SELECT 1 FROM history_items i WHERE i.thread=p.thread AND i.item_id=p.target_id AND i.item_type='user'))",
            params![thread],
        ).map_err(sql_error)?;
        Ok(())
    }

    /// Called inside the same SQLite read snapshot as the selected raw rows.
    pub(super) fn presentation_records(
        &self,
        thread: &str,
        history: &ThreadHistory,
        guarded: bool,
    ) -> io::Result<Vec<RuntimeJson>> {
        let mut selected = BTreeMap::new();
        for message in &history.history {
            let Message::User(user) = message else {
                continue;
            };
            let target = user.uuid.try_to_utf8().map_err(io::Error::other)?;
            let row: Option<(String, String, i64)> = self
                .conn
                .query_row(
                    "SELECT i.turn,p.display_id,p.ordinal FROM history_user_presentations p
                 JOIN history_items i ON i.thread=p.thread AND i.item_id=p.display_id
                 WHERE p.thread=?1 AND p.target_id=?2 ORDER BY p.ordinal DESC LIMIT 1",
                    params![thread, target],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()
                .map_err(sql_error)?;
            if let Some((turn, id, ordinal)) = row {
                selected.insert(
                    ordinal,
                    Record {
                        turn,
                        message_id: Some(id),
                    },
                );
            }
        }
        if selected.is_empty() {
            return Ok(Vec::new());
        }
        let (presentations, _) =
            self.read_projection(thread, &[], selected.into_values().collect(), true)?;
        let rows: Vec<_> = presentations
            .history
            .iter()
            .map(|message| runtime_codec::encode_value(message).map_err(io::Error::other))
            .collect::<io::Result<_>>()?;
        if guarded {
            let mut bytes = 0u64;
            for message in &history.history {
                let mut value = runtime_codec::encode_value(message).map_err(io::Error::other)?;
                crate::store::thread_history::substitute_image_sources(&mut value);
                bytes = bytes.saturating_add(
                    runtime_json::stringify(&value)
                        .try_to_utf8()
                        .map_err(io::Error::other)?
                        .len() as u64,
                );
            }
            for row in &rows {
                bytes = bytes.saturating_add(
                    runtime_json::stringify(row)
                        .try_to_utf8()
                        .map_err(io::Error::other)?
                        .len() as u64,
                );
            }
            if bytes > MAX_ITEMS_PAGE_BYTES {
                return Err(oversized(bytes));
            }
        }
        Ok(rows)
    }
}
