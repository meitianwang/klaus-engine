//! Keyed, materialized display items. The routing tables are replay bookkeeping;
//! readers never dereference their byte ranges into the rollout.
use super::*;
use crate::engine::session::{SessionFileChanges, TurnIndex};
use crate::runtime_values::{
    runtime_codec, runtime_json, RuntimeDecode, RuntimeEncode, RuntimeJson, RuntimeObject,
};
use std::collections::BTreeSet;
use std::io::Write;

const CHUNK_BYTES: usize = 64 * 1024;

#[derive(Default)]
pub(super) struct RefreshStats {
    pub cleanup_us: u64,
    pub metadata_us: u64,
    pub control_rows: usize,
}
#[derive(Default)]
pub(super) struct ApplyStats {
    pub records_us: u64,
    pub records_added: usize,
    pub turn_delta: i64,
    pub refresh: RefreshStats,
}

pub(super) fn create_schema(conn: &Connection) -> io::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS history_projection_state (
            thread TEXT PRIMARY KEY, next_rollout_byte_offset INTEGER NOT NULL,
            next_rollout_ordinal INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS history_item_chunks (
            thread TEXT NOT NULL, turn TEXT NOT NULL, item_id TEXT NOT NULL,
            part INTEGER NOT NULL, rollout_ordinal INTEGER NOT NULL, payload BLOB NOT NULL,
            PRIMARY KEY(thread,turn,item_id,part)
         );
         CREATE TABLE IF NOT EXISTS history_turn_projection (
            thread TEXT NOT NULL, turn TEXT NOT NULL, payload TEXT NOT NULL,
            ordinal INTEGER NOT NULL, byte_offset INTEGER NOT NULL,
            byte_len INTEGER NOT NULL, updated_ordinal INTEGER NOT NULL,
            PRIMARY KEY(thread,turn)
         );
         CREATE TABLE IF NOT EXISTS history_items (
            thread TEXT NOT NULL,turn TEXT NOT NULL,item_id TEXT NOT NULL,
            rollout_ordinal INTEGER NOT NULL,item_type TEXT NOT NULL,
            PRIMARY KEY(thread,turn,item_id)
         );
         CREATE INDEX IF NOT EXISTS history_item_order ON history_items(thread,turn,rollout_ordinal);
         CREATE TABLE IF NOT EXISTS history_legacy_imports (
            thread TEXT PRIMARY KEY,source_len INTEGER NOT NULL,
            source_stamp INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS history_rollout_ordinals (
            thread TEXT NOT NULL, byte_offset INTEGER NOT NULL, ordinal INTEGER NOT NULL,
            PRIMARY KEY(thread,byte_offset)
         );
         CREATE TABLE IF NOT EXISTS history_control_projection (thread TEXT NOT NULL, byte_offset INTEGER NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(thread,byte_offset));
         CREATE TABLE IF NOT EXISTS history_message_turns (thread TEXT NOT NULL,item_id TEXT NOT NULL,turn TEXT NOT NULL,PRIMARY KEY(thread,item_id));
         CREATE INDEX IF NOT EXISTS history_message_turn_order ON history_message_turns(thread,turn,item_id);
         CREATE INDEX IF NOT EXISTS page_record_context_tail_v3 ON page_records_v2(thread,offset DESC) WHERE retained=1 AND kind='turn_context';
         CREATE INDEX IF NOT EXISTS page_record_boundary_tail_v3 ON page_records_v2(thread,offset DESC) WHERE retained=1 AND boundary IS NOT NULL;
         CREATE INDEX IF NOT EXISTS page_record_identity_v3 ON page_records_v2(thread,message_id,retained,turn);
         CREATE INDEX IF NOT EXISTS page_record_event_tail_v3 ON page_records_v2(thread,event_turn,retained,offset DESC);
         CREATE TABLE IF NOT EXISTS history_goal_projection (
            thread TEXT PRIMARY KEY, payload TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS history_projection_failures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            thread TEXT NOT NULL, at_ms INTEGER NOT NULL,
            byte_offset INTEGER NOT NULL, ordinal INTEGER NOT NULL,
            message TEXT NOT NULL
         );
         CREATE INDEX IF NOT EXISTS history_projection_failure_thread
            ON history_projection_failures(thread, id DESC);"
    ).map_err(sql_error)
}

fn json_text(value: &impl RuntimeEncode) -> io::Result<String> {
    runtime_json::stringify(&runtime_codec::encode_value(value).map_err(io::Error::other)?)
        .try_to_utf8()
        .map_err(io::Error::other)
}
fn decode<T: RuntimeDecode>(text: &str) -> io::Result<T> {
    let value = runtime_json::parse(&text.into()).map_err(io::Error::other)?;
    runtime_codec::decode_value(&value).map_err(io::Error::other)
}

fn encode_files(changes: SessionFileChanges) -> io::Result<RuntimeJson> {
    let mut rows = Vec::new();
    for group in changes.turns {
        for file in group.files {
            let mut row = RuntimeObject::new();
            row.insert(
                "turnId",
                runtime_codec::encode_value(&group.turn_id).map_err(io::Error::other)?,
            );
            row.insert(
                "path",
                runtime_codec::encode_value(&file.path).map_err(io::Error::other)?,
            );
            row.insert(
                "kind",
                runtime_codec::encode_value(&file.kind).map_err(io::Error::other)?,
            );
            row.insert(
                "added",
                runtime_codec::encode_value(&file.added).map_err(io::Error::other)?,
            );
            row.insert(
                "removed",
                runtime_codec::encode_value(&file.removed).map_err(io::Error::other)?,
            );
            row.insert(
                "turnDiff",
                runtime_codec::encode_value(&file.turn_diff).map_err(io::Error::other)?,
            );
            rows.push(RuntimeJson::Object(row));
        }
    }
    Ok(RuntimeJson::Array(rows))
}
fn merge_files(
    changes: &mut SessionFileChanges,
    value: &RuntimeJson,
    fallback: Option<&RuntimeString>,
) -> io::Result<()> {
    if let Some(rows) = value.get("fileChanges").and_then(RuntimeJson::as_array) {
        for row in rows {
            let group: Option<RuntimeString> = runtime_codec::decode_value(
                row.get("turnId")
                    .ok_or_else(|| io::Error::other("missing file turn"))?,
            )
            .map_err(io::Error::other)?;
            let change: crate::types::events::FileChange =
                runtime_codec::decode_value(row).map_err(io::Error::other)?;
            changes.record_change(group.as_ref().or(fallback), &change);
        }
    }
    Ok(())
}
fn merge_index(index: &mut TurnIndex, incoming: TurnIndex) {
    for (id, status) in &incoming.turn_status {
        if status != "failed" {
            index.turn_error_details.remove(id);
        }
    }
    for (id, tools) in incoming.turn_cancelled_tools {
        let old = index.turn_cancelled_tools.entry(id).or_default();
        for tool in tools {
            if !old.contains(&tool) {
                old.push(tool);
            }
        }
    }
    macro_rules! extend {($($field:ident),* $(,)?)=>{$(index.$field.extend(incoming.$field);)*};}
    extend!(
        turn_goal_metadata,
        message_turn,
        turn_final_message,
        turn_status,
        turn_started_at_ms,
        turn_finished_at_ms,
        turn_duration_ms,
        turn_errors,
        turn_error_kinds,
        turn_error_details
    );
    for notification in incoming.agent_notifications {
        if !index
            .agent_notifications
            .iter()
            .any(|old| old.notification_id == notification.notification_id)
        {
            index.agent_notifications.push(notification);
        }
    }
}
pub(super) fn normalize_control(item: RolloutItem) -> io::Result<RuntimeJson> {
    let context = match &item {
        RolloutItem::TurnContext { ctx } => ctx.turn_id.clone(),
        _ => None,
    };
    let is_context = matches!(&item, RolloutItem::TurnContext { .. });
    let is_lifecycle = matches!(&item,RolloutItem::Event(event) if event.event=="turn_lifecycle");
    let changes = crate::store::jsonl_store::rollout_metadata(std::slice::from_ref(&item)).3;
    let mut history = project_thread_history(vec![item]);
    // Infer interruption after all retained controls have been combined.
    if !is_lifecycle {
        history.turn_index.turn_status.clear();
    }
    let mut value = runtime_codec::encode_value(&history).map_err(io::Error::other)?;
    let object = value.as_object_mut().expect("history object");
    object.insert("fileChanges", encode_files(changes)?);
    if is_context {
        object.insert(
            "contextTurnId",
            runtime_codec::encode_value(&context).map_err(io::Error::other)?,
        );
    }
    Ok(value)
}

impl TurnIndexDb {
    pub(super) fn summary_records(&self, thread: &str, turn: &str) -> io::Result<Vec<Record>> {
        let text: String = self
            .conn
            .query_row(
                "SELECT payload FROM history_turn_projection WHERE thread=?1 AND turn=?2",
                params![thread, turn],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        let index: TurnIndex = decode(&text)?;
        let first:Option<String>=self.conn.query_row("SELECT item_id FROM history_items WHERE thread=?1 AND turn=?2 AND item_type='user' ORDER BY rollout_ordinal LIMIT 1",params![thread,turn],|row|row.get(0)).optional().map_err(sql_error)?;
        let final_id=match index.turn_final_message.get(&RuntimeString::from(turn)) {
            Some(id)=>Some(id.try_to_utf8().map_err(io::Error::other)?),
            None=>self.conn.query_row("SELECT item_id FROM history_items WHERE thread=?1 AND turn=?2 AND item_type='assistant' ORDER BY rollout_ordinal DESC LIMIT 1",params![thread,turn],|row|row.get(0)).optional().map_err(sql_error)?,
        };
        let mut statement=self.conn.prepare("SELECT turn,item_id FROM history_items WHERE thread=?1 AND turn=?2 AND item_id IN (?3,?4) ORDER BY rollout_ordinal").map_err(sql_error)?;
        let records = statement
            .query_map(params![thread, turn, first, final_id], record_from_row)
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        Ok(records)
    }
    pub(super) fn clear_projection(&self, thread: &str) -> io::Result<()> {
        for table in [
            "history_item_chunks",
            "history_turn_projection",
            "history_projection_state",
            "history_rollout_ordinals",
            "history_goal_projection",
            "history_control_projection",
            "history_message_turns",
            "history_items",
            "history_user_presentations",
            "history_presentation_backfill",
        ] {
            self.conn
                .execute(
                    &format!("DELETE FROM {table} WHERE thread=?1"),
                    params![thread],
                )
                .map_err(sql_error)?;
        }
        Ok(())
    }

    /// Explicit import/recovery entry point, also used after durable live writes.
    pub(crate) fn materialize_rollout(&mut self, thread: &str, path: &str) -> io::Result<()> {
        let _lease = self.projection_lease()?;
        let source = crate::store::compression::open_seekable(Path::new(path))?;
        let metadata = source.metadata()?;
        let stamp = metadata
            .modified()?
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_nanos()
            .min(i64::MAX as u128) as i64;
        self.ensure(thread, path, metadata.len(), stamp, &source)
    }

    fn save_bytes(
        &self,
        thread: &str,
        turn: &str,
        id: &str,
        ordinal: i64,
        bytes: &[u8],
    ) -> io::Result<()> {
        self.save_reader(thread, turn, id, ordinal, &mut io::Cursor::new(bytes))
    }
    fn save_reader(
        &self,
        thread: &str,
        turn: &str,
        id: &str,
        ordinal: i64,
        reader: &mut impl Read,
    ) -> io::Result<()> {
        self.conn
            .execute(
                "DELETE FROM history_item_chunks WHERE thread=?1 AND turn=?2 AND item_id=?3",
                params![thread, turn, id],
            )
            .map_err(sql_error)?;
        let mut part = 0i64;
        let mut bytes = [0; CHUNK_BYTES];
        loop {
            let count = reader.read(&mut bytes)?;
            if count == 0 {
                break;
            }
            self.conn.execute("INSERT INTO history_item_chunks(thread,turn,item_id,part,rollout_ordinal,payload) VALUES(?1,?2,?3,?4,?5,?6)",params![thread,turn,id,part,ordinal,&bytes[..count]]).map_err(sql_error)?;
            part += 1;
        }
        Ok(())
    }
    fn exists(&self, thread: &str, record: &Record, id: &str) -> io::Result<bool> {
        self.conn.query_row("SELECT EXISTS(SELECT 1 FROM history_item_chunks WHERE thread=?1 AND turn=?2 AND item_id=?3)",params![thread,record.turn,id],|row|row.get(0)).map_err(sql_error)
    }

    pub(super) fn persist_step(
        &self,
        thread: &str,
        turn: &str,
        item_id: Option<&str>,
        step: &mut pipeline::Step,
    ) -> io::Result<()> {
        if let Some(id) = item_id {
            self.conn
                .execute(
                    "DELETE FROM history_user_presentations WHERE thread=?1 AND display_id=?2",
                    params![thread, id],
                )
                .map_err(sql_error)?;
            let kind = step
                .header
                .as_ref()
                .and_then(|header| header.message.as_ref())
                .and_then(|message| message.kind.as_deref())
                .unwrap_or("compact_boundary");
            self.conn.execute("INSERT INTO history_items(thread,turn,item_id,rollout_ordinal,item_type) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(thread,turn,item_id) DO UPDATE SET rollout_ordinal=excluded.rollout_ordinal,item_type=excluded.item_type",params![thread,turn,id,step.ordinal,kind]).map_err(sql_error)?;
        }
        if let Some(value) = &step.control {
            self.conn.execute("INSERT INTO history_control_projection(thread,byte_offset,payload) VALUES(?1,?2,?3)",params![thread,step.offset as i64,json_text(value)?]).map_err(sql_error)?;
        }
        if let Some(summary) = step.summary.take() {
            let id = item_id.ok_or_else(|| io::Error::other("missing compaction identity"))?;
            let message = Message::CompactBoundary(CompactBoundaryMessage {
                native_compaction: None,
                uuid: id.into(),
                summary,
            });
            self.save_bytes(
                thread,
                turn,
                &format!("message:{id}"),
                step.ordinal,
                json_text(&message)?.as_bytes(),
            )?;
        }
        if let Some(mut payload) = step.payload.take() {
            payload.rewind()?;
            let header = step.header.as_ref().expect("payload header");
            if let Some(id) = item_id {
                self.save_reader(
                    thread,
                    turn,
                    &format!("message:{id}"),
                    step.ordinal,
                    &mut payload,
                )?;
                if header.message.as_ref().is_some_and(|message| {
                    message.kind.as_deref() == Some("system")
                        && message.subtype.as_deref() == Some("user_display")
                }) {
                    payload.rewind()?;
                    self.index_presentation(thread, turn, id, step.ordinal, &mut payload)?;
                }
            } else if header.event.as_deref() == Some("tool_result_output") {
                let target = header
                    .turn_id
                    .as_deref()
                    .or_else(|| {
                        header
                            .payload
                            .as_ref()
                            .and_then(|payload| payload.turn_id.as_deref())
                    })
                    .unwrap_or(turn);
                let tool = header
                    .payload
                    .as_ref()
                    .and_then(|payload| payload.tool_use_id.as_deref());
                let result:Option<(String,String)>=self.conn.query_row("SELECT message_id,turn FROM page_records_v2 WHERE thread=?1 AND turn=?2 AND message_type='tool_result' AND retained=1 AND tool=?3 AND offset<?4 ORDER BY offset DESC LIMIT 1",params![thread,target,tool,step.offset as i64],|row|Ok((row.get(0)?,row.get(1)?))).optional().map_err(sql_error)?;
                if let Some((id, turn)) = result {
                    self.save_reader(
                        thread,
                        &turn,
                        &format!("output:{id}"),
                        step.ordinal,
                        &mut payload,
                    )?;
                }
            }
        }
        Ok(())
    }
    pub(super) fn active_context(&self, thread: &str) -> io::Result<Option<String>> {
        let text:Option<String>=self.conn.query_row("SELECT c.payload FROM history_control_projection c JOIN page_records_v2 r ON r.thread=c.thread AND r.offset=c.byte_offset WHERE r.thread=?1 AND r.kind='turn_context' AND r.retained=1 ORDER BY r.offset DESC LIMIT 1",params![thread],|row|row.get(0)).optional().map_err(sql_error)?;
        match text {
            Some(text) => {
                let value: RuntimeJson = decode(&text)?;
                Ok(value
                    .get("contextTurnId")
                    .and_then(RuntimeJson::as_string)
                    .and_then(|value| value.try_to_utf8().ok()))
            }
            None => Ok(None),
        }
    }
    pub(super) fn refresh_turns(
        &self,
        thread: &str,
        turns: &BTreeSet<String>,
        from: u64,
        rewound: bool,
        goals_changed: bool,
    ) -> io::Result<RefreshStats> {
        let cleanup_started = std::time::Instant::now();
        if rewound {
            // Append batches cannot invalidate older items. Only a conversation
            // rewind prunes retained projections, with exact message-key lookups.
            self.conn.execute("DELETE FROM history_item_chunks WHERE thread=?1 AND NOT EXISTS (SELECT 1 FROM page_records_v2 r WHERE r.thread=?1 AND r.retained=1 AND r.turn=history_item_chunks.turn AND r.message_id=CASE WHEN substr(history_item_chunks.item_id,1,8)='message:' THEN substr(history_item_chunks.item_id,9) ELSE substr(history_item_chunks.item_id,8) END)",params![thread]).map_err(sql_error)?;
            self.conn.execute("DELETE FROM history_message_turns WHERE thread=?1 AND NOT EXISTS(SELECT 1 FROM page_records_v2 r WHERE r.thread=?1 AND r.retained=1 AND r.message_id=history_message_turns.item_id)",params![thread]).map_err(sql_error)?;
            self.conn.execute("DELETE FROM history_items WHERE thread=?1 AND NOT EXISTS(SELECT 1 FROM page_records_v2 r WHERE r.thread=?1 AND r.retained=1 AND r.turn=history_items.turn AND r.message_id=history_items.item_id)",params![thread]).map_err(sql_error)?;
            self.prune_presentations(thread)?;
            self.conn.execute("DELETE FROM history_turn_projection WHERE thread=?1 AND turn NOT IN(SELECT id FROM page_turns_v2 WHERE thread=?1)",params![thread]).map_err(sql_error)?;
        }
        let cleanup_us = cleanup_started.elapsed().as_micros() as u64;
        let metadata_started = std::time::Instant::now();
        let mut control_rows = 0usize;
        for turn in turns {
            let present: bool = self
                .conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM page_turns_v2 WHERE thread=?1 AND id=?2)",
                    params![thread, turn],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            if !present {
                continue;
            }
            let previous: Option<String> = if rewound {
                None
            } else {
                self.conn
                    .query_row(
                        "SELECT payload FROM history_turn_projection WHERE thread=?1 AND turn=?2",
                        params![thread, turn],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(sql_error)?
            };
            let previous = previous
                .map(|text| decode::<RuntimeJson>(&text))
                .transpose()?;
            let incremental = previous.as_ref().is_some_and(|value| {
                value
                    .get("projectionMetadataVersion")
                    .and_then(RuntimeJson::as_i64)
                    == Some(2)
            });
            let mut index = TurnIndex::default();
            let mut changes = SessionFileChanges::default();
            if incremental {
                let value = previous.as_ref().expect("incremental state");
                index = runtime_codec::decode_value(value).map_err(io::Error::other)?;
                merge_files(&mut changes, value, None)?;
            }
            // A versioned cached state replaces historical control replay during
            // normal append. Rewinds and old cache upgrades still fold the full
            // retained control set once, preserving the existing reducer result.
            let replay_from = if incremental { from as i64 } else { 0 };
            let mut statement=self.conn.prepare("SELECT c.payload FROM page_records_v2 r JOIN history_control_projection c ON c.thread=r.thread AND c.byte_offset=r.offset WHERE r.thread=?1 AND r.offset>=?3 AND (r.turn=?2 OR r.event_turn=?2) AND r.retained=1 AND (r.event IS NOT 'file_change' OR COALESCE(r.event_turn,r.turn)=?2) ORDER BY r.offset").map_err(sql_error)?;
            for row in statement
                .query_map(params![thread, turn, replay_from], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(sql_error)?
            {
                let value: RuntimeJson = decode(&row.map_err(sql_error)?)?;
                control_rows += 1;
                merge_index(
                    &mut index,
                    runtime_codec::decode_value(&value).map_err(io::Error::other)?,
                );
                let fallback =
                    (!turn.ends_with("-preamble")).then(|| RuntimeString::from(turn.as_str()));
                merge_files(&mut changes, &value, fallback.as_ref())?;
            }
            let has_context:bool=self.conn.query_row("SELECT EXISTS(SELECT 1 FROM page_records_v2 WHERE thread=?1 AND turn=?2 AND kind='turn_context' AND retained=1)",params![thread,turn],|row|row.get(0)).map_err(sql_error)?;
            let has_messages:bool=self.conn.query_row("SELECT EXISTS(SELECT 1 FROM history_message_turns WHERE thread=?1 AND turn=?2)",params![thread,turn],|row|row.get(0)).map_err(sql_error)?;
            if has_context || has_messages {
                index
                    .turn_status
                    .entry(turn.clone().into())
                    .or_insert_with(|| "interrupted".into());
            }
            // Message ownership already has a stable keyed table. Re-encoding
            // the entire map for every batch would make a long turn quadratic.
            index.message_turn.clear();
            let mut value = runtime_codec::encode_value(&index).map_err(io::Error::other)?;
            let object = value.as_object_mut().expect("turn index object");
            object.insert("fileChanges", encode_files(changes)?);
            object.insert("projectionMetadataVersion", RuntimeJson::from(2));
            self.conn.execute("INSERT INTO history_turn_projection(thread,turn,payload,ordinal,byte_offset,byte_len,updated_ordinal)
                SELECT ?1,?2,?3,o.ordinal,t.offset,
                COALESCE((SELECT offset+len FROM page_records_v2 WHERE thread=?1 AND turn=?2 AND retained=1 ORDER BY offset DESC LIMIT 1),t.offset)-t.offset,
                MAX(COALESCE((SELECT x.ordinal FROM page_records_v2 r JOIN history_rollout_ordinals x ON x.thread=r.thread AND x.byte_offset=r.offset WHERE r.thread=?1 AND r.turn=?2 AND r.retained=1 ORDER BY r.offset DESC LIMIT 1),o.ordinal),
                    COALESCE((SELECT x.ordinal FROM page_records_v2 r JOIN history_rollout_ordinals x ON x.thread=r.thread AND x.byte_offset=r.offset WHERE r.thread=?1 AND r.event_turn=?2 AND r.retained=1 ORDER BY r.offset DESC LIMIT 1),o.ordinal))
                FROM page_turns_v2 t JOIN history_rollout_ordinals o ON o.thread=t.thread AND o.byte_offset=t.offset WHERE t.thread=?1 AND t.id=?2
                ON CONFLICT(thread,turn) DO UPDATE SET payload=excluded.payload,ordinal=excluded.ordinal,byte_offset=excluded.byte_offset,byte_len=excluded.byte_len,updated_ordinal=excluded.updated_ordinal",params![thread,turn,json_text(&value)?]).map_err(sql_error)?;
        }
        let goals_present: bool = self
            .conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM history_goal_projection WHERE thread=?1)",
                params![thread],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if goals_changed || !goals_present {
            let mut goals: Vec<crate::goal::retained_context::GoalCompletion> = Vec::new();
            let mut statement=self.conn.prepare("SELECT c.payload FROM history_control_projection c JOIN page_records_v2 r ON r.thread=c.thread AND r.offset=c.byte_offset WHERE r.thread=?1 AND r.event='goal_completed' ORDER BY r.offset").map_err(sql_error)?;
            for row in statement
                .query_map(params![thread], |row| row.get::<_, String>(0))
                .map_err(sql_error)?
            {
                let value: RuntimeJson = decode(&row.map_err(sql_error)?)?;
                let next: Vec<crate::goal::retained_context::GoalCompletion> =
                    runtime_codec::decode_value(
                        value
                            .get("goalCompletions")
                            .ok_or_else(|| io::Error::other("missing completion projection"))?,
                    )
                    .map_err(io::Error::other)?;
                for completion in next {
                    match goals
                        .iter_mut()
                        .find(|old| old.goal.goal_id == completion.goal.goal_id)
                    {
                        Some(old) if old.goal_revision < completion.goal_revision => {
                            *old = completion
                        }
                        Some(_) => {}
                        None => goals.push(completion),
                    }
                }
            }
            self.conn.execute("INSERT INTO history_goal_projection(thread,payload) VALUES(?1,?2) ON CONFLICT(thread) DO UPDATE SET payload=excluded.payload",params![thread,json_text(&goals)?]).map_err(sql_error)?;
        }
        Ok(RefreshStats {
            cleanup_us,
            metadata_us: metadata_started.elapsed().as_micros() as u64,
            control_rows,
        })
    }
    pub(crate) fn delete_thread(&mut self, thread: &str) -> io::Result<()> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(sql_error)?;
        let result = (|| {
            self.clear_projection(thread)?;
            self.conn
                .execute(
                    "DELETE FROM history_legacy_imports WHERE thread=?1",
                    params![thread],
                )
                .map_err(sql_error)?;
            for (table, key) in [
                ("page_records_v2", "thread"),
                ("page_turns_v2", "thread"),
                ("page_threads_v2", "id"),
                ("page_epochs_v1", "thread"),
            ] {
                self.conn
                    .execute(
                        &format!("DELETE FROM {table} WHERE {key}=?1"),
                        params![thread],
                    )
                    .map_err(sql_error)?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => self.conn.execute_batch("COMMIT").map_err(sql_error),
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    pub(super) fn payload_file(
        &self,
        thread: &str,
        turn: &str,
        id: &str,
        message: bool,
    ) -> io::Result<std::fs::File> {
        let mut file = tempfile::tempfile()?;
        if message {
            file.write_all(b"{\"message\":")?;
        }
        let mut stmt = self.conn.prepare("SELECT payload FROM history_item_chunks WHERE thread=?1 AND turn=?2 AND item_id=?3 ORDER BY part").map_err(sql_error)?;
        let mut rows = stmt.query(params![thread, turn, id]).map_err(sql_error)?;
        let mut found = false;
        while let Some(row) = rows.next().map_err(sql_error)? {
            let data = row
                .get_ref(0)
                .map_err(sql_error)?
                .as_blob()
                .map_err(|error| io::Error::other(error.to_string()))?;
            file.write_all(data)?;
            found = true;
        }
        if !found {
            return Err(io::Error::other("history projection is incomplete"));
        }
        if message {
            file.write_all(b"}")?;
        }
        file.rewind()?;
        Ok(file)
    }
    pub(super) fn read_projection(
        &self,
        thread: &str,
        turns: &[String],
        records: Vec<Record>,
        guarded: bool,
    ) -> io::Result<(ThreadHistory, SessionFileChanges)> {
        let mut index = TurnIndex::default();
        let goals_text: String = self
            .conn
            .query_row(
                "SELECT COALESCE((SELECT payload FROM history_goal_projection WHERE thread=?1),'[]')",
                params![thread],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        let goals = decode(&goals_text)?;
        let mut changes = SessionFileChanges::default();
        for turn in turns {
            let text: String = self
                .conn
                .query_row(
                    "SELECT payload FROM history_turn_projection WHERE thread=?1 AND turn=?2",
                    params![thread, turn],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            let value: RuntimeJson = decode(&text)?;
            let incoming: TurnIndex =
                runtime_codec::decode_value(&value).map_err(io::Error::other)?;
            macro_rules! extend {($($field:ident),* $(,)?) => {$(index.$field.extend(incoming.$field);)*};}
            extend!(
                turn_goal_metadata,
                message_turn,
                turn_final_message,
                turn_status,
                turn_started_at_ms,
                turn_finished_at_ms,
                turn_duration_ms,
                turn_errors,
                turn_error_kinds,
                turn_error_details,
                turn_cancelled_tools
            );
            for notification in incoming.agent_notifications {
                if !index
                    .agent_notifications
                    .iter()
                    .any(|old| old.notification_id == notification.notification_id)
                {
                    index.agent_notifications.push(notification);
                }
            }
            if let Some(rows) = value.get("fileChanges").and_then(RuntimeJson::as_array) {
                for row in rows {
                    let group: Option<RuntimeString> = runtime_codec::decode_value(
                        row.get("turnId")
                            .ok_or_else(|| io::Error::other("missing file turn"))?,
                    )
                    .map_err(io::Error::other)?;
                    let change: crate::types::events::FileChange =
                        runtime_codec::decode_value(row).map_err(io::Error::other)?;
                    changes.record_change(group.as_ref(), &change);
                }
            }
        }
        let mut history = ThreadHistory {
            history: Vec::new(),
            turn_index: index,
            tool_outputs: Default::default(),
            goal_completions: goals,
        };
        history.turn_index.message_turn.clear();
        let mut budget = 0usize;
        for record in records {
            let id = record
                .message_id
                .as_deref()
                .ok_or_else(|| io::Error::other("missing history item identity"))?;
            let owner: Option<String> = self
                .conn
                .query_row(
                    "SELECT turn FROM history_message_turns WHERE thread=?1 AND item_id=?2",
                    params![thread, id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(sql_error)?;
            if let Some(owner) = owner {
                history
                    .turn_index
                    .message_turn
                    .insert(id.into(), owner.into());
            }
            let mut file =
                self.payload_file(thread, &record.turn, &format!("message:{id}"), true)?;
            let len = file.metadata()?.len();
            let bytes = load::record(&mut file, 0, len, &mut budget)?;
            let text = std::str::from_utf8(&bytes).map_err(io::Error::other)?;
            let value = runtime_json::parse(&text.into()).map_err(io::Error::other)?;
            let message: Message = runtime_codec::decode_value(
                value
                    .get("message")
                    .ok_or_else(|| io::Error::other("missing message projection"))?,
            )
            .map_err(io::Error::other)?;
            if self.exists(thread, &record, &format!("output:{id}"))? {
                let mut file =
                    self.payload_file(thread, &record.turn, &format!("output:{id}"), false)?;
                let len = file.metadata()?.len();
                let bytes = load::record_raw(&mut file, 0, len)?;
                budget = budget.saturating_add(bytes.len());
                if budget > MAX_ITEMS_PAGE_BYTES as usize * 6 + 64 * 1024 {
                    return Err(oversized(MAX_ITEMS_PAGE_BYTES + 1));
                }
                let text = std::str::from_utf8(&bytes).map_err(io::Error::other)?;
                history.tool_outputs.insert(
                    id.into(),
                    runtime_json::parse(&text.into()).map_err(io::Error::other)?,
                );
            }
            history.history.push(message);
        }
        if guarded {
            let mut value = runtime_codec::encode_value(&history).map_err(io::Error::other)?;
            let mut bytes = 0u64;
            if let Some(rows) = value.get_mut("history").and_then(RuntimeJson::as_array_mut) {
                for row in rows {
                    crate::store::thread_history::substitute_image_sources(row);
                    bytes = bytes.saturating_add(
                        runtime_json::stringify(row)
                            .try_to_utf8()
                            .map_err(io::Error::other)?
                            .len() as u64,
                    );
                }
            }
            if bytes > MAX_ITEMS_PAGE_BYTES {
                return Err(oversized(bytes));
            }
        }
        Ok((history, changes))
    }
}
