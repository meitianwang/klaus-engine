//! Transactional goal storage under the engine's config home.
//!
//! A bounded queue moves SQLite work off the async executor and reuses worker-
//! owned connections. Read transactions keep snapshots consistent; write leases
//! serialize legacy migration and generation-guarded mutations across processes.
//! Cleared threads retain their revision, and continuation deferrals reference
//! the current goal through a cascading foreign key.

use crate::fs::paths::{config_home, sanitize_path_component};
use crate::goal::owned_resources::GoalOwnedResourcesManifest;
use crate::goal::types::{
    GoalAccountingMode, GoalAccountingOutcome, GoalUpdate, ThreadGoal, ThreadGoalStatus,
    GOAL_OBJECTIVE_MAX_LENGTH,
};
use crate::runtime_values::{runtime_json, BomPolicy, RuntimeDecode, RuntimeJson, RuntimeString};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

#[path = "store_worker.rs"]
mod storage_worker;

const WORK_QUEUE_CAPACITY: usize = 64;
const SCHEMA_VERSION: i64 = 9;
const MAX_SAFE_JSON_REVISION: i64 = 9_007_199_254_740_991;
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS goal_threads (
    thread_id TEXT PRIMARY KEY NOT NULL,
    revision INTEGER NOT NULL CHECK (revision >= 0),
    legacy_migrated INTEGER NOT NULL CHECK (legacy_migrated = 1)
);
CREATE TABLE IF NOT EXISTS goal_generations (
    thread_id TEXT NOT NULL REFERENCES goal_threads(thread_id),
    goal_id BLOB NOT NULL CHECK (length(goal_id) > 0 AND length(goal_id) % 2 = 0),
    PRIMARY KEY (thread_id, goal_id)
);
";
const GOAL_TABLE_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS goals (
    thread_id TEXT PRIMARY KEY NOT NULL REFERENCES goal_threads(thread_id),
    goal_id BLOB NOT NULL CHECK (length(goal_id) > 0 AND length(goal_id) % 2 = 0),
    objective BLOB NOT NULL CHECK (length(objective) > 0 AND length(objective) <= 16000 AND length(objective) % 2 = 0),
    status TEXT NOT NULL CHECK (status IN ('active', 'paused', 'blocked', 'usage_limited', 'budget_limited', 'complete')),
    token_budget INTEGER CHECK (token_budget > 0),
    tokens_used INTEGER NOT NULL CHECK (tokens_used >= 0),
    time_used_seconds INTEGER NOT NULL CHECK (time_used_seconds >= 0),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE (thread_id, goal_id),
    FOREIGN KEY (thread_id, goal_id) REFERENCES goal_generations(thread_id, goal_id),
    CHECK (status NOT IN ('active', 'paused') OR token_budget IS NULL OR tokens_used < token_budget)
);
";
const DEFERRAL_TABLE_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS goal_continuation_deferrals (
    thread_id TEXT PRIMARY KEY NOT NULL,
    goal_id BLOB NOT NULL,
    FOREIGN KEY (thread_id, goal_id) REFERENCES goals(thread_id, goal_id) ON DELETE CASCADE
);
";
const REQUEST_RECEIPT_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS goal_request_receipts (
    thread_id TEXT NOT NULL REFERENCES goal_threads(thread_id),
    request_id TEXT NOT NULL CHECK (length(CAST(request_id AS BLOB)) > 0),
    fingerprint TEXT NOT NULL CHECK (length(CAST(fingerprint AS BLOB)) > 0),
    revision INTEGER NOT NULL CHECK (revision > 0),
    goal_snapshot BLOB NOT NULL CHECK (length(goal_snapshot) > 0),
    PRIMARY KEY (thread_id, request_id)
);
";

const RUN_TABLE_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS goal_runs (
    thread_id TEXT PRIMARY KEY NOT NULL,
    goal_id BLOB NOT NULL,
    owner_id TEXT NOT NULL CHECK (length(CAST(owner_id AS BLOB)) > 0),
    FOREIGN KEY (thread_id, goal_id) REFERENCES goals(thread_id, goal_id) ON DELETE CASCADE
);
";

const BUDGET_WRAPUP_TABLE_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS goal_budget_wrapups (
    thread_id TEXT PRIMARY KEY NOT NULL,
    goal_id BLOB NOT NULL,
    delivered INTEGER NOT NULL CHECK (delivered IN (0, 1)),
    FOREIGN KEY (thread_id, goal_id) REFERENCES goals(thread_id, goal_id) ON DELETE CASCADE
);
";

const OWNED_RESOURCES_TABLE_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS goal_owned_resources (
    thread_id TEXT PRIMARY KEY NOT NULL,
    goal_id BLOB NOT NULL,
    manifest BLOB NOT NULL CHECK (length(manifest) > 0),
    FOREIGN KEY (thread_id, goal_id) REFERENCES goals(thread_id, goal_id) ON DELETE CASCADE
);
";

const INTENT_TABLE_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS goal_intents (
    thread_id TEXT PRIMARY KEY NOT NULL REFERENCES goal_threads(thread_id),
    intent_revision INTEGER NOT NULL CHECK (intent_revision >= 0)
);
";

const ELIGIBILITY_TABLE_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS goal_thread_eligibility (
    thread_key BLOB PRIMARY KEY NOT NULL CHECK (typeof(thread_key) = 'blob' AND length(thread_key) > 0 AND length(thread_key) % 2 = 0),
    eligible INTEGER NOT NULL CHECK (eligible IN (0, 1))
);
";

/// One authoritative snapshot, including a revision that survives goal clear.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GoalRecord {
    pub goal: Option<ThreadGoal>,
    pub continuation_deferred: bool,
    pub revision: i64,
    /// Durable control intent, independent of accounting and run bookkeeping.
    pub intent_revision: i64,
}

/// Immutable fork input captured after progress flush, including the generation's
/// delivery and resource ownership state. A delayed target bind never rereads the source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalForkSnapshot {
    pub record: GoalRecord,
    wrapup: Option<GoalBudgetWrapup>,
    owned_resources: Option<GoalOwnedResourcesManifest>,
}

/// A submission mutation whose receipt must commit with its resulting goal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GoalRequestMutation {
    Create {
        goal: ThreadGoal,
        expected_goal_id: Option<RuntimeString>,
        expected_absent: bool,
    },
    CreateChecked {
        goal: ThreadGoal,
        expected_goal_id: Option<RuntimeString>,
        expected_absent: bool,
        expected_revision: Option<i64>,
        expected_intent_revision: Option<i64>,
    },
    Update {
        update: GoalUpdate,
        now_ms: i64,
    },
}

/// The original result of a request, independent of later goal edits or clears.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalRequestOutcome {
    pub goal: ThreadGoal,
    pub revision: i64,
    pub replayed: bool,
    pub fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GoalControlMutation {
    Set(GoalRequestMutation),
    Clear {
        expected_goal_id: Option<RuntimeString>,
        expected_revision: Option<i64>,
        expected_intent_revision: Option<i64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalControlAccounting {
    pub mode: GoalAccountingMode,
    pub goal_id: RuntimeString,
    pub token_delta: i64,
    pub time_delta_seconds: i64,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalControlReceipt {
    pub request_id: String,
    pub fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalControlOutcome {
    /// The current authoritative record, including when a receipt is replayed.
    pub record: GoalRecord,
    pub accounting: Option<GoalAccountingOutcome>,
    pub removed_goal: Option<ThreadGoal>,
    /// Replayed receipts retain their original goal and revision.
    pub receipt: Option<GoalRequestOutcome>,
}

type ControlGuardCallback = dyn Fn(&mut dyn FnMut() -> io::Result<GoalControlOutcome>) -> io::Result<GoalControlOutcome>
    + Send
    + Sync;

/// A synchronous admission check held through the authoritative SQL commit.
///
/// Lock order: callers release synchronous admission locks before awaiting
/// storage. The database worker acquires BEGIN IMMEDIATE before invoking this
/// guard, so SQLite busy waits never hold the admission mutex. The callback
/// validates admission and returns `operation()` unchanged, invoking it exactly
/// once on admission and never rejecting after a successful commit. Legacy file
/// reads are prepared before the callback; only synchronous projection, SQL and
/// COMMIT run inside it. Reply and observer delivery follow guard release.
#[derive(Clone)]
pub struct GoalControlGuard {
    callback: Arc<ControlGuardCallback>,
}

impl std::fmt::Debug for GoalControlGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GoalControlGuard")
            .finish_non_exhaustive()
    }
}

impl PartialEq for GoalControlGuard {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.callback, &other.callback)
    }
}

impl Eq for GoalControlGuard {}

impl GoalControlGuard {
    pub fn new<F>(callback: F) -> Self
    where
        F: Fn(&mut dyn FnMut() -> io::Result<GoalControlOutcome>) -> io::Result<GoalControlOutcome>
            + Send
            + Sync
            + 'static,
    {
        Self {
            callback: Arc::new(callback),
        }
    }

    pub fn run(
        &self,
        operation: &mut dyn FnMut() -> io::Result<GoalControlOutcome>,
    ) -> io::Result<GoalControlOutcome> {
        (self.callback)(operation)
    }
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

fn invalid_record(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn validate_database(connection: &Connection, version: i64) -> io::Result<()> {
    let integrity: String = connection
        .query_row("PRAGMA quick_check(1)", [], |row| row.get(0))
        .map_err(sql_error)?;
    if integrity != "ok" {
        return Err(invalid_record(format!(
            "goal database integrity check failed: {integrity}"
        )));
    }
    if version > SCHEMA_VERSION {
        return Err(invalid_record(format!(
            "unsupported goal database schema version {version}"
        )));
    }
    if version >= 1 {
        let mut required_tables = vec![
            "goal_threads",
            "goal_generations",
            "goals",
            "goal_continuation_deferrals",
        ];
        if version >= 2 {
            required_tables.push("goal_request_receipts");
        }
        if version >= 4 {
            required_tables.push("goal_runs");
        }
        if version >= 5 {
            required_tables.push("goal_budget_wrapups");
        }
        if version >= 6 {
            required_tables.push("goal_owned_resources");
        }
        if version >= 7 {
            required_tables.push("goal_intents");
        }
        if version >= 9 {
            required_tables.push("goal_thread_eligibility");
        }
        for table in required_tables {
            let exists: bool = connection.query_row(
                "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)", [table], |row| row.get(0),
            ).map_err(sql_error)?;
            if !exists {
                return Err(invalid_record(format!(
                    "goal database is missing required table {table}"
                )));
            }
        }
    }
    let failure: Option<String> = connection
        .query_row("PRAGMA foreign_key_check", [], |row| row.get(0))
        .optional()
        .map_err(sql_error)?;
    if let Some(table) = failure {
        return Err(invalid_record(format!(
            "goal database has invalid references in {table}"
        )));
    }
    Ok(())
}

fn open_database(root: &Path) -> io::Result<Connection> {
    std::fs::create_dir_all(root)?;
    let mut connection = Connection::open(root.join("goals.sqlite3")).map_err(sql_error)?;
    connection
        .busy_timeout(Duration::from_secs(5))
        .map_err(sql_error)?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(sql_error)?;
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(sql_error)?;
    validate_database(&connection, version)?;
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .map_err(sql_error)?;
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(sql_error)?;
    if version == SCHEMA_VERSION {
        return Ok(connection);
    }
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sql_error)?;
    // Another process can migrate while this connection waits for the write lease.
    let version: i64 = transaction
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(sql_error)?;
    validate_database(&transaction, version)?;
    if version == SCHEMA_VERSION {
        transaction.commit().map_err(sql_error)?;
        return Ok(connection);
    }
    transaction.execute_batch(SCHEMA).map_err(sql_error)?;
    migrate_goal_tables(&transaction, version)?;
    transaction
        .execute_batch(REQUEST_RECEIPT_SCHEMA)
        .map_err(sql_error)?;
    transaction
        .execute_batch(RUN_TABLE_SCHEMA)
        .map_err(sql_error)?;
    transaction
        .execute_batch(BUDGET_WRAPUP_TABLE_SCHEMA)
        .map_err(sql_error)?;
    transaction
        .execute_batch(OWNED_RESOURCES_TABLE_SCHEMA)
        .map_err(sql_error)?;
    transaction
        .execute_batch(INTENT_TABLE_SCHEMA)
        .map_err(sql_error)?;
    transaction
        .execute_batch(ELIGIBILITY_TABLE_SCHEMA)
        .map_err(sql_error)?;
    if version < 7 {
        transaction
            .execute_batch(
                "INSERT INTO goal_intents (thread_id, intent_revision)
                 SELECT thread_id, 0 FROM goal_threads WHERE 1
                 ON CONFLICT(thread_id) DO NOTHING;",
            )
            .map_err(sql_error)?;
    }
    if version < 5 {
        transaction
            .execute_batch(
                "INSERT INTO goal_budget_wrapups (thread_id, goal_id, delivered)
                 SELECT thread_id, goal_id, 0 FROM goals WHERE status = 'budget_limited'
                 ON CONFLICT(thread_id) DO NOTHING;",
            )
            .map_err(sql_error)?;
    }
    let foreign_key_failure: Option<String> = transaction
        .query_row("PRAGMA foreign_key_check", [], |row| row.get(0))
        .optional()
        .map_err(sql_error)?;
    if let Some(table) = foreign_key_failure {
        return Err(invalid_record(format!(
            "goal database has invalid references in {table}"
        )));
    }
    transaction
        .pragma_update(None, "user_version", SCHEMA_VERSION)
        .map_err(sql_error)?;
    transaction.commit().map_err(sql_error)?;
    Ok(connection)
}

fn migrate_goal_tables(transaction: &Transaction<'_>, version: i64) -> io::Result<()> {
    let goals_exist: bool = transaction
        .query_row(
            "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'goals')",
            [],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if version < 8 && goals_exist {
        for schema in [
            DEFERRAL_TABLE_SCHEMA,
            RUN_TABLE_SCHEMA,
            BUDGET_WRAPUP_TABLE_SCHEMA,
            OWNED_RESOURCES_TABLE_SCHEMA,
        ] {
            transaction.execute_batch(schema).map_err(sql_error)?;
        }
        let schema_objects = {
            let mut query = transaction.prepare(
                "SELECT sql FROM sqlite_master WHERE type IN ('index', 'trigger') AND sql IS NOT NULL
                 AND tbl_name IN ('goals', 'goal_continuation_deferrals', 'goal_runs', 'goal_budget_wrapups', 'goal_owned_resources')
                 ORDER BY CASE type WHEN 'index' THEN 0 ELSE 1 END, name",
            ).map_err(sql_error)?;
            let rows = query
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(sql_error)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(sql_error)?
        };
        // The goal table owns cascading generation references. Save dependent
        // rows before rebuilding it, then restore indexes and triggers only
        // after every original row has been copied without firing callbacks.
        transaction.execute_batch(
            "CREATE TEMP TABLE saved_goal_deferrals AS SELECT thread_id, goal_id FROM goal_continuation_deferrals;
             CREATE TEMP TABLE saved_goal_runs AS SELECT thread_id, goal_id, owner_id FROM goal_runs;
             CREATE TEMP TABLE saved_goal_wrapups AS SELECT thread_id, goal_id, delivered FROM goal_budget_wrapups;
             CREATE TEMP TABLE saved_goal_resources AS SELECT thread_id, goal_id, manifest FROM goal_owned_resources;
             DROP TABLE goal_continuation_deferrals;
             DROP TABLE goal_runs;
             DROP TABLE goal_budget_wrapups;
             DROP TABLE goal_owned_resources;
             ALTER TABLE goals RENAME TO goals_before_signed_timestamps;
             INSERT INTO goal_generations (thread_id, goal_id)
                 SELECT thread_id, goal_id FROM goals_before_signed_timestamps WHERE 1
                 ON CONFLICT(thread_id, goal_id) DO NOTHING;",
        ).map_err(sql_error)?;
        transaction
            .execute_batch(GOAL_TABLE_SCHEMA)
            .map_err(sql_error)?;
        transaction.execute_batch(
            "INSERT INTO goals (thread_id, goal_id, objective, status, token_budget, tokens_used, time_used_seconds, created_at_ms, updated_at_ms)
                 SELECT thread_id, goal_id, objective, status, token_budget, tokens_used, time_used_seconds, created_at_ms, updated_at_ms FROM goals_before_signed_timestamps;
             DROP TABLE goals_before_signed_timestamps;",
        ).map_err(sql_error)?;
        for schema in [
            DEFERRAL_TABLE_SCHEMA,
            RUN_TABLE_SCHEMA,
            BUDGET_WRAPUP_TABLE_SCHEMA,
            OWNED_RESOURCES_TABLE_SCHEMA,
        ] {
            transaction.execute_batch(schema).map_err(sql_error)?;
        }
        transaction.execute_batch(
            "INSERT INTO goal_continuation_deferrals (thread_id, goal_id) SELECT thread_id, goal_id FROM saved_goal_deferrals;
             INSERT INTO goal_runs (thread_id, goal_id, owner_id) SELECT thread_id, goal_id, owner_id FROM saved_goal_runs;
             INSERT INTO goal_budget_wrapups (thread_id, goal_id, delivered) SELECT thread_id, goal_id, delivered FROM saved_goal_wrapups;
             INSERT INTO goal_owned_resources (thread_id, goal_id, manifest) SELECT thread_id, goal_id, manifest FROM saved_goal_resources;
             DROP TABLE saved_goal_deferrals;
             DROP TABLE saved_goal_runs;
             DROP TABLE saved_goal_wrapups;
             DROP TABLE saved_goal_resources;",
        ).map_err(sql_error)?;
        for schema_object in schema_objects {
            transaction
                .execute_batch(&schema_object)
                .map_err(sql_error)?;
        }
    } else {
        transaction
            .execute_batch(GOAL_TABLE_SCHEMA)
            .map_err(sql_error)?;
        transaction
            .execute_batch(DEFERRAL_TABLE_SCHEMA)
            .map_err(sql_error)?;
    }
    Ok(())
}

async fn with_database<T, F>(operation: F) -> io::Result<T>
where
    F: FnOnce(&mut Connection, &Path) -> io::Result<T> + Send + 'static,
    T: Send + 'static,
{
    // Capture the root before queueing, including in multi-root processes.
    storage_worker::run(PathBuf::from(config_home()), operation).await
}

pub(crate) async fn subscribe_changes() -> io::Result<tokio::sync::watch::Receiver<u64>> {
    storage_worker::subscribe(PathBuf::from(config_home())).await
}

fn read_transaction<'a>(
    connection: &'a mut Connection,
    root: &Path,
    thread_id: &str,
) -> io::Result<Transaction<'a>> {
    // Existing threads never acquire a write lease. An unseen thread imports
    // its legacy file exactly once, atomically with the durable migration mark.
    let migrated: Option<bool> = connection
        .query_row(
            "SELECT legacy_migrated FROM goal_threads WHERE thread_id = ?1",
            [thread_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    if migrated != Some(true) {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        migrate_legacy(&transaction, root, thread_id)?;
        transaction.commit().map_err(sql_error)?;
    }
    connection
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .map_err(sql_error)
}

fn decode_string(bytes: Vec<u8>) -> io::Result<RuntimeString> {
    if !bytes.len().is_multiple_of(2) {
        return Err(invalid_record(
            "goal string contains incomplete UTF-16 code units",
        ));
    }
    Ok(RuntimeString::from_utf16(
        bytes
            .chunks_exact(2)
            .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
            .collect::<Vec<_>>(),
    ))
}

fn validate_goal(goal: &ThreadGoal, thread_id: &str) -> io::Result<()> {
    if thread_id.is_empty() || goal.thread_id != thread_id {
        return Err(invalid_record(
            "goal thread identity does not match its owning thread",
        ));
    }
    if goal.goal_id.is_empty()
        || goal.objective.is_empty()
        || goal
            .objective
            .code_points()
            .take(GOAL_OBJECTIVE_MAX_LENGTH + 1)
            .count()
            > GOAL_OBJECTIVE_MAX_LENGTH
        || goal.token_budget.is_some_and(|budget| budget <= 0)
        || goal.tokens_used < 0
        || goal.time_used_seconds < 0
        || ((goal.status.is_active() || goal.status == ThreadGoalStatus::Paused)
            && goal.budget_exhausted())
    {
        return Err(invalid_record(
            "goal record contains invalid identity, objective, budget, usage or timestamps",
        ));
    }
    Ok(())
}

fn read_record(transaction: &Transaction<'_>, thread_id: &str) -> io::Result<GoalRecord> {
    let revisions = transaction
        .query_row(
            "SELECT thread.revision, intent.intent_revision FROM goal_threads AS thread LEFT JOIN goal_intents AS intent ON intent.thread_id = thread.thread_id WHERE thread.thread_id = ?1",
            [thread_id],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?)),
        )
        .optional()
        .map_err(sql_error)?;
    let (revision, intent_revision) = match revisions {
        Some((revision, Some(intent_revision))) if revision >= 0 && intent_revision >= 0 => {
            (revision, intent_revision)
        }
        Some(_) => {
            return Err(invalid_record(
                "goal thread has no valid control intent revision",
            ))
        }
        None => (0, 0),
    };
    let goal = transaction.query_row(
        "SELECT goal_id, objective, status, token_budget, tokens_used, time_used_seconds, created_at_ms, updated_at_ms FROM goals WHERE thread_id = ?1", [thread_id],
        |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?, row.get::<_, String>(2)?, row.get::<_, Option<i64>>(3)?, row.get::<_, i64>(4)?, row.get::<_, i64>(5)?, row.get::<_, i64>(6)?, row.get::<_, i64>(7)?)),
    ).optional().map_err(sql_error)?
    .map(|(goal_id, objective, status, token_budget, tokens_used, time_used_seconds, created_at_ms, updated_at_ms)| {
        let status = ThreadGoalStatus::decode_runtime(&RuntimeJson::String(status.into())).map_err(|error| invalid_record(format!("invalid stored goal status: {error}")))?;
        let goal = ThreadGoal { thread_id: thread_id.into(), goal_id: decode_string(goal_id)?, objective: decode_string(objective)?, status, token_budget, tokens_used, time_used_seconds, created_at_ms, updated_at_ms };
        validate_goal(&goal, thread_id)?;
        Ok::<_, io::Error>(goal)
    }).transpose()?;
    let deferral: Option<Vec<u8>> = transaction
        .query_row(
            "SELECT goal_id FROM goal_continuation_deferrals WHERE thread_id = ?1",
            [thread_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    if let Some(owner) = &deferral {
        if goal
            .as_ref()
            .is_none_or(|goal| goal.goal_id.to_utf16le(BomPolicy::Omit) != *owner)
        {
            return Err(invalid_record(
                "continuation deferral does not reference the current goal",
            ));
        }
    }
    Ok(GoalRecord {
        goal,
        continuation_deferred: deferral.is_some(),
        revision,
        intent_revision,
    })
}

fn persist_record(
    transaction: &Transaction<'_>,
    thread_id: &str,
    record: &GoalRecord,
) -> io::Result<()> {
    if record.revision < 0 || record.intent_revision < 0 {
        return Err(invalid_record("goal thread revisions must not be negative"));
    }
    if record.continuation_deferred && record.goal.is_none() {
        return Err(invalid_record(
            "continuation deferral requires a current goal",
        ));
    }
    let mut preserved_run = None;
    let mut preserved_wrapup = None;
    let mut preserved_resources = None;
    if let Some(goal) = &record.goal {
        validate_goal(goal, thread_id)?;
        let identity = goal.goal_id.to_utf16le(BomPolicy::Omit);
        let current: Option<Vec<u8>> = transaction
            .query_row(
                "SELECT goal_id FROM goals WHERE thread_id = ?1",
                [thread_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        if current.as_ref() != Some(&identity) {
            let already_used: bool = transaction.query_row(
                "SELECT EXISTS (SELECT 1 FROM goal_generations WHERE thread_id = ?1 AND goal_id = ?2)",
                params![thread_id, identity],
                |row| row.get(0),
            ).map_err(sql_error)?;
            if already_used {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "goal generation was already used by this thread",
                ));
            }
        } else {
            preserved_run = read_goal_run(transaction, thread_id)?;
            preserved_wrapup = read_budget_wrapup(transaction, thread_id)?;
            preserved_resources = read_owned_resources(transaction, thread_id, &goal.goal_id)?;
        }
        if preserved_wrapup.is_none() && goal.status == ThreadGoalStatus::BudgetLimited {
            preserved_wrapup = Some(GoalBudgetWrapup {
                goal_id: goal.goal_id.clone(),
                delivered: false,
            });
        }
    }
    transaction.execute(
        "INSERT INTO goal_threads (thread_id, revision, legacy_migrated) VALUES (?1, ?2, 1) ON CONFLICT(thread_id) DO UPDATE SET revision = excluded.revision, legacy_migrated = 1", params![thread_id, record.revision],
    ).map_err(sql_error)?;
    persist_intent_revision(transaction, thread_id, record.intent_revision)?;
    transaction
        .execute("DELETE FROM goals WHERE thread_id = ?1", [thread_id])
        .map_err(sql_error)?;
    if let Some(goal) = &record.goal {
        transaction
            .execute(
                "INSERT INTO goal_generations (thread_id, goal_id) VALUES (?1, ?2) ON CONFLICT(thread_id, goal_id) DO NOTHING",
                params![thread_id, goal.goal_id.to_utf16le(BomPolicy::Omit)],
            )
            .map_err(sql_error)?;
        transaction.execute(
            "INSERT INTO goals (thread_id, goal_id, objective, status, token_budget, tokens_used, time_used_seconds, created_at_ms, updated_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![thread_id, goal.goal_id.to_utf16le(BomPolicy::Omit), goal.objective.to_utf16le(BomPolicy::Omit), goal.status.wire_name(), goal.token_budget, goal.tokens_used, goal.time_used_seconds, goal.created_at_ms, goal.updated_at_ms],
        ).map_err(sql_error)?;
        if record.continuation_deferred {
            transaction
                .execute(
                    "INSERT INTO goal_continuation_deferrals (thread_id, goal_id) VALUES (?1, ?2)",
                    params![thread_id, goal.goal_id.to_utf16le(BomPolicy::Omit)],
                )
                .map_err(sql_error)?;
        }
        if let Some(run) = preserved_run {
            // Rewriting the same generation must not lose its in-flight owner
            // through the cascading delete inside this transaction.
            transaction
                .execute(
                    "INSERT INTO goal_runs (thread_id, goal_id, owner_id) VALUES (?1, ?2, ?3)",
                    params![
                        thread_id,
                        run.goal_id.to_utf16le(BomPolicy::Omit),
                        run.owner_id
                    ],
                )
                .map_err(sql_error)?;
        }
        if let Some(wrapup) = preserved_wrapup {
            write_budget_wrapup(transaction, thread_id, &wrapup)?;
        }
        if let Some(resources) = preserved_resources {
            write_owned_resources(transaction, thread_id, &goal.goal_id, &resources)?;
        }
    }
    Ok(())
}

#[derive(serde::Deserialize)]
struct LegacyNativeNumbers {
    goal: Option<LegacyGoalNumbers>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct LegacyGoalNumbers {
    token_budget: Option<i64>,
    tokens_used: i64,
    time_used_seconds: i64,
    created_at_ms: i64,
    updated_at_ms: i64,
}

fn parse_legacy_record(bytes: Vec<u8>, thread_id: &str) -> io::Result<GoalRecord> {
    let raw = String::from_utf8(bytes)
        .map_err(|error| invalid_record(format!("legacy goal is not UTF-8: {error}")))?;
    let parsed = runtime_json::parse(&RuntimeString::from(raw.as_str()))
        .map_err(|error| invalid_record(format!("corrupt legacy goal JSON: {error}")))?;
    if !parsed.is_object() {
        return Err(invalid_record("legacy goal record must be an object"));
    }
    // Decode native integers before any binary64 conversion. This projection
    // ignores text values, while the runtime decoder below retains their exact
    // UTF-16 code units, including escaped unpaired surrogates.
    let numbers: LegacyNativeNumbers = serde_json::from_str(&raw)
        .map_err(|error| invalid_record(format!("invalid legacy goal numbers: {error}")))?;
    let goal = parsed
        .get("goal")
        .map(|value| {
            let numbers = numbers
                .goal
                .as_ref()
                .ok_or_else(|| invalid_record("legacy goal must contain a goal object"))?;
            let mut projected = value.clone();
            if let Some(object) = projected.as_object_mut() {
                for field in [
                    "tokenBudget",
                    "tokensUsed",
                    "timeUsedSeconds",
                    "createdAtMs",
                    "updatedAtMs",
                ] {
                    object.insert(field, RuntimeJson::from(0_i64));
                }
            }
            let mut goal = ThreadGoal::decode_runtime(&projected)
                .map_err(|error| invalid_record(format!("invalid legacy goal record: {error}")))?;
            goal.token_budget = numbers.token_budget;
            goal.tokens_used = numbers.tokens_used;
            goal.time_used_seconds = numbers.time_used_seconds;
            goal.created_at_ms = numbers.created_at_ms;
            goal.updated_at_ms = numbers.updated_at_ms;
            Ok::<_, io::Error>(goal)
        })
        .transpose()?;
    if let Some(goal) = &goal {
        validate_goal(goal, thread_id)?;
    }
    let continuation_deferred = match parsed.get("continuationDeferred") {
        None => false,
        Some(RuntimeJson::Bool(deferred)) => *deferred,
        Some(_) => {
            return Err(invalid_record(
                "legacy continuation deferral must be a boolean",
            ))
        }
    };
    if continuation_deferred && goal.is_none() {
        return Err(invalid_record(
            "legacy continuation deferral has no owning goal",
        ));
    }
    Ok(GoalRecord {
        revision: i64::from(goal.is_some()),
        intent_revision: i64::from(goal.is_some()),
        goal,
        continuation_deferred,
    })
}

fn prepare_legacy_import(
    transaction: &Transaction<'_>,
    root: &Path,
    thread_id: &str,
) -> io::Result<Option<GoalRecord>> {
    let migrated: Option<bool> = transaction
        .query_row(
            "SELECT legacy_migrated FROM goal_threads WHERE thread_id = ?1",
            [thread_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    if migrated == Some(true) {
        return Ok(None);
    }
    let path = root
        .join("goals")
        .join(format!("{}.json", sanitize_path_component(thread_id)));
    let record = match std::fs::read(&path) {
        Ok(bytes) => parse_legacy_record(bytes, thread_id).map_err(|error| {
            io::Error::new(error.kind(), format!("{}: {error}", path.display()))
        })?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => GoalRecord::default(),
        Err(error) => {
            return Err(io::Error::new(
                error.kind(),
                format!("{}: {error}", path.display()),
            ))
        }
    };
    Ok(Some(record))
}

fn migrate_legacy(transaction: &Transaction<'_>, root: &Path, thread_id: &str) -> io::Result<()> {
    if let Some(record) = prepare_legacy_import(transaction, root, thread_id)? {
        // The marker and imported data commit together; originals remain inspectable.
        persist_record(transaction, thread_id, &record)?;
    }
    Ok(())
}

async fn with_record<T, F>(thread_id: &str, mutate: F) -> io::Result<T>
where
    F: FnOnce(GoalRecord) -> io::Result<(GoalRecord, T)> + Send + 'static,
    T: Send + 'static,
{
    with_record_guarded(thread_id, None, mutate).await
}

async fn with_record_guarded<T, F>(
    thread_id: &str,
    guard: Option<GoalControlGuard>,
    mutate: F,
) -> io::Result<T>
where
    F: FnOnce(GoalRecord) -> io::Result<(GoalRecord, T)> + Send + 'static,
    T: Send + 'static,
{
    let thread_id = thread_id.to_owned();
    with_database(move |connection, root| {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let legacy = prepare_legacy_import(&transaction, root, &thread_id)?;
        let mut pending = Some((transaction, mutate, legacy));
        let mut value = None;
        let guarded = {
            let mut operation = || {
                let (transaction, mutate, legacy) = pending
                    .take()
                    .ok_or_else(|| io::Error::other("goal transaction was already executed"))?;
                if let Some(record) = legacy {
                    persist_record(&transaction, &thread_id, &record)?;
                }
                let before = read_record(&transaction, &thread_id)?;
                let (mut after, result) = mutate(before.clone())?;
                persist_changed_record(&transaction, &thread_id, &before, &mut after)?;
                transaction.commit().map_err(sql_error)?;
                value = Some(result);
                Ok(GoalControlOutcome {
                    record: after,
                    accounting: None,
                    removed_goal: None,
                    receipt: None,
                })
            };
            if let Some(guard) = guard {
                guard.run(&mut operation)
            } else {
                operation()
            }
        };
        guarded?;
        value.ok_or_else(|| io::Error::other("goal guard did not execute its transaction"))
    })
    .await
}

fn persist_changed_record(
    transaction: &Transaction<'_>,
    thread_id: &str,
    before: &GoalRecord,
    after: &mut GoalRecord,
) -> io::Result<()> {
    if record_data_changed(before, after) {
        after.revision = before
            .revision
            .checked_add(1)
            .ok_or_else(|| invalid_record("goal revision exhausted"))?;
        persist_record(transaction, thread_id, after)?;
    } else if after.intent_revision != before.intent_revision {
        // Same-value intent still revokes captured authority, without changing
        // the public goal revision solely for private intent bookkeeping.
        persist_intent_revision(transaction, thread_id, after.intent_revision)?;
    }
    Ok(())
}

fn record_data_changed(before: &GoalRecord, after: &GoalRecord) -> bool {
    before.goal != after.goal
        || before.continuation_deferred != after.continuation_deferred
        || before.revision != after.revision
}

fn persist_intent_revision(
    transaction: &Transaction<'_>,
    thread_id: &str,
    intent_revision: i64,
) -> io::Result<()> {
    let changed = transaction
        .execute(
            "INSERT INTO goal_intents (thread_id, intent_revision) VALUES (?1, ?2)
             ON CONFLICT(thread_id) DO UPDATE SET intent_revision = excluded.intent_revision
             WHERE excluded.intent_revision >= goal_intents.intent_revision",
            params![thread_id, intent_revision],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(invalid_record(
            "goal control intent revision cannot move backward",
        ));
    }
    Ok(())
}

fn advance_intent(record: &mut GoalRecord) -> io::Result<()> {
    record.intent_revision = record
        .intent_revision
        .checked_add(1)
        .ok_or_else(|| invalid_record("goal control intent revision exhausted"))?;
    Ok(())
}

/// Corruption and migration failures are errors, distinct from absence.
pub async fn get_goal(thread_id: &str) -> io::Result<Option<ThreadGoal>> {
    Ok(get_record(thread_id).await?.goal)
}

pub async fn get_record(thread_id: &str) -> io::Result<GoalRecord> {
    let thread_id = thread_id.to_owned();
    with_database(move |connection, root| {
        let transaction = read_transaction(connection, root, &thread_id)?;
        let record = read_record(&transaction, &thread_id)?;
        transaction.commit().map_err(sql_error)?;
        Ok(record)
    })
    .await
}

fn eligibility_key(thread_id: &str) -> io::Result<Vec<u8>> {
    if thread_id.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "goal eligibility requires a thread identity",
        ));
    }
    Ok(RuntimeString::from(thread_id).to_utf16le(BomPolicy::Omit))
}

/// Persist the session's authoritative ownership eligibility without creating
/// a goal record or changing goal accounting, public revision or control intent.
pub async fn set_thread_goal_eligibility(thread_id: &str, eligible: bool) -> io::Result<()> {
    let key = eligibility_key(thread_id)?;
    with_database(move |connection, _| {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        transaction
            .execute(
                "INSERT INTO goal_thread_eligibility (thread_key, eligible) VALUES (?1, ?2)
                 ON CONFLICT(thread_key) DO UPDATE SET eligible = excluded.eligible",
                params![key, eligible],
            )
            .map_err(sql_error)?;
        transaction.commit().map_err(sql_error)
    })
    .await
}

/// Unknown eligibility remains explicit; callers must not infer ownership from
/// a saved goal or require a provider merely to inspect this metadata.
pub async fn thread_goal_eligibility(thread_id: &str) -> io::Result<Option<bool>> {
    let key = eligibility_key(thread_id)?;
    with_database(move |connection, _| {
        let eligible = connection
            .query_row(
                "SELECT eligible FROM goal_thread_eligibility WHERE thread_key = ?1",
                [key],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(sql_error)?;
        eligible
            .map(|value| match value {
                0 => Ok(false),
                1 => Ok(true),
                _ => Err(invalid_record("stored goal eligibility is not a Boolean")),
            })
            .transpose()
    })
    .await
}

fn read_owned_resources(
    transaction: &Transaction<'_>,
    thread_id: &str,
    goal_id: &RuntimeString,
) -> io::Result<Option<GoalOwnedResourcesManifest>> {
    let stored = transaction
        .query_row(
            "SELECT goal_id, manifest FROM goal_owned_resources WHERE thread_id = ?1",
            [thread_id],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()
        .map_err(sql_error)?;
    stored
        .map(|(owner, bytes)| {
            if decode_string(owner)? != *goal_id {
                return Err(invalid_record(
                    "owned resources do not reference the current goal generation",
                ));
            }
            serde_json::from_slice(&bytes).map_err(|error| {
                invalid_record(format!("invalid goal owned resources manifest: {error}"))
            })
        })
        .transpose()
}

fn write_owned_resources(
    transaction: &Transaction<'_>,
    thread_id: &str,
    goal_id: &RuntimeString,
    manifest: &GoalOwnedResourcesManifest,
) -> io::Result<()> {
    let bytes = serde_json::to_vec(manifest).map_err(|error| {
        invalid_record(format!(
            "cannot encode goal owned resources manifest: {error}"
        ))
    })?;
    transaction
        .execute(
            "INSERT INTO goal_owned_resources (thread_id, goal_id, manifest) VALUES (?1, ?2, ?3)
             ON CONFLICT(thread_id) DO UPDATE SET goal_id = excluded.goal_id, manifest = excluded.manifest",
            params![thread_id, goal_id.to_utf16le(BomPolicy::Omit), bytes],
        )
        .map_err(sql_error)?;
    Ok(())
}

/// Resolve only the current generation's typed ownership metadata. Resource
/// validation belongs to the trusted preparation service before the transaction.
pub async fn get_goal_owned_resources(
    thread_id: &str,
    expected_goal_id: &RuntimeString,
) -> io::Result<Option<GoalOwnedResourcesManifest>> {
    get_goal_owned_resources_checked(thread_id, expected_goal_id, None).await
}

/// Resolve ownership only while the captured intent and generation still match.
/// A revoked lease has no capability; current metadata corruption remains an error.
pub async fn get_goal_owned_resources_for_intent(
    thread_id: &str,
    expected_goal_id: &RuntimeString,
    expected_intent_revision: i64,
) -> io::Result<Option<GoalOwnedResourcesManifest>> {
    get_goal_owned_resources_checked(thread_id, expected_goal_id, Some(expected_intent_revision))
        .await
}

async fn get_goal_owned_resources_checked(
    thread_id: &str,
    expected_goal_id: &RuntimeString,
    expected_intent_revision: Option<i64>,
) -> io::Result<Option<GoalOwnedResourcesManifest>> {
    let thread_id = thread_id.to_owned();
    let expected_goal_id = expected_goal_id.clone();
    with_database(move |connection, root| {
        let transaction = read_transaction(connection, root, &thread_id)?;
        let record = read_record(&transaction, &thread_id)?;
        let resources = if expected_intent_revision
            .is_none_or(|expected| record.intent_revision == expected)
            && record
                .goal
                .as_ref()
                .is_some_and(|goal| goal.goal_id == expected_goal_id)
        {
            read_owned_resources(&transaction, &thread_id, &expected_goal_id)?
        } else {
            None
        };
        transaction.commit().map_err(sql_error)?;
        Ok(resources)
    })
    .await
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GoalRun {
    goal_id: RuntimeString,
    owner_id: String,
}

fn read_goal_run(transaction: &Transaction<'_>, thread_id: &str) -> io::Result<Option<GoalRun>> {
    let run = transaction
        .query_row(
            "SELECT goal_id, owner_id FROM goal_runs WHERE thread_id = ?1",
            [thread_id],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(sql_error)?;
    run.map(|(goal_id, owner_id)| {
        if owner_id.is_empty() {
            return Err(invalid_record("goal run has no owner identity"));
        }
        Ok(GoalRun {
            goal_id: decode_string(goal_id)?,
            owner_id,
        })
    })
    .transpose()
}

fn validate_run_owner(thread_id: &str, owner_id: &str) -> io::Result<()> {
    if thread_id.is_empty() || owner_id.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "goal run requires nonempty thread and owner identities",
        ));
    }
    Ok(())
}

fn bump_thread_revision(
    transaction: &Transaction<'_>,
    thread_id: &str,
    revision: i64,
) -> io::Result<()> {
    let next = revision
        .checked_add(1)
        .ok_or_else(|| invalid_record("goal revision exhausted"))?;
    let changed = transaction
        .execute(
            "UPDATE goal_threads SET revision = ?1 WHERE thread_id = ?2 AND revision = ?3",
            params![next, thread_id, revision],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(invalid_record(
            "goal thread revision changed inside its transaction",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GoalBudgetWrapup {
    goal_id: RuntimeString,
    delivered: bool,
}

fn read_budget_wrapup(
    transaction: &Transaction<'_>,
    thread_id: &str,
) -> io::Result<Option<GoalBudgetWrapup>> {
    let wrapup = transaction
        .query_row(
            "SELECT goal_id, delivered FROM goal_budget_wrapups WHERE thread_id = ?1",
            [thread_id],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, bool>(1)?)),
        )
        .optional()
        .map_err(sql_error)?;
    wrapup
        .map(|(goal_id, delivered)| {
            Ok(GoalBudgetWrapup {
                goal_id: decode_string(goal_id)?,
                delivered,
            })
        })
        .transpose()
}

fn write_budget_wrapup(
    transaction: &Transaction<'_>,
    thread_id: &str,
    wrapup: &GoalBudgetWrapup,
) -> io::Result<()> {
    transaction.execute(
        "INSERT INTO goal_budget_wrapups (thread_id, goal_id, delivered) VALUES (?1, ?2, ?3)
             ON CONFLICT(thread_id) DO UPDATE SET goal_id = excluded.goal_id, delivered = excluded.delivered",
        params![thread_id, wrapup.goal_id.to_utf16le(BomPolicy::Omit), wrapup.delivered],
    ).map_err(sql_error)?;
    Ok(())
}

/// Inspect a generation's delivery obligation without consuming it. A matching
/// budget-limited goal with no record receives a durable pending record.
pub async fn pending_budget_wrapup(thread_id: &str, goal_id: &RuntimeString) -> io::Result<bool> {
    pending_budget_wrapup_with_change(thread_id, goal_id)
        .await
        .map(|(pending, _)| pending)
}

pub(crate) async fn pending_budget_wrapup_with_change(
    thread_id: &str,
    goal_id: &RuntimeString,
) -> io::Result<(bool, bool)> {
    let thread_id = thread_id.to_owned();
    let goal_id = goal_id.clone();
    with_database(move |connection, root| {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        migrate_legacy(&transaction, root, &thread_id)?;
        let record = read_record(&transaction, &thread_id)?;
        let mut changed = false;
        let pending =
            if let Some(goal) = record.goal.as_ref().filter(|goal| goal.goal_id == goal_id) {
                if let Some(wrapup) = read_budget_wrapup(&transaction, &thread_id)? {
                    if wrapup.goal_id != goal_id {
                        return Err(invalid_record(
                            "budget wrap-up does not reference the current generation",
                        ));
                    }
                    !wrapup.delivered
                } else if goal.status == ThreadGoalStatus::BudgetLimited {
                    write_budget_wrapup(
                        &transaction,
                        &thread_id,
                        &GoalBudgetWrapup {
                            goal_id: goal_id.clone(),
                            delivered: false,
                        },
                    )?;
                    bump_thread_revision(&transaction, &thread_id, record.revision)?;
                    changed = true;
                    true
                } else {
                    false
                }
            } else {
                false
            };
        transaction.commit().map_err(sql_error)?;
        Ok((pending, changed))
    })
    .await
}

/// Acknowledge only after contextual input was accepted. Exactly one caller
/// changes a pending generation to delivered; stale and repeated calls return false.
pub async fn acknowledge_budget_wrapup(
    thread_id: &str,
    goal_id: &RuntimeString,
) -> io::Result<bool> {
    let thread_id = thread_id.to_owned();
    let goal_id = goal_id.clone();
    with_database(move |connection, root| {
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate).map_err(sql_error)?;
        migrate_legacy(&transaction, root, &thread_id)?;
        let record = read_record(&transaction, &thread_id)?;
        let changed = if record.goal.as_ref().is_some_and(|goal| goal.goal_id == goal_id) {
            transaction.execute(
                "UPDATE goal_budget_wrapups SET delivered = 1 WHERE thread_id = ?1 AND goal_id = ?2 AND delivered = 0",
                params![thread_id, goal_id.to_utf16le(BomPolicy::Omit)],
            ).map_err(sql_error)? != 0
        } else {
            false
        };
        if changed { bump_thread_revision(&transaction, &thread_id, record.revision)?; }
        transaction.commit().map_err(sql_error)?;
        Ok(changed)
    }).await
}

/// Claim active work or one pending budget wrap-up for this owner. An existing
/// owned budget run may finish its response; other stopped states cannot run.
pub async fn begin_goal_run(
    thread_id: &str,
    goal_id: &RuntimeString,
    owner_id: &str,
) -> io::Result<bool> {
    begin_goal_run_with_change(thread_id, goal_id, owner_id)
        .await
        .map(|(admitted, _)| admitted)
}

pub(crate) async fn begin_goal_run_with_change(
    thread_id: &str,
    goal_id: &RuntimeString,
    owner_id: &str,
) -> io::Result<(bool, bool)> {
    validate_run_owner(thread_id, owner_id)?;
    let thread_id = thread_id.to_owned();
    let goal_id = goal_id.clone();
    let owner_id = owner_id.to_owned();
    with_database(move |connection, root| {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        migrate_legacy(&transaction, root, &thread_id)?;
        let record = read_record(&transaction, &thread_id)?;
        let goal = record.goal.as_ref().filter(|goal| goal.goal_id == goal_id);
        let run = read_goal_run(&transaction, &thread_id)?;
        let pending_wrapup = if run.is_none()
            && goal.is_some_and(|goal| goal.status == ThreadGoalStatus::BudgetLimited)
        {
            read_budget_wrapup(&transaction, &thread_id)?
                .is_some_and(|wrapup| wrapup.goal_id == goal_id && !wrapup.delivered)
        } else {
            false
        };
        let mut changed = false;
        let admitted = if let Some(run) = run {
            run.goal_id == goal_id
                && run.owner_id == owner_id
                && goal.is_some_and(|goal| {
                    goal.status.is_active() || goal.status == ThreadGoalStatus::BudgetLimited
                })
        } else if goal.is_some_and(|goal| {
            goal.status.is_active()
                || (goal.status == ThreadGoalStatus::BudgetLimited && pending_wrapup)
        }) {
            transaction
                .execute(
                    "INSERT INTO goal_runs (thread_id, goal_id, owner_id) VALUES (?1, ?2, ?3)",
                    params![thread_id, goal_id.to_utf16le(BomPolicy::Omit), owner_id],
                )
                .map_err(sql_error)?;
            bump_thread_revision(&transaction, &thread_id, record.revision)?;
            changed = true;
            true
        } else {
            false
        };
        transaction.commit().map_err(sql_error)?;
        Ok((admitted, changed))
    })
    .await
}

/// Release only the matching owner and generation; late callbacks are no-ops.
pub async fn finish_goal_run(
    thread_id: &str,
    goal_id: &RuntimeString,
    owner_id: &str,
) -> io::Result<()> {
    finish_goal_run_with_change(thread_id, goal_id, owner_id)
        .await
        .map(|_| ())
}

pub(crate) async fn finish_goal_run_with_change(
    thread_id: &str,
    goal_id: &RuntimeString,
    owner_id: &str,
) -> io::Result<bool> {
    validate_run_owner(thread_id, owner_id)?;
    let thread_id = thread_id.to_owned();
    let goal_id = goal_id.clone();
    let owner_id = owner_id.to_owned();
    with_database(move |connection, root| {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        migrate_legacy(&transaction, root, &thread_id)?;
        let record = read_record(&transaction, &thread_id)?;
        let removed = transaction
            .execute(
                "DELETE FROM goal_runs WHERE thread_id = ?1 AND goal_id = ?2 AND owner_id = ?3",
                params![thread_id, goal_id.to_utf16le(BomPolicy::Omit), owner_id],
            )
            .map_err(sql_error)?;
        if removed != 0 {
            bump_thread_revision(&transaction, &thread_id, record.revision)?;
        }
        transaction.commit().map_err(sql_error)?;
        Ok(removed != 0)
    })
    .await
}

/// Recover a stale owner only after the caller holds the authoritative session
/// lock. Active work pauses for explicit resumption; fork deferral is retained.
pub async fn recover_goal_run(
    thread_id: &str,
    current_owner: &str,
    now_ms: i64,
) -> io::Result<Option<ThreadGoal>> {
    validate_run_owner(thread_id, current_owner)?;
    let thread_id = thread_id.to_owned();
    let current_owner = current_owner.to_owned();
    with_database(move |connection, root| {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        migrate_legacy(&transaction, root, &thread_id)?;
        let before = read_record(&transaction, &thread_id)?;
        let Some(run) = read_goal_run(&transaction, &thread_id)? else {
            transaction.commit().map_err(sql_error)?;
            return Ok(None);
        };
        if run.owner_id == current_owner {
            transaction.commit().map_err(sql_error)?;
            return Ok(None);
        }
        let goal = before
            .goal
            .as_ref()
            .filter(|goal| goal.goal_id == run.goal_id)
            .ok_or_else(|| invalid_record("goal run does not reference the current generation"))?;
        let mut after = if goal.status.is_active() {
            update_record(
                before.clone(),
                GoalUpdate {
                    status: Some(ThreadGoalStatus::Paused),
                    expected_goal_id: Some(run.goal_id.clone()),
                    ..Default::default()
                },
                now_ms,
                false,
            )?
            .0
        } else {
            before.clone()
        };
        transaction
            .execute(
                "DELETE FROM goal_runs WHERE thread_id = ?1 AND goal_id = ?2 AND owner_id = ?3",
                params![
                    thread_id,
                    run.goal_id.to_utf16le(BomPolicy::Omit),
                    run.owner_id
                ],
            )
            .map_err(sql_error)?;
        if after != before {
            persist_changed_record(&transaction, &thread_id, &before, &mut after)?;
        } else {
            bump_thread_revision(&transaction, &thread_id, before.revision)?;
        }
        transaction.commit().map_err(sql_error)?;
        Ok(after.goal)
    })
    .await
}

/// Install transactional storage fault fixtures without a second connection path.
#[cfg(feature = "testing")]
pub async fn testing_execute(sql: &str) -> io::Result<()> {
    let sql = sql.to_owned();
    with_database(move |connection, _| {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        transaction.execute_batch(&sql).map_err(sql_error)?;
        transaction.commit().map_err(sql_error)
    })
    .await
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GoalInsertRejection {
    Unfinished(ThreadGoal),
    GenerationChanged(Option<ThreadGoal>),
}

/// Create a goal, allowing replacement only after completion.
pub async fn insert_goal(goal: ThreadGoal) -> io::Result<Result<ThreadGoal, GoalInsertRejection>> {
    insert_goal_if(goal, None).await
}

/// Compare the observed generation and insert under the same write transaction.
pub async fn insert_goal_if(
    goal: ThreadGoal,
    expected_goal_id: Option<RuntimeString>,
) -> io::Result<Result<ThreadGoal, GoalInsertRejection>> {
    insert_goal_guarded(goal, expected_goal_id, false).await
}

/// Match either an observed generation or an explicitly absent goal atomically.
pub async fn insert_goal_guarded(
    goal: ThreadGoal,
    expected_goal_id: Option<RuntimeString>,
    expected_absent: bool,
) -> io::Result<Result<ThreadGoal, GoalInsertRejection>> {
    insert_goal_checked(goal, expected_goal_id, expected_absent, None).await
}

/// Compare the observed generation, absence and revision in one transaction.
pub async fn insert_goal_checked(
    goal: ThreadGoal,
    expected_goal_id: Option<RuntimeString>,
    expected_absent: bool,
    expected_revision: Option<i64>,
) -> io::Result<Result<ThreadGoal, GoalInsertRejection>> {
    insert_goal_intent_checked(
        goal,
        expected_goal_id,
        expected_absent,
        expected_revision,
        None,
    )
    .await
}

/// Match durable control intent without rejecting the caller's accounting or
/// run bookkeeping that may have advanced the ordinary goal revision.
pub async fn insert_goal_intent_checked(
    goal: ThreadGoal,
    expected_goal_id: Option<RuntimeString>,
    expected_absent: bool,
    expected_revision: Option<i64>,
    expected_intent_revision: Option<i64>,
) -> io::Result<Result<ThreadGoal, GoalInsertRejection>> {
    insert_goal_intent_guarded(
        goal,
        expected_goal_id,
        expected_absent,
        expected_revision,
        expected_intent_revision,
        None,
    )
    .await
}

/// Apply caller admission after obtaining the SQL write lease, through COMMIT.
pub async fn insert_goal_intent_guarded(
    goal: ThreadGoal,
    expected_goal_id: Option<RuntimeString>,
    expected_absent: bool,
    expected_revision: Option<i64>,
    expected_intent_revision: Option<i64>,
    control_guard: Option<GoalControlGuard>,
) -> io::Result<Result<ThreadGoal, GoalInsertRejection>> {
    let (thread_id, goal) = prepare_insert(goal)?;
    with_record_guarded(&thread_id, control_guard, move |record| {
        insert_into_record(
            record,
            goal,
            expected_goal_id,
            expected_absent,
            expected_revision,
            expected_intent_revision,
        )
    })
    .await
}

fn prepare_insert(mut goal: ThreadGoal) -> io::Result<(String, ThreadGoal)> {
    let thread_id = goal
        .thread_id
        .try_to_utf8()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    if (goal.status.is_active() || goal.status == ThreadGoalStatus::Paused)
        && goal.budget_exhausted()
    {
        goal.status = ThreadGoalStatus::BudgetLimited;
    }
    validate_goal(&goal, &thread_id)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    Ok((thread_id, goal))
}

fn insert_into_record(
    mut record: GoalRecord,
    goal: ThreadGoal,
    expected_goal_id: Option<RuntimeString>,
    expected_absent: bool,
    expected_revision: Option<i64>,
    expected_intent_revision: Option<i64>,
) -> io::Result<(GoalRecord, Result<ThreadGoal, GoalInsertRejection>)> {
    if expected_intent_revision.is_some_and(|expected| record.intent_revision != expected) {
        return Ok((
            record.clone(),
            Err(GoalInsertRejection::GenerationChanged(record.goal.clone())),
        ));
    }
    if expected_revision.is_some_and(|expected| record.revision != expected) {
        return Ok((
            record.clone(),
            Err(GoalInsertRejection::GenerationChanged(record.goal.clone())),
        ));
    }
    if expected_absent && record.goal.is_some() {
        return Ok((
            record.clone(),
            Err(GoalInsertRejection::GenerationChanged(record.goal.clone())),
        ));
    }
    if let Some(expected) = &expected_goal_id {
        if record
            .goal
            .as_ref()
            .is_none_or(|current| &current.goal_id != expected)
        {
            return Ok((
                record.clone(),
                Err(GoalInsertRejection::GenerationChanged(record.goal.clone())),
            ));
        }
    }
    if let Some(existing) = &record.goal {
        if !existing.status.allows_replacement() {
            return Ok((
                record.clone(),
                Err(GoalInsertRejection::Unfinished(existing.clone())),
            ));
        }
        if existing.goal_id == goal.goal_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "replacement requires a fresh goal generation",
            ));
        }
    }
    record.goal = Some(goal.clone());
    record.continuation_deferred = false;
    advance_intent(&mut record)?;
    Ok((record, Ok(goal)))
}

fn apply_request_create(
    record: GoalRecord,
    thread_id: &str,
    goal: ThreadGoal,
    expected_goal_id: Option<RuntimeString>,
    expected_absent: bool,
    expected_revision: Option<i64>,
    expected_intent_revision: Option<i64>,
) -> io::Result<(GoalRecord, ThreadGoal)> {
    let (owner, goal) = prepare_insert(goal)?;
    if owner != thread_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "goal request thread identity does not match the new goal",
        ));
    }
    let (after, result) = insert_into_record(
        record,
        goal,
        expected_goal_id,
        expected_absent,
        expected_revision,
        expected_intent_revision,
    )?;
    let goal = result.map_err(|_| {
        io::Error::new(
            io::ErrorKind::AlreadyExists,
            "goal request create guard was refused",
        )
    })?;
    Ok((after, goal))
}

pub async fn update_goal(
    thread_id: &str,
    update: GoalUpdate,
    now_ms: i64,
) -> io::Result<Result<Option<ThreadGoal>, ThreadGoal>> {
    let guard = update.control_guard.clone();
    with_record_guarded(thread_id, guard, move |record| {
        update_record(record, update, now_ms, true)
    })
    .await
}

fn update_record(
    mut record: GoalRecord,
    update: GoalUpdate,
    now_ms: i64,
    control_intent: bool,
) -> io::Result<(GoalRecord, Result<Option<ThreadGoal>, ThreadGoal>)> {
    if update
        .expected_intent_revision
        .is_some_and(|expected| record.intent_revision != expected)
    {
        let Some(current) = record.goal.clone() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "GOAL_CONFLICT: goal control intent changed while the current goal is absent",
            ));
        };
        return Ok((record, Err(current)));
    }
    if update
        .expected_revision
        .is_some_and(|expected| record.revision != expected)
    {
        let Some(current) = record.goal.clone() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "GOAL_CONFLICT: goal revision changed while the current goal is absent",
            ));
        };
        return Ok((record, Err(current)));
    }
    let Some(mut goal) = record.goal.clone() else {
        return Ok((record, Ok(None)));
    };
    let explicit_intent = control_intent
        && (update.objective.is_some() || update.status.is_some() || update.token_budget.is_some());
    let was_budget_limited = goal.status == ThreadGoalStatus::BudgetLimited;
    if update
        .expected_goal_id
        .as_ref()
        .is_some_and(|expected| &goal.goal_id != expected)
    {
        return Ok((record, Err(goal)));
    }
    if let Some(objective) = update.objective {
        goal.objective = objective;
    }
    if let Some(status) = update.status {
        goal.status = status;
    }
    if let Some(budget) = update.token_budget {
        goal.token_budget = budget;
    }
    if (goal.status.is_active()
        || goal.status == ThreadGoalStatus::Paused
        || (was_budget_limited && goal.status == ThreadGoalStatus::Blocked))
        && goal.budget_exhausted()
    {
        goal.status = ThreadGoalStatus::BudgetLimited;
    }
    goal.updated_at_ms = now_ms;
    if update.require_safe_json {
        crate::goal::tools::validate_goal_result(&goal)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
        validate_json_revision(record.revision, record.goal.as_ref() != Some(&goal))?;
    }
    record.goal = Some(goal.clone());
    if explicit_intent {
        advance_intent(&mut record)?;
    }
    Ok((record, Ok(Some(goal))))
}

// Receipt snapshots preserve every UTF-16 code unit and i64 counter, including
// values that cannot round-trip through a JSON binary64 number.
fn encode_request_snapshot(goal: &ThreadGoal) -> Vec<u8> {
    let ThreadGoal {
        thread_id,
        goal_id,
        objective,
        status,
        token_budget,
        tokens_used,
        time_used_seconds,
        created_at_ms,
        updated_at_ms,
    } = goal;
    let mut bytes = b"KGR\x01".to_vec();
    let status = RuntimeString::from(status.wire_name());
    for string in [thread_id, goal_id, objective, &status] {
        let encoded = string.to_utf16le(BomPolicy::Omit);
        bytes.extend_from_slice(&(encoded.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&encoded);
    }
    match token_budget {
        Some(budget) => {
            bytes.push(1);
            bytes.extend_from_slice(&budget.to_le_bytes());
        }
        None => bytes.push(0),
    }
    for integer in [tokens_used, time_used_seconds, created_at_ms, updated_at_ms] {
        bytes.extend_from_slice(&integer.to_le_bytes());
    }
    bytes
}

struct RequestSnapshotDecoder<'a> {
    remaining: &'a [u8],
}

impl<'a> RequestSnapshotDecoder<'a> {
    fn take(&mut self, length: usize) -> io::Result<&'a [u8]> {
        let value = self
            .remaining
            .get(..length)
            .ok_or_else(|| invalid_record("truncated goal request snapshot"))?;
        self.remaining = &self.remaining[length..];
        Ok(value)
    }

    fn integer(&mut self) -> io::Result<i64> {
        Ok(i64::from_le_bytes(
            self.take(8)?
                .try_into()
                .expect("eight bytes were requested"),
        ))
    }

    fn string(&mut self) -> io::Result<RuntimeString> {
        let length = u64::from_le_bytes(
            self.take(8)?
                .try_into()
                .expect("eight bytes were requested"),
        );
        let length = usize::try_from(length)
            .map_err(|_| invalid_record("goal request snapshot string is too large"))?;
        decode_string(self.take(length)?.to_vec())
    }
}

fn decode_request_snapshot(bytes: &[u8], thread_id: &str) -> io::Result<ThreadGoal> {
    let mut decoder = RequestSnapshotDecoder { remaining: bytes };
    if decoder.take(4)? != b"KGR\x01" {
        return Err(invalid_record("unsupported goal request snapshot format"));
    }
    let stored_thread_id = decoder.string()?;
    let goal_id = decoder.string()?;
    let objective = decoder.string()?;
    let status = ThreadGoalStatus::decode_runtime(&RuntimeJson::String(decoder.string()?))
        .map_err(|error| {
            invalid_record(format!("invalid goal request snapshot status: {error}"))
        })?;
    let token_budget = match decoder.take(1)?[0] {
        0 => None,
        1 => Some(decoder.integer()?),
        _ => return Err(invalid_record("invalid goal request snapshot budget flag")),
    };
    let goal = ThreadGoal {
        thread_id: stored_thread_id,
        goal_id,
        objective,
        status,
        token_budget,
        tokens_used: decoder.integer()?,
        time_used_seconds: decoder.integer()?,
        created_at_ms: decoder.integer()?,
        updated_at_ms: decoder.integer()?,
    };
    if !decoder.remaining.is_empty() {
        return Err(invalid_record(
            "unexpected trailing goal request snapshot data",
        ));
    }
    validate_goal(&goal, thread_id)?;
    Ok(goal)
}

fn validate_request_key(thread_id: &str, request_id: &str) -> io::Result<()> {
    if thread_id.is_empty() || request_id.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "goal request requires nonempty thread and request identities",
        ));
    }
    Ok(())
}

fn read_request_receipt(
    transaction: &Transaction<'_>,
    thread_id: &str,
    request_id: &str,
) -> io::Result<Option<GoalRequestOutcome>> {
    let receipt = transaction.query_row(
        "SELECT receipt.fingerprint, receipt.revision, receipt.goal_snapshot, thread.revision FROM goal_request_receipts AS receipt JOIN goal_threads AS thread ON thread.thread_id = receipt.thread_id WHERE receipt.thread_id = ?1 AND receipt.request_id = ?2",
        params![thread_id, request_id],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get::<_, Vec<u8>>(2)?, row.get::<_, i64>(3)?)),
    ).optional().map_err(sql_error)?;
    receipt
        .map(|(fingerprint, revision, snapshot, current_revision)| {
            if fingerprint.is_empty() || revision <= 0 || revision > current_revision {
                return Err(invalid_record("invalid goal request receipt metadata"));
            }
            Ok(GoalRequestOutcome {
                goal: decode_request_snapshot(&snapshot, thread_id)?,
                revision,
                replayed: true,
                fingerprint,
            })
        })
        .transpose()
}

/// Look up the original submission result before choosing a retry mutation.
pub async fn get_goal_request_receipt(
    thread_id: &str,
    request_id: &str,
) -> io::Result<Option<GoalRequestOutcome>> {
    validate_request_key(thread_id, request_id)?;
    let thread_id = thread_id.to_owned();
    let request_id = request_id.to_owned();
    with_database(move |connection, _| {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let receipt = read_request_receipt(&transaction, &thread_id, &request_id)?;
        transaction.commit().map_err(sql_error)?;
        Ok(receipt)
    })
    .await
}

/// Apply and acknowledge a submission in one transaction. The named service
/// supplies a canonical semantic fingerprint; matching retries return the
/// saved snapshot without mutating the goal or admitting another turn.
pub async fn apply_goal_request_once(
    thread_id: &str,
    request_id: &str,
    fingerprint: &str,
    mutation: GoalRequestMutation,
) -> io::Result<GoalRequestOutcome> {
    let outcome = apply_goal_control(
        thread_id,
        GoalControlMutation::Set(mutation),
        None,
        Some(GoalControlReceipt {
            request_id: request_id.to_owned(),
            fingerprint: fingerprint.to_owned(),
        }),
        None,
    )
    .await?;
    outcome
        .receipt
        .ok_or_else(|| io::Error::other("goal submission did not produce its receipt"))
}

fn apply_request_mutation(
    record: GoalRecord,
    thread_id: &str,
    mutation: GoalRequestMutation,
) -> io::Result<(GoalRecord, ThreadGoal)> {
    match mutation {
        GoalRequestMutation::Create {
            goal,
            expected_goal_id,
            expected_absent,
        } => apply_request_create(
            record,
            thread_id,
            goal,
            expected_goal_id,
            expected_absent,
            None,
            None,
        ),
        GoalRequestMutation::CreateChecked {
            goal,
            expected_goal_id,
            expected_absent,
            expected_revision,
            expected_intent_revision,
        } => apply_request_create(
            record,
            thread_id,
            goal,
            expected_goal_id,
            expected_absent,
            expected_revision,
            expected_intent_revision,
        ),
        GoalRequestMutation::Update { update, now_ms } => {
            let (after, result) = update_record(record, update, now_ms, true)?;
            let goal = result
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "GOAL_CONFLICT: goal request update changed",
                    )
                })?
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        "goal request has no current goal to update",
                    )
                })?;
            Ok((after, goal))
        }
    }
}

fn check_control_guard(record: &GoalRecord, mutation: &GoalControlMutation) -> io::Result<()> {
    let (expected_goal_id, expected_absent, expected_revision, expected_intent_revision) =
        match mutation {
            GoalControlMutation::Set(GoalRequestMutation::Create {
                expected_goal_id,
                expected_absent,
                ..
            }) => (expected_goal_id.as_ref(), *expected_absent, None, None),
            GoalControlMutation::Set(GoalRequestMutation::CreateChecked {
                expected_goal_id,
                expected_absent,
                expected_revision,
                expected_intent_revision,
                ..
            }) => (
                expected_goal_id.as_ref(),
                *expected_absent,
                *expected_revision,
                *expected_intent_revision,
            ),
            GoalControlMutation::Set(GoalRequestMutation::Update { update, .. }) => (
                update.expected_goal_id.as_ref(),
                false,
                update.expected_revision,
                update.expected_intent_revision,
            ),
            GoalControlMutation::Clear {
                expected_goal_id,
                expected_revision,
                expected_intent_revision,
            } => (
                expected_goal_id.as_ref(),
                false,
                *expected_revision,
                *expected_intent_revision,
            ),
        };
    let reason =
        if expected_intent_revision.is_some_and(|expected| record.intent_revision != expected) {
            Some("goal control intent changed")
        } else if expected_revision.is_some_and(|expected| record.revision != expected) {
            Some("goal revision changed")
        } else if expected_absent && record.goal.is_some() {
            Some("goal is no longer absent")
        } else if expected_goal_id.is_some_and(|expected| {
            record
                .goal
                .as_ref()
                .is_none_or(|goal| &goal.goal_id != expected)
        }) {
            Some("goal generation changed")
        } else {
            None
        };
    if let Some(reason) = reason {
        let kind = if record.goal.is_some() {
            io::ErrorKind::AlreadyExists
        } else {
            io::ErrorKind::InvalidInput
        };
        return Err(io::Error::new(kind, format!("GOAL_CONFLICT: {reason}")));
    }
    Ok(())
}

fn validate_json_revision(revision: i64, changes_public_record: bool) -> io::Result<()> {
    let next = revision.checked_add(i64::from(changes_public_record));
    if !next.is_some_and(|revision| (0..=MAX_SAFE_JSON_REVISION).contains(&revision)) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "goal revision cannot be represented as a safe JSON integer",
        ));
    }
    Ok(())
}

/// Compare host intent before its own pending accounting flush. Accounting and
/// the control mutation share one revision and transaction, so an external edit
/// cannot be mistaken for the caller's accounting update.
/// Resource metadata is preserved by `None`, revoked by `Some(None)`, or bound
/// to the resulting generation by `Some(Some(manifest))`. Replacement and clear
/// revoke previous ownership unless a replacement supplies a new manifest.
pub async fn apply_goal_control(
    thread_id: &str,
    mutation: GoalControlMutation,
    accounting: Option<GoalControlAccounting>,
    receipt: Option<GoalControlReceipt>,
    owned_resources: Option<Option<GoalOwnedResourcesManifest>>,
) -> io::Result<GoalControlOutcome> {
    if thread_id.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "goal control requires a thread identity",
        ));
    }
    if let Some(receipt) = &receipt {
        validate_request_key(thread_id, &receipt.request_id)?;
        if receipt.fingerprint.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "goal request fingerprint must not be empty",
            ));
        }
        if matches!(&mutation, GoalControlMutation::Clear { .. }) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "goal clear does not support submission receipts",
            ));
        }
    }
    let guard = match &mutation {
        GoalControlMutation::Set(GoalRequestMutation::Update { update, .. }) => {
            update.control_guard.clone()
        }
        _ => None,
    };
    let thread_id = thread_id.to_owned();
    with_database(move |connection, root| {
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate).map_err(sql_error)?;
        let legacy = prepare_legacy_import(&transaction, root, &thread_id)?;
        let mut pending = Some((transaction, mutation, accounting, receipt, owned_resources, legacy));
        let guarded = {
            let mut operation = || {
                let (transaction, mutation, accounting, receipt, owned_resources, legacy) = pending.take()
                    .ok_or_else(|| io::Error::other("goal transaction was already executed"))?;
        if let Some(receipt) = &receipt {
            if let Some(saved) = read_request_receipt(&transaction, &thread_id, &receipt.request_id)? {
                if saved.fingerprint != receipt.fingerprint {
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, "goal request identity was already used with different semantics"));
                }
                let record = read_record(&transaction, &thread_id)?;
                transaction.commit().map_err(sql_error)?;
                return Ok(GoalControlOutcome { record, accounting: None, removed_goal: None, receipt: Some(saved) });
            }
        }
        if let Some(record) = legacy { persist_record(&transaction, &thread_id, &record)?; }
        let before = read_record(&transaction, &thread_id)?;
        check_control_guard(&before, &mutation)?;
        let require_safe_json = matches!(&mutation, GoalControlMutation::Set(GoalRequestMutation::Update { update, .. }) if update.require_safe_json);
        let before_resources = before.goal.as_ref()
            .map(|goal| read_owned_resources(&transaction, &thread_id, &goal.goal_id))
            .transpose()?.flatten();
        let (working, accounted) = if let Some(accounting) = accounting {
            let (record, outcome) = account_record(&thread_id, before.clone(), accounting.mode, accounting.token_delta, accounting.time_delta_seconds, Some(accounting.goal_id), accounting.now_ms)?;
            (record, Some(outcome))
        } else {
            (before.clone(), None)
        };
        let (mut after, removed_goal) = match mutation {
            GoalControlMutation::Set(mutation) => (apply_request_mutation(working, &thread_id, mutation)?.0, None),
            GoalControlMutation::Clear { expected_goal_id, expected_revision, expected_intent_revision } => {
                let (record, result) = clear_record(working, expected_goal_id, expected_revision, expected_intent_revision)?;
                let removed = result.map_err(|_| io::Error::new(io::ErrorKind::AlreadyExists, "GOAL_CONFLICT: goal clear changed"))?;
                (record, removed)
            }
        };
        let same_generation = before.goal.as_ref().zip(after.goal.as_ref())
            .is_some_and(|(before, after)| before.goal_id == after.goal_id);
        let explicit_resource_intent = owned_resources.is_some();
        let final_resources = match owned_resources {
            Some(Some(resources)) => {
                if after.goal.is_none() {
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, "owned resources require a resulting goal"));
                }
                Some(resources)
            }
            Some(None) => None,
            None if same_generation => before_resources.clone(),
            None => None,
        };
        if explicit_resource_intent && after.intent_revision == before.intent_revision {
            advance_intent(&mut after)?;
        }
        if before_resources != final_resources && !record_data_changed(&before, &after) {
            after.revision = before.revision.checked_add(1).ok_or_else(|| invalid_record("goal revision exhausted"))?;
        }
        if require_safe_json {
            validate_json_revision(before.revision, record_data_changed(&before, &after))?;
        }
        persist_changed_record(&transaction, &thread_id, &before, &mut after)?;
        if let Some(goal) = &after.goal {
            let persisted = read_owned_resources(&transaction, &thread_id, &goal.goal_id)?;
            if persisted != final_resources {
                if let Some(resources) = &final_resources {
                    write_owned_resources(&transaction, &thread_id, &goal.goal_id, resources)?;
                } else {
                    transaction.execute("DELETE FROM goal_owned_resources WHERE thread_id = ?1", [&thread_id]).map_err(sql_error)?;
                }
            }
        }
        // A simultaneous budget edit may change the final status, but a limit
        // crossed by the flush still creates its generation's delivery record.
        if let Some(GoalAccountingOutcome::Updated(charged)) = &accounted {
            if charged.status == ThreadGoalStatus::BudgetLimited
                && after.goal.as_ref().is_some_and(|goal| goal.goal_id == charged.goal_id)
                && read_budget_wrapup(&transaction, &thread_id)?.is_none()
            {
                write_budget_wrapup(&transaction, &thread_id, &GoalBudgetWrapup { goal_id: charged.goal_id.clone(), delivered: false })?;
                if after.revision == before.revision {
                    bump_thread_revision(&transaction, &thread_id, before.revision)?;
                    after.revision = before.revision.checked_add(1).ok_or_else(|| invalid_record("goal revision exhausted"))?;
                }
            }
        }
        let saved = if let Some(receipt) = receipt {
            let goal = after.goal.clone().ok_or_else(|| invalid_record("goal submission receipt has no resulting goal"))?;
            transaction.execute(
                "INSERT INTO goal_request_receipts (thread_id, request_id, fingerprint, revision, goal_snapshot) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![thread_id, receipt.request_id, receipt.fingerprint, after.revision, encode_request_snapshot(&goal)],
            ).map_err(sql_error)?;
            Some(GoalRequestOutcome { goal, revision: after.revision, replayed: false, fingerprint: receipt.fingerprint })
        } else { None };
        transaction.commit().map_err(sql_error)?;
        Ok(GoalControlOutcome { record: after, accounting: accounted, removed_goal, receipt: saved })
            };
            if let Some(guard) = guard { guard.run(&mut operation) } else { operation() }
        };
        if guarded.is_ok() && pending.is_some() {
            return Err(io::Error::other("goal guard did not execute its transaction"));
        }
        guarded
    }).await
}

pub async fn account_usage(
    thread_id: &str,
    mode: GoalAccountingMode,
    token_delta: i64,
    time_delta_seconds: i64,
    expected_goal_id: Option<RuntimeString>,
    now_ms: i64,
) -> io::Result<GoalAccountingOutcome> {
    let key = thread_id.to_owned();
    with_record(thread_id, move |record| {
        account_record(
            &key,
            record,
            mode,
            token_delta,
            time_delta_seconds,
            expected_goal_id,
            now_ms,
        )
    })
    .await
}

fn account_record(
    thread_id: &str,
    mut record: GoalRecord,
    mode: GoalAccountingMode,
    token_delta: i64,
    time_delta_seconds: i64,
    expected_goal_id: Option<RuntimeString>,
    now_ms: i64,
) -> io::Result<(GoalRecord, GoalAccountingOutcome)> {
    let Some(mut goal) = record.goal.clone() else {
        return Ok((record, GoalAccountingOutcome::Unchanged(None)));
    };
    if expected_goal_id
        .as_ref()
        .is_some_and(|expected| &goal.goal_id != expected)
        || !mode.admits_status(goal.status)
        || (token_delta <= 0 && time_delta_seconds <= 0)
    {
        return Ok((record, GoalAccountingOutcome::Unchanged(Some(goal))));
    }
    goal.tokens_used = goal.tokens_used.saturating_add(token_delta.max(0));
    goal.time_used_seconds = goal
        .time_used_seconds
        .saturating_add(time_delta_seconds.max(0));
    if (goal.status.is_active() || goal.status == ThreadGoalStatus::Paused)
        && goal.budget_exhausted()
    {
        goal.status = ThreadGoalStatus::BudgetLimited;
    }
    goal.updated_at_ms = now_ms;
    validate_goal(&goal, thread_id)?;
    record.goal = Some(goal.clone());
    Ok((record, GoalAccountingOutcome::Updated(goal)))
}

/// Clear only the observed generation and its continuation deferral.
pub async fn clear_goal_if(
    thread_id: &str,
    expected_goal_id: Option<RuntimeString>,
) -> io::Result<Result<Option<ThreadGoal>, ThreadGoal>> {
    clear_goal_if_revision(thread_id, expected_goal_id, None).await
}

/// Clear only the generation and revision that the host actually observed.
pub async fn clear_goal_if_revision(
    thread_id: &str,
    expected_goal_id: Option<RuntimeString>,
    expected_revision: Option<i64>,
) -> io::Result<Result<Option<ThreadGoal>, ThreadGoal>> {
    clear_goal_if_intent_revision(thread_id, expected_goal_id, expected_revision, None).await
}

/// Clear only the captured generation, public revision and durable intent.
pub async fn clear_goal_if_intent_revision(
    thread_id: &str,
    expected_goal_id: Option<RuntimeString>,
    expected_revision: Option<i64>,
    expected_intent_revision: Option<i64>,
) -> io::Result<Result<Option<ThreadGoal>, ThreadGoal>> {
    with_record(thread_id, move |record| {
        clear_record(
            record,
            expected_goal_id,
            expected_revision,
            expected_intent_revision,
        )
    })
    .await
}

fn clear_record(
    mut record: GoalRecord,
    expected_goal_id: Option<RuntimeString>,
    expected_revision: Option<i64>,
    expected_intent_revision: Option<i64>,
) -> io::Result<(GoalRecord, Result<Option<ThreadGoal>, ThreadGoal>)> {
    if expected_intent_revision.is_some_and(|expected| record.intent_revision != expected) {
        let Some(current) = record.goal.clone() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "GOAL_CONFLICT: goal control intent changed while the current goal is absent",
            ));
        };
        return Ok((record, Err(current)));
    }
    if expected_revision.is_some_and(|expected| record.revision != expected) {
        let Some(current) = record.goal.clone() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "GOAL_CONFLICT: goal revision changed while the current goal is absent",
            ));
        };
        return Ok((record, Err(current)));
    }
    if let Some(goal) = &record.goal {
        if expected_goal_id
            .as_ref()
            .is_some_and(|expected| &goal.goal_id != expected)
        {
            return Ok((record.clone(), Err(goal.clone())));
        }
    } else if expected_goal_id.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "GOAL_CONFLICT: goal generation changed while the current goal is absent",
        ));
    }
    let previous = record.goal.take();
    record.continuation_deferred = false;
    // Explicit clears publish revocation even if the current goal is absent.
    record.revision = record
        .revision
        .checked_add(1)
        .ok_or_else(|| invalid_record("goal revision exhausted"))?;
    advance_intent(&mut record)?;
    Ok((record, Ok(previous)))
}

pub async fn clear_goal(thread_id: &str) -> io::Result<Option<ThreadGoal>> {
    match clear_goal_if(thread_id, None).await? {
        Ok(goal) => Ok(goal),
        Err(_) => Err(io::Error::other(
            "unconditional goal clear was unexpectedly refused",
        )),
    }
}

/// A stale callback may not release or defer a replacement generation.
pub async fn set_continuation_deferral_if(
    thread_id: &str,
    deferred: bool,
    expected_goal_id: Option<RuntimeString>,
) -> io::Result<Result<(), ThreadGoal>> {
    set_continuation_deferral_if_with_change(thread_id, deferred, expected_goal_id)
        .await
        .map(|result| result.map(|_| ()))
}

pub(crate) async fn set_continuation_deferral_if_with_change(
    thread_id: &str,
    deferred: bool,
    expected_goal_id: Option<RuntimeString>,
) -> io::Result<Result<bool, ThreadGoal>> {
    with_record(thread_id, move |mut record| {
        if let Some(goal) = &record.goal {
            if expected_goal_id
                .as_ref()
                .is_some_and(|expected| &goal.goal_id != expected)
            {
                return Ok((record.clone(), Err(goal.clone())));
            }
        } else if deferred {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "continuation deferral requires a current goal",
            ));
        }
        let changed = record.continuation_deferred != deferred;
        record.continuation_deferred = deferred;
        Ok((record, Ok(changed)))
    })
    .await
}

pub async fn set_continuation_deferral(thread_id: &str, deferred: bool) -> io::Result<()> {
    match set_continuation_deferral_if(thread_id, deferred, None).await? {
        Ok(()) => Ok(()),
        Err(_) => Err(io::Error::other(
            "unconditional continuation deferral was unexpectedly refused",
        )),
    }
}

pub async fn has_continuation_deferral(thread_id: &str) -> io::Result<bool> {
    Ok(get_record(thread_id).await?.continuation_deferred)
}

/// Copy the complete source snapshot and defer the target in one transaction.
/// The source is unchanged and an existing target is never replaced.
pub async fn copy_goal_deferred(
    source_thread_id: &str,
    target_thread_id: &str,
) -> io::Result<Option<ThreadGoal>> {
    if source_thread_id == target_thread_id || target_thread_id.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "deferred goal copy requires a distinct target thread",
        ));
    }
    let source = source_thread_id.to_owned();
    let target = target_thread_id.to_owned();
    with_database(move |connection, root| {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        migrate_legacy(&transaction, root, &source)?;
        let snapshot = read_goal_fork_snapshot(&transaction, &source)?;
        let goal = copy_snapshot_into_thread(&transaction, root, &target, &snapshot)?;
        transaction.commit().map_err(sql_error)?;
        Ok(goal)
    })
    .await
}

fn read_goal_fork_snapshot(
    transaction: &Transaction<'_>,
    thread_id: &str,
) -> io::Result<GoalForkSnapshot> {
    let record = read_record(transaction, thread_id)?;
    let owned_resources = record
        .goal
        .as_ref()
        .map(|goal| read_owned_resources(transaction, thread_id, &goal.goal_id))
        .transpose()?
        .flatten();
    Ok(GoalForkSnapshot {
        record,
        wrapup: read_budget_wrapup(transaction, thread_id)?,
        owned_resources,
    })
}

/// Capture this after flushing source progress and before awaiting target startup.
pub async fn capture_goal_fork_snapshot(thread_id: &str) -> io::Result<GoalForkSnapshot> {
    let thread_id = thread_id.to_owned();
    with_database(move |connection, root| {
        let transaction = read_transaction(connection, root, &thread_id)?;
        let snapshot = read_goal_fork_snapshot(&transaction, &thread_id)?;
        transaction.commit().map_err(sql_error)?;
        Ok(snapshot)
    })
    .await
}

fn copy_snapshot_into_thread(
    transaction: &Transaction<'_>,
    root: &Path,
    target: &str,
    snapshot: &GoalForkSnapshot,
) -> io::Result<Option<ThreadGoal>> {
    migrate_legacy(transaction, root, target)?;
    let target_record = read_record(transaction, target)?;
    if target_record.goal.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "deferred goal target already has a goal",
        ));
    }
    let Some(mut goal) = snapshot.record.goal.clone() else {
        if snapshot.wrapup.is_some() || snapshot.owned_resources.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "fork metadata has no captured goal",
            ));
        }
        return Ok(None);
    };
    if snapshot
        .wrapup
        .as_ref()
        .is_some_and(|wrapup| wrapup.goal_id != goal.goal_id)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "fork delivery state does not match its captured generation",
        ));
    }
    goal.thread_id = target.into();
    let revision = target_record
        .revision
        .checked_add(1)
        .ok_or_else(|| invalid_record("goal revision exhausted"))?;
    persist_record(
        transaction,
        target,
        &GoalRecord {
            goal: Some(goal.clone()),
            continuation_deferred: true,
            revision,
            intent_revision: target_record
                .intent_revision
                .checked_add(1)
                .ok_or_else(|| invalid_record("goal control intent revision exhausted"))?,
        },
    )?;
    // Delivery snapshots remain independent after the fork. In particular, an
    // already delivered notice cannot become pending again in the target.
    if let Some(wrapup) = &snapshot.wrapup {
        write_budget_wrapup(transaction, target, wrapup)?;
    }
    if let Some(resources) = &snapshot.owned_resources {
        write_owned_resources(transaction, target, &goal.goal_id, resources)?;
    }
    Ok(Some(goal))
}

/// Persist exactly the captured goal and delivery snapshot, even if the source
/// was edited, cleared or replaced while the target was starting.
pub async fn copy_goal_snapshot_deferred(
    target_thread_id: &str,
    snapshot: &GoalForkSnapshot,
) -> io::Result<Option<ThreadGoal>> {
    if target_thread_id.is_empty()
        || snapshot
            .record
            .goal
            .as_ref()
            .is_some_and(|goal| goal.thread_id == target_thread_id)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "deferred goal copy requires a distinct target thread",
        ));
    }
    let target = target_thread_id.to_owned();
    let snapshot = snapshot.clone();
    with_database(move |connection, root| {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let goal = copy_snapshot_into_thread(&transaction, root, &target, &snapshot)?;
        transaction.commit().map_err(sql_error)?;
        Ok(goal)
    })
    .await
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;
