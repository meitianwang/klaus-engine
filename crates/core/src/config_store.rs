//! The engine's own configuration file: `<config home>/config.toml`.
//!
//! Why the engine and not the host: the same argument the permission rule store
//! settles. While the host owned the file it also had to re-derive every value
//! into `session/start`, so one fact had two owners — the host's idea of a gate
//! and the engine's schema — with nothing to catch the drift, and a settings
//! change only reached the engine on the NEXT session. Here the file IS the User
//! layer of the config governance stack, so what a settings page writes and what
//! a session resolves are the same bytes. The host still decides WHERE the config
//! home is (`KLAUS_CONFIG_DIR`); the content is the engine's.
//!
//! TOML rather than JSON because this file is meant to survive hand editing: it
//! carries comments (a server parked behind `#` instead of deleted) and
//! `toml_edit` rewrites one key without reflowing the rest. A JSON store has no
//! comments at all, and every write reserializes the whole document, which is
//! what forced the host to keep a hand-maintained key-order table just to make
//! diffs readable.
//!
//! Two kinds of top-level keys live here. Engine keys are the governance
//! schema's field names, validated on every write and folded into
//! a config layer. [`PRODUCT_SECTIONS`] are stored verbatim and never
//! interpreted: they are the product surfaces (desktop preferences, MCP servers,
//! the plugin ledger, project trust, BYO model-provider definitions) whose shape
//! belongs to their own subsystem.
//! The `features` table retains unimplemented entries as raw data with notices;
//! only the supported sandbox preferences enter an engine configuration layer.
//! Anything else is a typo in an engine key and is rejected rather than silently
//! parked as product data.

use std::collections::VecDeque;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use toml_edit::{Array, DocumentMut, InlineTable, Item, Table, TableLike};

use crate::config_governance::{govern_config, ConfigGovernanceLayer, ConfigGovernanceSource};
use crate::engine::session::{config_layer_from_value, resolve_config_layers};
use crate::fs::paths::config_home;
use crate::schemas::config::{
    engine_config_governance_schema, normalize_sandbox_config, normalize_sandbox_workspace_roots,
    retain_supported_sandbox_features, ConfigMigrationWarning,
};

#[derive(Default)]
struct StoreState {
    // A background session read may migrate before the settings page reads the file.
    // Retain notices for this process, with a fixed bound for arbitrary config paths.
    migration_notices: VecDeque<(PathBuf, Vec<ConfigMigrationWarning>)>,
}

fn store_state() -> Result<MutexGuard<'static, StoreState>, ConfigStoreError> {
    static STATE: OnceLock<Mutex<StoreState>> = OnceLock::new();
    STATE
        .get_or_init(|| Mutex::new(StoreState::default()))
        .lock()
        .map_err(|_| ConfigStoreError::Rejected("configuration store lock was poisoned".into()))
}

impl StoreState {
    fn notices(
        &mut self,
        path: &Path,
        warnings: Vec<ConfigMigrationWarning>,
    ) -> Vec<ConfigMigrationWarning> {
        let path = resolve_symlink(path);
        let stored = self
            .migration_notices
            .iter()
            .position(|(stored, _)| stored == &path);
        let mut notices = stored
            .and_then(|index| self.migration_notices.remove(index))
            .map(|(_, notices)| notices)
            .unwrap_or_default();
        for warning in warnings {
            if !notices.contains(&warning) {
                tracing::warn!(
                    code = %warning.code,
                    keys = ?warning.keys,
                    path = %path.display(),
                    "{}",
                    warning.message
                );
                if notices.len() == 32 {
                    notices.remove(0);
                }
                notices.push(warning);
            }
        }
        if !notices.is_empty() {
            if self.migration_notices.len() == 64 {
                self.migration_notices.pop_front();
            }
            self.migration_notices.push_back((path, notices.clone()));
        }
        notices
    }
}

/// The config file name, in the config home and in a project's `.klaus`.
pub const CONFIG_FILE: &str = "config.toml";

/// Top-level tables the engine stores but does not interpret. Each is owned by
/// the subsystem or host surface named by the key; the engine's only job is to
/// keep them in one file with everything else so there is a single thing to
/// back up, sync, and hand-edit.
pub const PRODUCT_SECTIONS: &[&str] = &[
    "desktop",
    "mcpServers",
    "plugins",
    "marketplaces",
    "projects",
    "modelProviders",
];

/// `<config home>/config.toml`.
pub fn user_config_path() -> PathBuf {
    PathBuf::from(config_home()).join(CONFIG_FILE)
}

/// `<cwd>/.klaus/config.toml` — the Project layer. A scoped settings edit targets
/// this file explicitly through the same validated configuration writer.
pub fn project_config_path(cwd: &Path) -> PathBuf {
    cwd.join(".klaus").join(CONFIG_FILE)
}

/// Whether a top-level key is stored verbatim rather than validated as engine
/// config.
pub fn is_product_section(key: &str) -> bool {
    PRODUCT_SECTIONS.contains(&key)
}

/// How one edit combines with what is already at its key path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeStrategy {
    /// Merge an object into the table already there, key by key; anything else
    /// overwrites.
    Upsert,
    /// Overwrite whatever is there, wholesale.
    Replace,
    /// Remove the key.
    Delete,
}

/// One edit in a write request. A `null` value deletes, whatever the strategy
/// says — TOML cannot hold a null, so writing one can only mean "unset".
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigEdit {
    pub key_path: String,
    pub value: Value,
    pub merge: MergeStrategy,
}

/// The file as the rest of the engine sees it: every top-level key as JSON, plus
/// the content version a writer echoes back to prove it read the current file.
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigDocument {
    pub values: Map<String, Value>,
    pub version: String,
    pub migration_warnings: Vec<ConfigMigrationWarning>,
    normalized_engine_values: Map<String, Value>,
}

impl ConfigDocument {
    /// The engine slice: everything that is not a product section. A key that is
    /// neither is left in here on purpose so the governance schema can reject it
    /// by name instead of the store swallowing it. Writable roots are resolved
    /// against this document's directory while `values` retains their raw text.
    pub fn engine_values(&self) -> Map<String, Value> {
        self.normalized_engine_values.clone()
    }

    /// The engine slice as a governance layer.
    pub fn engine_layer(&self, source: ConfigGovernanceSource) -> ConfigGovernanceLayer {
        ConfigGovernanceLayer {
            source,
            values: self.engine_values().into_iter().collect(),
        }
    }

    /// One product section, or an empty object when it is absent.
    pub fn product_section(&self, key: &str) -> Value {
        self.values
            .get(key)
            .cloned()
            .unwrap_or_else(|| Value::Object(Map::new()))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigStoreError {
    #[error("{path} is not valid TOML: {message}")]
    Parse { path: String, message: String },
    #[error("{path}: {source}")]
    Io {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error(
        "config changed on disk since it was read (expected version {expected}, found {actual})"
    )]
    VersionConflict { expected: String, actual: String },
    #[error("{0}")]
    InvalidKeyPath(String),
    #[error("{0}")]
    Rejected(String),
}

impl ConfigStoreError {
    /// Stable code for the wire, so a host can tell a stale-read conflict (retry
    /// after re-reading) from a rejected value (show the message).
    pub fn code(&self) -> &'static str {
        match self {
            Self::Parse { .. } => "parse",
            Self::Io { .. } => "io",
            Self::VersionConflict { .. } => "versionConflict",
            Self::InvalidKeyPath(_) => "invalidKeyPath",
            Self::Rejected(_) => "rejected",
        }
    }
}

/// What a session would actually resolve from a stack of config layers, with the
/// layer that won each key. The second half is what lets a settings page mark a
/// value it cannot change from where it is standing (a repo's config wins over
/// the user file), instead of writing it and appearing to do nothing.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConfigView {
    pub resolved: Map<String, Value>,
    pub sources: Map<String, Value>,
}

/// Fold layers through the engine's governance schema into a [`ConfigView`].
pub fn resolve_view(layers: &[ConfigGovernanceLayer]) -> Result<ConfigView, ConfigStoreError> {
    let layers = layers
        .iter()
        .map(|layer| {
            config_layer_from_value(
                layer.source,
                Value::Object(layer.values.clone().into_iter().collect()),
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(ConfigStoreError::Rejected)?;
    let governed = govern_config(&engine_config_governance_schema(), &layers)
        .map_err(|error| ConfigStoreError::Rejected(error.to_string()))?;
    let mut sources: Map<String, Value> = governed
        .entries()
        .map(|(key, entry)| (key.clone(), Value::from(entry.source.wire_name())))
        .collect();
    sources.extend(
        governed
            .sandbox_field_sources()
            .map(|(key, source)| (key.clone(), Value::from(source.wire_name()))),
    );
    Ok(ConfigView {
        resolved: governed.to_json_object(),
        sources,
    })
}

/// Read and parse the file. A missing file is an empty document, because "no
/// config" and "every default" are the same state; anything else that fails is
/// an error the caller must surface rather than start on defaults the user did
/// not ask for.
pub async fn load(path: &Path) -> Result<ConfigDocument, ConfigStoreError> {
    let path = path.to_path_buf();
    let error_path = path.display().to_string();
    tokio::task::spawn_blocking(move || load_blocking(&path))
        .await
        .map_err(|source| ConfigStoreError::Io {
            path: error_path,
            source: io::Error::other(source),
        })?
}

/// [`load`] for the synchronous callers: session assembly resolves config while
/// building a session, which is not an async path, and a config file is a small
/// local read.
pub fn load_blocking(path: &Path) -> Result<ConfigDocument, ConfigStoreError> {
    let mut state = store_state()?;
    let (raw, version) = read_raw(path)?;
    let (document, warnings, changed) = normalized_document(path, &raw)?;
    let values = table_to_map(document.as_table());
    let normalized_engine_values = engine_values_for_path(&values, path)?;
    let body = document.to_string();
    let version = if !changed {
        version
    } else {
        validate_engine_slice(&normalized_engine_values)?;
        write_atomically(path, &body, &version)?;
        content_version(body.as_bytes())
    };
    Ok(ConfigDocument {
        values,
        version,
        migration_warnings: state.notices(path, warnings),
        normalized_engine_values,
    })
}

/// Apply edits in order and write the result atomically.
///
/// `expected_version` is the version the caller last read. A mismatch aborts the
/// whole batch: a settings page and a hand edit must not silently overwrite each
/// other. The engine slice of the RESULT is validated before anything touches
/// disk, so a rejected write leaves the file exactly as it was.
pub async fn apply_edits(
    path: &Path,
    edits: &[ConfigEdit],
    expected_version: Option<&str>,
) -> Result<ConfigDocument, ConfigStoreError> {
    let path = path.to_path_buf();
    let error_path = path.display().to_string();
    let edits = edits.to_vec();
    let expected_version = expected_version.map(str::to_owned);
    tokio::task::spawn_blocking(move || {
        apply_edits_blocking(&path, &edits, expected_version.as_deref())
    })
    .await
    .map_err(|source| ConfigStoreError::Io {
        path: error_path,
        source: io::Error::other(source),
    })?
}

fn apply_edits_blocking(
    path: &Path,
    edits: &[ConfigEdit],
    expected_version: Option<&str>,
) -> Result<ConfigDocument, ConfigStoreError> {
    let mut state = store_state()?;
    let (raw, version) = read_raw(path)?;
    if let Some(expected) = expected_version {
        if expected != version {
            return Err(ConfigStoreError::VersionConflict {
                expected: expected.to_string(),
                actual: version,
            });
        }
    }

    let (mut document, mut warnings, _) = normalized_document(path, &raw)?;
    for edit in edits {
        if edit.key_path == "maxTurns" && !edit.value.is_null() {
            return Err(ConfigStoreError::Rejected(
                "maxTurns is retired; main conversations do not have a global turn cap".into(),
            ));
        }
        let segments = parse_key_path(&edit.key_path)?;
        if !edit.value.is_null() && edit.merge != MergeStrategy::Delete {
            reject_legacy_write(&segments, &edit.value)?;
        }
        apply_edit(document.as_table_mut(), &segments, edit)?;
    }

    let values = table_to_map(document.as_table());
    warnings
        .extend(normalize_sandbox_config(&mut values.clone()).map_err(ConfigStoreError::Rejected)?);
    let normalized_engine_values = engine_values_for_path(&values, path)?;
    validate_engine_slice(&normalized_engine_values)?;

    let body = document.to_string();
    write_atomically(path, &body, &version)?;
    Ok(ConfigDocument {
        values,
        version: content_version(body.as_bytes()),
        migration_warnings: state.notices(path, warnings),
        normalized_engine_values,
    })
}

fn reject_legacy_write(segments: &[String], value: &Value) -> Result<(), ConfigStoreError> {
    let key = segments[0].as_str();
    if [
        "approvalPolicy",
        "approvalsReviewer",
        "sandboxMode",
        "sandboxWorkspaceWrite",
        "permission_mode",
        "sandbox",
        "sandboxNetworkAllowed",
        "sandbox_network_allowed",
        "additionalWritableRoots",
        "additional_writable_roots",
        "additionalReadableRoots",
        "additional_readable_roots",
        "requireRealSandbox",
        "require_real_sandbox",
        "sandboxRetryOnDenial",
        "sandbox_retry_on_denial",
        "confineReads",
        "confine_reads",
    ]
    .contains(&key)
        || (key == "permissionMode"
            && value.as_str().is_some_and(|mode| {
                ["auto", "acceptEdits", "bypassPermissions", "dontAsk"].contains(&mode)
            }))
    {
        return Err(ConfigStoreError::Rejected(format!(
            "'{key}' is retired; write approval_policy, approvals_reviewer, sandbox_mode or sandbox_workspace_write"
        )));
    }
    if key == "sandbox_workspace_write" {
        let aliases = [
            "writableRoots",
            "networkAccess",
            "excludeTmpdirEnvVar",
            "excludeSlashTmp",
        ];
        if segments
            .get(1)
            .is_some_and(|key| aliases.contains(&key.as_str()))
            || (segments.len() == 1
                && value
                    .as_object()
                    .is_some_and(|table| aliases.iter().any(|key| table.contains_key(*key))))
        {
            return Err(ConfigStoreError::Rejected(
                "sandbox_workspace_write fields must use canonical snake_case names".into(),
            ));
        }
    }
    Ok(())
}

fn normalized_document(
    path: &Path,
    raw: &str,
) -> Result<(DocumentMut, Vec<ConfigMigrationWarning>, bool), ConfigStoreError> {
    let mut document = parse_document(path, raw)?;
    let original = table_to_map(document.as_table());
    let mut canonical = original.clone();
    let warnings = normalize_sandbox_config(&mut canonical).map_err(ConfigStoreError::Rejected)?;
    let changed = canonical != original;
    apply_canonical_table(document.as_table_mut(), &canonical);
    Ok((document, warnings, changed))
}

/// Change migrated entries in place so retained tables keep their header comments,
/// document positions, and unchanged inline values.
fn apply_canonical_table(table: &mut dyn TableLike, canonical: &Map<String, Value>) {
    let removed: Vec<_> = table
        .iter()
        .filter(|(key, _)| !canonical.contains_key(*key))
        .map(|(key, _)| key.to_owned())
        .collect();
    for key in removed {
        table.remove(&key);
    }
    for (key, value) in canonical {
        let Some(item) = table.get_mut(key) else {
            table.insert(key, json_to_item(value));
            continue;
        };
        if item_to_json(item).as_ref() == Some(value) {
            continue;
        }
        if let Some(canonical) = value.as_object() {
            if let Some(existing) = item.as_table_like_mut() {
                apply_canonical_table(existing, canonical);
                continue;
            }
        }
        let mut replacement = json_to_item(value);
        if let (Some(existing), Some(replacement)) = (item.as_value(), replacement.as_value_mut()) {
            *replacement.decor_mut() = existing.decor().clone();
        }
        *item = replacement;
    }
}

fn engine_values_for_path(
    values: &Map<String, Value>,
    path: &Path,
) -> Result<Map<String, Value>, ConfigStoreError> {
    let mut engine_values = values
        .iter()
        // A retired main-conversation cap remains readable without affecting sessions.
        .filter(|(key, _)| !is_product_section(key) && key.as_str() != "maxTurns")
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    retain_supported_sandbox_features(&mut engine_values);
    normalize_sandbox_workspace_roots(&mut engine_values, path.parent())
        .map_err(ConfigStoreError::Rejected)?;
    Ok(engine_values)
}

/// Every engine key resolved through the real config resolver: unknown keys,
/// wrong types, and cross-field conflicts all fail here, with the engine's own
/// message. Product sections are skipped — their shape is not the engine's to
/// judge.
///
/// The layer is `User` so a rejection names where the value actually came from;
/// resolving it as anything else puts a source in the message that the person
/// editing the file cannot map to a file.
fn validate_engine_slice(values: &Map<String, Value>) -> Result<(), ConfigStoreError> {
    let engine: Map<String, Value> = values
        .iter()
        .filter(|(key, _)| !is_product_section(key) && key.as_str() != "maxTurns")
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    if engine.is_empty() {
        return Ok(());
    }
    let layer = config_layer_from_value(ConfigGovernanceSource::User, Value::Object(engine))
        .map_err(ConfigStoreError::Rejected)?;
    resolve_config_layers(vec![layer])
        .map(|_| ())
        .map_err(ConfigStoreError::Rejected)
}

fn read_raw(path: &Path) -> Result<(String, String), ConfigStoreError> {
    match std::fs::read(path) {
        Ok(bytes) => decode_raw(path, bytes),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            Ok((String::new(), content_version(&[])))
        }
        Err(source) => Err(ConfigStoreError::Io {
            path: path.display().to_string(),
            source,
        }),
    }
}

fn decode_raw(path: &Path, bytes: Vec<u8>) -> Result<(String, String), ConfigStoreError> {
    let version = content_version(&bytes);
    let text = String::from_utf8(bytes).map_err(|error| ConfigStoreError::Parse {
        path: path.display().to_string(),
        message: error.to_string(),
    })?;
    Ok((text, version))
}

fn parse_document(path: &Path, raw: &str) -> Result<DocumentMut, ConfigStoreError> {
    raw.parse::<DocumentMut>()
        .map_err(|error| ConfigStoreError::Parse {
            path: path.display().to_string(),
            message: error.to_string(),
        })
}

fn content_version(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Write through a symlink to the file it points at (replacing the link itself
/// would silently detach a user who keeps their config in a dotfiles repo), via
/// a temp file in the same directory so a crash mid-write cannot truncate the
/// config. 0600 because the file is not world-readable state.
fn write_atomically(
    path: &Path,
    body: &str,
    expected_version: &str,
) -> Result<(), ConfigStoreError> {
    let target = resolve_symlink(path);
    let io_error = |source: io::Error| ConfigStoreError::Io {
        path: target.display().to_string(),
        source,
    };

    let parent = target.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent).map_err(io_error)?;

    let file_name = target
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| CONFIG_FILE.to_string());
    let temp = parent.join(format!(".{file_name}.{}.tmp", std::process::id()));
    std::fs::write(&temp, body).map_err(io_error)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&temp, std::fs::Permissions::from_mode(0o600))
            .map_err(io_error)?;
    }
    let (_, actual) = read_raw(path)?;
    if actual != expected_version {
        let _ = std::fs::remove_file(&temp);
        return Err(ConfigStoreError::VersionConflict {
            expected: expected_version.into(),
            actual,
        });
    }
    match std::fs::rename(&temp, &target) {
        Ok(()) => Ok(()),
        Err(source) => {
            let _ = std::fs::remove_file(&temp);
            Err(io_error(source))
        }
    }
}

fn resolve_symlink(path: &Path) -> PathBuf {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return path.to_path_buf();
    };
    if !metadata.file_type().is_symlink() {
        return path.to_path_buf();
    }
    match crate::fs::node_path::canonicalize_host(path) {
        Ok(resolved) => resolved,
        Err(_) => path.to_path_buf(),
    }
}

/// Split a dotted key path, honoring `"quoted"` segments so a key that itself
/// contains dots (a project path, a `name@marketplace` plugin id) is addressable.
fn parse_key_path(path: &str) -> Result<Vec<String>, ConfigStoreError> {
    let invalid = |reason: &str| {
        ConfigStoreError::InvalidKeyPath(format!("invalid config key path '{path}': {reason}"))
    };

    let mut segments = Vec::new();
    let mut chars = path.chars().peekable();
    loop {
        let mut segment = String::new();
        if chars.peek() == Some(&'"') {
            chars.next();
            let mut closed = false;
            while let Some(ch) = chars.next() {
                match ch {
                    '\\' => match chars.next() {
                        Some(escaped) => segment.push(escaped),
                        None => return Err(invalid("dangling escape")),
                    },
                    '"' => {
                        closed = true;
                        break;
                    }
                    other => segment.push(other),
                }
            }
            if !closed {
                return Err(invalid("unterminated quoted segment"));
            }
            match chars.next() {
                None => {
                    segments.push(segment);
                    break;
                }
                Some('.') => {}
                Some(other) => return Err(invalid(&format!("unexpected '{other}' after a quote"))),
            }
        } else {
            for ch in chars.by_ref() {
                if ch == '.' {
                    break;
                }
                segment.push(ch);
            }
            if segment.is_empty() {
                return Err(invalid("empty segment"));
            }
            if chars.peek().is_none() {
                segments.push(segment);
                break;
            }
        }
        segments.push(segment);
    }

    if segments.is_empty() || segments.iter().any(String::is_empty) {
        return Err(invalid("empty segment"));
    }
    Ok(segments)
}

fn apply_edit(
    root: &mut Table,
    segments: &[String],
    edit: &ConfigEdit,
) -> Result<(), ConfigStoreError> {
    let (leaf, parents) = segments
        .split_last()
        .expect("parse_key_path rejects empty paths");
    let mut table = root;
    for segment in parents {
        table = descend(table, segment)?;
    }

    if edit.merge == MergeStrategy::Delete || edit.value.is_null() {
        table.remove(leaf);
        return Ok(());
    }

    match (&edit.value, edit.merge) {
        (Value::Object(map), MergeStrategy::Upsert) if is_table_like(table.get(leaf)) => {
            let target = descend(table, leaf)?;
            merge_object(target, map);
        }
        _ => {
            table.insert(leaf, json_to_item(&edit.value));
        }
    }
    Ok(())
}

/// Get (or create) the table at `key`. An inline table we have to write inside
/// becomes a regular table: the alternative is a second write path for inline
/// containers, and the only formatting that changes is the subtree being edited.
fn descend<'a>(table: &'a mut Table, key: &str) -> Result<&'a mut Table, ConfigStoreError> {
    let entry = table.entry(key).or_insert_with(|| {
        let mut created = Table::new();
        created.set_implicit(true);
        Item::Table(created)
    });
    if let Item::Value(value) = entry {
        if let Some(inline) = value.as_inline_table() {
            *entry = Item::Table(inline.clone().into_table());
        }
    }
    match entry {
        Item::Table(table) => Ok(table),
        other => Err(ConfigStoreError::InvalidKeyPath(format!(
            "'{key}' holds a {} value, so it cannot contain nested keys",
            item_kind(other)
        ))),
    }
}

fn is_table_like(item: Option<&Item>) -> bool {
    match item {
        Some(Item::Table(_)) => true,
        Some(Item::Value(value)) => value.is_inline_table(),
        _ => false,
    }
}

fn item_kind(item: &Item) -> &'static str {
    match item {
        Item::None => "empty",
        Item::Table(_) => "table",
        Item::ArrayOfTables(_) => "array-of-tables",
        Item::Value(value) => match value {
            toml_edit::Value::String(_) => "string",
            toml_edit::Value::Integer(_) => "integer",
            toml_edit::Value::Float(_) => "float",
            toml_edit::Value::Boolean(_) => "boolean",
            toml_edit::Value::Datetime(_) => "datetime",
            toml_edit::Value::Array(_) => "array",
            toml_edit::Value::InlineTable(_) => "table",
        },
    }
}

fn merge_object(table: &mut Table, map: &Map<String, Value>) {
    for (key, value) in map {
        if value.is_null() {
            table.remove(key);
            continue;
        }
        match value.as_object() {
            Some(nested) if is_table_like(table.get(key)) => {
                let target = descend(table, key).expect("checked table-like above");
                merge_object(target, nested);
            }
            _ => {
                table.insert(key, json_to_item(value));
            }
        }
    }
}

fn json_to_item(value: &Value) -> Item {
    match value {
        Value::Object(map) => {
            let mut table = Table::new();
            for (key, nested) in map {
                if nested.is_null() {
                    continue;
                }
                table.insert(key, json_to_item(nested));
            }
            Item::Table(table)
        }
        other => Item::Value(json_to_value(other)),
    }
}

/// JSON has a null and TOML does not, so a null anywhere inside a value is
/// dropped rather than invented into an empty string.
fn json_to_value(value: &Value) -> toml_edit::Value {
    match value {
        Value::Null => toml_edit::Value::from(""),
        Value::Bool(flag) => toml_edit::Value::from(*flag),
        Value::Number(number) => match (number.as_i64(), number.as_f64()) {
            (Some(int), _) => toml_edit::Value::from(int),
            (None, Some(float)) => toml_edit::Value::from(float),
            _ => toml_edit::Value::from(number.to_string()),
        },
        Value::String(text) => toml_edit::Value::from(text.as_str()),
        Value::Array(items) => {
            let mut array = Array::new();
            for item in items.iter().filter(|item| !item.is_null()) {
                array.push(json_to_value(item));
            }
            toml_edit::Value::Array(array)
        }
        Value::Object(map) => {
            let mut inline = InlineTable::new();
            for (key, nested) in map.iter().filter(|(_, nested)| !nested.is_null()) {
                inline.insert(key, json_to_value(nested));
            }
            toml_edit::Value::InlineTable(inline)
        }
    }
}

fn table_to_map(table: &Table) -> Map<String, Value> {
    table
        .iter()
        .filter_map(|(key, item)| item_to_json(item).map(|value| (key.to_string(), value)))
        .collect()
}

fn item_to_json(item: &Item) -> Option<Value> {
    match item {
        Item::None => None,
        Item::Value(value) => Some(value_to_json(value)),
        Item::Table(table) => Some(Value::Object(table_to_map(table))),
        Item::ArrayOfTables(tables) => Some(Value::Array(
            tables
                .iter()
                .map(|table| Value::Object(table_to_map(table)))
                .collect(),
        )),
    }
}

fn value_to_json(value: &toml_edit::Value) -> Value {
    match value {
        toml_edit::Value::String(text) => Value::String(text.value().clone()),
        toml_edit::Value::Integer(number) => Value::from(*number.value()),
        toml_edit::Value::Float(number) => serde_json::Number::from_f64(*number.value())
            .map(Value::Number)
            .unwrap_or(Value::Null),
        toml_edit::Value::Boolean(flag) => Value::Bool(*flag.value()),
        toml_edit::Value::Datetime(stamp) => Value::String(stamp.value().to_string()),
        toml_edit::Value::Array(items) => Value::Array(items.iter().map(value_to_json).collect()),
        toml_edit::Value::InlineTable(table) => Value::Object(
            table
                .iter()
                .map(|(key, item)| (key.to_string(), value_to_json(item)))
                .collect(),
        ),
    }
}

#[cfg(test)]
#[path = "config_store_tests.rs"]
mod tests;
