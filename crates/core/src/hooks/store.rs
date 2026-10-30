//! The hooks file the engine owns: `<config home>/hooks.json` and a project's
//! `<cwd>/.klaus/hooks.json`.
//!
//! Why the engine and not the host: the file's schema IS the engine's hook
//! roster. While the host wrote it, the host also had to keep its own copy of
//! [`HOOK_EVENTS`] to validate against, and the two drifted — the host's list
//! carried a `PermissionRequest` event the engine has never dispatched, so a
//! hook the settings page happily saved could never fire. Here the roster and
//! the validator are the same list.
//!
//! The file format is unchanged, because it is an ecosystem format: event name →
//! `[{ matcher?, hooks: [{ type, command?, timeout?, async? }] }]`.

use std::io;
use std::path::{Path, PathBuf};

use serde_json::{Map, Value};

use crate::fs::paths::config_home;
use crate::hooks::types::HOOK_EVENTS;

/// The hooks file name, in the config home and in a project's `.klaus`.
pub const HOOKS_FILE: &str = "hooks.json";

/// `<config home>/hooks.json`.
pub fn user_hooks_path() -> PathBuf {
    PathBuf::from(config_home()).join(HOOKS_FILE)
}

/// `<cwd>/.klaus/hooks.json`.
pub fn project_hooks_path(cwd: &Path) -> PathBuf {
    cwd.join(".klaus").join(HOOKS_FILE)
}

/// The event names a hooks file may key on, in roster order.
pub fn hook_event_names() -> Vec<&'static str> {
    HOOK_EVENTS.iter().map(|event| event.wire_name()).collect()
}

#[derive(Debug, thiserror::Error)]
pub enum HooksStoreError {
    #[error("{path} is not valid JSON: {message}")]
    Parse { path: String, message: String },
    #[error("{path}: {source}")]
    Io {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("{0}")]
    Rejected(String),
}

impl HooksStoreError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Parse { .. } => "parse",
            Self::Io { .. } => "io",
            Self::Rejected(_) => "rejected",
        }
    }
}

/// Read the file. A missing file is an empty hook set; malformed content is an
/// error, because silently answering "no hooks" would look like the user's hooks
/// were deleted.
pub async fn load(path: &Path) -> Result<Map<String, Value>, HooksStoreError> {
    let raw = match tokio::fs::read_to_string(path).await {
        Ok(raw) => raw,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Map::new()),
        Err(source) => {
            return Err(HooksStoreError::Io {
                path: path.display().to_string(),
                source,
            })
        }
    };
    if raw.trim().is_empty() {
        return Ok(Map::new());
    }
    match serde_json::from_str::<Value>(&raw) {
        Ok(Value::Object(map)) => Ok(map),
        Ok(_) => Err(HooksStoreError::Parse {
            path: path.display().to_string(),
            message: "the hooks file must hold an object".to_string(),
        }),
        Err(error) => Err(HooksStoreError::Parse {
            path: path.display().to_string(),
            message: error.to_string(),
        }),
    }
}

/// Validate against the engine's own roster and shape, then write. An empty hook
/// set REMOVES the file rather than leaving `{}` behind, so "no hooks" is one
/// state on disk instead of two.
pub async fn store(path: &Path, hooks: &Map<String, Value>) -> Result<(), HooksStoreError> {
    validate(hooks)?;
    if hooks.is_empty() {
        return match tokio::fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(HooksStoreError::Io {
                path: path.display().to_string(),
                source,
            }),
        };
    }

    let io_error = |source: io::Error| HooksStoreError::Io {
        path: path.display().to_string(),
        source,
    };
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(io_error)?;
    }
    let mut body = serde_json::to_string_pretty(&Value::Object(hooks.clone()))
        .map_err(|error| HooksStoreError::Rejected(error.to_string()))?;
    body.push('\n');
    tokio::fs::write(path, body).await.map_err(io_error)
}

/// The shape a hooks file must have. Rejecting rather than skipping, because a
/// settings page that saves a hook the engine will silently ignore is worse than
/// one that says why it cannot.
pub fn validate(hooks: &Map<String, Value>) -> Result<(), HooksStoreError> {
    let known = hook_event_names();
    for (event, groups) in hooks {
        if !known.contains(&event.as_str()) {
            return Err(HooksStoreError::Rejected(format!(
                "unknown hook event: {event}"
            )));
        }
        let Some(groups) = groups.as_array() else {
            return Err(HooksStoreError::Rejected(format!(
                "hooks for {event} must be an array"
            )));
        };
        for group in groups {
            validate_group(group).map_err(|error| {
                HooksStoreError::Rejected(format!("{event}: {}", plain(&error)))
            })?;
        }
    }
    Ok(())
}

fn plain(error: &HooksStoreError) -> String {
    match error {
        HooksStoreError::Rejected(message) => message.clone(),
        other => other.to_string(),
    }
}

fn validate_group(group: &Value) -> Result<(), HooksStoreError> {
    let reject = |message: &str| HooksStoreError::Rejected(message.to_string());
    let Some(group) = group.as_object() else {
        return Err(reject("matcher group must be an object"));
    };
    if let Some(matcher) = group.get("matcher") {
        if !matcher.is_string() {
            return Err(reject("matcher must be a string"));
        }
    }
    let Some(entries) = group.get("hooks").and_then(Value::as_array) else {
        return Err(reject("group requires a hooks array"));
    };
    for entry in entries {
        let Some(entry) = entry.as_object() else {
            return Err(reject("hook entry must be an object"));
        };
        let Some(kind) = entry.get("type").and_then(Value::as_str) else {
            return Err(reject("hook entry requires a string type"));
        };
        if kind == "command" && !entry.get("command").is_some_and(Value::is_string) {
            return Err(reject("command hook requires a string command"));
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "hooks_store_tests.rs"]
mod tests;
