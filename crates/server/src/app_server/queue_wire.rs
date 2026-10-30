//! Durable next-turn queue controls. Runtime payloads remain opaque until admission.

fn include_input_default() -> bool {
    true
}

use super::runtime_wire::{RawJson, RuntimeValue};
use super::wire::{TurnSubmitMode, TurnSubmitReason, TurnSubmitStatus};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueuedSubmission {
    pub id: String,
    #[serde(default)]
    #[schemars(with = "Option<Vec<RuntimeValue>>")]
    pub input: Vec<RuntimeValue>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub input_omitted: bool,
    #[serde(default)]
    pub input_fingerprint: String,
    #[serde(default)]
    pub row_version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_user_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_metadata: Option<RuntimeValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paused_reason: Option<String>,
    pub created_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatching: Option<QueueDispatch>,
}

impl Serialize for QueuedSubmission {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("id", &self.id)?;
        if self.input_omitted {
            map.serialize_entry("inputOmitted", &true)?;
        } else {
            map.serialize_entry("input", &self.input)?;
        }
        map.serialize_entry("inputFingerprint", &self.input_fingerprint)?;
        map.serialize_entry("rowVersion", &self.row_version)?;
        if let Some(value) = &self.client_user_message_id {
            map.serialize_entry("clientUserMessageId", value)?;
        }
        if let Some(value) = &self.client_metadata {
            map.serialize_entry("clientMetadata", value)?;
        }
        if let Some(value) = &self.paused_reason {
            map.serialize_entry("pausedReason", value)?;
        }
        map.serialize_entry("createdAt", &self.created_at)?;
        if let Some(value) = &self.dispatching {
            map.serialize_entry("dispatching", value)?;
        }
        map.end()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueueDispatch {
    pub submission_id: String,
    pub mode: TurnSubmitMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadQueueListParams {
    #[serde(default = "include_input_default")]
    pub include_input: bool,
    pub thread_id: String,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadQueueListResult {
    pub data: Vec<QueuedSubmission>,
    pub next_cursor: Option<String>,
    pub revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadQueueAddParams {
    #[serde(default = "include_input_default")]
    pub include_input: bool,
    pub thread_id: String,
    #[serde(default)]
    pub input: Vec<RuntimeValue>,
    pub client_user_message_id: String,
    #[serde(default)]
    pub client_metadata: Option<RuntimeValue>,
    #[serde(default)]
    pub paused_reason: Option<String>,
    #[serde(default)]
    pub created_at: Option<u64>,
    #[serde(default)]
    pub expected_revision: Option<u64>,
    #[serde(default)]
    pub mutation_id: Option<String>,
    #[serde(default)]
    pub mutation_fingerprint: Option<String>,
    #[serde(default)]
    pub before_id: Option<String>,
    #[serde(default)]
    pub after_id: Option<String>,
    #[serde(default)]
    pub restore: bool,
    #[serde(default)]
    pub lookup_only: bool,
    #[serde(default)]
    pub require_removed_receipt: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadQueueItemResult {
    pub queued_submission: Option<QueuedSubmission>,
    pub revision: u64,
    #[serde(default)]
    pub replayed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt: Option<RawJson>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub found: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadQueueUpdateParams {
    #[serde(default = "include_input_default")]
    pub include_input: bool,
    pub thread_id: String,
    pub queued_submission_id: String,
    #[serde(default)]
    pub input: Option<Vec<RuntimeValue>>,
    #[serde(
        default,
        deserialize_with = "nullable_reason",
        skip_serializing_if = "Option::is_none"
    )]
    pub paused_reason: Option<Option<String>>,
    #[serde(default)]
    pub client_metadata: Option<RuntimeValue>,
    #[serde(default)]
    pub expected_revision: Option<u64>,
}

fn nullable_reason<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Option<String>>, D::Error> {
    Option::<String>::deserialize(deserializer).map(Some)
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadQueueDeleteParams {
    #[serde(default = "include_input_default")]
    pub include_input: bool,
    pub thread_id: String,
    pub queued_submission_id: String,
    #[serde(default)]
    pub expected_revision: Option<u64>,
    #[serde(default)]
    pub mutation_id: Option<String>,
    #[serde(default)]
    pub mutation_fingerprint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadQueueDeleteResult {
    pub deleted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued_submission: Option<QueuedSubmission>,
    pub revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt: Option<RawJson>,
    #[serde(default)]
    pub replayed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadQueueReorderParams {
    pub thread_id: String,
    pub queued_submission_ids: Vec<String>,
    #[serde(default)]
    pub expected_revision: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadQueueMutationResult {
    pub revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadQueueClearParams {
    pub thread_id: String,
    #[serde(default)]
    pub expected_revision: Option<u64>,
    #[serde(default = "include_input_default")]
    pub include_input: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadQueueClearResult {
    pub cleared: Vec<QueuedSubmission>,
    pub revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadQueueDiscardParams {
    pub thread_id: String,
    #[serde(default)]
    pub expected_revision: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadQueuePauseParams {
    pub thread_id: String,
    pub reason: String,
    #[serde(default)]
    pub queued_submission_ids: Option<Vec<String>>,
    #[serde(default)]
    pub expected_revision: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadQueueResumeParams {
    pub thread_id: String,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub queued_submission_ids: Option<Vec<String>>,
    #[serde(default)]
    pub expected_revision: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadQueueStartParams {
    #[serde(default)]
    pub expected_input_fingerprint: Option<String>,
    #[serde(default)]
    pub expected_row_version: Option<u64>,
    #[serde(default = "include_input_default")]
    pub include_input: bool,
    pub thread_id: String,
    #[serde(default)]
    pub queued_submission_id: Option<String>,
    #[serde(default)]
    pub mode: Option<TurnSubmitMode>,
    #[serde(default)]
    pub expected_turn_id: Option<String>,
    #[serde(default)]
    pub submission_id: Option<String>,
    #[serde(default)]
    pub retry_unconfirmed: bool,
    #[serde(default)]
    pub queue_message_id: Option<String>,
    #[serde(default)]
    pub options: Option<RuntimeValue>,
    #[serde(default)]
    pub display_metadata: Option<RuntimeValue>,
    #[serde(default)]
    pub delivery: Option<String>,
    #[serde(default)]
    pub expected_revision: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueueTurn {
    pub id: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadQueueStartResult {
    pub status: TurnSubmitStatus,
    pub submission_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn: Option<QueueTurn>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<TurnSubmitReason>,
    pub queued_submission: QueuedSubmission,
    pub revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadQueueChangedNotification {
    pub thread_id: String,
    pub revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueueImportedSubmission {
    pub input: Vec<RuntimeValue>,
    pub client_user_message_id: String,
    #[serde(default)]
    pub client_metadata: Option<RuntimeValue>,
    #[serde(default)]
    pub paused_reason: Option<String>,
    #[serde(default)]
    pub created_at: Option<u64>,
    #[serde(default)]
    pub dispatching: Option<RuntimeValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadQueueImportParams {
    pub thread_id: String,
    pub migration_id: String,
    pub minimum_revision: u64,
    pub queued_submissions: Vec<QueueImportedSubmission>,
    #[serde(default)]
    pub receipts: Vec<QueueImportedReceipt>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueueImportedReceipt {
    pub kind: String,
    pub mutation_id: String,
    pub fingerprint: String,
    #[serde(default)]
    pub result: Option<RawJson>,
    pub created_at: u64,
}
