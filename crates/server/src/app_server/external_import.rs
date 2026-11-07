//! Validation and conversion for the single named external-history import.

use super::wire::{EngineImportExternalSessionParams, ExternalSessionRole};
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{AssistantMessage, Message, UserMessage};
use klaus_agent_core::schemas::rollout::{SessionMeta, SessionRole};
use klaus_agent_core::store::external_import::{ExternalImportMessage, ExternalSessionImport};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::Path;

const MAX_MESSAGES: usize = 10_000;
const MAX_TEXT: usize = 1024 * 1024;
const MAX_TOTAL_TEXT: usize = 8 * 1024 * 1024;
const MAX_ENCODED_REQUEST: usize = 16 * 1024 * 1024;
const MAX_TIMESTAMP: u64 = 253_402_300_799_999;

fn canonical(value: Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut entries: Vec<_> = object.into_iter().collect();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonical(value)))
                    .collect(),
            )
        }
        Value::Array(values) => Value::Array(values.into_iter().map(canonical).collect()),
        value => value,
    }
}

fn runtime(value: Value) -> Result<RuntimeJson, String> {
    runtime_json::parse(&RuntimeString::from(value.to_string())).map_err(|error| error.to_string())
}

fn absolute_path(value: &str, name: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > 32_768
        || value.contains('\0')
        || !Path::new(value).is_absolute()
    {
        return Err(format!(
            "{name} must be an absolute path within 32768 UTF-8 bytes, without NUL"
        ));
    }
    Ok(())
}

pub(super) fn prepare(
    params: EngineImportExternalSessionParams,
) -> Result<ExternalSessionImport, String> {
    let id = uuid::Uuid::parse_str(&params.thread_id)
        .map_err(|_| "threadId must be a canonical UUID")?;
    if id.hyphenated().to_string() != params.thread_id || id.is_nil() {
        return Err("threadId must be a non-nil canonical lowercase UUID".into());
    }
    absolute_path(&params.cwd, "cwd")?;
    absolute_path(&params.source.path, "source.path")?;
    let provider = &params.source.provider_id;
    if provider.is_empty()
        || provider.len() > 64
        || !provider.as_bytes()[0].is_ascii_lowercase()
        || !provider.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
        })
    {
        return Err("source.providerId must match [a-z][a-z0-9._-]{0,63}".into());
    }
    if params.source.content_sha256.len() != 64
        || !params
            .source
            .content_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("source.contentSha256 must be lowercase SHA-256 hex".into());
    }
    if params
        .title
        .as_ref()
        .is_some_and(|title| title.trim().is_empty() || title.len() > 4096)
    {
        return Err("title must be nonempty and at most 4096 UTF-8 bytes".into());
    }
    if params.messages.is_empty() || params.messages.len() > MAX_MESSAGES {
        return Err("messages must contain between 1 and 10000 records".into());
    }
    let mut total = 0usize;
    for message in &params.messages {
        if message.text.trim().is_empty() || message.text.len() > MAX_TEXT {
            return Err("each message text must be nonempty and at most 1 MiB UTF-8".into());
        }
        total = total
            .checked_add(message.text.len())
            .ok_or("message text length overflow")?;
        if total > MAX_TOTAL_TEXT {
            return Err("total message text must be at most 8 MiB UTF-8".into());
        }
        if message
            .timestamp_ms
            .is_some_and(|timestamp| timestamp > MAX_TIMESTAMP)
        {
            return Err("timestampMs must be an integer between 0 and 253402300799999".into());
        }
        if message
            .source_message_id
            .as_ref()
            .is_some_and(|id| id.is_empty() || id.len() > 1024 || id.contains('\0'))
        {
            return Err(
                "sourceMessageId must be nonempty and at most 1024 UTF-8 bytes, without NUL".into(),
            );
        }
    }
    if let Some(metadata) = &params.host_metadata {
        if serde_json::to_vec(metadata)
            .map_err(|error| error.to_string())?
            .len()
            > 64 * 1024
        {
            return Err("hostMetadata must be at most 64 KiB of JSON".into());
        }
    }
    let normalized = canonical(serde_json::to_value(&params).map_err(|error| error.to_string())?);
    let encoded = serde_json::to_vec(&normalized).map_err(|error| error.to_string())?;
    if encoded.len() > MAX_ENCODED_REQUEST {
        return Err("normalized import must be at most 16 MiB of JSON".into());
    }
    let request_sha256 = format!("{:x}", Sha256::digest(&encoded));
    let provenance =
        runtime(json!({"version":1,"source":params.source,"requestSha256":request_sha256}))?;
    let receipt =
        runtime(json!({"requestSha256":request_sha256,"messageCount":params.messages.len()}))?;
    let created_at = params
        .messages
        .iter()
        .filter_map(|message| message.timestamp_ms)
        .next()
        .map(|timestamp| timestamp as i64)
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
    let mut messages = Vec::with_capacity(params.messages.len());
    for (position, source) in params.messages.iter().enumerate() {
        let mut digest = Sha256::new();
        digest.update(b"klaus-external-message-v1\0");
        digest.update(params.thread_id.as_bytes());
        digest.update(b"\0");
        digest.update(request_sha256.as_bytes());
        digest.update((position as u64).to_be_bytes());
        let mut bytes: [u8; 16] = digest.finalize()[..16]
            .try_into()
            .expect("SHA-256 has 16 bytes");
        bytes[6] = (bytes[6] & 0x0f) | 0x50;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        let message_id = uuid::Uuid::from_bytes(bytes).hyphenated().to_string();
        let content = vec![ContentBlock::Text(TextBlock {
            text: source.text.clone().into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })];
        let message = match source.role {
            ExternalSessionRole::User => Message::User(UserMessage {
                question_answer: None,
                uuid: message_id.clone().into(),
                content,
                is_meta: None,
            }),
            ExternalSessionRole::Assistant => Message::Assistant(AssistantMessage {
                uuid: message_id.clone().into(),
                content,
                model: "external".into(),
                usage: None,
            }),
        };
        let mut origin = json!({"messageId":message_id,"providerId":params.source.provider_id,"format":params.source.format});
        if let Some(timestamp) = source.timestamp_ms {
            origin["timestampMs"] = json!(timestamp);
        }
        if let Some(id) = &source.source_message_id {
            origin["sourceMessageId"] = json!(id);
        }
        messages.push(ExternalImportMessage {
            message,
            origin: runtime(origin)?,
        });
    }
    let import = ExternalSessionImport {
        allow_create: true,
        meta: SessionMeta {
            id: params.thread_id.into(),
            created_at,
            cwd: params.cwd.into(),
            model: "external".into(),
            title: params.title.map(Into::into),
            originator: Some("external-import".into()),
            cli_version: None,
            source: Some(format!("external:{}", params.source.provider_id).into()),
            role: Some(SessionRole::Main),
            parent_thread_id: None,
            forked_from_id: None,
            git: None,
            host_metadata: params
                .host_metadata
                .map(|object| runtime(canonical(Value::Object(object))))
                .transpose()?,
        },
        provenance,
        messages,
        receipt,
    };
    let header = klaus_agent_core::schemas::rollout::RolloutItem::SessionMeta {
        schema_version: klaus_agent_core::schemas::rollout::ROLLOUT_SCHEMA_VERSION,
        meta: import.meta.clone(),
    };
    if klaus_agent_core::store::rollout::serialize_rollout_item(&header).len() >= 64 * 1024 {
        return Err(
            "combined cwd, title and hostMetadata exceed the 64 KiB durable header limit".into(),
        );
    }
    Ok(import)
}
