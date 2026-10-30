//! Explicit native audio/video input admission shared by sessions and adapters.

use crate::runtime_values::RuntimeString;
use crate::schemas::content::{ContentBlock, MediaBlock, TextBlock};
use crate::schemas::message::Message;
use crate::types::errors::{
    InputValidationError, InputValidationErrorCode, ProviderError, ProviderErrorKind,
};
use crate::types::provider::{ModelRequest, ModelStream, ModelStreamEvent};

pub const MAX_MEDIA_INPUT_BYTES: usize = 18 * 1024 * 1024;
pub const MAX_MEDIA_INPUT_TOTAL_BYTES: usize = 45 * 1024 * 1024;
pub const MAX_MEDIA_INPUT_PARTS: usize = 20;
pub const MAX_MEDIA_INPUT_BASE64_BYTES: usize = MAX_MEDIA_INPUT_BYTES.div_ceil(3) * 4;

/// Keep parameters because a source may advertise a codec-specific MIME type.
pub fn normalize_media_mime(mime: &RuntimeString) -> Option<RuntimeString> {
    let mime = mime.try_to_utf8().ok()?.trim().to_ascii_lowercase();
    if mime.len() > 256 || !mime.bytes().all(|byte| (0x21..=0x7e).contains(&byte)) {
        return None;
    }
    let base = mime.split(';').next()?;
    let (kind, subtype) = base.split_once('/')?;
    if !matches!(kind, "audio" | "video") || subtype.is_empty() {
        return None;
    }
    Some(mime.into())
}

#[allow(clippy::result_large_err)]
pub fn validate_mime(
    model: &RuntimeString,
    mime: &RuntimeString,
    supported: &[RuntimeString],
) -> Result<RuntimeString, InputValidationError> {
    let normalized = normalize_media_mime(mime).ok_or_else(|| {
        InputValidationError::new(
            InputValidationErrorCode::UnsupportedInput,
            "native media MIME type must identify audio or video",
        )
    })?;
    if supported
        .iter()
        .any(|value| normalize_media_mime(value).as_ref() == Some(&normalized))
    {
        return Ok(normalized);
    }
    let mut error = InputValidationError::new(
        InputValidationErrorCode::UnsupportedInput,
        format!("The selected model does not support native media input with MIME type {}. Choose a model that supports this input.", normalized.try_to_utf8().expect("normalized MIME is ASCII")),
    );
    error.detail.feature = Some(normalized);
    error.detail.model = Some(model.clone());
    Err(error)
}

pub fn size_error() -> InputValidationError {
    let mut error = InputValidationError::new(
        InputValidationErrorCode::RequestTooLarge,
        format!("Native media input exceeds the {MAX_MEDIA_INPUT_BYTES}-byte per-file or {MAX_MEDIA_INPUT_TOTAL_BYTES}-byte per-request limit"),
    );
    error.detail.limit_bytes = Some(MAX_MEDIA_INPUT_TOTAL_BYTES as u64);
    error
}

fn blocks(message: &Message) -> &[ContentBlock] {
    match message {
        Message::User(user) => &user.content,
        Message::Assistant(assistant) => &assistant.content,
        Message::ToolResult(result) => result.extra_user_blocks.as_deref().unwrap_or_default(),
        Message::System(_) | Message::CompactBoundary(_) => &[],
    }
}

/// Project only already admitted media into a selected model's request view.
/// Unrecognized payloads stay native so strict admission can still reject them.
pub(crate) fn project_history_media(
    messages: &mut [Message],
    admitted_history: &[Message],
    supported: &[RuntimeString],
) {
    fn uuid(message: &Message) -> &RuntimeString {
        match message {
            Message::User(message) => &message.uuid,
            Message::Assistant(message) => &message.uuid,
            Message::ToolResult(message) => &message.uuid,
            Message::System(message) => &message.uuid,
            Message::CompactBoundary(message) => &message.uuid,
        }
    }

    let supported: Vec<_> = supported.iter().filter_map(normalize_media_mime).collect();
    for message in messages {
        let (id, content) = match message {
            Message::User(user) => (&user.uuid, &mut user.content),
            Message::Assistant(assistant) => (&assistant.uuid, &mut assistant.content),
            Message::ToolResult(result) => {
                let Some(content) = result.extra_user_blocks.as_mut() else {
                    continue;
                };
                (&result.uuid, content)
            }
            Message::System(_) | Message::CompactBoundary(_) => continue,
        };
        for block in content {
            let ContentBlock::Media(media) = block else {
                continue;
            };
            if normalize_media_mime(&media.mime_type).is_some_and(|mime| supported.contains(&mime))
            {
                continue;
            }
            let admitted = admitted_history.iter().any(|message| {
                uuid(message) == id
                    && blocks(message).iter().any(|block| {
                        matches!(block, ContentBlock::Media(original) if original == &*media)
                    })
            });
            if admitted {
                *block = ContentBlock::Text(history_media_omission(media));
            }
        }
    }
}

fn history_media_omission(media: &MediaBlock) -> TextBlock {
    let mime = normalize_media_mime(&media.mime_type)
        .and_then(|mime| mime.try_to_utf8().ok())
        .unwrap_or_else(|| "unknown audio/video MIME".to_string());
    let filename = media
        .filename
        .as_ref()
        .and_then(|name| name.try_to_utf8().ok())
        .filter(|name| {
            !name.is_empty()
                && name.len() <= 255
                && name != "."
                && name != ".."
                && !name.contains('/')
                && !name.contains('\\')
                && !name.chars().any(char::is_control)
        })
        .map(|name| format!("; filename {name:?}"))
        .unwrap_or_default();
    TextBlock {
        text: format!(
            "[Native audio/video omitted from this request: MIME {mime:?}{filename}. This model cannot read this media. The original attachment remains in conversation history. Tell the user it was not read; use a model supporting this MIME to inspect it.]"
        )
        .into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    }
}

/// Recheck the complete request after model switches, fallbacks and history replay.
#[allow(clippy::result_large_err)]
pub fn validate_request(
    request: &ModelRequest,
    supported: &[RuntimeString],
) -> Result<(), InputValidationError> {
    let mut count = 0usize;
    let mut encoded_bytes = 0usize;
    for block in request.messages.iter().flat_map(blocks) {
        let ContentBlock::Media(media) = block else {
            continue;
        };
        validate_mime(&request.model, &media.mime_type, supported)?;
        count += 1;
        encoded_bytes = encoded_bytes
            .checked_add(media.base64.len_utf16())
            .ok_or_else(size_error)?;
        if count > MAX_MEDIA_INPUT_PARTS
            || media.base64.len_utf16() > MAX_MEDIA_INPUT_BASE64_BYTES
            || encoded_bytes > MAX_MEDIA_INPUT_TOTAL_BYTES.div_ceil(3) * 4
        {
            return Err(size_error());
        }
    }
    Ok(())
}

/// Adapters with no native audio/video transport pass an empty MIME list.
pub fn rejected_stream(error: InputValidationError) -> ModelStream {
    let oversized = error.detail.code == InputValidationErrorCode::RequestTooLarge;
    Box::pin(futures::stream::once(async move {
        ModelStreamEvent::Error {
            error: ProviderError {
                kind: if oversized {
                    ProviderErrorKind::RequestTooLarge
                } else {
                    ProviderErrorKind::InvalidRequest
                },
                message: error.message.into(),
                status: Some(if oversized { 413 } else { 400 }),
                code: Some(
                    if oversized {
                        "request_too_large"
                    } else {
                        "unsupported_media_input"
                    }
                    .into(),
                ),
                error_type: None,
                media_origins: None,
                media_repair_complete: false,
                retry_after_ms: None,
                request_id: None,
            },
        }
    }))
}
